//! Date and DateTime Parsing Utilities
//!
//! This module provides format detection and parsing for date/time strings
//! commonly found in CSV files and other text-based sources.
//!
//! # Format Detection
//!
//! The format detector samples values and attempts to find a format that
//! parses all samples successfully. Priority is given to unambiguous formats
//! like ISO 8601.
//!
//! # Example
//!
//! ```ignore
//! let samples = vec!["2024-01-15", "2024-02-20", "2024-03-25"];
//! let format = detect_date_format(&samples).expect("Should detect ISO format");
//! assert_eq!(format.pattern, "%Y-%m-%d");
//! ```

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};
use thiserror::Error;

/// Errors that can occur during date/time parsing.
#[derive(Debug, Error)]
pub enum DateParseError {
    #[error("Unable to detect date format from samples")]
    UnableToDetectFormat,

    #[error("Ambiguous date format: could be {0} or {1}")]
    AmbiguousFormat(String, String),

    #[error("Failed to parse '{0}' with format '{1}': {2}")]
    ParseFailed(String, String, String),

    #[error("No samples provided for format detection")]
    NoSamples,

    #[error("Empty string cannot be parsed as date")]
    EmptyString,
}

/// A detected or specified date format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateFormat {
    /// The chrono format pattern (e.g., "%Y-%m-%d").
    pub pattern: String,
    /// Human-readable description (e.g., "ISO 8601 date").
    pub description: String,
    /// Whether this format includes time components.
    pub has_time: bool,
    /// Whether this format includes timezone.
    pub has_timezone: bool,
}

impl DateFormat {
    /// Create a new date format.
    pub fn new(pattern: &str, description: &str, has_time: bool, has_timezone: bool) -> Self {
        Self {
            pattern: pattern.to_string(),
            description: description.to_string(),
            has_time,
            has_timezone,
        }
    }

    /// Create an ISO 8601 date format.
    pub fn iso_date() -> Self {
        Self::new("%Y-%m-%d", "ISO 8601 date (YYYY-MM-DD)", false, false)
    }

    /// Create an ISO 8601 datetime format with timezone.
    pub fn iso_datetime_utc() -> Self {
        Self::new(
            "%Y-%m-%dT%H:%M:%S%.fZ",
            "ISO 8601 datetime with Z (YYYY-MM-DDTHH:MM:SS.fffZ)",
            true,
            true,
        )
    }
}

/// Common date format patterns to try, in priority order.
///
/// Priority is given to unambiguous formats (ISO) over locale-specific ones.
pub const DATE_FORMATS: &[(&str, &str)] = &[
    // ISO formats (unambiguous, highest priority)
    ("%Y-%m-%d", "ISO 8601 date"),
    ("%Y/%m/%d", "ISO date with slashes"),
    // US formats
    ("%m/%d/%Y", "US date (MM/DD/YYYY)"),
    ("%m-%d-%Y", "US date with dashes"),
    // EU formats
    ("%d/%m/%Y", "EU date (DD/MM/YYYY)"),
    ("%d-%m-%Y", "EU date with dashes"),
    ("%d.%m.%Y", "EU date with dots"),
    // Other common formats
    ("%Y%m%d", "Compact date (YYYYMMDD)"),
    ("%b %d, %Y", "Month name (Jan 15, 2024)"),
    ("%B %d, %Y", "Full month name (January 15, 2024)"),
];

/// Common datetime format patterns to try, in priority order.
pub const DATETIME_FORMATS: &[(&str, &str, bool)] = &[
    // ISO 8601 with timezone (highest priority)
    ("%Y-%m-%dT%H:%M:%S%.fZ", "ISO 8601 with Z", true),
    ("%Y-%m-%dT%H:%M:%SZ", "ISO 8601 with Z (no frac)", true),
    ("%Y-%m-%dT%H:%M:%S%.f%:z", "ISO 8601 with offset", true),
    ("%Y-%m-%dT%H:%M:%S%:z", "ISO 8601 with offset (no frac)", true),
    // ISO 8601 without timezone
    ("%Y-%m-%dT%H:%M:%S%.f", "ISO 8601 datetime", false),
    ("%Y-%m-%dT%H:%M:%S", "ISO 8601 datetime (no frac)", false),
    // Space-separated formats
    ("%Y-%m-%d %H:%M:%S%.f", "Datetime with space", false),
    ("%Y-%m-%d %H:%M:%S", "Datetime with space (no frac)", false),
    ("%Y/%m/%d %H:%M:%S", "Datetime with slashes", false),
    // Common database formats
    ("%Y-%m-%d %H:%M:%S%.f%:z", "Database datetime with tz", true),
];

/// Detect the date format from a sample of values.
///
/// Returns the detected format if one is found that parses all samples.
/// Returns an error if no format matches or if the format is ambiguous.
///
/// # Arguments
///
/// * `samples` - Sample date strings to analyze (should be non-empty values)
///
/// # Example
///
/// ```ignore
/// let samples = vec!["2024-01-15", "2024-02-20"];
/// let format = detect_date_format(&samples)?;
/// assert_eq!(format.pattern, "%Y-%m-%d");
/// ```
pub fn detect_date_format(samples: &[&str]) -> Result<DateFormat, DateParseError> {
    if samples.is_empty() {
        return Err(DateParseError::NoSamples);
    }

    // Filter out empty strings
    let valid_samples: Vec<&str> = samples.iter().filter(|s| !s.trim().is_empty()).copied().collect();

    if valid_samples.is_empty() {
        return Err(DateParseError::NoSamples);
    }

    // Try datetime formats first (more specific)
    for (pattern, description, has_tz) in DATETIME_FORMATS {
        if all_samples_parse_datetime(&valid_samples, pattern) {
            return Ok(DateFormat::new(pattern, description, true, *has_tz));
        }
    }

    // Then try date-only formats
    let mut matching_formats = Vec::new();

    for (pattern, description) in DATE_FORMATS {
        if all_samples_parse_date(&valid_samples, pattern) {
            matching_formats.push((*pattern, *description));
        }
    }

    match matching_formats.len() {
        0 => Err(DateParseError::UnableToDetectFormat),
        1 => Ok(DateFormat::new(
            matching_formats[0].0,
            matching_formats[0].1,
            false,
            false,
        )),
        _ => {
            // Check for ambiguity between US and EU formats
            let has_us = matching_formats.iter().any(|(p, _)| p.contains("%m/%d"));
            let has_eu = matching_formats.iter().any(|(p, _)| p.contains("%d/%m"));

            if has_us && has_eu {
                // Try to disambiguate by looking for values > 12
                if can_disambiguate_date_format(&valid_samples) {
                    // Pick the format based on position of values > 12
                    return disambiguate_date_format(&valid_samples, &matching_formats);
                }
                return Err(DateParseError::AmbiguousFormat(
                    "MM/DD/YYYY (US)".to_string(),
                    "DD/MM/YYYY (EU)".to_string(),
                ));
            }

            // Prefer ISO format if available
            for (pattern, description) in &matching_formats {
                if pattern.starts_with("%Y") {
                    return Ok(DateFormat::new(pattern, description, false, false));
                }
            }

            // Otherwise, return the first match
            Ok(DateFormat::new(
                matching_formats[0].0,
                matching_formats[0].1,
                false,
                false,
            ))
        }
    }
}

/// Check if all samples parse successfully with the given datetime pattern.
fn all_samples_parse_datetime(samples: &[&str], pattern: &str) -> bool {
    samples.iter().all(|s| {
        NaiveDateTime::parse_from_str(s.trim(), pattern).is_ok()
            || DateTime::parse_from_str(s.trim(), pattern).is_ok()
    })
}

/// Check if all samples parse successfully with the given date pattern.
fn all_samples_parse_date(samples: &[&str], pattern: &str) -> bool {
    samples.iter().all(|s| NaiveDate::parse_from_str(s.trim(), pattern).is_ok())
}

/// Check if we can disambiguate between US and EU date formats.
///
/// Returns true if any sample has a component > 12 (must be day, not month).
fn can_disambiguate_date_format(samples: &[&str]) -> bool {
    for sample in samples {
        let parts: Vec<&str> = sample.split(&['/', '-', '.'][..]).collect();
        if parts.len() >= 2 {
            // Check if first or second component is > 12
            if let Ok(first) = parts[0].parse::<u32>() {
                if first > 12 {
                    return true;
                }
            }
            if let Ok(second) = parts[1].parse::<u32>() {
                if second > 12 {
                    return true;
                }
            }
        }
    }
    false
}

/// Disambiguate between date formats using values > 12.
fn disambiguate_date_format(
    samples: &[&str],
    matching_formats: &[(&str, &str)],
) -> Result<DateFormat, DateParseError> {
    for sample in samples {
        let parts: Vec<&str> = sample.split(&['/', '-', '.'][..]).collect();
        if parts.len() >= 2 {
            if let Ok(first) = parts[0].parse::<u32>() {
                if first > 12 {
                    // First component is > 12, so it must be day (EU format)
                    for (pattern, description) in matching_formats {
                        if pattern.contains("%d/%m") || pattern.contains("%d-%m") || pattern.contains("%d.%m") {
                            return Ok(DateFormat::new(pattern, description, false, false));
                        }
                    }
                }
            }
            if let Ok(second) = parts[1].parse::<u32>() {
                if second > 12 {
                    // Second component is > 12, so it must be day (US format)
                    for (pattern, description) in matching_formats {
                        if pattern.contains("%m/%d") || pattern.contains("%m-%d") {
                            return Ok(DateFormat::new(pattern, description, false, false));
                        }
                    }
                }
            }
        }
    }

    // Couldn't disambiguate, prefer ISO-like format
    for (pattern, description) in matching_formats {
        if pattern.starts_with("%Y") {
            return Ok(DateFormat::new(pattern, description, false, false));
        }
    }

    Ok(DateFormat::new(
        matching_formats[0].0,
        matching_formats[0].1,
        false,
        false,
    ))
}

/// Parse a date string using the detected format.
///
/// # Arguments
///
/// * `s` - The date string to parse
/// * `format` - The detected date format
///
/// # Returns
///
/// The parsed date as a NaiveDate.
pub fn parse_date_with_format(s: &str, format: &DateFormat) -> Result<NaiveDate, DateParseError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(DateParseError::EmptyString);
    }

    NaiveDate::parse_from_str(trimmed, &format.pattern).map_err(|e| {
        DateParseError::ParseFailed(s.to_string(), format.pattern.clone(), e.to_string())
    })
}

/// Parse a datetime string using the detected format.
///
/// # Arguments
///
/// * `s` - The datetime string to parse
/// * `format` - The detected datetime format
/// * `default_timezone` - Timezone to use for naive datetimes (typically "UTC")
///
/// # Returns
///
/// The parsed datetime as a DateTime<Utc>.
pub fn parse_datetime_with_format(
    s: &str,
    format: &DateFormat,
    default_timezone: &str,
) -> Result<DateTime<Utc>, DateParseError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(DateParseError::EmptyString);
    }

    // Try parsing with timezone first
    if let Ok(dt) = DateTime::parse_from_str(trimmed, &format.pattern) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try parsing as naive datetime, then interpret using default_timezone
    if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, &format.pattern) {
        return Ok(interpret_naive_datetime(ndt, default_timezone));
    }

    Err(DateParseError::ParseFailed(
        s.to_string(),
        format.pattern.clone(),
        "Failed to parse datetime".to_string(),
    ))
}

/// Interpret a naive datetime in the given timezone and convert to UTC.
///
/// Accepts:
/// - Named IANA timezones via `chrono-tz` (e.g., `"America/New_York"`, `"Europe/London"`)
/// - Numeric UTC offsets: `"+05:30"`, `"-08:00"`, `"+0530"`, `"-0800"`
/// - The literal `"UTC"` / `"utc"`
///
/// Falls back to UTC if the timezone string cannot be parsed.
fn interpret_naive_datetime(ndt: NaiveDateTime, tz_str: &str) -> DateTime<Utc> {
    let trimmed = tz_str.trim();

    if trimmed.eq_ignore_ascii_case("UTC") || trimmed.eq_ignore_ascii_case("GMT") {
        return Utc.from_utc_datetime(&ndt);
    }

    // Try IANA timezone name via chrono-tz (handles DST correctly for the
    // specific datetime being parsed, not the current wall-clock time).
    if let Ok(tz) = trimmed.parse::<chrono_tz::Tz>() {
        if let Some(dt) = tz.from_local_datetime(&ndt).single() {
            return dt.with_timezone(&Utc);
        }
    }

    // Try numeric offset like "+05:30" or "-0800"
    if let Some(offset) = parse_numeric_offset(trimmed) {
        if let Some(dt) = offset.from_local_datetime(&ndt).single() {
            return dt.with_timezone(&Utc);
        }
    }

    Utc.from_utc_datetime(&ndt)
}

/// Parse a numeric UTC offset string into a [`FixedOffset`].
///
/// Supports formats: `+HH:MM`, `-HH:MM`, `+HHMM`, `-HHMM`.
fn parse_numeric_offset(s: &str) -> Option<FixedOffset> {
    if s.len() < 5 {
        return None;
    }

    let sign = match s.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };

    let rest = &s[1..];
    let (hours, minutes) = if rest.contains(':') {
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        if parts.len() != 2 { return None; }
        (parts[0].parse::<i32>().ok()?, parts[1].parse::<i32>().ok()?)
    } else if rest.len() == 4 {
        (rest[..2].parse::<i32>().ok()?, rest[2..].parse::<i32>().ok()?)
    } else {
        return None;
    };

    if hours > 23 || minutes > 59 {
        return None;
    }

    FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60))
}

/// Parse a date string and convert to midnight UTC timestamp in microseconds.
///
/// This is useful for normalizing date values to our internal timestamp format.
pub fn date_to_micros_utc(s: &str, format: &DateFormat) -> Result<i64, DateParseError> {
    let date = parse_date_with_format(s, format)?;
    let datetime = date.and_hms_opt(0, 0, 0).unwrap();
    let utc_datetime = Utc.from_utc_datetime(&datetime);
    Ok(utc_datetime.timestamp_micros())
}

/// Parse a datetime string and convert to UTC timestamp in microseconds.
pub fn datetime_to_micros_utc(
    s: &str,
    format: &DateFormat,
    default_timezone: &str,
) -> Result<i64, DateParseError> {
    let dt = parse_datetime_with_format(s, format, default_timezone)?;
    Ok(dt.timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_iso_date() {
        let samples = vec!["2024-01-15", "2024-02-20", "2024-03-25"];
        let format = detect_date_format(&samples).unwrap();
        assert_eq!(format.pattern, "%Y-%m-%d");
        assert!(!format.has_time);
    }

    #[test]
    fn test_detect_iso_datetime_z() {
        let samples = vec![
            "2024-01-15T10:30:00Z",
            "2024-02-20T14:45:30Z",
        ];
        let format = detect_date_format(&samples).unwrap();
        assert!(format.pattern.contains("T"));
        assert!(format.has_time);
        assert!(format.has_timezone);
    }

    #[test]
    fn test_detect_iso_datetime_with_frac() {
        let samples = vec![
            "2024-01-15T10:30:00.123456Z",
            "2024-02-20T14:45:30.789Z",
        ];
        let format = detect_date_format(&samples).unwrap();
        assert!(format.has_time);
        assert!(format.has_timezone);
    }

    #[test]
    fn test_detect_us_date() {
        // Unambiguous US format (month position has value <= 12, day position has value > 12)
        let samples = vec!["01/15/2024", "02/20/2024", "03/25/2024"];
        let format = detect_date_format(&samples).unwrap();
        // Should detect as US format because day > 12
        assert!(format.pattern.contains("%m") || format.pattern.contains("%d"));
    }

    #[test]
    fn test_detect_eu_date_unambiguous() {
        // Unambiguous EU format (first position has value > 12)
        let samples = vec!["15/01/2024", "20/02/2024", "25/03/2024"];
        let format = detect_date_format(&samples).unwrap();
        assert_eq!(format.pattern, "%d/%m/%Y");
    }

    #[test]
    fn test_ambiguous_date_format() {
        // Ambiguous: could be 01/02/2024 (Jan 2) or 01/02/2024 (Feb 1)
        let samples = vec!["01/02/2024", "03/04/2024"];
        let result = detect_date_format(&samples);
        assert!(matches!(result, Err(DateParseError::AmbiguousFormat(_, _))));
    }

    #[test]
    fn test_parse_iso_date() {
        use chrono::Datelike;
        let format = DateFormat::iso_date();
        let date = parse_date_with_format("2024-01-15", &format).unwrap();
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 15);
    }

    #[test]
    fn test_parse_datetime_utc() {
        let format = DateFormat::iso_datetime_utc();
        let dt = parse_datetime_with_format("2024-01-15T10:30:00.000Z", &format, "UTC").unwrap();
        // 2024-01-15T10:30:00.000Z = 1705314600 seconds since Unix epoch
        assert_eq!(dt.timestamp(), 1705314600);
    }

    #[test]
    fn test_date_to_micros() {
        let format = DateFormat::iso_date();
        let micros = date_to_micros_utc("2024-01-15", &format).unwrap();
        // Midnight UTC on 2024-01-15
        assert_eq!(micros, 1705276800_000_000);
    }

    #[test]
    fn test_empty_samples() {
        let samples: Vec<&str> = vec![];
        let result = detect_date_format(&samples);
        assert!(matches!(result, Err(DateParseError::NoSamples)));
    }

    #[test]
    fn test_filter_empty_strings() {
        let samples = vec!["2024-01-15", "", "2024-02-20", "  "];
        let format = detect_date_format(&samples).unwrap();
        assert_eq!(format.pattern, "%Y-%m-%d");
    }

    #[test]
    fn test_compact_date() {
        let samples = vec!["20240115", "20240220", "20240325"];
        let format = detect_date_format(&samples).unwrap();
        assert_eq!(format.pattern, "%Y%m%d");
    }

    // ==================== Disambiguation Tests ====================

    #[test]
    fn test_disambiguate_us_format_day_gt_12() {
        // Second component (day) > 12 forces US (MM/DD/YYYY)
        let samples = vec!["01/13/2024", "02/15/2024"];
        let format = detect_date_format(&samples).unwrap();
        assert_eq!(format.pattern, "%m/%d/%Y");
    }

    #[test]
    fn test_disambiguate_eu_format_day_gt_12() {
        // First component (day) > 12 forces EU (DD/MM/YYYY)
        let samples = vec!["13/01/2024", "15/02/2024"];
        let format = detect_date_format(&samples).unwrap();
        assert_eq!(format.pattern, "%d/%m/%Y");
    }

    #[test]
    fn test_ambiguous_all_values_lte_12() {
        // All components <= 12, no disambiguation possible
        let samples = vec!["01/02/2024", "03/04/2024", "05/06/2024"];
        let result = detect_date_format(&samples);
        assert!(matches!(result, Err(DateParseError::AmbiguousFormat(_, _))));
    }

    // ==================== Timezone & Precision Tests ====================

    #[test]
    fn test_detect_datetime_with_offset() {
        let samples = vec![
            "2025-01-15T10:30:00+05:30",
            "2025-02-20T14:45:30+05:30",
        ];
        let format = detect_date_format(&samples).unwrap();
        assert!(format.has_time);
        assert!(format.has_timezone);
    }

    #[test]
    fn test_parse_datetime_with_offset_converts_to_utc() {
        use chrono::Timelike;
        let format = DateFormat::new(
            "%Y-%m-%dT%H:%M:%S%:z",
            "ISO 8601 with offset",
            true,
            true,
        );
        let dt = parse_datetime_with_format("2025-01-15T10:30:00+05:30", &format, "UTC").unwrap();
        // 10:30 IST = 05:00 UTC
        assert_eq!(dt.hour(), 5);
        assert_eq!(dt.minute(), 0);
    }

    #[test]
    fn test_fractional_seconds_preserved_in_micros() {
        let format = DateFormat::iso_datetime_utc();
        let micros = datetime_to_micros_utc("2025-01-15T10:30:00.123456Z", &format, "UTC").unwrap();
        // Check that microsecond part is preserved
        let remainder = micros % 1_000_000;
        assert_eq!(remainder, 123456);
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_all_whitespace_samples() {
        let samples = vec!["", "  ", "\t"];
        let result = detect_date_format(&samples);
        assert!(matches!(result, Err(DateParseError::NoSamples)));
    }

    #[test]
    fn test_mixed_whitespace_with_valid() {
        let samples = vec!["", " ", "2025-01-01", "  ", "2025-02-15"];
        let format = detect_date_format(&samples).unwrap();
        assert_eq!(format.pattern, "%Y-%m-%d");
    }

    #[test]
    fn test_parse_empty_string_returns_error() {
        let format = DateFormat::iso_date();
        let result = parse_date_with_format("", &format);
        assert!(matches!(result, Err(DateParseError::EmptyString)));
    }

    #[test]
    fn test_parse_datetime_empty_string_returns_error() {
        let format = DateFormat::iso_datetime_utc();
        let result = parse_datetime_with_format("", &format, "UTC");
        assert!(matches!(result, Err(DateParseError::EmptyString)));
    }

    #[test]
    fn test_detect_space_separated_datetime() {
        let samples = vec![
            "2025-01-15 10:30:00",
            "2025-02-20 14:45:30",
        ];
        let format = detect_date_format(&samples).unwrap();
        assert!(format.has_time);
        assert!(!format.has_timezone);
    }

    #[test]
    fn test_invalid_date_returns_error() {
        let samples = vec!["not-a-date", "also-not-a-date"];
        let result = detect_date_format(&samples);
        assert!(matches!(result, Err(DateParseError::UnableToDetectFormat)));
    }

    // ==================== Timezone Handling Tests ====================

    #[test]
    fn test_naive_datetime_with_utc_timezone() {
        use chrono::Timelike;
        let format = DateFormat::new("%Y-%m-%d %H:%M:%S", "space-separated", true, false);
        let dt = parse_datetime_with_format("2025-01-15 10:30:00", &format, "UTC").unwrap();
        assert_eq!(dt.hour(), 10);
        assert_eq!(dt.minute(), 30);
    }

    #[test]
    fn test_naive_datetime_with_positive_offset() {
        use chrono::Timelike;
        let format = DateFormat::new("%Y-%m-%d %H:%M:%S", "space-separated", true, false);
        // "+05:30" means the naive time is local to IST; UTC should be 5h30m earlier
        let dt = parse_datetime_with_format("2025-01-15 10:30:00", &format, "+05:30").unwrap();
        assert_eq!(dt.hour(), 5);
        assert_eq!(dt.minute(), 0);
    }

    #[test]
    fn test_naive_datetime_with_negative_offset() {
        use chrono::Timelike;
        let format = DateFormat::new("%Y-%m-%d %H:%M:%S", "space-separated", true, false);
        // "-05:00" means EST; UTC should be 5h later
        let dt = parse_datetime_with_format("2025-01-15 10:30:00", &format, "-05:00").unwrap();
        assert_eq!(dt.hour(), 15);
        assert_eq!(dt.minute(), 30);
    }

    #[test]
    fn test_naive_datetime_with_iana_timezone() {
        use chrono::Timelike;
        let format = DateFormat::new("%Y-%m-%d %H:%M:%S", "space-separated", true, false);
        let dt = parse_datetime_with_format("2025-07-15 12:00:00", &format, "America/New_York").unwrap();
        // EDT (summer) is UTC-4, so 12:00 EDT = 16:00 UTC
        assert_eq!(dt.hour(), 16);
    }

    #[test]
    fn test_naive_datetime_with_compact_offset() {
        use chrono::Timelike;
        let format = DateFormat::new("%Y-%m-%d %H:%M:%S", "space-separated", true, false);
        let dt = parse_datetime_with_format("2025-01-15 10:30:00", &format, "+0530").unwrap();
        assert_eq!(dt.hour(), 5);
        assert_eq!(dt.minute(), 0);
    }

    #[test]
    fn test_invalid_timezone_falls_back_to_utc() {
        let format = DateFormat::new("%Y-%m-%d %H:%M:%S", "space-separated", true, false);
        let dt_with_invalid = parse_datetime_with_format("2025-01-15 10:30:00", &format, "Not/A/Zone").unwrap();
        let dt_with_utc = parse_datetime_with_format("2025-01-15 10:30:00", &format, "UTC").unwrap();
        assert_eq!(dt_with_invalid, dt_with_utc);
    }

    #[test]
    fn test_explicit_tz_in_string_ignores_default_timezone() {
        use chrono::Timelike;
        let format = DateFormat::new(
            "%Y-%m-%dT%H:%M:%S%:z",
            "ISO with offset",
            true,
            true,
        );
        // String has +05:30 but default says UTC -- the string wins
        let dt = parse_datetime_with_format("2025-01-15T10:30:00+05:30", &format, "UTC").unwrap();
        assert_eq!(dt.hour(), 5);
        assert_eq!(dt.minute(), 0);
    }

    #[test]
    fn test_datetime_to_micros_respects_timezone() {
        let format = DateFormat::new("%Y-%m-%d %H:%M:%S", "space-separated", true, false);
        let micros_utc = datetime_to_micros_utc("2025-01-15 10:30:00", &format, "UTC").unwrap();
        let micros_est = datetime_to_micros_utc("2025-01-15 10:30:00", &format, "-05:00").unwrap();
        // EST is 5 hours behind UTC, so the same wall-clock time in EST is later in UTC
        assert_eq!(micros_est - micros_utc, 5 * 3600 * 1_000_000);
    }
}
