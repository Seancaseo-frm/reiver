//! Fingerprint calculation for metric time series identification.
//!
//! A fingerprint is a unique identifier for a time series, computed from
//! the metric name and its labels. This allows efficient grouping and
//! querying of metrics data.

#![allow(dead_code)] // Fingerprint utilities - some functions for future JSON-based fingerprinting

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

/// Compute a fingerprint for a metric time series.
///
/// The fingerprint is a hash of the metric name and sorted labels,
/// ensuring consistent identification across different data points
/// of the same time series.
///
/// # Arguments
/// * `metric_name` - The name of the metric
/// * `labels` - Key-value pairs of labels (must be sorted for consistency)
///
/// # Returns
/// A 64-bit unsigned integer fingerprint
///
/// # Example
/// ```
/// use std::collections::BTreeMap;
/// use reiver_watch::metrics::compute_fingerprint;
///
/// let mut labels = BTreeMap::new();
/// labels.insert("host".to_string(), "web-1".to_string());
/// labels.insert("env".to_string(), "production".to_string());
///
/// let fp = compute_fingerprint("http.requests", &labels);
/// ```
pub fn compute_fingerprint(metric_name: &str, labels: &BTreeMap<String, String>) -> u64 {
    let mut hasher = DefaultHasher::new();

    // Hash the metric name
    metric_name.hash(&mut hasher);

    // Hash each label key-value pair (BTreeMap is already sorted by key)
    for (key, value) in labels {
        key.hash(&mut hasher);
        value.hash(&mut hasher);
    }

    hasher.finish()
}

/// Compute fingerprint from a labels JSON string.
///
/// This is useful when labels are already serialized as JSON.
pub fn compute_fingerprint_from_json(
    metric_name: &str,
    labels_json: &str,
) -> Result<u64, serde_json::Error> {
    let labels: BTreeMap<String, String> = if labels_json.is_empty() {
        BTreeMap::new()
    } else {
        serde_json::from_str(labels_json)?
    };
    Ok(compute_fingerprint(metric_name, &labels))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_deterministic() {
        let mut labels = BTreeMap::new();
        labels.insert("host".to_string(), "web-1".to_string());
        labels.insert("env".to_string(), "prod".to_string());

        let fp1 = compute_fingerprint("http.requests", &labels);
        let fp2 = compute_fingerprint("http.requests", &labels);

        assert_eq!(fp1, fp2, "Same inputs should produce same fingerprint");
    }

    #[test]
    fn test_fingerprint_different_labels() {
        let mut labels1 = BTreeMap::new();
        labels1.insert("host".to_string(), "web-1".to_string());

        let mut labels2 = BTreeMap::new();
        labels2.insert("host".to_string(), "web-2".to_string());

        let fp1 = compute_fingerprint("http.requests", &labels1);
        let fp2 = compute_fingerprint("http.requests", &labels2);

        assert_ne!(
            fp1, fp2,
            "Different labels should produce different fingerprints"
        );
    }

    #[test]
    fn test_fingerprint_different_metrics() {
        let mut labels = BTreeMap::new();
        labels.insert("host".to_string(), "web-1".to_string());

        let fp1 = compute_fingerprint("http.requests", &labels);
        let fp2 = compute_fingerprint("http.errors", &labels);

        assert_ne!(
            fp1, fp2,
            "Different metric names should produce different fingerprints"
        );
    }

    #[test]
    fn test_fingerprint_empty_labels() {
        let labels = BTreeMap::new();
        let fp = compute_fingerprint("http.requests", &labels);

        // Should not panic and should produce a valid fingerprint
        assert!(fp > 0);
    }

    #[test]
    fn test_fingerprint_label_order_independent() {
        // BTreeMap ensures consistent ordering regardless of insertion order
        let mut labels1 = BTreeMap::new();
        labels1.insert("host".to_string(), "web-1".to_string());
        labels1.insert("env".to_string(), "prod".to_string());

        let mut labels2 = BTreeMap::new();
        labels2.insert("env".to_string(), "prod".to_string());
        labels2.insert("host".to_string(), "web-1".to_string());

        let fp1 = compute_fingerprint("http.requests", &labels1);
        let fp2 = compute_fingerprint("http.requests", &labels2);

        assert_eq!(
            fp1, fp2,
            "Label insertion order should not affect fingerprint"
        );
    }

    #[test]
    fn test_fingerprint_from_json() {
        let fp =
            compute_fingerprint_from_json("http.requests", r#"{"env": "prod", "host": "web-1"}"#)
                .unwrap();

        let mut labels = BTreeMap::new();
        labels.insert("host".to_string(), "web-1".to_string());
        labels.insert("env".to_string(), "prod".to_string());
        let expected_fp = compute_fingerprint("http.requests", &labels);

        assert_eq!(fp, expected_fp);
    }

    #[test]
    fn test_fingerprint_empty_json() {
        let fp = compute_fingerprint_from_json("http.requests", "{}").unwrap();
        let expected = compute_fingerprint("http.requests", &BTreeMap::new());
        assert_eq!(fp, expected);
    }
}
