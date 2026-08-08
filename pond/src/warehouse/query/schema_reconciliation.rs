//! Schema Reconciliation for Cross-Source JOINs
//!
//! This module provides schema compatibility analysis for federated queries
//! that join data across different sources. It handles:
//!
//! - Type compatibility checking between JOIN keys
//! - Identifier case sensitivity normalization
//! - Semantic type conflict detection (e.g., cents vs dollars)
//! - Generation of user-facing warnings for potential issues
//!
//! # Example
//!
//! ```ignore
//! let analysis = analyze_join_keys(&stripe_customer_id, &postgres_customer_id);
//! if let JoinKeyCompatibility::Incompatible(reason) = analysis.compatibility {
//!     return Err(FederationError::SchemaIncompatible(reason));
//! }
//! for warning in analysis.warnings {
//!     log::warn!("{}", warning);
//! }
//! ```

use ahash::AHashMap;
use std::fmt;

use crate::warehouse::types::{
    coerce_types, CoercionResult, NullSemantics, SemanticType, SourceType, TimestampPrecision,
    TypedColumn, TypedSchema,
};

// ============================================================================
// Schema Warnings
// ============================================================================

/// Warnings generated during schema reconciliation.
///
/// These are non-fatal issues that users should be aware of when joining
/// data across different sources.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaWarning {
    /// Case sensitivity differs between sources.
    ///
    /// Example: PostgreSQL lowercases unquoted identifiers, but CSV is case-sensitive.
    CaseSensitivityMismatch {
        left_source: String,
        left_sensitivity: CaseSensitivity,
        right_source: String,
        right_sensitivity: CaseSensitivity,
        column_name: String,
    },

    /// Automatic type coercion was applied.
    ///
    /// Example: Int32 was widened to Int64 for the JOIN.
    TypeCoercionApplied {
        left_column: String,
        left_type: String,
        right_column: String,
        right_type: String,
        target_type: String,
    },

    /// Precision may be lost during type coercion.
    ///
    /// Example: Decimal(18,4) joined with Float64 may lose precision.
    PrecisionLoss {
        column: String,
        from_type: String,
        to_type: String,
        details: String,
    },

    /// Semantic types have different representations.
    ///
    /// Example: Stripe amounts are in cents, but PostgreSQL stores dollars.
    SemanticMismatch {
        left_column: String,
        left_semantic: String,
        right_column: String,
        right_semantic: String,
        suggestion: String,
    },

    /// Nullability differs between columns.
    ///
    /// Example: Left column is NOT NULL, right column allows NULL.
    NullabilityDifference {
        left_column: String,
        left_nullable: bool,
        right_column: String,
        right_nullable: bool,
    },

    /// UUID format differs between sources.
    ///
    /// Example: One source stores UUID as string, another as binary.
    UuidFormatMismatch {
        left_column: String,
        left_format: String,
        right_column: String,
        right_format: String,
    },

    /// Timestamp timezone handling differs.
    ///
    /// Example: One timestamp has timezone, the other doesn't.
    TimestampTimezoneMismatch {
        left_column: String,
        left_has_tz: bool,
        right_column: String,
        right_has_tz: bool,
    },

    /// NULL semantics differ between sources.
    ///
    /// Example: One source treats empty strings as NULL, the other doesn't.
    NullSemanticsMismatch {
        left_source: String,
        left_treats_empty_as_null: bool,
        right_source: String,
        right_treats_empty_as_null: bool,
        column_name: String,
        suggestion: String,
    },

    /// Timestamp precision differs between sources.
    ///
    /// Example: Stripe uses seconds, Parquet uses nanoseconds.
    TimestampPrecisionMismatch {
        left_column: String,
        left_precision: String,
        right_column: String,
        right_precision: String,
        normalized_to: String,
    },

    /// Date is being compared to timestamp.
    ///
    /// When comparing DATE = TIMESTAMP, the time component is ignored.
    DateTimestampComparison {
        date_column: String,
        timestamp_column: String,
        suggestion: String,
    },

    /// Naive timestamp (without timezone) is assumed to be UTC.
    ///
    /// This warning informs users that a timezone-naive timestamp
    /// is being interpreted as UTC.
    NaiveTimestampAssumedUtc {
        column: String,
        source: String,
    },
}

impl fmt::Display for SchemaWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaWarning::CaseSensitivityMismatch {
                left_source,
                left_sensitivity,
                right_source,
                right_sensitivity,
                column_name,
            } => {
                write!(
                    f,
                    "Case sensitivity mismatch on '{}': {} is {:?}, {} is {:?}",
                    column_name, left_source, left_sensitivity, right_source, right_sensitivity
                )
            }
            SchemaWarning::TypeCoercionApplied {
                left_column,
                left_type,
                right_column,
                right_type,
                target_type,
            } => {
                write!(
                    f,
                    "Type coercion applied: {} ({}) and {} ({}) will be coerced to {}",
                    left_column, left_type, right_column, right_type, target_type
                )
            }
            SchemaWarning::PrecisionLoss {
                column,
                from_type,
                to_type,
                details,
            } => {
                write!(
                    f,
                    "Precision loss on '{}': {} to {} - {}",
                    column, from_type, to_type, details
                )
            }
            SchemaWarning::SemanticMismatch {
                left_column,
                left_semantic,
                right_column,
                right_semantic,
                suggestion,
            } => {
                write!(
                    f,
                    "Semantic mismatch: {} is {} but {} is {}. {}",
                    left_column, left_semantic, right_column, right_semantic, suggestion
                )
            }
            SchemaWarning::NullabilityDifference {
                left_column,
                left_nullable,
                right_column,
                right_nullable,
            } => {
                let left_null_str = if *left_nullable { "nullable" } else { "NOT NULL" };
                let right_null_str = if *right_nullable { "nullable" } else { "NOT NULL" };
                write!(
                    f,
                    "Nullability differs: {} is {}, {} is {}",
                    left_column, left_null_str, right_column, right_null_str
                )
            }
            SchemaWarning::UuidFormatMismatch {
                left_column,
                left_format,
                right_column,
                right_format,
            } => {
                write!(
                    f,
                    "UUID format mismatch: {} is {}, {} is {}",
                    left_column, left_format, right_column, right_format
                )
            }
            SchemaWarning::TimestampTimezoneMismatch {
                left_column,
                left_has_tz,
                right_column,
                right_has_tz,
            } => {
                let left_tz = if *left_has_tz { "with timezone" } else { "without timezone" };
                let right_tz = if *right_has_tz { "with timezone" } else { "without timezone" };
                write!(
                    f,
                    "Timestamp timezone mismatch: {} is {}, {} is {}",
                    left_column, left_tz, right_column, right_tz
                )
            }
            SchemaWarning::NullSemanticsMismatch {
                left_source,
                left_treats_empty_as_null,
                right_source,
                right_treats_empty_as_null,
                column_name,
                suggestion,
            } => {
                let left_behavior = if *left_treats_empty_as_null {
                    "treats empty as NULL"
                } else {
                    "treats empty as valid string"
                };
                let right_behavior = if *right_treats_empty_as_null {
                    "treats empty as NULL"
                } else {
                    "treats empty as valid string"
                };
                write!(
                    f,
                    "NULL semantics mismatch on '{}': {} {}, {} {}. {}",
                    column_name, left_source, left_behavior, right_source, right_behavior, suggestion
                )
            }
            SchemaWarning::TimestampPrecisionMismatch {
                left_column,
                left_precision,
                right_column,
                right_precision,
                normalized_to,
            } => {
                write!(
                    f,
                    "Timestamp precision mismatch: {} ({}) vs {} ({}). Normalizing to {}.",
                    left_column, left_precision, right_column, right_precision, normalized_to
                )
            }
            SchemaWarning::DateTimestampComparison {
                date_column,
                timestamp_column,
                suggestion,
            } => {
                write!(
                    f,
                    "Comparing DATE ({}) with TIMESTAMP ({}). Time component will be ignored. {}",
                    date_column, timestamp_column, suggestion
                )
            }
            SchemaWarning::NaiveTimestampAssumedUtc { column, source } => {
                write!(
                    f,
                    "Timestamp column '{}' from '{}' has no timezone - assuming UTC.",
                    column, source
                )
            }
        }
    }
}

// ============================================================================
// JOIN Key Compatibility
// ============================================================================

/// Result of analyzing JOIN key compatibility.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinKeyCompatibility {
    /// Types are compatible (either identical or can be auto-coerced).
    Compatible,

    /// Types can be joined but require an explicit CAST.
    RequiresExplicitCast {
        reason: String,
        suggestion: String,
    },

    /// Types are fundamentally incompatible and cannot be joined.
    Incompatible {
        reason: String,
    },
}

impl JoinKeyCompatibility {
    /// Check if the join is allowed (either compatible or with explicit cast).
    pub fn is_allowed(&self) -> bool {
        !matches!(self, JoinKeyCompatibility::Incompatible { .. })
    }

    /// Check if the join is directly compatible without any user intervention.
    pub fn is_compatible(&self) -> bool {
        matches!(self, JoinKeyCompatibility::Compatible)
    }
}

// ============================================================================
// JOIN Key Analysis
// ============================================================================

/// Complete analysis of a JOIN key pair.
#[derive(Debug, Clone)]
pub struct JoinKeyAnalysis {
    /// The left column in the JOIN.
    pub left_column: TypedColumn,
    /// The right column in the JOIN.
    pub right_column: TypedColumn,
    /// Whether the types are compatible for joining.
    pub compatibility: JoinKeyCompatibility,
    /// Warnings about potential issues (even if compatible).
    pub warnings: Vec<SchemaWarning>,
    /// The target type after coercion (if applicable).
    pub coerced_type: Option<arrow::datatypes::DataType>,
}

impl JoinKeyAnalysis {
    /// Check if the analysis found any warnings.
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Get the first error if the join is incompatible.
    pub fn error(&self) -> Option<&str> {
        match &self.compatibility {
            JoinKeyCompatibility::Incompatible { reason } => Some(reason.as_str()),
            _ => None,
        }
    }
}

/// Analyze the compatibility of two columns for a JOIN operation.
///
/// This function:
/// 1. Checks structural type compatibility using Arrow type coercion
/// 2. Detects semantic type conflicts (e.g., cents vs dollars)
/// 3. Checks for nullability differences
/// 4. Generates appropriate warnings
///
/// # Arguments
/// * `left` - The left column in the JOIN condition
/// * `right` - The right column in the JOIN condition
///
/// # Returns
/// A `JoinKeyAnalysis` containing compatibility status and any warnings.
#[tracing::instrument(name = "warehouse.schema.analyze_join_keys", skip_all)]
pub fn analyze_join_keys(left: &TypedColumn, right: &TypedColumn) -> JoinKeyAnalysis {
    let mut warnings = Vec::with_capacity(4);
    let mut coerced_type = None;

    // Get Arrow types
    let left_arrow = left.arrow_data_type_or_string();
    let right_arrow = right.arrow_data_type_or_string();

    // Check structural type compatibility
    let coercion_result = coerce_types(
        &left_arrow,
        left.semantic.as_ref(),
        &right_arrow,
        right.semantic.as_ref(),
    );

    let compatibility = match coercion_result {
        CoercionResult::Same => JoinKeyCompatibility::Compatible,

        CoercionResult::AutoCoerce { target, warning } => {
            coerced_type = Some(target.clone());

            // Add type coercion warning
            warnings.push(SchemaWarning::TypeCoercionApplied {
                left_column: left.name.clone(),
                left_type: left.source_type_name.clone(),
                right_column: right.name.clone(),
                right_type: right.source_type_name.clone(),
                target_type: format!("{:?}", target),
            });

            // Add precision loss warning if applicable
            if let Some(warn_msg) = warning {
                warnings.push(SchemaWarning::PrecisionLoss {
                    column: format!("{} / {}", left.name, right.name),
                    from_type: left.source_type_name.clone(),
                    to_type: format!("{:?}", target),
                    details: warn_msg,
                });
            }

            JoinKeyCompatibility::Compatible
        }

        CoercionResult::RequiresExplicit { reason, suggestion } => {
            JoinKeyCompatibility::RequiresExplicitCast { reason, suggestion }
        }

        CoercionResult::Incompatible { reason } => {
            JoinKeyCompatibility::Incompatible { reason }
        }
    };

    // Check semantic type conflicts
    check_semantic_compatibility(left, right, &mut warnings);

    // Check nullability differences
    if left.nullable != right.nullable {
        warnings.push(SchemaWarning::NullabilityDifference {
            left_column: left.name.clone(),
            left_nullable: left.nullable,
            right_column: right.name.clone(),
            right_nullable: right.nullable,
        });
    }

    // Check UUID format differences
    check_uuid_compatibility(&left_arrow, &right_arrow, left, right, &mut warnings);

    // Check timestamp timezone and precision differences
    check_timestamp_compatibility(&left_arrow, &right_arrow, left, right, &mut warnings);

    // Check DATE vs TIMESTAMP comparisons
    check_date_timestamp_comparison(&left_arrow, &right_arrow, left, right, &mut warnings);

    JoinKeyAnalysis {
        left_column: left.clone(),
        right_column: right.clone(),
        compatibility,
        warnings,
        coerced_type,
    }
}

/// Check for semantic type conflicts between columns.
fn check_semantic_compatibility(
    left: &TypedColumn,
    right: &TypedColumn,
    warnings: &mut Vec<SchemaWarning>,
) {
    match (&left.semantic, &right.semantic) {
        // Money: cents vs dollars
        (
            Some(SemanticType::Money {
                in_cents: left_cents,
                currency: left_currency,
            }),
            Some(SemanticType::Money {
                in_cents: right_cents,
                currency: right_currency,
            }),
        ) => {
            if left_cents != right_cents {
                let left_repr = if *left_cents { "cents" } else { "dollars" };
                let right_repr = if *right_cents { "cents" } else { "dollars" };
                
                let suggestion = if *left_cents {
                    format!("Use cents_to_dollars({}) or dollars_to_cents({})", left.name, right.name)
                } else {
                    format!("Use dollars_to_cents({}) or cents_to_dollars({})", left.name, right.name)
                };

                warnings.push(SchemaWarning::SemanticMismatch {
                    left_column: left.name.clone(),
                    left_semantic: format!("Money in {}", left_repr),
                    right_column: right.name.clone(),
                    right_semantic: format!("Money in {}", right_repr),
                    suggestion,
                });
            }

            // Check currency mismatch
            if let (Some(lc), Some(rc)) = (left_currency, right_currency) {
                if lc != rc {
                    warnings.push(SchemaWarning::SemanticMismatch {
                        left_column: left.name.clone(),
                        left_semantic: format!("Currency: {}", lc),
                        right_column: right.name.clone(),
                        right_semantic: format!("Currency: {}", rc),
                        suggestion: "Ensure currency conversion is applied before joining".to_string(),
                    });
                }
            }
        }

        // Percentage: different scales
        (
            Some(SemanticType::Percentage { scale: left_scale }),
            Some(SemanticType::Percentage { scale: right_scale }),
        ) => {
            if left_scale != right_scale {
                warnings.push(SchemaWarning::SemanticMismatch {
                    left_column: left.name.clone(),
                    left_semantic: format!("{:?} scale", left_scale),
                    right_column: right.name.clone(),
                    right_semantic: format!("{:?} scale", right_scale),
                    suggestion: "Use percent_to_fraction() or fraction_to_percent() to align scales"
                        .to_string(),
                });
            }
        }

        // Duration: different units
        (
            Some(SemanticType::Duration { unit: left_unit }),
            Some(SemanticType::Duration { unit: right_unit }),
        ) => {
            if left_unit != right_unit {
                warnings.push(SchemaWarning::SemanticMismatch {
                    left_column: left.name.clone(),
                    left_semantic: format!("{:?}", left_unit),
                    right_column: right.name.clone(),
                    right_semantic: format!("{:?}", right_unit),
                    suggestion: "Convert to the same duration unit before joining".to_string(),
                });
            }
        }

        _ => {}
    }
}

/// Check UUID format compatibility.
fn check_uuid_compatibility(
    left_arrow: &arrow::datatypes::DataType,
    right_arrow: &arrow::datatypes::DataType,
    left: &TypedColumn,
    right: &TypedColumn,
    warnings: &mut Vec<SchemaWarning>,
) {
    use arrow::datatypes::DataType;

    let left_is_uuid = left.is_uuid();
    let right_is_uuid = right.is_uuid();

    if left_is_uuid && right_is_uuid {
        let left_format = if matches!(left_arrow, DataType::FixedSizeBinary(16)) {
            "binary (16 bytes)"
        } else {
            "string"
        };
        let right_format = if matches!(right_arrow, DataType::FixedSizeBinary(16)) {
            "binary (16 bytes)"
        } else {
            "string"
        };

        if left_format != right_format {
            warnings.push(SchemaWarning::UuidFormatMismatch {
                left_column: left.name.clone(),
                left_format: left_format.to_string(),
                right_column: right.name.clone(),
                right_format: right_format.to_string(),
            });
        }
    }
}

/// Check timestamp timezone and precision compatibility.
fn check_timestamp_compatibility(
    left_arrow: &arrow::datatypes::DataType,
    right_arrow: &arrow::datatypes::DataType,
    left: &TypedColumn,
    right: &TypedColumn,
    warnings: &mut Vec<SchemaWarning>,
) {
    use arrow::datatypes::DataType;

    if let (DataType::Timestamp(left_unit, left_tz), DataType::Timestamp(right_unit, right_tz)) =
        (left_arrow, right_arrow)
    {
        // Check timezone mismatch
        let left_has_tz = left_tz.is_some();
        let right_has_tz = right_tz.is_some();

        if left_has_tz != right_has_tz {
            warnings.push(SchemaWarning::TimestampTimezoneMismatch {
                left_column: left.name.clone(),
                left_has_tz,
                right_column: right.name.clone(),
                right_has_tz,
            });
        }

        // Check precision mismatch
        if left_unit != right_unit {
            let left_precision = TimestampPrecision::from_arrow_time_unit(*left_unit);
            let right_precision = TimestampPrecision::from_arrow_time_unit(*right_unit);
            
            // Normalize to the higher precision
            let normalized = std::cmp::max(*left_unit, *right_unit);
            let normalized_precision = TimestampPrecision::from_arrow_time_unit(normalized);

            warnings.push(SchemaWarning::TimestampPrecisionMismatch {
                left_column: left.name.clone(),
                left_precision: format!("{:?}", left_precision),
                right_column: right.name.clone(),
                right_precision: format!("{:?}", right_precision),
                normalized_to: format!("{:?}", normalized_precision),
            });
        }
    }
}

/// Check if a DATE is being compared to a TIMESTAMP.
///
/// When comparing DATE = TIMESTAMP, the time component is truncated.
/// This function emits a warning to inform the user.
fn check_date_timestamp_comparison(
    left_arrow: &arrow::datatypes::DataType,
    right_arrow: &arrow::datatypes::DataType,
    left: &TypedColumn,
    right: &TypedColumn,
    warnings: &mut Vec<SchemaWarning>,
) {
    use arrow::datatypes::DataType;

    let left_is_date = matches!(left_arrow, DataType::Date32 | DataType::Date64);
    let right_is_date = matches!(right_arrow, DataType::Date32 | DataType::Date64);
    let left_is_timestamp = matches!(left_arrow, DataType::Timestamp(_, _));
    let right_is_timestamp = matches!(right_arrow, DataType::Timestamp(_, _));

    if left_is_date && right_is_timestamp {
        warnings.push(SchemaWarning::DateTimestampComparison {
            date_column: left.name.clone(),
            timestamp_column: right.name.clone(),
            suggestion: "The timestamp's time component will be truncated. Use DATE_TRUNC() or CAST explicitly.".to_string(),
        });
    } else if left_is_timestamp && right_is_date {
        warnings.push(SchemaWarning::DateTimestampComparison {
            date_column: right.name.clone(),
            timestamp_column: left.name.clone(),
            suggestion: "The timestamp's time component will be truncated. Use DATE_TRUNC() or CAST explicitly.".to_string(),
        });
    }
}

/// Check if a timestamp column is naive (no timezone) and emit a warning.
///
/// Naive timestamps are assumed to be UTC.
pub fn check_naive_timestamp(
    column: &TypedColumn,
    source_name: &str,
    warnings: &mut Vec<SchemaWarning>,
) {
    use arrow::datatypes::DataType;

    if let DataType::Timestamp(_, None) = column.arrow_data_type_or_string() {
        let has_semantic_tz = matches!(
            &column.semantic,
            Some(SemanticType::Timestamp { .. })
        );

        if !has_semantic_tz {
            warnings.push(SchemaWarning::NaiveTimestampAssumedUtc {
                column: column.name.clone(),
                source: source_name.to_string(),
            });
        }
    }
}

// ============================================================================
// Case Sensitivity
// ============================================================================

/// Case sensitivity rules for identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseSensitivity {
    /// Identifiers are case-insensitive (e.g., PostgreSQL unquoted).
    CaseInsensitive,
    /// Identifiers are case-sensitive (e.g., most file formats).
    CaseSensitive,
    /// Case sensitivity depends on platform/configuration (e.g., MySQL).
    PlatformDependent,
}

impl Default for CaseSensitivity {
    fn default() -> Self {
        CaseSensitivity::CaseSensitive
    }
}

/// Normalizes identifiers across different data sources.
///
/// Different sources have different case sensitivity rules:
/// - PostgreSQL: Lowercases unquoted identifiers
/// - MySQL: Case-sensitive on Linux, insensitive on Windows
/// - CSV/Parquet: Case-sensitive
/// - Stripe: Case-sensitive
pub struct IdentifierNormalizer {
    /// Per-source case sensitivity rules.
    source_rules: AHashMap<SourceType, CaseSensitivity>,
    /// Custom overrides per source name.
    custom_overrides: AHashMap<String, CaseSensitivity>,
}

impl IdentifierNormalizer {
    /// Create a new identifier normalizer with default rules.
    pub fn new() -> Self {
        let mut source_rules = AHashMap::new();

        // PostgreSQL: case-insensitive by default (lowercases unquoted identifiers)
        source_rules.insert(SourceType::PostgreSQL, CaseSensitivity::CaseInsensitive);

        // MySQL: platform-dependent (case-sensitive on Linux, insensitive on Windows)
        source_rules.insert(SourceType::MySQL, CaseSensitivity::PlatformDependent);

        // File formats: case-sensitive
        source_rules.insert(SourceType::Csv, CaseSensitivity::CaseSensitive);
        source_rules.insert(SourceType::Json, CaseSensitivity::CaseSensitive);
        source_rules.insert(SourceType::Excel, CaseSensitivity::CaseSensitive);
        source_rules.insert(SourceType::ExternalParquet, CaseSensitivity::CaseSensitive);

        // Derived tables are materialized Parquet — case-sensitive (M6)
        source_rules.insert(SourceType::Derived, CaseSensitivity::CaseSensitive);

        // SaaS APIs: case-sensitive
        source_rules.insert(SourceType::Stripe, CaseSensitivity::CaseSensitive);
        source_rules.insert(SourceType::Salesforce, CaseSensitivity::CaseInsensitive);
        source_rules.insert(SourceType::HubSpot, CaseSensitivity::CaseSensitive);

        // Cloud warehouses
        source_rules.insert(SourceType::Snowflake, CaseSensitivity::CaseInsensitive);
        source_rules.insert(SourceType::BigQuery, CaseSensitivity::CaseSensitive);
        source_rules.insert(SourceType::Redshift, CaseSensitivity::CaseInsensitive);

        Self {
            source_rules,
            custom_overrides: AHashMap::new(),
        }
    }

    /// Get the case sensitivity for a source type.
    pub fn get_sensitivity(&self, source_type: SourceType) -> CaseSensitivity {
        self.source_rules
            .get(&source_type)
            .copied()
            .unwrap_or(CaseSensitivity::CaseSensitive)
    }

    /// Get the case sensitivity for a named source (with overrides).
    pub fn get_sensitivity_for_source(&self, source_name: &str, source_type: SourceType) -> CaseSensitivity {
        // Check custom overrides first
        if let Some(sensitivity) = self.custom_overrides.get(source_name) {
            return *sensitivity;
        }
        self.get_sensitivity(source_type)
    }

    /// Set a custom override for a specific source.
    pub fn set_override(&mut self, source_name: &str, sensitivity: CaseSensitivity) {
        self.custom_overrides.insert(source_name.to_string(), sensitivity);
    }

    /// Normalize an identifier according to source rules.
    ///
    /// For case-insensitive sources, converts to lowercase.
    /// For case-sensitive sources, returns as-is.
    pub fn normalize(&self, identifier: &str, source_type: SourceType) -> String {
        match self.get_sensitivity(source_type) {
            CaseSensitivity::CaseInsensitive => identifier.to_lowercase(),
            CaseSensitivity::CaseSensitive => identifier.to_string(),
            CaseSensitivity::PlatformDependent => {
                // Default to case-sensitive for safety
                identifier.to_string()
            }
        }
    }

    /// Check if two identifiers match, considering case sensitivity.
    pub fn identifiers_match(
        &self,
        left: &str,
        left_source: SourceType,
        right: &str,
        right_source: SourceType,
    ) -> bool {
        let left_norm = self.normalize(left, left_source);
        let right_norm = self.normalize(right, right_source);
        left_norm == right_norm
    }

    /// Check for case sensitivity mismatches between sources.
    pub fn check_sensitivity_mismatch(
        &self,
        left_source_name: &str,
        left_source_type: SourceType,
        right_source_name: &str,
        right_source_type: SourceType,
        column_name: &str,
    ) -> Option<SchemaWarning> {
        let left_sensitivity = self.get_sensitivity_for_source(left_source_name, left_source_type);
        let right_sensitivity = self.get_sensitivity_for_source(right_source_name, right_source_type);

        if left_sensitivity != right_sensitivity {
            Some(SchemaWarning::CaseSensitivityMismatch {
                left_source: left_source_name.to_string(),
                left_sensitivity,
                right_source: right_source_name.to_string(),
                right_sensitivity,
                column_name: column_name.to_string(),
            })
        } else {
            None
        }
    }
}

impl Default for IdentifierNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// NULL Semantics Registry
// ============================================================================

/// Registry for NULL value handling semantics across different sources.
///
/// Reiver uses uniform NULL semantics by default:
/// - **Empty string `""`** = Valid string with empty value (NOT NULL)
/// - **Missing field/object** = NULL
///
/// This registry allows per-source overrides for legacy data compatibility.
pub struct NullSemanticsRegistry {
    /// Default NULL semantics (used for all sources unless overridden).
    default_semantics: NullSemantics,
    /// Per-source overrides.
    overrides: AHashMap<String, NullSemantics>,
}

impl NullSemanticsRegistry {
    /// Create a new NULL semantics registry with default settings.
    pub fn new() -> Self {
        Self {
            default_semantics: NullSemantics::default(),
            overrides: AHashMap::new(),
        }
    }

    /// Get the NULL semantics for a source.
    ///
    /// Returns the source-specific override if set, otherwise the default.
    pub fn get_semantics(&self, source_name: &str) -> &NullSemantics {
        self.overrides
            .get(source_name)
            .unwrap_or(&self.default_semantics)
    }

    /// Set a custom override for a specific source.
    pub fn set_override(&mut self, source_name: &str, semantics: NullSemantics) {
        self.overrides.insert(source_name.to_string(), semantics);
    }

    /// Remove a custom override for a specific source.
    pub fn remove_override(&mut self, source_name: &str) {
        self.overrides.remove(source_name);
    }

    /// Check if NULL semantics differ between two sources in a way that could
    /// affect JOIN results.
    ///
    /// Returns a warning if one source treats empty strings as NULL and the other doesn't.
    pub fn check_semantics_mismatch(
        &self,
        left_source: &str,
        right_source: &str,
        column_name: &str,
    ) -> Option<SchemaWarning> {
        let left_semantics = self.get_semantics(left_source);
        let right_semantics = self.get_semantics(right_source);

        // Check if treat_empty_as_null differs
        if left_semantics.treat_empty_as_null != right_semantics.treat_empty_as_null {
            let suggestion = if left_semantics.treat_empty_as_null {
                format!(
                    "Source '{}' treats empty strings as NULL, but '{}' does not. \
                     Consider using NULLIF({}.{}, '') on the right side.",
                    left_source, right_source, right_source, column_name
                )
            } else {
                format!(
                    "Source '{}' treats empty strings as NULL, but '{}' does not. \
                     Consider using NULLIF({}.{}, '') on the left side.",
                    right_source, left_source, left_source, column_name
                )
            };

            return Some(SchemaWarning::NullSemanticsMismatch {
                left_source: left_source.to_string(),
                left_treats_empty_as_null: left_semantics.treat_empty_as_null,
                right_source: right_source.to_string(),
                right_treats_empty_as_null: right_semantics.treat_empty_as_null,
                column_name: column_name.to_string(),
                suggestion,
            });
        }

        None
    }
}

impl Default for NullSemanticsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Multi-Join Analysis
// ============================================================================

/// Analysis result for a complete JOIN operation (potentially multi-way).
#[derive(Debug, Clone)]
pub struct JoinAnalysisResult {
    /// Individual key analyses for each JOIN condition.
    pub key_analyses: Vec<JoinKeyAnalysis>,
    /// Aggregated warnings from all JOIN conditions.
    pub all_warnings: Vec<SchemaWarning>,
    /// Whether all JOINs are compatible.
    pub all_compatible: bool,
    /// First error encountered (if any).
    pub first_error: Option<String>,
}

impl JoinAnalysisResult {
    /// Create an empty result (for queries with no JOINs).
    pub fn empty() -> Self {
        Self {
            key_analyses: Vec::new(),
            all_warnings: Vec::new(),
            all_compatible: true,
            first_error: None,
        }
    }

    /// Create a result from multiple key analyses.
    pub fn from_analyses(analyses: Vec<JoinKeyAnalysis>) -> Self {
        let mut all_warnings = Vec::new();
        let mut all_compatible = true;
        let mut first_error = None;

        for analysis in &analyses {
            all_warnings.extend(analysis.warnings.clone());

            match &analysis.compatibility {
                JoinKeyCompatibility::Incompatible { reason } => {
                    all_compatible = false;
                    if first_error.is_none() {
                        first_error = Some(reason.clone());
                    }
                }
                JoinKeyCompatibility::RequiresExplicitCast { reason, .. } => {
                    all_warnings.push(SchemaWarning::PrecisionLoss {
                        column: analysis.left_column.name.clone(),
                        from_type: format!("{:?}", analysis.left_column.arrow_data_type_or_string()),
                        to_type: format!("{:?}", analysis.right_column.arrow_data_type_or_string()),
                        details: reason.clone(),
                    });
                }
                JoinKeyCompatibility::Compatible => {}
            }
        }

        Self {
            key_analyses: analyses,
            all_warnings,
            all_compatible,
            first_error,
        }
    }
}

// ============================================================================
// Schema Reconciler
// ============================================================================

/// Main entry point for schema reconciliation.
///
/// This struct coordinates type checking, identifier normalization,
/// and warning generation for cross-source JOINs.
pub struct SchemaReconciler {
    /// Identifier normalizer for handling case sensitivity.
    normalizer: IdentifierNormalizer,
    /// NULL semantics registry for handling NULL value differences.
    null_registry: NullSemanticsRegistry,
}

impl SchemaReconciler {
    /// Create a new schema reconciler.
    pub fn new() -> Self {
        Self {
            normalizer: IdentifierNormalizer::new(),
            null_registry: NullSemanticsRegistry::new(),
        }
    }

    /// Create with a custom identifier normalizer.
    pub fn with_normalizer(normalizer: IdentifierNormalizer) -> Self {
        Self {
            normalizer,
            null_registry: NullSemanticsRegistry::new(),
        }
    }

    /// Create with custom identifier normalizer and NULL semantics registry.
    pub fn with_normalizer_and_null_registry(
        normalizer: IdentifierNormalizer,
        null_registry: NullSemanticsRegistry,
    ) -> Self {
        Self {
            normalizer,
            null_registry,
        }
    }

    /// Get a reference to the identifier normalizer.
    pub fn normalizer(&self) -> &IdentifierNormalizer {
        &self.normalizer
    }

    /// Get a mutable reference to the identifier normalizer.
    pub fn normalizer_mut(&mut self) -> &mut IdentifierNormalizer {
        &mut self.normalizer
    }

    /// Get a reference to the NULL semantics registry.
    pub fn null_registry(&self) -> &NullSemanticsRegistry {
        &self.null_registry
    }

    /// Get a mutable reference to the NULL semantics registry.
    pub fn null_registry_mut(&mut self) -> &mut NullSemanticsRegistry {
        &mut self.null_registry
    }

    /// Analyze a single JOIN condition.
    #[tracing::instrument(name = "warehouse.schema.analyze_join", skip_all)]
    pub fn analyze_join(
        &self,
        left: &TypedColumn,
        right: &TypedColumn,
        left_source_type: SourceType,
        right_source_type: SourceType,
    ) -> JoinKeyAnalysis {
        let mut analysis = analyze_join_keys(left, right);

        // Add case sensitivity warning if applicable
        if let Some(warning) = self.normalizer.check_sensitivity_mismatch(
            &left.source_name,
            left_source_type,
            &right.source_name,
            right_source_type,
            &left.name,
        ) {
            analysis.warnings.push(warning);
        }

        // Add NULL semantics warning if applicable
        if let Some(warning) = self.null_registry.check_semantics_mismatch(
            &left.source_name,
            &right.source_name,
            &left.name,
        ) {
            analysis.warnings.push(warning);
        }

        analysis
    }

    /// Analyze multiple JOIN conditions.
    pub fn analyze_joins(
        &self,
        joins: Vec<(TypedColumn, TypedColumn, SourceType, SourceType)>,
    ) -> JoinAnalysisResult {
        let analyses: Vec<JoinKeyAnalysis> = joins
            .into_iter()
            .map(|(left, right, left_type, right_type)| {
                self.analyze_join(&left, &right, left_type, right_type)
            })
            .collect();

        JoinAnalysisResult::from_analyses(analyses)
    }

    /// Find a column in a schema by name, handling case sensitivity.
    pub fn find_column<'a>(
        &self,
        schema: &'a TypedSchema,
        column_name: &str,
        source_type: SourceType,
    ) -> Option<&'a TypedColumn> {
        let normalized = self.normalizer.normalize(column_name, source_type);

        schema.columns.iter().find(|col| {
            let col_normalized = self.normalizer.normalize(&col.name, source_type);
            col_normalized == normalized
        })
    }
}

impl Default for SchemaReconciler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn make_typed_column(
        name: &str,
        data_type: DataType,
        source_type_name: &str,
        source_name: &str,
        semantic: Option<SemanticType>,
    ) -> TypedColumn {
        let mut col = TypedColumn::new(name, &data_type, true, source_type_name, source_name);
        if let Some(sem) = semantic {
            col = col.with_semantic(sem);
        }
        col
    }

    #[test]
    fn test_compatible_same_types() {
        let left = make_typed_column("id", DataType::Int64, "bigint", "postgres", None);
        let right = make_typed_column("id", DataType::Int64, "bigint", "mysql", None);

        let analysis = analyze_join_keys(&left, &right);
        assert!(analysis.compatibility.is_compatible());
        assert!(analysis.warnings.is_empty());
    }

    #[test]
    fn test_compatible_with_coercion() {
        let left = make_typed_column("id", DataType::Int32, "integer", "postgres", None);
        let right = make_typed_column("id", DataType::Int64, "bigint", "mysql", None);

        let analysis = analyze_join_keys(&left, &right);
        assert!(analysis.compatibility.is_compatible());
        assert!(!analysis.warnings.is_empty()); // Should have coercion warning
    }

    #[test]
    fn test_money_semantic_mismatch() {
        let left = make_typed_column(
            "amount",
            DataType::Int64,
            "bigint",
            "stripe",
            Some(SemanticType::Money {
                currency: Some("USD".to_string()),
                in_cents: true,
            }),
        );
        let right = make_typed_column(
            "total",
            DataType::Decimal128(10, 2),
            "decimal(10,2)",
            "postgres",
            Some(SemanticType::Money {
                currency: Some("USD".to_string()),
                in_cents: false,
            }),
        );

        let analysis = analyze_join_keys(&left, &right);
        
        // Should have a semantic mismatch warning
        let has_semantic_warning = analysis.warnings.iter().any(|w| {
            matches!(w, SchemaWarning::SemanticMismatch { .. })
        });
        assert!(has_semantic_warning, "Expected semantic mismatch warning for cents vs dollars");
    }

    #[test]
    fn test_nullability_warning() {
        let mut left = make_typed_column("id", DataType::Int64, "bigint", "postgres", None);
        left.nullable = false;
        
        let mut right = make_typed_column("id", DataType::Int64, "bigint", "mysql", None);
        right.nullable = true;

        let analysis = analyze_join_keys(&left, &right);
        
        let has_null_warning = analysis.warnings.iter().any(|w| {
            matches!(w, SchemaWarning::NullabilityDifference { .. })
        });
        assert!(has_null_warning, "Expected nullability difference warning");
    }

    #[test]
    fn test_identifier_normalizer_postgres() {
        let normalizer = IdentifierNormalizer::new();
        
        // PostgreSQL should be case-insensitive
        assert_eq!(
            normalizer.get_sensitivity(SourceType::PostgreSQL),
            CaseSensitivity::CaseInsensitive
        );
        assert_eq!(
            normalizer.normalize("CustomerID", SourceType::PostgreSQL),
            "customerid"
        );
    }

    #[test]
    fn test_identifier_normalizer_csv() {
        let normalizer = IdentifierNormalizer::new();
        
        // CSV should be case-sensitive
        assert_eq!(
            normalizer.get_sensitivity(SourceType::Csv),
            CaseSensitivity::CaseSensitive
        );
        assert_eq!(
            normalizer.normalize("CustomerID", SourceType::Csv),
            "CustomerID"
        );
    }

    #[test]
    fn test_identifiers_match() {
        let normalizer = IdentifierNormalizer::new();
        
        // Same case - should match for both
        assert!(normalizer.identifiers_match(
            "customer_id",
            SourceType::PostgreSQL,
            "customer_id",
            SourceType::Csv
        ));
        
        // Different case - should match for PostgreSQL (lowercased)
        assert!(normalizer.identifiers_match(
            "Customer_ID",
            SourceType::PostgreSQL,
            "customer_id",
            SourceType::PostgreSQL
        ));
        
        // Different case - should NOT match when CSV is involved (case-sensitive)
        assert!(!normalizer.identifiers_match(
            "Customer_ID",
            SourceType::Csv,
            "customer_id",
            SourceType::Csv
        ));
    }

    #[test]
    fn test_case_sensitivity_mismatch_warning() {
        let normalizer = IdentifierNormalizer::new();
        
        let warning = normalizer.check_sensitivity_mismatch(
            "postgres_source",
            SourceType::PostgreSQL,
            "csv_source",
            SourceType::Csv,
            "customer_id",
        );
        
        assert!(warning.is_some());
        if let Some(SchemaWarning::CaseSensitivityMismatch { left_sensitivity, right_sensitivity, .. }) = warning {
            assert_eq!(left_sensitivity, CaseSensitivity::CaseInsensitive);
            assert_eq!(right_sensitivity, CaseSensitivity::CaseSensitive);
        }
    }

    #[test]
    fn test_schema_reconciler_full_flow() {
        let reconciler = SchemaReconciler::new();
        
        let left = make_typed_column("id", DataType::Int64, "bigint", "postgres", None);
        let right = make_typed_column("id", DataType::Int64, "BIGINT", "stripe", None);
        
        let analysis = reconciler.analyze_join(
            &left,
            &right,
            SourceType::PostgreSQL,
            SourceType::Stripe,
        );
        
        assert!(analysis.compatibility.is_compatible());
        
        // Should have case sensitivity warning
        let has_case_warning = analysis.warnings.iter().any(|w| {
            matches!(w, SchemaWarning::CaseSensitivityMismatch { .. })
        });
        assert!(has_case_warning, "Expected case sensitivity mismatch warning");
    }

    #[test]
    fn test_join_analysis_result() {
        let left1 = make_typed_column("id", DataType::Int64, "bigint", "postgres", None);
        let right1 = make_typed_column("id", DataType::Int64, "bigint", "mysql", None);
        
        let left2 = make_typed_column("name", DataType::Utf8, "text", "postgres", None);
        let right2 = make_typed_column("name", DataType::Utf8, "varchar", "mysql", None);
        
        let analysis1 = analyze_join_keys(&left1, &right1);
        let analysis2 = analyze_join_keys(&left2, &right2);
        
        let result = JoinAnalysisResult::from_analyses(vec![analysis1, analysis2]);
        
        assert!(result.all_compatible);
        assert!(result.first_error.is_none());
        assert_eq!(result.key_analyses.len(), 2);
    }

    #[test]
    fn test_timestamp_timezone_warning() {
        use arrow::datatypes::TimeUnit;
        
        let left = make_typed_column(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "timestamptz",
            "postgres",
            None,
        );
        let right = make_typed_column(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            "timestamp",
            "mysql",
            None,
        );
        
        let analysis = analyze_join_keys(&left, &right);
        
        let has_tz_warning = analysis.warnings.iter().any(|w| {
            matches!(w, SchemaWarning::TimestampTimezoneMismatch { .. })
        });
        assert!(has_tz_warning, "Expected timestamp timezone mismatch warning");
    }

    // ===== NullSemanticsRegistry Tests =====

    #[test]
    fn test_null_registry_default() {
        let registry = NullSemanticsRegistry::new();
        
        // Default semantics should not treat empty as NULL
        let semantics = registry.get_semantics("any_source");
        assert!(!semantics.treat_empty_as_null);
    }

    #[test]
    fn test_null_registry_override() {
        let mut registry = NullSemanticsRegistry::new();
        
        // Set a legacy override for CSV source
        registry.set_override("csv_source", NullSemantics::legacy());
        
        // CSV source should use legacy semantics
        let csv_semantics = registry.get_semantics("csv_source");
        assert!(csv_semantics.treat_empty_as_null);
        
        // Other sources should use default
        let postgres_semantics = registry.get_semantics("postgres");
        assert!(!postgres_semantics.treat_empty_as_null);
    }

    #[test]
    fn test_null_registry_remove_override() {
        let mut registry = NullSemanticsRegistry::new();
        
        registry.set_override("csv_source", NullSemantics::legacy());
        assert!(registry.get_semantics("csv_source").treat_empty_as_null);
        
        registry.remove_override("csv_source");
        assert!(!registry.get_semantics("csv_source").treat_empty_as_null);
    }

    #[test]
    fn test_null_semantics_mismatch_warning() {
        let mut registry = NullSemanticsRegistry::new();
        
        // One source treats empty as NULL, the other doesn't
        registry.set_override("legacy_csv", NullSemantics::legacy());
        
        let warning = registry.check_semantics_mismatch(
            "legacy_csv",
            "postgres",
            "email",
        );
        
        assert!(warning.is_some());
        if let Some(SchemaWarning::NullSemanticsMismatch {
            left_treats_empty_as_null,
            right_treats_empty_as_null,
            ..
        }) = warning
        {
            assert!(left_treats_empty_as_null);
            assert!(!right_treats_empty_as_null);
        }
    }

    #[test]
    fn test_null_semantics_no_mismatch() {
        let registry = NullSemanticsRegistry::new();
        
        // Both use default semantics - no mismatch
        let warning = registry.check_semantics_mismatch(
            "postgres",
            "stripe",
            "email",
        );
        
        assert!(warning.is_none());
    }

    #[test]
    fn test_schema_reconciler_with_null_semantics() {
        let mut reconciler = SchemaReconciler::new();
        
        // Set one source to legacy mode
        reconciler.null_registry_mut().set_override("legacy_csv", NullSemantics::legacy());
        
        let left = make_typed_column("email", DataType::Utf8, "text", "legacy_csv", None);
        let right = make_typed_column("email", DataType::Utf8, "text", "postgres", None);
        
        let analysis = reconciler.analyze_join(
            &left,
            &right,
            SourceType::Csv,
            SourceType::PostgreSQL,
        );
        
        // Should have NULL semantics mismatch warning
        let has_null_warning = analysis.warnings.iter().any(|w| {
            matches!(w, SchemaWarning::NullSemanticsMismatch { .. })
        });
        assert!(has_null_warning, "Expected NULL semantics mismatch warning");
    }

    // ===== Timestamp Compatibility Tests =====

    #[test]
    fn test_timestamp_precision_mismatch_warning() {
        use arrow::datatypes::TimeUnit;
        
        let left = make_typed_column(
            "created_at",
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            "timestamp",
            "stripe",
            None,
        );
        let right = make_typed_column(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "timestamp",
            "postgres",
            None,
        );
        
        let analysis = analyze_join_keys(&left, &right);
        
        let has_precision_warning = analysis.warnings.iter().any(|w| {
            matches!(w, SchemaWarning::TimestampPrecisionMismatch { .. })
        });
        assert!(has_precision_warning, "Expected timestamp precision mismatch warning");
    }

    #[test]
    fn test_date_timestamp_comparison_warning() {
        use arrow::datatypes::TimeUnit;
        
        let left = make_typed_column(
            "order_date",
            DataType::Date32,
            "date",
            "csv",
            None,
        );
        let right = make_typed_column(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "timestamp",
            "postgres",
            None,
        );
        
        let analysis = analyze_join_keys(&left, &right);
        
        let has_date_ts_warning = analysis.warnings.iter().any(|w| {
            matches!(w, SchemaWarning::DateTimestampComparison { .. })
        });
        assert!(has_date_ts_warning, "Expected date/timestamp comparison warning");
    }

    #[test]
    fn test_timestamp_timezone_mismatch_warning() {
        use arrow::datatypes::TimeUnit;
        
        let left = make_typed_column(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "timestamptz",
            "postgres1",
            None,
        );
        let right = make_typed_column(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None), // No timezone
            "timestamp",
            "postgres2",
            None,
        );
        
        let analysis = analyze_join_keys(&left, &right);
        
        let has_tz_warning = analysis.warnings.iter().any(|w| {
            matches!(w, SchemaWarning::TimestampTimezoneMismatch { .. })
        });
        assert!(has_tz_warning, "Expected timestamp timezone mismatch warning");
    }

    #[test]
    fn test_schema_warning_display_timestamp_precision() {
        let warning = SchemaWarning::TimestampPrecisionMismatch {
            left_column: "stripe.created".to_string(),
            left_precision: "Seconds".to_string(),
            right_column: "pg.created_at".to_string(),
            right_precision: "Microseconds".to_string(),
            normalized_to: "Microseconds".to_string(),
        };
        
        let display = format!("{}", warning);
        assert!(display.contains("Timestamp precision mismatch"));
        assert!(display.contains("Seconds"));
        assert!(display.contains("Microseconds"));
    }

    #[test]
    fn test_schema_warning_display_date_timestamp() {
        let warning = SchemaWarning::DateTimestampComparison {
            date_column: "order_date".to_string(),
            timestamp_column: "created_at".to_string(),
            suggestion: "Use DATE_TRUNC()".to_string(),
        };
        
        let display = format!("{}", warning);
        assert!(display.contains("DATE"));
        assert!(display.contains("TIMESTAMP"));
    }

    #[test]
    fn test_schema_warning_display_naive_utc() {
        let warning = SchemaWarning::NaiveTimestampAssumedUtc {
            column: "updated_at".to_string(),
            source: "postgres_orders".to_string(),
        };
        
        let display = format!("{}", warning);
        assert!(display.contains("updated_at"));
        assert!(display.contains("UTC"));
    }
}
