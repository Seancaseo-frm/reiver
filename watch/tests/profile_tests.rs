//! Profile API Tests
//!
//! Tests for the OpenTelemetry profiling endpoints and profile comparison features.

mod helpers;

use serde_json::json;

use helpers::*;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    // ========================================================================
    // OTLP Profiles Payload Tests
    // ========================================================================

    /// Create a minimal valid OTLP profiles export request (JSON format)
    fn create_otlp_profiles_request() -> serde_json::Value {
        let timestamp_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;

        json!({
            "resourceProfiles": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "test-service"}},
                        {"key": "service.version", "value": {"stringValue": "1.0.0"}}
                    ]
                },
                "scopeProfiles": [{
                    "scope": {
                        "name": "reiver-profiler",
                        "version": "1.0.0"
                    },
                    "profiles": [{
                        "profileId": "0123456789abcdef0123456789abcdef",
                        "timeUnixNano": timestamp_nanos,
                        "durationNano": 1000000000_u64, // 1 second
                        "period": 10000000, // 10ms sampling period
                        "periodType": {
                            "typeStrindex": 0,
                            "unitStrindex": 1
                        },
                        "sampleType": [{
                            "typeStrindex": 0,
                            "unitStrindex": 1
                        }],
                        "sample": [{
                            "locationIndex": 0,
                            "value": [100],
                            "attributeIndices": []
                        }],
                        "link": [{
                            "traceId": "0123456789abcdef0123456789abcdef",
                            "spanId": "0123456789abcdef"
                        }]
                    }]
                }]
            }],
            "dictionary": {
                "stringTable": ["cpu", "nanoseconds", "main", "app.rs"],
                "functionTable": [{
                    "nameStrindex": 2,
                    "filenameStrindex": 3,
                    "startLine": 10
                }],
                "locationTable": [{
                    "mappingIndex": 0,
                    "address": 0,
                    "line": [{
                        "functionIndex": 0,
                        "line": 10
                    }]
                }],
                "linkTable": [],
                "attributeTable": []
            }
        })
    }

    #[test]
    fn test_otlp_profiles_request_structure() {
        let request = create_otlp_profiles_request();

        assert!(request["resourceProfiles"].is_array());
        assert!(request["resourceProfiles"][0]["resource"].is_object());
        assert!(request["resourceProfiles"][0]["scopeProfiles"].is_array());
        assert!(request["resourceProfiles"][0]["scopeProfiles"][0]["profiles"].is_array());
    }

    #[test]
    fn test_profiles_endpoint_request_creation() {
        let request = create_otlp_profiles_request();
        let req = json_post("/v1/profiles", &request);

        assert_eq!(req.method(), "POST");
        assert_eq!(req.uri(), "/v1/profiles");
    }

    #[test]
    fn test_profile_has_link_for_trace_correlation() {
        let request = create_otlp_profiles_request();
        let profile = &request["resourceProfiles"][0]["scopeProfiles"][0]["profiles"][0];

        assert!(profile["link"].is_array());
        assert!(!profile["link"].as_array().unwrap().is_empty());

        let link = &profile["link"][0];
        assert!(link["traceId"].is_string());
        assert!(link["spanId"].is_string());
    }

    #[test]
    fn test_profile_dictionary_structure() {
        let request = create_otlp_profiles_request();
        let dictionary = &request["dictionary"];

        assert!(dictionary["stringTable"].is_array());
        assert!(dictionary["functionTable"].is_array());
        assert!(dictionary["locationTable"].is_array());
    }

    // ========================================================================
    // Profile with Trace Correlation Tests
    // ========================================================================

    fn create_profile_with_trace_link() -> serde_json::Value {
        let timestamp_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let trace_id = "fedcba9876543210fedcba9876543210";
        let span_id = "fedcba9876543210";

        json!({
            "profileId": "abcd1234abcd1234abcd1234abcd1234",
            "timeUnixNano": timestamp_nanos,
            "durationNano": 500000000_u64,
            "period": 10000000,
            "sample": [{
                "locationIndex": 0,
                "value": [50],
                "linkIndex": 0
            }],
            "link": [{
                "traceId": trace_id,
                "spanId": span_id
            }]
        })
    }

    #[test]
    fn test_profile_trace_link_extraction() {
        let profile = create_profile_with_trace_link();
        let link = &profile["link"][0];

        assert_eq!(
            link["traceId"].as_str().unwrap(),
            "fedcba9876543210fedcba9876543210"
        );
        assert_eq!(link["spanId"].as_str().unwrap(), "fedcba9876543210");
    }

    #[test]
    fn test_sample_link_index_reference() {
        let profile = create_profile_with_trace_link();
        let sample = &profile["sample"][0];

        // Sample should have linkIndex that references the link array
        assert_eq!(sample["linkIndex"].as_i64().unwrap(), 0);
    }

    // ========================================================================
    // Profile Comparison Tests
    // ========================================================================

    #[derive(serde::Serialize)]
    struct ProfileVersionStats {
        version: String,
        profile_count: u64,
        total_samples: u64,
        avg_duration_nano: f64,
    }

    fn calculate_profile_diff(v1: &ProfileVersionStats, v2: &ProfileVersionStats) -> (i64, f64) {
        let sample_diff = v2.total_samples as i64 - v1.total_samples as i64;
        let duration_pct_change = if v1.avg_duration_nano > 0.0 {
            ((v2.avg_duration_nano - v1.avg_duration_nano) / v1.avg_duration_nano) * 100.0
        } else {
            0.0
        };
        (sample_diff, duration_pct_change)
    }

    #[test]
    fn test_profile_comparison_diff_calculation() {
        let v1 = ProfileVersionStats {
            version: "1.0.0".to_string(),
            profile_count: 100,
            total_samples: 10000,
            avg_duration_nano: 1000000.0, // 1ms
        };

        let v2 = ProfileVersionStats {
            version: "2.0.0".to_string(),
            profile_count: 120,
            total_samples: 12000,
            avg_duration_nano: 800000.0, // 0.8ms - 20% improvement
        };

        let (sample_diff, duration_pct_change) = calculate_profile_diff(&v1, &v2);

        assert_eq!(sample_diff, 2000);
        assert!((duration_pct_change - (-20.0)).abs() < 0.01);
    }

    #[test]
    fn test_profile_comparison_regression_detection() {
        let v1 = ProfileVersionStats {
            version: "1.0.0".to_string(),
            profile_count: 100,
            total_samples: 10000,
            avg_duration_nano: 1000000.0,
        };

        let v2 = ProfileVersionStats {
            version: "2.0.0".to_string(),
            profile_count: 100,
            total_samples: 10000,
            avg_duration_nano: 1500000.0, // 50% regression
        };

        let (_, duration_pct_change) = calculate_profile_diff(&v1, &v2);

        // Positive change means regression (slower)
        assert!(duration_pct_change > 0.0);
        assert!((duration_pct_change - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_profile_comparison_no_baseline() {
        let v1 = ProfileVersionStats {
            version: "1.0.0".to_string(),
            profile_count: 0,
            total_samples: 0,
            avg_duration_nano: 0.0,
        };

        let v2 = ProfileVersionStats {
            version: "2.0.0".to_string(),
            profile_count: 100,
            total_samples: 10000,
            avg_duration_nano: 1000000.0,
        };

        let (sample_diff, duration_pct_change) = calculate_profile_diff(&v1, &v2);

        assert_eq!(sample_diff, 10000);
        assert_eq!(duration_pct_change, 0.0); // No baseline, so 0% change
    }

    // ========================================================================
    // Profile API Endpoint Tests
    // ========================================================================

    #[test]
    fn test_list_profiles_endpoint_creation() {
        let project_id = uuid::Uuid::new_v4();
        let uri = format!("/api/profiles/projects/{}/profiles", project_id);
        let req = json_get(&uri);

        assert_eq!(req.method(), "GET");
        assert!(req.uri().to_string().contains("/profiles"));
    }

    #[test]
    fn test_get_profile_endpoint_creation() {
        let project_id = uuid::Uuid::new_v4();
        let profile_id = "test-profile-id";
        let uri = format!(
            "/api/profiles/projects/{}/profiles/{}",
            project_id, profile_id
        );
        let req = json_get(&uri);

        assert_eq!(req.method(), "GET");
        assert!(req.uri().to_string().contains(profile_id));
    }

    #[test]
    fn test_profile_for_trace_endpoint_creation() {
        let project_id = uuid::Uuid::new_v4();
        let trace_id = "0123456789abcdef0123456789abcdef";
        let uri = format!(
            "/api/profiles/projects/{}/traces/{}/profile",
            project_id, trace_id
        );
        let req = json_get(&uri);

        assert_eq!(req.method(), "GET");
        assert!(req.uri().to_string().contains(trace_id));
    }

    #[test]
    fn test_service_profiles_endpoint_creation() {
        let project_id = uuid::Uuid::new_v4();
        let service = "my-service";
        let uri = format!(
            "/api/profiles/projects/{}/services/{}/profiles",
            project_id, service
        );
        let req = json_get(&uri);

        assert_eq!(req.method(), "GET");
        assert!(req.uri().to_string().contains(service));
    }

    #[test]
    fn test_profile_comparison_endpoint_creation() {
        let project_id = uuid::Uuid::new_v4();
        let service = "my-service";
        let uri = format!(
            "/api/profiles/projects/{}/services/{}/profiles/comparison?version1=1.0.0&version2=2.0.0",
            project_id, service
        );
        let req = json_get(&uri);

        assert_eq!(req.method(), "GET");
        assert!(req.uri().to_string().contains("comparison"));
        assert!(req.uri().to_string().contains("version1"));
        assert!(req.uri().to_string().contains("version2"));
    }

    #[test]
    fn test_list_versions_endpoint_creation() {
        let project_id = uuid::Uuid::new_v4();
        let service = "my-service";
        let uri = format!(
            "/api/profiles/projects/{}/services/{}/profiles/versions",
            project_id, service
        );
        let req = json_get(&uri);

        assert_eq!(req.method(), "GET");
        assert!(req.uri().to_string().contains("versions"));
    }

    #[test]
    fn test_version_stats_endpoint_creation() {
        let project_id = uuid::Uuid::new_v4();
        let service = "my-service";
        let version = "1.0.0";
        let uri = format!(
            "/api/profiles/projects/{}/services/{}/profiles/version/{}",
            project_id, service, version
        );
        let req = json_get(&uri);

        assert_eq!(req.method(), "GET");
        assert!(req.uri().to_string().contains(version));
    }

    // ========================================================================
    // Flame Graph Structure Tests
    // ========================================================================

    fn create_minimal_flame_graph() -> serde_json::Value {
        json!({
            "root": {
                "name": "root",
                "value": 100,
                "children": [{
                    "name": "main",
                    "value": 80,
                    "children": [{
                        "name": "process_request",
                        "value": 60,
                        "children": []
                    }]
                }]
            },
            "total_value": 100,
            "metadata": {
                "profile_type": "cpu",
                "sample_count": 100,
                "duration_nano": 1000000000_u64,
                "period": 10000000
            }
        })
    }

    #[test]
    fn test_flame_graph_structure() {
        let fg = create_minimal_flame_graph();

        assert!(fg["root"].is_object());
        assert_eq!(fg["root"]["name"], "root");
        assert!(fg["root"]["children"].is_array());
        assert!(fg["total_value"].is_number());
        assert!(fg["metadata"].is_object());
    }

    #[test]
    fn test_flame_graph_node_values() {
        let fg = create_minimal_flame_graph();
        let root = &fg["root"];

        assert_eq!(root["value"].as_u64().unwrap(), 100);

        let main = &root["children"][0];
        assert_eq!(main["name"], "main");
        assert_eq!(main["value"].as_u64().unwrap(), 80);

        let process = &main["children"][0];
        assert_eq!(process["name"], "process_request");
        assert_eq!(process["value"].as_u64().unwrap(), 60);
    }

    #[test]
    fn test_flame_graph_metadata() {
        let fg = create_minimal_flame_graph();
        let metadata = &fg["metadata"];

        assert_eq!(metadata["profile_type"], "cpu");
        assert_eq!(metadata["sample_count"].as_u64().unwrap(), 100);
    }

    // ========================================================================
    // Profile ID Validation Tests
    // ========================================================================

    fn is_valid_profile_id(profile_id: &str) -> bool {
        // Profile IDs should be 32 hex chars (128-bit) or valid UUIDs
        (profile_id.len() == 32 && profile_id.chars().all(|c| c.is_ascii_hexdigit()))
            || uuid::Uuid::parse_str(profile_id).is_ok()
    }

    #[test]
    fn test_valid_profile_id_hex() {
        assert!(is_valid_profile_id("0123456789abcdef0123456789abcdef"));
        assert!(is_valid_profile_id("ABCDEF0123456789ABCDEF0123456789"));
    }

    #[test]
    fn test_valid_profile_id_uuid() {
        let uuid = uuid::Uuid::new_v4().to_string();
        assert!(is_valid_profile_id(&uuid));
    }

    #[test]
    fn test_invalid_profile_id() {
        assert!(!is_valid_profile_id("short"));
        assert!(!is_valid_profile_id("0123456789ghijkl0123456789ghijkl")); // Invalid hex
    }

    // ========================================================================
    // Service Version Format Tests
    // ========================================================================

    fn is_valid_semver(version: &str) -> bool {
        // Basic semver check: x.y.z format where x, y, z start with digits
        let parts: Vec<&str> = version.split('.').collect();
        if parts.len() < 2 || parts.len() > 3 {
            return false;
        }
        // Each part must start with a digit (rejects "v1.0.0")
        // and can contain digits, hyphens, and alphanumerics (for prerelease like "1.0.0-beta")
        parts.iter().all(|p| {
            !p.is_empty()
                && p.chars()
                    .next()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false)
                && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        })
    }

    #[test]
    fn test_valid_semver() {
        assert!(is_valid_semver("1.0.0"));
        assert!(is_valid_semver("2.1.3"));
        assert!(is_valid_semver("1.0.0-beta"));
        assert!(is_valid_semver("1.0")); // Minor version only
    }

    #[test]
    fn test_invalid_semver() {
        assert!(!is_valid_semver("v1.0.0")); // Has 'v' prefix
        assert!(!is_valid_semver("1")); // Just major
    }
}
