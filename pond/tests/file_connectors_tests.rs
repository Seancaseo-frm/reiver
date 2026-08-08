//! Integration tests for file connectors (JSON, Excel)
//!
//! Tests the JSON and Excel connectors with sample data.

use std::sync::Arc;
use arrow::array::{Float64Array, Int64Array, StringArray};
use reiver_pond::warehouse::connectors::files::{
    JsonConnector, JsonConnectorConfig, ExcelConnector, ExcelConnectorConfig,
};
use reiver_pond::warehouse::connectors::Connector;

mod json_connector_tests {
    use super::*;

    #[tokio::test]
    async fn test_ndjson_schema_inference() {
        let ndjson_data = br#"{"id":1,"name":"Alice","score":95.5}
{"id":2,"name":"Bob","score":87.3}
{"id":3,"name":"Carol","score":92.0}"#
            .to_vec();

        let config = JsonConnectorConfig::new("test.ndjson").with_ndjson(true);
        let connector = JsonConnector::with_data(config, ndjson_data);

        let tables = connector.list_tables().await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "test");
        assert_eq!(tables[0].schema.columns.len(), 3);

        // Verify column names exist
        let column_names: Vec<_> = tables[0].schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(column_names.contains(&"id"));
        assert!(column_names.contains(&"name"));
        assert!(column_names.contains(&"score"));
    }

    #[tokio::test]
    async fn test_ndjson_data_fetch() {
        let ndjson_data = br#"{"id":1,"value":100}
{"id":2,"value":200}
{"id":3,"value":300}"#
            .to_vec();

        let config = JsonConnectorConfig::new("data.ndjson").with_ndjson(true);
        let connector = JsonConnector::with_data(config, ndjson_data);

        let batches = connector.fetch_table("data", None, None).await.unwrap();
        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_standard_json_with_records_path() {
        let json_data = br#"{
            "meta": {"total": 2},
            "data": {
                "items": [
                    {"id": 1, "name": "Product A", "price": 19.99},
                    {"id": 2, "name": "Product B", "price": 29.99}
                ]
            }
        }"#
            .to_vec();

        let config = JsonConnectorConfig::new("api_response.json")
            .with_ndjson(false)
            .with_records_path("data.items");

        let connector = JsonConnector::with_data(config, json_data);

        let batches = connector.fetch_table("api_response", None, None).await.unwrap();
        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_json_with_nested_objects() {
        let ndjson_data = br#"{"id":1,"user":{"name":"Alice","email":"alice@example.com"}}
{"id":2,"user":{"name":"Bob","email":"bob@example.com"}}"#
            .to_vec();

        let config = JsonConnectorConfig::new("users.ndjson").with_ndjson(true);
        let connector = JsonConnector::with_data(config, ndjson_data);

        let batches = connector.fetch_table("users", None, None).await.unwrap();
        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_json_with_arrays() {
        let ndjson_data = br#"{"id":1,"tags":["rust","programming"]}
{"id":2,"tags":["data","analytics"]}"#
            .to_vec();

        let config = JsonConnectorConfig::new("articles.ndjson").with_ndjson(true);
        let connector = JsonConnector::with_data(config, ndjson_data);

        let batches = connector.fetch_table("articles", None, None).await.unwrap();
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn test_empty_json_array() {
        let json_data = br#"{"items": []}"#.to_vec();

        let config = JsonConnectorConfig::new("empty.json")
            .with_ndjson(false)
            .with_records_path("items");

        let connector = JsonConnector::with_data(config, json_data);

        let result = connector.fetch_table("empty", None, None).await;
        // Empty arrays at the records path should produce either an empty
        // batch or a clear error, but must not panic.
        if let Ok(batches) = &result {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 0, "Empty JSON array should produce zero rows");
        }
    }

    #[tokio::test]
    async fn test_json_source_type() {
        let config = JsonConnectorConfig::new("test.json");
        let connector = JsonConnector::new(config);
        assert_eq!(
            connector.source_type(),
            reiver_pond::warehouse::types::SourceType::Json
        );
    }

    #[tokio::test]
    async fn test_json_table_name_extraction() {
        // Test various file extensions
        let config1 = JsonConnectorConfig::new("/path/to/data.json");
        assert_eq!(config1.get_table_name(), "data");

        let config2 = JsonConnectorConfig::new("/path/to/events.ndjson");
        assert_eq!(config2.get_table_name(), "events");

        let config3 = JsonConnectorConfig::new("/path/to/logs.jsonl");
        assert_eq!(config3.get_table_name(), "logs");

        // Test with explicit table name
        let config4 = JsonConnectorConfig::new("/path/to/data.json").with_table_name("custom");
        assert_eq!(config4.get_table_name(), "custom");
    }
}

mod excel_connector_tests {
    use super::*;

    #[test]
    fn test_excel_config_builders() {
        // Test sheet selection by index
        let config = ExcelConnectorConfig::new("/path/to/file.xlsx")
            .with_sheet_index(0)
            .with_header(true);
        assert!(config.options.has_header);

        // Test sheet selection by name
        let config = ExcelConnectorConfig::new("/path/to/file.xlsx")
            .with_sheet_name("Sales Data");
        if let reiver_pond::warehouse::connectors::file::SheetSelector::Name(name) = config.options.sheet {
            assert_eq!(name, "Sales Data");
        } else {
            panic!("Expected SheetSelector::Name");
        }

        // Test range selection
        let config = ExcelConnectorConfig::new("/path/to/file.xlsx")
            .with_range("A1:D100");
        assert_eq!(config.options.range, Some("A1:D100".to_string()));

        // Test skip rows
        let config = ExcelConnectorConfig::new("/path/to/file.xlsx")
            .with_skip_rows(2);
        assert_eq!(config.options.skip_rows, 2);
    }

    #[test]
    fn test_excel_file_type_detection() {
        // xlsx detection
        let config = ExcelConnectorConfig::new("/path/to/file.xlsx");
        assert!(config.is_xlsx());

        let config = ExcelConnectorConfig::new("/path/to/file.XLSX");
        assert!(config.is_xlsx());

        // xls detection
        let config = ExcelConnectorConfig::new("/path/to/file.xls");
        assert!(!config.is_xlsx());

        let config = ExcelConnectorConfig::new("/path/to/file.XLS");
        assert!(!config.is_xlsx());
    }

    #[test]
    fn test_excel_table_name_extraction() {
        let config = ExcelConnectorConfig::new("/path/to/report.xlsx");
        assert_eq!(config.get_table_name(), "report");

        let config = ExcelConnectorConfig::new("/path/to/data.xls");
        assert_eq!(config.get_table_name(), "data");

        let config = ExcelConnectorConfig::new("/path/to/file.xlsx")
            .with_table_name("custom_table");
        assert_eq!(config.get_table_name(), "custom_table");
    }

    #[test]
    fn test_excel_source_type() {
        let config = ExcelConnectorConfig::new("test.xlsx");
        let connector = ExcelConnector::new(config);
        assert_eq!(
            connector.source_type(),
            reiver_pond::warehouse::types::SourceType::Excel
        );
    }

}

mod storage_tests {
    use reiver_pond::warehouse::connectors::file::FileStorage;

    #[test]
    fn test_local_storage_creation() {
        let storage = FileStorage::local("/tmp/data");
        assert!(matches!(storage, FileStorage::Local(_)));
    }

    #[test]
    fn test_s3_storage_creation() {
        let storage = FileStorage::s3("my-bucket", "data/prefix");
        if let FileStorage::S3 { bucket, prefix, .. } = storage {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(prefix, "data/prefix");
        } else {
            panic!("Expected S3 storage");
        }
    }

    #[test]
    fn test_gcs_storage_creation() {
        let storage = FileStorage::gcs("my-bucket", "data/prefix");
        if let FileStorage::Gcs { bucket, prefix, .. } = storage {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(prefix, "data/prefix");
        } else {
            panic!("Expected GCS storage");
        }
    }

    #[test]
    fn test_http_storage_creation() {
        let storage = FileStorage::http("https://example.com/data");
        if let FileStorage::Http { base_url, headers } = storage {
            assert_eq!(base_url, "https://example.com/data");
            assert!(headers.is_empty());
        } else {
            panic!("Expected HTTP storage");
        }
    }
}

// ============================================================================
// File Connector Error Handling / Edge Case Tests
// ============================================================================

mod json_connector_edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_empty_ndjson_file() {
        let empty_data = Vec::new();
        let config = JsonConnectorConfig::new("empty.ndjson").with_ndjson(true);
        let connector = JsonConnector::with_data(config, empty_data);

        // Empty data should not panic; either empty result or error is acceptable
        let result = connector.list_tables().await;
        // Just verify no panic
        let _ = result;
    }

    #[tokio::test]
    async fn test_malformed_ndjson_lines() {
        // Mix of valid JSON and garbage
        let data = b"{ \"id\": 1, \"name\": \"Alice\" }\nNOT VALID JSON\n{ \"id\": 2, \"name\": \"Bob\" }\n".to_vec();
        let config = JsonConnectorConfig::new("malformed.ndjson").with_ndjson(true);
        let connector = JsonConnector::with_data(config, data);

        // Should either skip bad lines (partial results) or return a clear error
        let result = connector.fetch_table("malformed", None, None).await;
        match result {
            Ok(batches) => {
                // If it succeeds, it should have at least the valid rows
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert!(total_rows >= 1, "Should have at least one valid row");
            }
            Err(e) => {
                // If it errors, the error message should be clear
                let msg = e.to_string();
                assert!(
                    msg.contains("JSON") || msg.contains("parse") || msg.contains("invalid"),
                    "Error should mention JSON/parse issue: {}", msg
                );
            }
        }
    }

    #[tokio::test]
    async fn test_json_invalid_records_path() {
        let json_data = br#"{"data": {"items": [{"id": 1}]}}"#.to_vec();
        let config = JsonConnectorConfig::new("api.json")
            .with_ndjson(false)
            .with_records_path("nonexistent.path");
        let connector = JsonConnector::with_data(config, json_data);

        let result = connector.fetch_table("api", None, None).await;
        // Should return an error when path doesn't exist
        assert!(result.is_err(), "Invalid records_path should produce an error");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            !err_msg.is_empty(),
            "Error message should be non-empty"
        );
    }

    #[tokio::test]
    async fn test_json_empty_array_at_records_path() {
        let json_data = br#"{"data": []}"#.to_vec();
        let config = JsonConnectorConfig::new("empty_arr.json")
            .with_ndjson(false)
            .with_records_path("data");
        let connector = JsonConnector::with_data(config, json_data);

        let result = connector.fetch_table("empty_arr", None, None).await;
        // Empty array should either return empty batches or an error, not panic
        match result {
            Ok(batches) => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 0, "Empty array should produce 0 rows");
            }
            Err(_) => {
                // An error for empty data is also acceptable
            }
        }
    }

    #[tokio::test]
    async fn test_extremely_wide_ndjson() {
        use reiver_pond::warehouse::connectors::ConnectorError;
        // Build a single row with 500+ columns
        let mut obj = serde_json::Map::new();
        for i in 0..500 {
            obj.insert(format!("col_{}", i), serde_json::json!(i));
        }
        let line = serde_json::to_string(&obj).unwrap();
        let data = line.into_bytes();

        let config = JsonConnectorConfig::new("wide.ndjson").with_ndjson(true);
        let connector = JsonConnector::with_data(config, data);

        // Should handle 500+ columns without panic
        let tables: Result<_, ConnectorError> = connector.list_tables().await;
        match tables {
            Ok(t) => {
                assert!(!t.is_empty());
                assert!(
                    t[0].schema.columns.len() >= 500,
                    "Should infer all 500+ columns, got {}",
                    t[0].schema.columns.len()
                );
            }
            Err(e) => {
                // Clear error is also acceptable
                assert!(!e.to_string().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn test_ndjson_inconsistent_schemas() {
        use reiver_pond::warehouse::connectors::ConnectorError;
        // Row 1 has {a, b}, row 2 has {a, c} -- schema union behavior
        let data = b"{\"a\":1,\"b\":2}\n{\"a\":3,\"c\":4}\n".to_vec();
        let config = JsonConnectorConfig::new("inconsistent.ndjson").with_ndjson(true);
        let connector = JsonConnector::with_data(config, data);

        let result: Result<_, ConnectorError> = connector.list_tables().await;
        match result {
            Ok(tables) => {
                assert!(!tables.is_empty());
                let col_names: Vec<_> = tables[0].schema.columns.iter().map(|c| c.name.as_str()).collect();
                // Should at least have column "a" which is in both rows
                assert!(col_names.contains(&"a"), "Common column 'a' should be present");
            }
            Err(_) => {
                // An error for inconsistent schemas is acceptable behavior
            }
        }
    }
}
