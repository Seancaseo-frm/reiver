//! File Format Connectors
//!
//! Connectors for various file formats.
//!
//! # Supported Formats
//!
//! - CSV (with schema inference)
//! - JSON/NDJSON (newline-delimited JSON)
//! - Excel (.xlsx, .xls)
//!
//! # Features
//!
//! All file connectors support:
//! - Local files, HTTP URLs, S3, and GCS sources
//! - Automatic schema inference
//! - ETag-based change detection for efficient re-sync
//! - Caching for repeated reads

pub mod csv;
pub mod json;
pub mod excel;

pub use csv::{CsvConnector, CsvConnectorConfig};
pub use json::{JsonConnector, JsonConnectorConfig};
pub use excel::{ExcelConnector, ExcelConnectorConfig};
