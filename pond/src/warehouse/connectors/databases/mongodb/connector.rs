//! MongoDB Connector Implementation
//!
//! Cold tier connector for MongoDB with optional ClickHouse index acceleration.
//!
//! # Features
//!
//! - Cursor-based streaming for memory-efficient large collection processing
//! - Schema inference from document sampling
//! - Nested document flattening with configurable depth
//! - Optional ClickHouse index layer for accelerated queries
//! - Predicate pushdown to MongoDB

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bson::{doc, Document};
use futures::stream::StreamExt;
use mongodb::options::{ClientOptions, FindOptions};
use mongodb::{Client, Collection, Cursor};
use parking_lot::RwLock;
use tokio::sync::OnceCell;

use super::config::MongoDBConfig;
use super::schema::{infer_schema, BsonToArrowConverter, InferredSchema};
use crate::warehouse::connectors::{
    Connector, ConnectorError, ConnectorResult, FetchOptions, RecordBatchStream, TableInfo,
};
use crate::warehouse::types::{SourceType, TableSchema};

/// Cache entry for schema information.
struct SchemaCacheEntry {
    schema: InferredSchema,
    arrow_schema: Arc<Schema>,
    cached_at: Instant,
}

/// MongoDB data source connector.
///
/// Provides cold tier access to MongoDB collections with optional
/// ClickHouse index acceleration for improved query performance.
pub struct MongoDBConnector {
    config: MongoDBConfig,
    /// MongoDB client - lazily initialized
    client: OnceCell<Client>,
    /// Schema cache per collection
    schema_cache: RwLock<std::collections::HashMap<String, SchemaCacheEntry>>,
}

impl MongoDBConnector {
    /// Create a new MongoDB connector with the given configuration.
    pub fn new(config: MongoDBConfig) -> Self {
        Self {
            config,
            client: OnceCell::new(),
            schema_cache: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Create a connector with an existing MongoDB client (for testing).
    pub fn with_client(config: MongoDBConfig, client: Client) -> Self {
        let client_cell = OnceCell::new();
        let _ = client_cell.set(client);
        Self {
            config,
            client: client_cell,
            schema_cache: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Get or create the MongoDB client.
    async fn get_client(&self) -> ConnectorResult<&Client> {
        self.client
            .get_or_try_init(|| async {
                let mut options = ClientOptions::parse(&self.config.connection_string)
                    .await
                    .map_err(|e| {
                        ConnectorError::Config(format!("Invalid MongoDB connection string: {}", e))
                    })?;

                options.connect_timeout = Some(self.config.connect_timeout);
                options.server_selection_timeout = Some(self.config.server_selection_timeout);
                options.default_database = Some(self.config.database.clone());
                
                // Set read preference via selection criteria
                options.selection_criteria = Some(mongodb::options::SelectionCriteria::ReadPreference(
                    self.config.read_preference.to_mongodb_read_preference()
                ));

                Client::with_options(options).map_err(|e| {
                    ConnectorError::Network(format!("Failed to create MongoDB client: {}", e))
                })
            })
            .await
    }

    /// Get the database handle.
    async fn get_database(&self) -> ConnectorResult<mongodb::Database> {
        let client = self.get_client().await?;
        Ok(client.database(&self.config.database))
    }

    /// Get the collection handle.
    async fn get_collection(&self, name: &str) -> ConnectorResult<Collection<Document>> {
        let db = self.get_database().await?;
        Ok(db.collection(name))
    }

    /// Infer schema for a collection by sampling documents.
    async fn infer_collection_schema(
        &self,
        collection_name: &str,
    ) -> ConnectorResult<(InferredSchema, Arc<Schema>)> {
        // Check cache first
        let cache_ttl = std::time::Duration::from_secs(self.config.cache_ttl_secs);
        {
            let cache = self.schema_cache.read();
            if let Some(entry) = cache.get(collection_name) {
                if entry.cached_at.elapsed() < cache_ttl {
                    return Ok((entry.schema.clone(), entry.arrow_schema.clone()));
                }
            }
        }

        // Sample documents
        let collection = self.get_collection(collection_name).await?;
        let find_options = FindOptions::builder()
            .limit(Some(self.config.schema_sample_size as i64))
            .build();

        let mut cursor = collection.find(doc! {}).with_options(find_options).await.map_err(|e| {
            ConnectorError::Internal(format!(
                "Failed to sample documents from {}: {}",
                collection_name, e
            ))
        })?;

        let mut sample_docs = Vec::new();
        while let Some(result) = cursor.next().await {
            match result {
                Ok(doc) => sample_docs.push(doc),
                Err(e) => {
                    tracing::warn!(
                        collection = %collection_name,
                        error = %e,
                        "Error reading sample document"
                    );
                }
            }
        }

        if sample_docs.is_empty() {
            return Err(ConnectorError::Internal(format!(
                "Collection {} is empty, cannot infer schema",
                collection_name
            )));
        }

        let inferred = infer_schema(&sample_docs, self.config.max_nested_depth);
        let arrow_schema = Arc::new(inferred.to_arrow_schema());

        // Cache the result
        {
            let mut cache = self.schema_cache.write();
            cache.insert(
                collection_name.to_string(),
                SchemaCacheEntry {
                    schema: inferred.clone(),
                    arrow_schema: arrow_schema.clone(),
                    cached_at: Instant::now(),
                },
            );
        }

        Ok((inferred, arrow_schema))
    }

    /// Fetch documents with an optional filter and convert to RecordBatches.
    async fn fetch_with_filter(
        &self,
        collection_name: &str,
        filter: Document,
        projection: Option<Document>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let collection = self.get_collection(collection_name).await?;
        let (_inferred_schema, arrow_schema) =
            self.infer_collection_schema(collection_name).await?;

        let mut find_options = FindOptions::builder()
            .batch_size(Some(self.config.batch_size as u32))
            .build();

        if let Some(proj) = projection {
            find_options.projection = Some(proj);
        }

        let mut cursor = collection.find(filter).with_options(find_options).await.map_err(|e| {
            ConnectorError::Network(format!(
                "Failed to query collection {}: {}",
                collection_name, e
            ))
        })?;

        let converter = BsonToArrowConverter::new(arrow_schema.clone());
        let mut batches = Vec::new();
        let mut batch_docs = Vec::with_capacity(self.config.batch_size);
        let mut total_rows = 0u64;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(doc) => {
                    batch_docs.push(doc);
                    if batch_docs.len() >= self.config.batch_size {
                        let batch = converter.convert(&batch_docs)?;
                        total_rows += batch.num_rows() as u64;
                        batches.push(batch);
                        batch_docs.clear();
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        collection = %collection_name,
                        error = %e,
                        "Error reading document"
                    );
                }
            }
        }

        // Handle remaining documents
        if !batch_docs.is_empty() {
            let batch = converter.convert(&batch_docs)?;
            total_rows += batch.num_rows() as u64;
            batches.push(batch);
        }

        tracing::info!(
            collection = %collection_name,
            rows = total_rows,
            batches = batches.len(),
            "Fetched MongoDB collection data"
        );

        Ok(batches)
    }

    /// Fetch documents directly from MongoDB with filter pushdown.
    pub async fn fetch_direct(
        &self,
        collection_name: &str,
        filter: Document,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        self.fetch_with_filter(collection_name, filter, None).await
    }

    /// Create a streaming cursor for large collection fetches.
    async fn create_streaming_cursor(
        &self,
        collection_name: &str,
        filter: Document,
        batch_size: usize,
    ) -> ConnectorResult<(Cursor<Document>, BsonToArrowConverter)> {
        let collection = self.get_collection(collection_name).await?;
        let (_, arrow_schema) = self.infer_collection_schema(collection_name).await?;

        let find_options = FindOptions::builder()
            .batch_size(Some(batch_size as u32))
            .build();

        let cursor = collection.find(filter).with_options(find_options).await.map_err(|e| {
            ConnectorError::Network(format!(
                "Failed to create cursor for {}: {}",
                collection_name, e
            ))
        })?;

        let converter = BsonToArrowConverter::new(arrow_schema);
        Ok((cursor, converter))
    }

    /// Estimate the number of documents in a collection.
    async fn estimate_count(&self, collection_name: &str) -> ConnectorResult<Option<u64>> {
        let collection = self.get_collection(collection_name).await?;
        match collection.estimated_document_count().await {
            Ok(count) => Ok(Some(count)),
            Err(e) => {
                tracing::warn!(
                    collection = %collection_name,
                    error = %e,
                    "Failed to estimate document count"
                );
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl Connector for MongoDBConnector {
    fn source_type(&self) -> SourceType {
        SourceType::MongoDB
    }

    async fn list_tables(&self) -> ConnectorResult<Vec<TableInfo>> {
        let db = self.get_database().await?;
        
        let collection_names: Vec<String> = db
            .list_collection_names()
            .await
            .map_err(|e| {
                ConnectorError::Internal(format!("Failed to list collections: {}", e))
            })?;

        // Filter collections first
        let collections_to_process: Vec<String> = collection_names
            .into_iter()
            .filter(|name| {
                // Skip system collections
                if name.starts_with("system.") {
                    return false;
                }
                // Filter by configured collections if specified
                if !self.config.collections.is_empty()
                    && !self.config.collections.contains(name)
                {
                    return false;
                }
                true
            })
            .collect();

        // Parallelize schema inference with concurrency limit to avoid overwhelming MongoDB
        const MAX_CONCURRENT_INFERENCES: usize = 10;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_INFERENCES));
        
        let inference_futures: Vec<_> = collections_to_process
            .iter()
            .map(|name| {
                let name = name.clone();
                let semaphore = Arc::clone(&semaphore);
                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let schema_result = self.infer_collection_schema(&name).await;
                    let count_result = self.estimate_count(&name).await;
                    (name, schema_result, count_result)
                }
            })
            .collect();

        let results = futures::future::join_all(inference_futures).await;

        let mut tables = Vec::with_capacity(results.len());
        for (name, schema_result, count_result) in results {
            match (schema_result, count_result) {
                (Ok((inferred, _)), Ok(estimated_rows)) => {
                    let table_schema = inferred.to_table_schema();
                    
                    // MongoDB supports incremental by _id (ObjectId contains timestamp)
                    let supports_incremental = table_schema
                        .columns
                        .iter()
                        .any(|c| c.name == "_id");

                    tables.push(TableInfo {
                        name,
                        schema: table_schema,
                        supports_incremental,
                        incremental_key: Some("_id".to_string()),
                        estimated_rows,
                        primary_key_columns: vec!["_id".to_string()],
                    });
                }
                (Err(e), _) => {
                    tracing::warn!(
                        collection = %name,
                        error = %e,
                        "Failed to infer schema for collection, skipping"
                    );
                }
                (_, Err(e)) => {
                    tracing::warn!(
                        collection = %name,
                        error = %e,
                        "Failed to estimate count for collection, skipping"
                    );
                }
            }
        }

        tracing::debug!(
            database = %self.config.database,
            collection_count = tables.len(),
            "Listed MongoDB collections"
        );

        Ok(tables)
    }

    async fn get_schema(&self, table: &str) -> ConnectorResult<TableSchema> {
        let (inferred, _) = self.infer_collection_schema(table).await?;
        Ok(inferred.to_table_schema())
    }

    async fn fetch_table(
        &self,
        table: &str,
        incremental_key: Option<&str>,
        last_value: Option<&str>,
    ) -> ConnectorResult<Vec<RecordBatch>> {
        let mut filter = doc! {};

        // Add incremental filter if specified
        if let (Some(key), Some(value)) = (incremental_key, last_value) {
            // Parse the value to the appropriate BSON type for correct comparison
            let bson_value = parse_incremental_value(value);
            filter.insert(key, doc! { "$gt": bson_value });
        }

        self.fetch_with_filter(table, filter, None).await
    }

    fn fetch_table_stream<'a>(
        &'a self,
        table: &'a str,
        options: FetchOptions,
    ) -> Pin<Box<dyn futures::Future<Output = ConnectorResult<RecordBatchStream>> + Send + 'a>>
    {
        Box::pin(async move {
            let batch_size = options.batch_size.unwrap_or(self.config.batch_size);
            
            let mut filter = doc! {};
            if let (Some(key), Some(value)) = (&options.incremental_key, &options.last_value) {
                // Parse the value to the appropriate BSON type for correct comparison
                let bson_value = parse_incremental_value(value);
                filter.insert(key.as_str(), doc! { "$gt": bson_value });
            }

            let (cursor, converter) = self
                .create_streaming_cursor(table, filter, batch_size)
                .await?;

            let stream = create_record_batch_stream(cursor, converter, batch_size);
            
            Ok(Box::pin(stream) as RecordBatchStream)
        })
    }

    async fn validate_credentials(&self) -> ConnectorResult<()> {
        let client = self.get_client().await?;
        
        // Ping the database to validate connection
        client
            .database(&self.config.database)
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|e| {
                ConnectorError::Network(format!("MongoDB connection validation failed: {}", e))
            })?;

        tracing::debug!(
            database = %self.config.database,
            "MongoDB credentials validated successfully"
        );
        Ok(())
    }
    
    fn supports_cdc(&self) -> bool {
        true // MongoDB supports change streams (oplog-based change tracking)
    }
}

/// Create a stream of RecordBatches from a MongoDB cursor.
fn create_record_batch_stream(
    cursor: Cursor<Document>,
    converter: BsonToArrowConverter,
    batch_size: usize,
) -> impl futures::Stream<Item = ConnectorResult<RecordBatch>> {
    async_stream::stream! {
        let converter = Arc::new(converter);
        let mut cursor = cursor;
        let mut batch_docs = Vec::with_capacity(batch_size);

        while let Some(result) = cursor.next().await {
            match result {
                Ok(doc) => {
                    batch_docs.push(doc);
                    if batch_docs.len() >= batch_size {
                        match converter.convert(&batch_docs) {
                            Ok(batch) => yield Ok(batch),
                            Err(e) => yield Err(e),
                        }
                        batch_docs.clear();
                    }
                }
                Err(e) => {
                    yield Err(ConnectorError::Internal(format!(
                        "Error reading from cursor: {}", e
                    )));
                }
            }
        }

        // Yield remaining documents
        if !batch_docs.is_empty() {
            match converter.convert(&batch_docs) {
                Ok(batch) => yield Ok(batch),
                Err(e) => yield Err(e),
            }
        }
    }
}

/// Parse an incremental key value string into an appropriate BSON value.
///
/// MongoDB comparisons require type-appropriate values. This function attempts
/// to parse the string value as various types in order of specificity:
/// 1. ObjectId (if it looks like a 24-character hex string)
/// 2. Integer (i64)
/// 3. Float (f64)
/// 4. DateTime (ISO 8601 format)
/// 5. Boolean ("true"/"false")
/// 6. Falls back to string if none match
fn parse_incremental_value(value: &str) -> bson::Bson {
    // Try ObjectId first (24 hex characters)
    if value.len() == 24 && value.chars().all(|c| c.is_ascii_hexdigit()) {
        if let Ok(oid) = bson::oid::ObjectId::parse_str(value) {
            return bson::Bson::ObjectId(oid);
        }
    }
    
    // Try integer
    if let Ok(i) = value.parse::<i64>() {
        return bson::Bson::Int64(i);
    }
    
    // Try float (only if it has a decimal point to avoid confusing with integers)
    if value.contains('.') {
        if let Ok(f) = value.parse::<f64>() {
            return bson::Bson::Double(f);
        }
    }
    
    // Try ISO 8601 datetime
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
        return bson::Bson::DateTime(bson::DateTime::from_millis(dt.timestamp_millis()));
    }
    
    // Try boolean
    match value.to_lowercase().as_str() {
        "true" => return bson::Bson::Boolean(true),
        "false" => return bson::Bson::Boolean(false),
        _ => {}
    }
    
    // Fall back to string
    bson::Bson::String(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_creation() {
        let config = MongoDBConfig::new("mongodb://localhost:27017", "testdb");
        let connector = MongoDBConnector::new(config);
        assert_eq!(connector.source_type(), SourceType::MongoDB);
    }

    #[test]
    fn test_parse_incremental_value() {
        // ObjectId (24 hex characters)
        let oid_result = parse_incremental_value("507f1f77bcf86cd799439011");
        assert!(matches!(oid_result, bson::Bson::ObjectId(_)));
        
        // Integer
        let int_result = parse_incremental_value("12345");
        assert!(matches!(int_result, bson::Bson::Int64(12345)));
        
        // Negative integer
        let neg_int_result = parse_incremental_value("-999");
        assert!(matches!(neg_int_result, bson::Bson::Int64(-999)));
        
        // Float (must have decimal point)
        let float_result = parse_incremental_value("3.14159");
        assert!(matches!(float_result, bson::Bson::Double(_)));
        if let bson::Bson::Double(f) = float_result {
            assert!((f - 3.14159).abs() < 0.0001);
        }
        
        // Boolean true
        let true_result = parse_incremental_value("true");
        assert!(matches!(true_result, bson::Bson::Boolean(true)));
        
        // Boolean false
        let false_result = parse_incremental_value("FALSE");
        assert!(matches!(false_result, bson::Bson::Boolean(false)));
        
        // DateTime (ISO 8601)
        let dt_result = parse_incremental_value("2023-06-15T10:30:00Z");
        assert!(matches!(dt_result, bson::Bson::DateTime(_)));
        
        // Fallback to string
        let string_result = parse_incremental_value("some random text");
        assert!(matches!(string_result, bson::Bson::String(_)));
        
        // Short hex string (not a valid ObjectId - 22 chars)
        let short_hex = parse_incremental_value("507f1f77bcf86cd7994390");
        assert!(matches!(short_hex, bson::Bson::String(_)));
    }
}
