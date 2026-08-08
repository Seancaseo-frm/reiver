//! Parquet File Size Benchmark (64 MB vs 128 MB) -- Multi-Volume
//!
//! Measures real end-to-end ClickHouse query performance against identical data
//! split into different Parquet file sizes, uploaded to MinIO. Uses real Pond
//! skip indexes (FST-based `HierarchicalSkipIndex`) for file pruning.
//!
//! Runs at multiple data volumes (500 MB, 2 GB, 8 GB) to show how the
//! performance delta scales with data size.
//!
//! The schema covers all major data types (string, int, float, boolean,
//! nullable, high-cardinality, large text) and data shapes (skewed,
//! null-heavy, uniform, clustered).
//!
//! ## Prerequisites
//!
//! ```bash
//! docker-compose up -d   # ClickHouse + MinIO
//! ```
//!
//! ## Usage
//!
//! ```bash
//! cargo bench --bench file_size_benchmark -- --nocapture
//! ```

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Builder, Int64Array, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use reiver_pond::warehouse::indexes::skip_index::{
    FileSkipIndex, HierarchicalSkipIndex, SkipPredicates,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

// =============================================================================
// Configuration
// =============================================================================

const MINIO_ENDPOINT: &str = "http://localhost:19000";
const BUCKET_NAME: &str = "warehouse";

/// ClickHouse HTTP endpoint on the host.
const CLICKHOUSE_URL: &str = "http://localhost:8123";

/// MinIO URL as seen from inside the ClickHouse Docker container.
const MINIO_URL_FROM_CLICKHOUSE: &str = "http://minio:9000";

/// Upload parallelism.
const UPLOAD_PARALLELISM: usize = 10;

/// Number of timing iterations per query (after warmup).
const QUERY_ITERATIONS: usize = 3;

/// Data volumes to benchmark. Each volume runs the full query suite.
/// Variants are cleaned up between volumes to bound MinIO usage.
const DATA_VOLUMES: &[(usize, &str)] = &[
    (512 * 1024 * 1024, "500 MB"),
    (2 * 1024 * 1024 * 1024, "2 GB"),
    (8 * 1024 * 1024 * 1024, "8 GB"),
];

/// The two file sizes we are comparing.
const FILE_SIZES: [(usize, &str); 2] = [
    (128 * 1024 * 1024, "128mb"),
    (64 * 1024 * 1024, "64mb"),
];

/// S3 key prefix inside the bucket.
const KEY_PREFIX: &str = "bench_filesize";

/// Partition key used in the hierarchical index (single partition).
const PARTITION_KEY: &str = "data";

// =============================================================================
// Schema: 14 columns covering all major data types and shapes
// =============================================================================

fn bench_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("is_premium", DataType::Boolean, false),
        Field::new("event_ts", DataType::Int64, false),
        Field::new("score", DataType::Int32, true),
        Field::new("email", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("tags", DataType::Utf8, false),
        Field::new("country_code", DataType::Utf8, false),
    ]))
}

// =============================================================================
// Data generation constants
// =============================================================================

const STATUSES: [&str; 3] = ["active", "inactive", "pending"];
const REGIONS: [&str; 8] = [
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-south-1",
    "ap-northeast-1",
    "sa-east-1",
    "af-south-1",
];
const CATEGORIES: [&str; 10] = [
    "electronics",
    "clothing",
    "books",
    "food",
    "sports",
    "home",
    "toys",
    "health",
    "automotive",
    "garden",
];
const COUNTRY_CODES: [&str; 5] = ["US", "GB", "DE", "JP", "BR"];
const TAGS: [&str; 50] = [
    "fitness", "travel", "cooking", "tech", "music", "art", "gaming", "reading",
    "photography", "gardening", "yoga", "running", "cycling", "swimming", "hiking",
    "camping", "fishing", "painting", "dancing", "singing", "writing", "coding",
    "design", "marketing", "finance", "health", "wellness", "beauty", "fashion",
    "food", "wine", "coffee", "tea", "pets", "dogs", "cats", "birds", "nature",
    "science", "history", "math", "physics", "biology", "chemistry", "astronomy",
    "philosophy", "psychology", "sociology", "economics", "politics",
];

const EVENT_TS_BASE: i64 = 1_704_067_200;
const WEEK_SECS: i64 = 7 * 24 * 3600;

const DESCRIPTIONS: [&str; 10] = [
    "Premium customer account with extended support plan and dedicated account manager for enterprise needs",
    "Standard subscription tier providing access to core features with monthly billing cycle enabled",
    "Trial account created during promotional campaign with limited access to advanced analytics tools",
    "Enterprise agreement signed for multi-year commitment including custom integrations and SLA guarantees",
    "Small business package optimized for teams under fifty members with collaborative workspace access",
    "Developer sandbox environment used for testing API integrations and building custom workflows",
    "Educational institution license with special pricing and unlimited student seat allocations",
    "Non-profit organization account with discounted rates and enhanced reporting capabilities enabled",
    "Government sector deployment with compliance certifications and data residency requirements met",
    "Startup accelerator program participant with credits and mentorship support for rapid scaling",
];

// =============================================================================
// Data generation
// =============================================================================

struct GeneratedFile {
    data: Bytes,
    key: String,
    column_values: HashMap<String, Vec<String>>,
    rows: usize,
}

fn rows_for_target_bytes(target_file_bytes: usize) -> usize {
    let approx_row_bytes = 250usize;
    target_file_bytes / approx_row_bytes
}

fn simple_hash(seed: usize) -> usize {
    let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    x ^= x >> 16;
    x = x.wrapping_mul(0x45d9f3b);
    x ^= x >> 16;
    x
}

fn generate_parquet_file(
    file_idx: usize,
    rows_per_file: usize,
    global_row_offset: usize,
    prefix: &str,
) -> Result<GeneratedFile> {
    let schema = bench_schema();

    let ids: Vec<i64> = (0..rows_per_file)
        .map(|r| (global_row_offset + r) as i64)
        .collect();

    let file_status = STATUSES[file_idx % STATUSES.len()];
    let status_vals: Vec<&str> = vec![file_status; rows_per_file];

    let primary_region = REGIONS[file_idx % REGIONS.len()];
    let region_vals: Vec<&str> = vec![primary_region; rows_per_file];

    let customer_base = file_idx * rows_per_file;
    let customer_ids: Vec<String> = (0..rows_per_file)
        .map(|i| format!("cust_{:010}", (customer_base + i) % 10_000_000))
        .collect();

    let amount_base = ((file_idx % 10) * 1000) as i64;
    let amounts: Vec<i64> = (0..rows_per_file)
        .map(|i| amount_base + (i as i64 % 1000))
        .collect();

    let prices: Vec<f64> = amounts.iter().map(|a| *a as f64 * 1.07).collect();

    let file_category = CATEGORIES[file_idx % CATEGORIES.len()];
    let category_vals: Vec<&str> = vec![file_category; rows_per_file];

    let file_is_premium = file_idx % 5 == 0;
    let is_premium: Vec<bool> = vec![file_is_premium; rows_per_file];

    let ts_base = EVENT_TS_BASE + (file_idx as i64) * WEEK_SECS;
    let event_ts: Vec<i64> = (0..rows_per_file)
        .map(|i| ts_base + (i as i64 % WEEK_SECS))
        .collect();

    let score_band_start = ((file_idx % 10) * 10) as i32;
    let mut score_builder = Int32Builder::with_capacity(rows_per_file);
    let mut score_fst_values: Vec<String> = Vec::new();
    for i in 0..rows_per_file {
        if simple_hash(global_row_offset + i) % 100 < 30 {
            score_builder.append_null();
        } else {
            let val = score_band_start + (i as i32 % 10).min(100 - score_band_start);
            score_builder.append_value(val);
            score_fst_values.push(format!("{:04}", val));
        }
    }
    let score_array = score_builder.finish();

    let emails: Vec<String> = (0..rows_per_file)
        .map(|i| format!("user_{}_{:06}@example.com", file_idx, i))
        .collect();

    let mut desc_builder = StringBuilder::with_capacity(rows_per_file, rows_per_file * 80);
    for i in 0..rows_per_file {
        if simple_hash(global_row_offset + i + 7777) % 100 < 20 {
            desc_builder.append_null();
        } else {
            desc_builder.append_value(DESCRIPTIONS[(file_idx + i) % DESCRIPTIONS.len()]);
        }
    }
    let desc_array = desc_builder.finish();

    let tag_a = TAGS[file_idx % TAGS.len()];
    let tag_b = TAGS[(file_idx * 7 + 3) % TAGS.len()];
    let tag_c = TAGS[(file_idx * 13 + 11) % TAGS.len()];
    let tags_str = format!("{},{},{}", tag_a, tag_b, tag_c);
    let tags_vals: Vec<&str> = vec![tags_str.as_str(); rows_per_file];

    let file_country = if file_idx % 10 < 8 {
        "US"
    } else if file_idx % 10 == 8 {
        COUNTRY_CODES[(file_idx / 10) % 4 + 1]
    } else {
        COUNTRY_CODES[(file_idx / 10 + 2) % 4 + 1]
    };
    let country_vals: Vec<&str> = vec![file_country; rows_per_file];

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(status_vals)) as ArrayRef,
            Arc::new(StringArray::from(region_vals)) as ArrayRef,
            Arc::new(StringArray::from(customer_ids.clone())) as ArrayRef,
            Arc::new(Int64Array::from(amounts)) as ArrayRef,
            Arc::new(Float64Array::from(prices)) as ArrayRef,
            Arc::new(StringArray::from(category_vals)) as ArrayRef,
            Arc::new(BooleanArray::from(is_premium)) as ArrayRef,
            Arc::new(Int64Array::from(event_ts.clone())) as ArrayRef,
            Arc::new(score_array) as ArrayRef,
            Arc::new(StringArray::from(emails.clone())) as ArrayRef,
            Arc::new(desc_array) as ArrayRef,
            Arc::new(StringArray::from(tags_vals)) as ArrayRef,
            Arc::new(StringArray::from(country_vals)) as ArrayRef,
        ],
    )?;

    let mut buffer = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    let key = format!("{}/file_{:04}.parquet", prefix, file_idx);

    let mut column_values: HashMap<String, Vec<String>> = HashMap::new();
    column_values.insert("status".to_string(), vec![file_status.to_string()]);
    column_values.insert("region".to_string(), vec![primary_region.to_string()]);
    column_values.insert("category".to_string(), vec![file_category.to_string()]);
    column_values.insert("country_code".to_string(), vec![file_country.to_string()]);
    column_values.insert(
        "tags".to_string(),
        vec![tags_str.clone(), tag_a.to_string(), tag_b.to_string(), tag_c.to_string()],
    );
    column_values.insert(
        "is_premium".to_string(),
        vec![if file_is_premium { "true".to_string() } else { "false".to_string() }],
    );

    let mut unique_customers: Vec<String> = customer_ids;
    unique_customers.sort();
    unique_customers.dedup();
    column_values.insert("customer_id".to_string(), unique_customers);

    let amt_min = amount_base;
    let amt_max = amount_base + 999;
    let amount_strings: Vec<String> = (amt_min..=amt_max)
        .map(|a| format!("{:010}", a))
        .collect();
    column_values.insert("amount".to_string(), amount_strings);

    let ts_min = ts_base;
    let ts_max = ts_base + WEEK_SECS - 1;
    column_values.insert(
        "event_ts".to_string(),
        vec![format!("{:020}", ts_min), format!("{:020}", ts_max)],
    );

    score_fst_values.sort();
    score_fst_values.dedup();
    column_values.insert("score".to_string(), score_fst_values);

    let mut unique_emails: Vec<String> = emails;
    unique_emails.sort();
    unique_emails.dedup();
    column_values.insert("email".to_string(), unique_emails);

    Ok(GeneratedFile {
        data: Bytes::from(buffer),
        key,
        column_values,
        rows: rows_per_file,
    })
}

// =============================================================================
// S3 (MinIO) helpers
// =============================================================================

async fn create_s3_client() -> Result<S3Client> {
    let credentials = aws_sdk_s3::config::Credentials::new(
        "minioadmin", "minioadmin", None, None, "static",
    );
    let s3_config = aws_sdk_s3::config::Builder::new()
        .credentials_provider(credentials)
        .endpoint_url(MINIO_ENDPOINT)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .force_path_style(true)
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .build();
    Ok(S3Client::from_conf(s3_config))
}

async fn ensure_bucket(client: &S3Client) -> Result<()> {
    match client.head_bucket().bucket(BUCKET_NAME).send().await {
        Ok(_) => return Ok(()),
        Err(_) => {}
    }
    client.create_bucket().bucket(BUCKET_NAME).send().await.context("create MinIO bucket")?;
    Ok(())
}

// =============================================================================
// Data upload + index building
// =============================================================================

struct VariantInfo {
    label: &'static str,
    prefix: String,
    file_count: usize,
    rows_per_file: usize,
    total_bytes: u64,
    index: HierarchicalSkipIndex,
}

/// Lightweight metadata retained after cleanup (index dropped).
struct VariantMeta {
    label: String,
    file_count: usize,
    rows_per_file: usize,
    total_bytes: u64,
}

impl VariantInfo {
    fn meta(&self) -> VariantMeta {
        VariantMeta {
            label: self.label.to_string(),
            file_count: self.file_count,
            rows_per_file: self.rows_per_file,
            total_bytes: self.total_bytes,
        }
    }
}

async fn generate_upload_and_index(
    client: &S3Client,
    target_file_bytes: usize,
    label: &'static str,
    total_data_bytes: usize,
    volume_label: &str,
) -> Result<VariantInfo> {
    let rows_per_file = rows_for_target_bytes(target_file_bytes);
    let file_count = total_data_bytes / target_file_bytes;
    let prefix = format!("{}/{}/{}", KEY_PREFIX, volume_label.replace(' ', "_"), label);

    println!(
        "    {} variant: {} files x ~{} rows ...",
        label, file_count, rows_per_file
    );

    let gen_start = Instant::now();
    let mut generated_files = Vec::with_capacity(file_count);
    for i in 0..file_count {
        let gf = generate_parquet_file(i, rows_per_file, i * rows_per_file, &prefix)?;
        generated_files.push(gf);
    }
    println!("      Generated in {:?}", gen_start.elapsed());

    let upload_start = Instant::now();
    let sem = Arc::new(tokio::sync::Semaphore::new(UPLOAD_PARALLELISM));
    let mut join_set = JoinSet::new();

    for gf in &generated_files {
        let client = client.clone();
        let key = gf.key.clone();
        let data = gf.data.clone();
        let sem = sem.clone();
        join_set.spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let len = data.len() as u64;
            client.put_object().bucket(BUCKET_NAME).key(&key)
                .body(ByteStream::from(data)).send().await
                .context("upload to MinIO")?;
            Ok::<_, anyhow::Error>(len)
        });
    }

    let mut total_bytes = 0u64;
    while let Some(res) = join_set.join_next().await {
        total_bytes += res??;
    }
    println!(
        "      Uploaded in {:?} ({} MB compressed)",
        upload_start.elapsed(),
        total_bytes / (1024 * 1024)
    );

    let idx_start = Instant::now();
    let mut index = HierarchicalSkipIndex::new();
    for gf in &generated_files {
        let file_index = FileSkipIndex::build(&gf.key, gf.column_values.clone())
            .map_err(|e| anyhow::anyhow!("skip index build: {}", e))?;
        index.add_file(PARTITION_KEY, file_index, gf.rows as u64)
            .map_err(|e| anyhow::anyhow!("skip index add_file: {}", e))?;
    }
    println!("      Index built in {:?}", idx_start.elapsed());

    Ok(VariantInfo {
        label,
        prefix,
        file_count,
        rows_per_file,
        total_bytes,
        index,
    })
}

// =============================================================================
// Cleanup
// =============================================================================

async fn cleanup_variant(client: &S3Client, prefix: &str, file_count: usize) -> Result<()> {
    let sem = Arc::new(tokio::sync::Semaphore::new(UPLOAD_PARALLELISM));
    let mut join_set = JoinSet::new();

    for i in 0..file_count {
        let client = client.clone();
        let key = format!("{}/file_{:04}.parquet", prefix, i);
        let sem = sem.clone();
        join_set.spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            client.delete_object().bucket(BUCKET_NAME).key(&key).send().await?;
            Ok::<_, anyhow::Error>(())
        });
    }

    while let Some(res) = join_set.join_next().await {
        res??;
    }
    Ok(())
}

// =============================================================================
// ClickHouse query runner
// =============================================================================

async fn clickhouse_query(http: &reqwest::Client, sql: &str) -> Result<(Duration, String)> {
    let start = Instant::now();
    let resp = http.post(CLICKHOUSE_URL).body(sql.to_string()).send().await
        .context("ClickHouse HTTP request")?;
    let status = resp.status();
    let body = resp.text().await.context("read ClickHouse response")?;
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "ClickHouse returned {}: {}",
            status,
            body.chars().take(300).collect::<String>()
        ));
    }
    Ok((start.elapsed(), body))
}

async fn run_timed_query(http: &reqwest::Client, sql: &str) -> Result<(Duration, String)> {
    let _ = clickhouse_query(http, sql).await?;
    let mut durations = Vec::with_capacity(QUERY_ITERATIONS);
    let mut last_body = String::new();
    for _ in 0..QUERY_ITERATIONS {
        let (d, body) = clickhouse_query(http, sql).await?;
        durations.push(d);
        last_body = body;
    }
    durations.sort();
    let median = durations[durations.len() / 2];
    Ok((median, last_body))
}

// =============================================================================
// Benchmark queries
// =============================================================================

struct BenchQuery {
    name: &'static str,
    build_predicates: fn() -> SkipPredicates,
    where_clause: &'static str,
    suffix: &'static str,
    select_override: Option<&'static str>,
}

fn bench_queries() -> Vec<BenchQuery> {
    vec![
        BenchQuery {
            name: "Full scan (count)",
            build_predicates: || SkipPredicates::new(),
            where_clause: "", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Eq (status)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_equals("status", "active"); p },
            where_clause: "WHERE status = 'active'", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Eq (region)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_equals("region", "us-east-1"); p },
            where_clause: "WHERE region = 'us-east-1'", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Combined (stat+reg)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_equals("status", "active"); p.add_equals("region", "us-east-1"); p },
            where_clause: "WHERE status = 'active' AND region = 'us-east-1'", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Eq (category)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_equals("category", "books"); p },
            where_clause: "WHERE category = 'books'", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Substr (customer_id)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_substring("customer_id", "0001234"); p },
            where_clause: "WHERE customer_id LIKE '%0001234%'", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Aggregation",
            build_predicates: || SkipPredicates::new(),
            where_clause: "", suffix: "GROUP BY region FORMAT TabSeparated",
            select_override: Some("region, count(*), avg(amount)"),
        },
        BenchQuery {
            name: "Bool (is_premium)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_equals("is_premium", "true"); p },
            where_clause: "WHERE is_premium = true", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Timestamp range",
            build_predicates: || SkipPredicates::new(),
            where_clause: "WHERE event_ts BETWEEN 1705276800 AND 1706486400", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Nullable eq (score)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_equals("score", "0050"); p },
            where_clause: "WHERE score = 50", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "NULL check (score)",
            build_predicates: || SkipPredicates::new(),
            where_clause: "WHERE score IS NULL", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Hi-card eq (email)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_equals("email", "user_3_000042@example.com"); p },
            where_clause: "WHERE email = 'user_3_000042@example.com'", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Substr (tags)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_substring("tags", "fitness"); p },
            where_clause: "WHERE tags LIKE '%fitness%'", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Skewed rare (JP)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_equals("country_code", "JP"); p },
            where_clause: "WHERE country_code = 'JP'", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Skewed common (US)",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_equals("country_code", "US"); p },
            where_clause: "WHERE country_code = 'US'", suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Multi-type combined",
            build_predicates: || { let mut p = SkipPredicates::new(); p.add_equals("status", "active"); p.add_equals("country_code", "US"); p.add_equals("is_premium", "true"); p },
            where_clause: "WHERE status = 'active' AND country_code = 'US' AND is_premium = true",
            suffix: "", select_override: None,
        },
        BenchQuery {
            name: "Wide aggregation",
            build_predicates: || SkipPredicates::new(),
            where_clause: "", suffix: "GROUP BY region, country_code, status FORMAT TabSeparated",
            select_override: Some("region, country_code, status, count(*), avg(amount), avg(score)"),
        },
    ]
}

// =============================================================================
// Index-based file pruning
// =============================================================================

fn prune_files<'a>(
    index: &'a HierarchicalSkipIndex,
    predicates: &SkipPredicates,
    total_files: usize,
) -> (Vec<&'a str>, usize) {
    if predicates.equality.is_empty()
        && predicates.substring.is_empty()
        && predicates.prefix.is_empty()
        && predicates.in_lists.is_empty()
        && predicates.ranges.is_empty()
    {
        let all = index.all_file_paths();
        return (all, 0);
    }

    let mut matching = index.filter_with_partition_hint(&predicates.equality, None);

    if !predicates.substring.is_empty() {
        let flat_subs: HashMap<String, String> = predicates.substring.iter()
            .filter_map(|(col, subs)| subs.first().map(|s| (col.clone(), s.clone())))
            .collect();
        let sub_files: std::collections::HashSet<&str> = index
            .filter_substring_with_partition_hint(&flat_subs, None)
            .into_iter().collect();
        matching.retain(|f| sub_files.contains(f));

        for (col, subs) in &predicates.substring {
            for sub in subs.iter().skip(1) {
                let single: HashMap<String, String> = [(col.clone(), sub.clone())].into_iter().collect();
                let extra: std::collections::HashSet<&str> = index
                    .filter_substring_with_partition_hint(&single, None)
                    .into_iter().collect();
                matching.retain(|f| extra.contains(f));
            }
        }
    }

    let pruned = total_files - matching.len();
    (matching, pruned)
}

fn build_s3_url(files: &[&str], prefix: &str) -> String {
    let base = format!("{}/{}", MINIO_URL_FROM_CLICKHOUSE, BUCKET_NAME);
    match files.len() {
        0 => format!("{}/{}/__nonexistent__.parquet", base, KEY_PREFIX),
        1 => format!("{}/{}", base, files[0]),
        _ => {
            let file_names: Vec<&str> = files.iter()
                .filter_map(|f| f.strip_prefix(prefix).and_then(|s| s.strip_prefix('/')))
                .collect();
            if file_names.is_empty() {
                return format!("{}/{}/*.parquet", base, prefix);
            }
            format!("{}/{}/{{{}}}", base, prefix, file_names.join(","))
        }
    }
}

fn build_wildcard_s3_url(prefix: &str) -> String {
    format!("{}/{}/{}/*.parquet", MINIO_URL_FROM_CLICKHOUSE, BUCKET_NAME, prefix)
}

fn build_sql(s3_url: &str, select: &str, where_clause: &str, suffix: &str) -> String {
    let mut sql = format!(
        "SELECT {} FROM s3('{}', 'minioadmin', 'minioadmin', 'Parquet')",
        select, s3_url
    );
    if !where_clause.is_empty() { sql.push(' '); sql.push_str(where_clause); }
    if !suffix.is_empty() { sql.push(' '); sql.push_str(suffix); }
    sql
}

// =============================================================================
// Result types
// =============================================================================

struct QueryTiming {
    name: String,
    results: Vec<VariantQueryResult>,
}

struct VariantQueryResult {
    label: String,
    duration: Duration,
    files_read: usize,
}

/// Per-volume collected results (after cleanup, index is dropped).
struct VolumeResult {
    volume_label: String,
    variant_metas: Vec<VariantMeta>,
    timings: Vec<QueryTiming>,
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    println!();
    println!("{}", "=".repeat(78));
    println!("     PARQUET FILE SIZE BENCHMARK (64 MB vs 128 MB)");
    println!("     Multi-volume scaling: {}", DATA_VOLUMES.iter().map(|(_, l)| *l).collect::<Vec<_>>().join(", "));
    println!("     14-column schema, real Pond HierarchicalSkipIndex pruning");
    println!("{}", "=".repeat(78));
    println!();

    let s3 = create_s3_client().await?;
    match s3.list_buckets().send().await {
        Ok(_) => println!("[ok] Connected to MinIO at {}", MINIO_ENDPOINT),
        Err(e) => {
            eprintln!("[error] Cannot reach MinIO: {}", e);
            eprintln!("        Run: docker-compose up -d");
            return Err(e.into());
        }
    }
    ensure_bucket(&s3).await?;

    let http = reqwest::Client::new();
    let (_, ch_version) = clickhouse_query(&http, "SELECT version()").await?;
    println!("[ok] Connected to ClickHouse at {} (v{})", CLICKHOUSE_URL, ch_version.trim());

    let queries = bench_queries();
    let mut all_volume_results: Vec<VolumeResult> = Vec::new();

    // === Outer loop: one pass per data volume ===
    for &(volume_bytes, volume_label) in DATA_VOLUMES {
        println!();
        println!("{}", "#".repeat(78));
        println!("  VOLUME: {} (target {} bytes uncompressed)", volume_label, volume_bytes);
        println!("{}", "#".repeat(78));

        // -- Generate, upload & build indexes for both variants ---------------
        println!();
        println!("  Data generation + index building:");
        let mut variants = Vec::new();
        for &(size, label) in &FILE_SIZES {
            let v = generate_upload_and_index(&s3, size, label, volume_bytes, volume_label).await?;
            variants.push(v);
        }

        // -- Run all queries --------------------------------------------------
        println!();
        println!(
            "  Running {} queries ({} iters + warmup):",
            queries.len(),
            QUERY_ITERATIONS
        );

        let mut timings: Vec<QueryTiming> = Vec::new();

        for q in &queries {
            print!("    {:<25}", q.name);
            std::io::Write::flush(&mut std::io::stdout())?;

            let predicates = (q.build_predicates)();
            let mut variant_results = Vec::new();

            for v in &variants {
                let (matching_files, _pruned) = prune_files(&v.index, &predicates, v.file_count);

                let select = q.select_override.unwrap_or_else(|| {
                    if q.suffix.contains("GROUP BY") { "region, count(*), avg(amount)" } else { "count(*)" }
                });

                let median = if matching_files.is_empty() {
                    Duration::ZERO
                } else {
                    let s3_url = if matching_files.len() == v.file_count {
                        build_wildcard_s3_url(&v.prefix)
                    } else {
                        build_s3_url(&matching_files, &v.prefix)
                    };
                    let sql = build_sql(&s3_url, select, q.where_clause, q.suffix);
                    let (m, _) = run_timed_query(&http, &sql).await?;
                    m
                };

                print!("  {} {:>7} ({}/{}f)", v.label, format!("{:.0?}", median), matching_files.len(), v.file_count);
                std::io::Write::flush(&mut std::io::stdout())?;

                variant_results.push(VariantQueryResult {
                    label: v.label.to_string(),
                    duration: median,
                    files_read: matching_files.len(),
                });
            }
            println!();

            timings.push(QueryTiming {
                name: q.name.to_string(),
                results: variant_results,
            });
        }

        // -- Collect metadata before cleanup ----------------------------------
        let variant_metas: Vec<VariantMeta> = variants.iter().map(|v| v.meta()).collect();

        // -- Cleanup ----------------------------------------------------------
        println!();
        print!("  Cleanup: ");
        std::io::Write::flush(&mut std::io::stdout())?;
        for v in &variants {
            print!("{} ({} files)... ", v.prefix, v.file_count);
            std::io::Write::flush(&mut std::io::stdout())?;
            cleanup_variant(&s3, &v.prefix, v.file_count).await?;
        }
        println!("done.");

        all_volume_results.push(VolumeResult {
            volume_label: volume_label.to_string(),
            variant_metas,
            timings,
        });
    }

    // === Print per-volume tables ===
    for vr in &all_volume_results {
        print_volume_summary(vr);
    }

    // === Print cross-volume scaling summary ===
    print_scaling_summary(&all_volume_results);

    Ok(())
}

// =============================================================================
// Per-volume summary (same table format as before)
// =============================================================================

fn compute_delta(a: Duration, b: Duration) -> f64 {
    if a.as_nanos() > 0 {
        ((b.as_secs_f64() - a.as_secs_f64()) / a.as_secs_f64()) * 100.0
    } else {
        0.0
    }
}

fn print_volume_summary(vr: &VolumeResult) {
    println!();
    println!("{}", "=".repeat(78));
    println!("  RESULTS: {}", vr.volume_label);
    println!("{}", "=".repeat(78));
    println!();

    for v in &vr.variant_metas {
        println!(
            "  {:<8}  {} files x ~{} rows  ({} MB compressed)",
            v.label, v.file_count, v.rows_per_file, v.total_bytes / (1024 * 1024)
        );
    }

    println!();
    println!(
        "  {:<25} {:>10} {:>6} {:>10} {:>6} {:>8}",
        "Query", "128 MB", "Read", "64 MB", "Read", "Delta"
    );
    println!("  {}", "-".repeat(70));

    for t in &vr.timings {
        let r128 = t.results.iter().find(|r| r.label == "128mb");
        let r64 = t.results.iter().find(|r| r.label == "64mb");

        match (r128, r64) {
            (Some(a), Some(b)) => {
                let delta_pct = compute_delta(a.duration, b.duration);
                let sign = if delta_pct >= 0.0 { "+" } else { "" };
                println!(
                    "  {:<25} {:>10} {:>5}f {:>10} {:>5}f {:>7}%",
                    t.name,
                    format!("{:.0?}", a.duration),
                    a.files_read,
                    format!("{:.0?}", b.duration),
                    b.files_read,
                    format!("{}{:.1}", sign, delta_pct),
                );
            }
            _ => println!("  {:<25} (incomplete data)", t.name),
        }
    }

    println!("  {}", "-".repeat(70));
    println!();
    println!("  Positive delta = 64 MB is slower; negative = 64 MB is faster.");
    println!("  Times are medians of {} iterations.", QUERY_ITERATIONS);
}

// =============================================================================
// Cross-volume scaling summary
// =============================================================================

fn print_scaling_summary(all: &[VolumeResult]) {
    if all.is_empty() {
        return;
    }

    // Collect all query names from the first volume
    let query_names: Vec<&str> = all[0].timings.iter().map(|t| t.name.as_str()).collect();

    println!();
    println!("{}", "=".repeat(78));
    println!("  SCALING SUMMARY");
    println!("  (delta% = how much slower/faster 64 MB is vs 128 MB)");
    println!("{}", "=".repeat(78));
    println!();

    // Header
    print!("  {:<25}", "Query");
    for vr in all {
        print!(" {:>10}", vr.volume_label);
    }
    println!();
    print!("  {}", "-".repeat(25));
    for _ in all {
        print!(" {}", "-".repeat(10));
    }
    println!();

    // One row per query
    for qname in &query_names {
        print!("  {:<25}", qname);
        for vr in all {
            if let Some(t) = vr.timings.iter().find(|t| t.name == *qname) {
                let r128 = t.results.iter().find(|r| r.label == "128mb");
                let r64 = t.results.iter().find(|r| r.label == "64mb");
                match (r128, r64) {
                    (Some(a), Some(b)) => {
                        let d = compute_delta(a.duration, b.duration);
                        let sign = if d >= 0.0 { "+" } else { "" };
                        print!(" {:>9}%", format!("{}{:.1}", sign, d));
                    }
                    _ => print!(" {:>10}", "-"),
                }
            } else {
                print!(" {:>10}", "-");
            }
        }
        println!();
    }

    print!("  {}", "-".repeat(25));
    for _ in all {
        print!(" {}", "-".repeat(10));
    }
    println!();
    println!();
    println!("  Negative = 64 MB faster.  Positive = 128 MB faster.");
    println!("  Growing negative values = 64 MB advantage increases with data size.");
    println!();
    println!("{}", "=".repeat(78));
}
