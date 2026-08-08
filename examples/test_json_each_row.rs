/// Test script to verify if clickhouse-rs supports JSONEachRow format
/// This test checks if fetch_all() returns Vec<String> when using FORMAT JSONEachRow
/// 
/// Usage:
///   cargo run --example test_json_each_row
///   CLICKHOUSE_URL=http://localhost:8123 cargo run --example test_json_each_row

use clickhouse::Client;
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing JSONEachRow format with clickhouse-rs...\n");

    // Connect to ClickHouse
    // You can set CLICKHOUSE_URL environment variable, or it will use default
    let clickhouse_url = std::env::var("CLICKHOUSE_URL")
        .unwrap_or_else(|_| "http://localhost:8123".to_string());
    
    println!("Connecting to ClickHouse at: {}", clickhouse_url);

    let client = Client::default()
        .with_url(&clickhouse_url)
        .with_database("reiver");

    // Test 1: Try to use FORMAT JSONEachRow with fetch_all() returning Vec<String>
    println!("\n=== Test 1: JSONEachRow with Vec<String> ===");
    
    match test_json_each_row_strings(&client).await {
        Ok(_) => {
            println!("\n✅ SUCCESS: JSONEachRow returns Vec<String> as expected!");
            println!("The current implementation in graphql.rs should work correctly.");
        },
        Err(e) => {
            println!("\n❌ FAILED: {}", e);
            println!("\nThis means we need an alternative approach.");
            println!("The current implementation in graphql.rs will need to be fixed.");
            println!("\nLet's try alternative approaches...\n");
            
            // Test 2: Try using query_raw or other methods
            println!("=== Test 2: Alternative approaches ===");
            test_alternatives(&client).await?;
        }
    }

    Ok(())
}

async fn test_json_each_row_strings(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple test query with JSONEachRow format
    // Using a query that should work even if tables don't exist
    let query = "SELECT 'test-id-123' as id, 'test-project-456' as project_id, 'info' as level, 'Test message' as message FORMAT JSONEachRow";
    
    println!("Query: {}", query);
    println!("Attempting to fetch as Vec<String>...");

    // Try to fetch as Vec<String> (what our current code assumes)
    let json_lines: Vec<String> = client
        .query(query)
        .fetch_all()
        .await?;

    println!("✓ fetch_all() succeeded!");
    println!("Result count: {} lines", json_lines.len());

    // Try to parse the JSON
    if !json_lines.is_empty() {
        println!("\nParsing JSON lines...");
        for (i, line) in json_lines.iter().enumerate() {
            println!("Line {}: {}", i + 1, line);
            
            match serde_json::from_str::<Value>(line) {
                Ok(json) => {
                    println!("  ✓ Valid JSON");
                    // Pretty print the JSON
                    if let Ok(pretty) = serde_json::to_string_pretty(&json) {
                        println!("  {}", pretty.split('\n').map(|l| format!("    {}", l)).collect::<Vec<_>>().join("\n"));
                    }
                },
                Err(e) => {
                    println!("  ❌ Invalid JSON: {}", e);
                    return Err(format!("Failed to parse JSON: {}", e).into());
                }
            }
        }
        println!("\n✅ JSON parsing successful!");
    } else {
        println!("⚠️  No rows returned (this is OK for this test)");
    }

    // Test with multiple rows
    println!("\nTesting with multiple rows...");
    let multi_query = "SELECT number as id, toString(number * 2) as project_id, 'info' as level FORMAT JSONEachRow LIMIT 3";
    println!("Query: {}", multi_query);
    
    let multi_lines: Vec<String> = client
        .query(multi_query)
        .fetch_all()
        .await?;

    println!("✓ Got {} rows", multi_lines.len());
    for (i, line) in multi_lines.iter().enumerate() {
        println!("  Row {}: {}", i + 1, line);
    }

    Ok(())
}

async fn test_alternatives(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nTrying alternative approaches...");
    
    // Alternative 1: Try using a typed struct (but we need to know fields ahead of time)
    println!("\nAlternative 1: Using typed struct (standard approach)");
    #[derive(serde::Deserialize, clickhouse::Row)]
    #[allow(dead_code)]
    struct TestRow {
        id: String,
        project_id: String,
        level: String,
    }
    
    let query = "SELECT 'test' as id, 'project' as project_id, 'info' as level";
    let rows: Vec<TestRow> = client
        .query(query)
        .fetch_all()
        .await?;
    
    println!("✓ Typed struct approach works: {} rows", rows.len());
    println!("Note: This requires knowing all fields at compile time.");
    
    // Alternative 2: Check if we can get raw bytes and parse manually
    println!("\nAlternative 2: Would need to check clickhouse-rs source for raw response access");
    println!("This might require modifying the query or using a different client method.");
    
    Ok(())
}

