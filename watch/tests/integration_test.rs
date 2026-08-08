//! Integration tests for Reiver error monitoring
//!
//! These tests send actual errors to the Reiver service
//!
//! To run:
//!   cargo test --test integration_test periodic_error_sending -- --ignored --nocapture
//!
//! Or with environment variables:
//!   REIVER_EMAIL=test@example.com REIVER_PASSWORD=your_password \
//!   cargo test --test integration_test periodic_error_sending -- --ignored --nocapture

mod helpers;

use reiver_sdk::ClientOptions;
use std::time::Duration as StdDuration;
use tokio::time;

/// Test that periodically sends errors to Reiver
///
/// This test:
/// 1. Initializes the Reiver SDK with a project key
/// 2. Periodically sends test errors
/// 3. Verifies errors are being sent successfully
///
/// To run: `cargo test --test integration_test periodic_error_sending -- --nocapture`
#[tokio::test]
#[ignore] // Ignore by default - run with --ignored flag
async fn periodic_error_sending() {
    let api_url =
        std::env::var("REIVER_API_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());

    // Get project key - either from env var or by logging in
    let project_key = if let Ok(key) = std::env::var("REIVER_PROJECT_KEY") {
        println!("Using REIVER_PROJECT_KEY from environment");
        key
    } else {
        // Try to get it by logging in
        let email = std::env::var("REIVER_EMAIL")
            .unwrap_or_else(|_| "test@example.com".to_string());
        let password = std::env::var("REIVER_PASSWORD")
            .expect("Either REIVER_PROJECT_KEY or REIVER_PASSWORD must be set");

        println!("Logging in as {} to get project key...", email);
        helpers::get_project_key_for_user(&api_url, &email, &password)
            .await
            .expect("Failed to get project key")
    };

    println!(
        "Initializing Reiver SDK with project key: {}...",
        &project_key[..8]
    );
    println!("API URL: {}", api_url);

    // Initialize Reiver SDK with optimized batching for stress test
    let _guard = reiver_sdk::init((
        project_key.clone(),
        ClientOptions {
            api_url: Some(api_url.clone()),
            environment: Some("test".to_string()),
            batch_size: 50, // Larger batches for better throughput
            batch_timeout: StdDuration::from_secs(2), // Send every 2 seconds even if batch not full
            ..Default::default()
        },
    ));

    println!(
        "Reiver SDK initialized. Starting stress test - sending errors as fast as possible..."
    );

    let num_workers = 10; // Number of concurrent workers

    // Different error counts per group to verify grouping is correct
    let group1_errors_per_worker = 500; // Total: 5000 errors
    let group2_errors_per_worker = 700; // Total: 7000 errors
    let group3_errors_per_worker = 800; // Total: 8000 errors

    let start_time = std::time::Instant::now();

    // Spawn multiple concurrent workers to send errors in parallel
    // We'll create 3 distinct error groups with different counts:
    // Group 1: Messages (5000 total)
    // Group 2: TypeError exceptions (7000 total)
    // Group 3: ReferenceError exceptions (8000 total)
    let mut handles = vec![];
    for worker_id in 0..num_workers {
        // Spawn 3 parallel tasks per worker - one for each error group
        // This ensures all groups are sending errors simultaneously

        // Group 1: Send messages - these will all have the same fingerprint
        let handle1 = tokio::spawn(async move {
            for counter in 1..=group1_errors_per_worker {
                let error_num = worker_id * group1_errors_per_worker + counter;
                reiver_sdk::capture_message(
                    &format!(
                        "Group 1: Database connection failed for user #{}",
                        error_num
                    ),
                    "error",
                );
            }
        });
        handles.push(handle1);

        // Group 2: Send TypeError exceptions - these will have a different fingerprint
        // (TypeError is a standard exception type that's included in fingerprint)
        let handle2 = tokio::spawn(async move {
            for counter in 1..=group2_errors_per_worker {
                let error_num = worker_id * group2_errors_per_worker + counter;
                let type_error = TypeError {
                    message: format!(
                        "Group 2: Cannot read property 'value' of user #{}",
                        error_num
                    ),
                };
                reiver_sdk::capture_exception(&type_error);
            }
        });
        handles.push(handle2);

        // Group 3: Send ReferenceError exceptions - these will have yet another different fingerprint
        // (ReferenceError is a standard exception type that's included in fingerprint)
        let handle3 = tokio::spawn(async move {
            for counter in 1..=group3_errors_per_worker {
                let error_num = worker_id * group3_errors_per_worker + counter;
                let ref_error = ReferenceError {
                    message: format!(
                        "Group 3: User variable is not defined in request #{}",
                        error_num
                    ),
                };
                reiver_sdk::capture_exception(&ref_error);
            }
        });
        handles.push(handle3);
    }

    // Wait for all workers to complete
    for handle in handles {
        handle.await.expect("Worker panicked");
    }

    let elapsed = start_time.elapsed();
    let group1_total = num_workers * group1_errors_per_worker;
    let group2_total = num_workers * group2_errors_per_worker;
    let group3_total = num_workers * group3_errors_per_worker;
    let total_errors = group1_total + group2_total + group3_total;
    let errors_per_sec = total_errors as f64 / elapsed.as_secs_f64();

    println!("Stress test completed!");
    println!(
        "Sent {} total errors in {:.2} seconds",
        total_errors,
        elapsed.as_secs_f64()
    );
    println!("Rate: {:.2} errors/second", errors_per_sec);
    println!("Expected 3 error groups with different counts:");
    println!(
        "  - Group 1: {} messages (Database connection failed)",
        group1_total
    );
    println!(
        "  - Group 2: {} TypeError exceptions (Cannot read property)",
        group2_total
    );
    println!(
        "  - Group 3: {} ReferenceError exceptions (Variable not defined)",
        group3_total
    );
    println!("Verify on dashboard that each group shows the correct count!");

    // Flush all pending events and wait for them to be sent
    println!("Flushing all pending events...");
    let pending = _guard.flush(60).await; // Wait up to 60 seconds for all events
    let (sent, failed, still_pending) = _guard.stats();
    println!(
        "Flush complete. Stats: sent={}, failed={}, still_pending={}",
        sent, failed, still_pending
    );

    if pending > 0 {
        println!(
            "WARNING: {} errors were still pending after flush timeout!",
            pending
        );
    }

    // Give additional time for server-side processing
    time::sleep(StdDuration::from_secs(10)).await;
    println!("Allowing time for server-side processing...");
}

/// Test error types for different groups
/// These will create different fingerprints because they have different exception types
#[derive(Debug)]
struct TypeError {
    message: String,
}

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TypeError {}

#[derive(Debug)]
struct ReferenceError {
    message: String,
}

impl std::fmt::Display for ReferenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ReferenceError {}
