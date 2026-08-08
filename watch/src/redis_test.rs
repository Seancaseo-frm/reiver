#[cfg(test)]
mod tests {
    use redis::aio::ConnectionManager;
    use redis::AsyncCommands;
    use std::time::Duration;

    #[tokio::test]
    #[ignore] // Requires actual Redis infrastructure
    async fn test_redis_connection() {
        // Use the same Redis URL as the app
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

        println!("Connecting to Redis at: {}", redis_url);

        // Create Redis client
        let client = redis::Client::open(redis_url.clone()).expect("Failed to create Redis client");

        // Create connection manager
        let mut conn = ConnectionManager::new(client)
            .await
            .expect("Failed to create connection manager");

        println!("Connection manager created successfully");

        // Test PING
        let result: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .expect("PING failed");
        println!("PING result: {}", result);
        assert_eq!(result, "PONG");

        // Test INCR using cmd
        let key = "test:incr:cmd";
        let count: i32 = tokio::time::timeout(
            Duration::from_secs(5),
            redis::cmd("INCR").arg(key).query_async(&mut conn),
        )
        .await
        .expect("INCR timeout")
        .expect("INCR failed");
        println!("INCR (cmd) result: {}", count);

        // Test INCR using AsyncCommands trait (incr takes key and delta, default delta is 1)
        let key2 = "test:incr:trait";
        let mut conn2 = conn.clone();
        let count2: isize = tokio::time::timeout(Duration::from_secs(5), conn2.incr(key2, 1))
            .await
            .expect("INCR timeout")
            .expect("INCR failed");
        println!("INCR (trait) result: {}", count2);

        // Clean up
        let _: () = redis::cmd("DEL")
            .arg(key)
            .arg(key2)
            .query_async(&mut conn)
            .await
            .expect("DEL failed");

        println!("All tests passed!");
    }
}
