//! Aggregation Worker Tests
//!
//! Tests for data aggregation, statistics calculation, and rollups.

use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

#[cfg(test)]
mod tests {
    use super::*;
    
    // ========================================================================
    // Statistics Calculation Tests
    // ========================================================================
    
    fn calculate_percentile(sorted_values: &[f64], percentile: f64) -> f64 {
        if sorted_values.is_empty() {
            return 0.0;
        }
        
        let index = (percentile / 100.0) * (sorted_values.len() - 1) as f64;
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;
        
        if lower == upper {
            sorted_values[lower]
        } else {
            let fraction = index - lower as f64;
            sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction
        }
    }
    
    #[test]
    fn test_percentile_p50() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let p50 = calculate_percentile(&values, 50.0);
        assert!((p50 - 5.5).abs() < 0.01);
    }
    
    #[test]
    fn test_percentile_p95() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let p95 = calculate_percentile(&values, 95.0);
        assert!((p95 - 9.55).abs() < 0.01);
    }
    
    #[test]
    fn test_percentile_p99() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let p99 = calculate_percentile(&values, 99.0);
        assert!((p99 - 9.91).abs() < 0.01);
    }
    
    #[test]
    fn test_percentile_single_value() {
        let values = vec![42.0];
        assert_eq!(calculate_percentile(&values, 50.0), 42.0);
        assert_eq!(calculate_percentile(&values, 95.0), 42.0);
    }
    
    #[test]
    fn test_percentile_empty() {
        let values: Vec<f64> = vec![];
        assert_eq!(calculate_percentile(&values, 50.0), 0.0);
    }
    
    // ========================================================================
    // Aggregation Bucket Tests
    // ========================================================================
    
    #[derive(Debug, Clone)]
    struct AggregationBucket {
        timestamp: chrono::DateTime<Utc>,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        avg: f64,
    }
    
    impl AggregationBucket {
        fn new(timestamp: chrono::DateTime<Utc>) -> Self {
            Self {
                timestamp,
                count: 0,
                sum: 0.0,
                min: f64::MAX,
                max: f64::MIN,
                avg: 0.0,
            }
        }
        
        fn add(&mut self, value: f64) {
            self.count += 1;
            self.sum += value;
            self.min = self.min.min(value);
            self.max = self.max.max(value);
            self.avg = self.sum / self.count as f64;
        }
        
        fn merge(&mut self, other: &AggregationBucket) {
            if other.count == 0 {
                return;
            }
            
            let new_count = self.count + other.count;
            self.sum += other.sum;
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
            self.avg = self.sum / new_count as f64;
            self.count = new_count;
        }
    }
    
    #[test]
    fn test_bucket_single_value() {
        let mut bucket = AggregationBucket::new(Utc::now());
        bucket.add(10.0);
        
        assert_eq!(bucket.count, 1);
        assert_eq!(bucket.sum, 10.0);
        assert_eq!(bucket.min, 10.0);
        assert_eq!(bucket.max, 10.0);
        assert_eq!(bucket.avg, 10.0);
    }
    
    #[test]
    fn test_bucket_multiple_values() {
        let mut bucket = AggregationBucket::new(Utc::now());
        bucket.add(10.0);
        bucket.add(20.0);
        bucket.add(30.0);
        
        assert_eq!(bucket.count, 3);
        assert_eq!(bucket.sum, 60.0);
        assert_eq!(bucket.min, 10.0);
        assert_eq!(bucket.max, 30.0);
        assert_eq!(bucket.avg, 20.0);
    }
    
    #[test]
    fn test_bucket_merge() {
        let mut bucket1 = AggregationBucket::new(Utc::now());
        bucket1.add(10.0);
        bucket1.add(20.0);
        
        let mut bucket2 = AggregationBucket::new(Utc::now());
        bucket2.add(30.0);
        bucket2.add(40.0);
        
        bucket1.merge(&bucket2);
        
        assert_eq!(bucket1.count, 4);
        assert_eq!(bucket1.sum, 100.0);
        assert_eq!(bucket1.min, 10.0);
        assert_eq!(bucket1.max, 40.0);
        assert_eq!(bucket1.avg, 25.0);
    }
    
    #[test]
    fn test_bucket_merge_empty() {
        let mut bucket1 = AggregationBucket::new(Utc::now());
        bucket1.add(10.0);
        
        let bucket2 = AggregationBucket::new(Utc::now());
        
        bucket1.merge(&bucket2);
        
        // Should remain unchanged
        assert_eq!(bucket1.count, 1);
        assert_eq!(bucket1.sum, 10.0);
    }
    
    // ========================================================================
    // Time Bucket Tests
    // ========================================================================
    
    fn bucket_timestamp(ts: chrono::DateTime<Utc>, interval_seconds: i64) -> chrono::DateTime<Utc> {
        let epoch_seconds = ts.timestamp();
        let bucket_seconds = (epoch_seconds / interval_seconds) * interval_seconds;
        chrono::DateTime::from_timestamp(bucket_seconds, 0).unwrap()
    }
    
    #[test]
    fn test_bucket_timestamp_minute() {
        // 10:15:30 should bucket to 10:15:00
        let ts = chrono::DateTime::parse_from_rfc3339("2024-01-15T10:15:30Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let bucketed = bucket_timestamp(ts, 60);
        
        assert_eq!(bucketed.minute(), 15);
        assert_eq!(bucketed.second(), 0);
    }
    
    #[test]
    fn test_bucket_timestamp_5_minutes() {
        // 10:17:30 should bucket to 10:15:00
        let ts = chrono::DateTime::parse_from_rfc3339("2024-01-15T10:17:30Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let bucketed = bucket_timestamp(ts, 300); // 5 minutes
        
        assert_eq!(bucketed.minute(), 15);
        assert_eq!(bucketed.second(), 0);
    }
    
    #[test]
    fn test_bucket_timestamp_hour() {
        // 10:30:00 should bucket to 10:00:00
        let ts = chrono::DateTime::parse_from_rfc3339("2024-01-15T10:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let bucketed = bucket_timestamp(ts, 3600); // 1 hour
        
        assert_eq!(bucketed.hour(), 10);
        assert_eq!(bucketed.minute(), 0);
    }
    
    // ========================================================================
    // Error Rate Calculation Tests
    // ========================================================================
    
    fn calculate_error_rate(error_count: u64, total_count: u64) -> f64 {
        if total_count == 0 {
            return 0.0;
        }
        (error_count as f64 / total_count as f64) * 100.0
    }
    
    #[test]
    fn test_error_rate_calculation() {
        assert!((calculate_error_rate(5, 100) - 5.0).abs() < 0.01);
        assert!((calculate_error_rate(1, 1000) - 0.1).abs() < 0.01);
        assert!((calculate_error_rate(100, 100) - 100.0).abs() < 0.01);
    }
    
    #[test]
    fn test_error_rate_zero_total() {
        assert_eq!(calculate_error_rate(0, 0), 0.0);
    }
    
    // ========================================================================
    // Throughput Calculation Tests
    // ========================================================================
    
    fn calculate_throughput(count: u64, duration_seconds: f64) -> f64 {
        if duration_seconds <= 0.0 {
            return 0.0;
        }
        count as f64 / duration_seconds
    }
    
    #[test]
    fn test_throughput_calculation() {
        // 1000 requests in 60 seconds = 16.67 req/s
        let throughput = calculate_throughput(1000, 60.0);
        assert!((throughput - 16.67).abs() < 0.01);
    }
    
    #[test]
    fn test_throughput_zero_duration() {
        assert_eq!(calculate_throughput(100, 0.0), 0.0);
    }
    
    // ========================================================================
    // Rollup Configuration Tests
    // ========================================================================
    
    #[test]
    fn test_rollup_config_structure() {
        let config = json!({
            "rollup_intervals": [
                {"source": "1m", "target": "5m", "retention_days": 7},
                {"source": "5m", "target": "1h", "retention_days": 30},
                {"source": "1h", "target": "1d", "retention_days": 365}
            ],
            "metrics": ["request_count", "error_count", "latency_p50", "latency_p95", "latency_p99"]
        });
        
        let intervals = config["rollup_intervals"].as_array().unwrap();
        assert_eq!(intervals.len(), 3);
        
        let metrics = config["metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 5);
    }
    
    // ========================================================================
    // Aggregation Result Tests
    // ========================================================================
    
    #[test]
    fn test_aggregation_result_json() {
        let result = json!({
            "timestamp": Utc::now().to_rfc3339(),
            "project_id": Uuid::new_v4().to_string(),
            "metric": "http_requests",
            "dimensions": {
                "service": "api",
                "endpoint": "/users",
                "method": "GET",
                "status_code": "200"
            },
            "values": {
                "count": 1000,
                "sum": 15000.0,
                "min": 5.0,
                "max": 500.0,
                "avg": 15.0,
                "p50": 10.0,
                "p95": 100.0,
                "p99": 250.0
            }
        });
        
        assert!(result["dimensions"]["service"].is_string());
        assert_eq!(result["values"]["count"], 1000);
    }
}
