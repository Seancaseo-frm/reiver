//! Alert Worker Tests
//!
//! Tests for alert evaluation, state transitions, and notifications.

use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

#[cfg(test)]
mod tests {
    use super::*;
    
    // ========================================================================
    // Alert Condition Types
    // ========================================================================
    
    #[derive(Debug, Clone)]
    enum AlertOperator {
        Above,
        Below,
        Equals,
        NotEquals,
        AboveOrEqual,
        BelowOrEqual,
    }
    
    #[derive(Debug, Clone)]
    struct ThresholdCondition {
        metric: String,
        operator: AlertOperator,
        threshold: f64,
        duration_seconds: u64,
    }
    
    #[derive(Debug, Clone, PartialEq)]
    enum AlertState {
        Ok,
        Pending,
        Alerting,
        NoData,
    }
    
    // ========================================================================
    // Threshold Evaluation Tests
    // ========================================================================
    
    fn evaluate_threshold(operator: &AlertOperator, value: f64, threshold: f64) -> bool {
        match operator {
            AlertOperator::Above => value > threshold,
            AlertOperator::Below => value < threshold,
            AlertOperator::Equals => (value - threshold).abs() < f64::EPSILON,
            AlertOperator::NotEquals => (value - threshold).abs() >= f64::EPSILON,
            AlertOperator::AboveOrEqual => value >= threshold,
            AlertOperator::BelowOrEqual => value <= threshold,
        }
    }
    
    #[test]
    fn test_threshold_above() {
        assert!(evaluate_threshold(&AlertOperator::Above, 100.0, 50.0));
        assert!(!evaluate_threshold(&AlertOperator::Above, 50.0, 100.0));
        assert!(!evaluate_threshold(&AlertOperator::Above, 50.0, 50.0)); // Equal should not trigger
    }
    
    #[test]
    fn test_threshold_below() {
        assert!(evaluate_threshold(&AlertOperator::Below, 50.0, 100.0));
        assert!(!evaluate_threshold(&AlertOperator::Below, 100.0, 50.0));
        assert!(!evaluate_threshold(&AlertOperator::Below, 50.0, 50.0)); // Equal should not trigger
    }
    
    #[test]
    fn test_threshold_equals() {
        assert!(evaluate_threshold(&AlertOperator::Equals, 50.0, 50.0));
        assert!(!evaluate_threshold(&AlertOperator::Equals, 50.1, 50.0));
    }
    
    #[test]
    fn test_threshold_not_equals() {
        assert!(evaluate_threshold(&AlertOperator::NotEquals, 50.1, 50.0));
        assert!(!evaluate_threshold(&AlertOperator::NotEquals, 50.0, 50.0));
    }
    
    #[test]
    fn test_threshold_above_or_equal() {
        assert!(evaluate_threshold(&AlertOperator::AboveOrEqual, 100.0, 50.0));
        assert!(evaluate_threshold(&AlertOperator::AboveOrEqual, 50.0, 50.0));
        assert!(!evaluate_threshold(&AlertOperator::AboveOrEqual, 49.0, 50.0));
    }
    
    #[test]
    fn test_threshold_below_or_equal() {
        assert!(evaluate_threshold(&AlertOperator::BelowOrEqual, 50.0, 100.0));
        assert!(evaluate_threshold(&AlertOperator::BelowOrEqual, 50.0, 50.0));
        assert!(!evaluate_threshold(&AlertOperator::BelowOrEqual, 51.0, 50.0));
    }
    
    // ========================================================================
    // Alert State Transition Tests
    // ========================================================================
    
    fn compute_next_state(
        current_state: &AlertState,
        condition_met: bool,
        pending_since: Option<chrono::DateTime<Utc>>,
        required_duration: Duration,
    ) -> (AlertState, Option<chrono::DateTime<Utc>>) {
        match (current_state, condition_met) {
            // OK -> Condition met: start pending
            (AlertState::Ok, true) => (AlertState::Pending, Some(Utc::now())),
            
            // OK -> Condition not met: stay OK
            (AlertState::Ok, false) => (AlertState::Ok, None),
            
            // Pending -> Condition still met: check duration
            (AlertState::Pending, true) => {
                if let Some(since) = pending_since {
                    if Utc::now() - since >= required_duration {
                        (AlertState::Alerting, None)
                    } else {
                        (AlertState::Pending, pending_since)
                    }
                } else {
                    (AlertState::Pending, Some(Utc::now()))
                }
            }
            
            // Pending -> Condition no longer met: back to OK
            (AlertState::Pending, false) => (AlertState::Ok, None),
            
            // Alerting -> Condition still met: stay alerting
            (AlertState::Alerting, true) => (AlertState::Alerting, None),
            
            // Alerting -> Condition no longer met: back to OK
            (AlertState::Alerting, false) => (AlertState::Ok, None),
            
            // NoData cases
            (AlertState::NoData, true) => (AlertState::Pending, Some(Utc::now())),
            (AlertState::NoData, false) => (AlertState::Ok, None),
        }
    }
    
    #[test]
    fn test_state_ok_to_pending() {
        let (new_state, pending_since) = compute_next_state(
            &AlertState::Ok,
            true, // condition met
            None,
            Duration::minutes(5),
        );
        
        assert_eq!(new_state, AlertState::Pending);
        assert!(pending_since.is_some());
    }
    
    #[test]
    fn test_state_ok_stays_ok() {
        let (new_state, pending_since) = compute_next_state(
            &AlertState::Ok,
            false, // condition not met
            None,
            Duration::minutes(5),
        );
        
        assert_eq!(new_state, AlertState::Ok);
        assert!(pending_since.is_none());
    }
    
    #[test]
    fn test_state_pending_to_alerting() {
        // Set pending_since to 10 minutes ago (longer than 5 minute requirement)
        let pending_since = Some(Utc::now() - Duration::minutes(10));
        
        let (new_state, _) = compute_next_state(
            &AlertState::Pending,
            true,
            pending_since,
            Duration::minutes(5),
        );
        
        assert_eq!(new_state, AlertState::Alerting);
    }
    
    #[test]
    fn test_state_pending_stays_pending() {
        // Set pending_since to 1 minute ago (shorter than 5 minute requirement)
        let pending_since = Some(Utc::now() - Duration::minutes(1));
        
        let (new_state, new_pending) = compute_next_state(
            &AlertState::Pending,
            true,
            pending_since,
            Duration::minutes(5),
        );
        
        assert_eq!(new_state, AlertState::Pending);
        assert!(new_pending.is_some());
    }
    
    #[test]
    fn test_state_pending_to_ok() {
        let (new_state, pending_since) = compute_next_state(
            &AlertState::Pending,
            false, // condition no longer met
            Some(Utc::now() - Duration::minutes(3)),
            Duration::minutes(5),
        );
        
        assert_eq!(new_state, AlertState::Ok);
        assert!(pending_since.is_none());
    }
    
    #[test]
    fn test_state_alerting_stays_alerting() {
        let (new_state, _) = compute_next_state(
            &AlertState::Alerting,
            true,
            None,
            Duration::minutes(5),
        );
        
        assert_eq!(new_state, AlertState::Alerting);
    }
    
    #[test]
    fn test_state_alerting_to_ok() {
        let (new_state, _) = compute_next_state(
            &AlertState::Alerting,
            false, // condition no longer met
            None,
            Duration::minutes(5),
        );
        
        assert_eq!(new_state, AlertState::Ok);
    }
    
    // ========================================================================
    // Alert Rule Configuration Tests
    // ========================================================================
    
    #[test]
    fn test_alert_rule_json_structure() {
        let rule = json!({
            "id": Uuid::new_v4().to_string(),
            "project_id": Uuid::new_v4().to_string(),
            "name": "High Error Rate",
            "description": "Alert when error rate exceeds 5%",
            "enabled": true,
            "condition": {
                "type": "threshold",
                "metric": "error_rate_percent",
                "operator": "above",
                "threshold": 5.0,
                "duration_seconds": 300
            },
            "channels": [
                {
                    "type": "slack",
                    "webhook_url": "https://hooks.slack.com/..."
                },
                {
                    "type": "email",
                    "addresses": ["team@example.com"]
                }
            ],
            "labels": {
                "severity": "critical",
                "team": "backend"
            }
        });
        
        assert!(rule["enabled"].as_bool().unwrap());
        assert_eq!(rule["condition"]["type"], "threshold");
        assert_eq!(rule["channels"].as_array().unwrap().len(), 2);
    }
    
    #[test]
    fn test_anomaly_detection_rule() {
        let rule = json!({
            "id": Uuid::new_v4().to_string(),
            "name": "Anomaly Detection",
            "enabled": true,
            "condition": {
                "type": "anomaly",
                "metric": "response_time_p99",
                "sensitivity": 0.8,
                "baseline_window_hours": 168 // 1 week
            }
        });
        
        assert_eq!(rule["condition"]["type"], "anomaly");
        assert_eq!(rule["condition"]["baseline_window_hours"], 168);
    }
    
    #[test]
    fn test_composite_alert_rule() {
        let rule = json!({
            "id": Uuid::new_v4().to_string(),
            "name": "High Error + Low Traffic",
            "enabled": true,
            "condition": {
                "type": "composite",
                "operator": "and",
                "conditions": [
                    {
                        "type": "threshold",
                        "metric": "error_rate",
                        "operator": "above",
                        "threshold": 5.0
                    },
                    {
                        "type": "threshold",
                        "metric": "request_rate",
                        "operator": "below",
                        "threshold": 100.0
                    }
                ]
            }
        });
        
        assert_eq!(rule["condition"]["type"], "composite");
        assert_eq!(rule["condition"]["operator"], "and");
        assert_eq!(rule["condition"]["conditions"].as_array().unwrap().len(), 2);
    }
    
    // ========================================================================
    // Notification Channel Tests
    // ========================================================================
    
    #[test]
    fn test_slack_notification_format() {
        let notification = json!({
            "channel": "#alerts",
            "username": "Reiver Alerts",
            "icon_emoji": ":warning:",
            "attachments": [{
                "color": "danger",
                "title": "High Error Rate Alert",
                "text": "Error rate is 8.5%, threshold is 5%",
                "fields": [
                    {"title": "Project", "value": "api-service", "short": true},
                    {"title": "Environment", "value": "production", "short": true}
                ],
                "footer": "Reiver Monitoring",
                "ts": 1234567890
            }]
        });
        
        assert_eq!(notification["attachments"][0]["color"], "danger");
    }
    
    #[test]
    fn test_pagerduty_notification_format() {
        let notification = json!({
            "routing_key": "service_key_here",
            "event_action": "trigger",
            "dedup_key": "alert_123_high_error_rate",
            "payload": {
                "summary": "High Error Rate in api-service",
                "severity": "critical",
                "source": "reiver",
                "custom_details": {
                    "current_value": 8.5,
                    "threshold": 5.0,
                    "metric": "error_rate_percent"
                }
            }
        });
        
        assert_eq!(notification["event_action"], "trigger");
        assert_eq!(notification["payload"]["severity"], "critical");
    }
    
    // ========================================================================
    // Rate Limiting Tests
    // ========================================================================
    
    fn should_send_notification(
        last_notification: Option<chrono::DateTime<Utc>>,
        min_interval_seconds: i64,
    ) -> bool {
        match last_notification {
            None => true,
            Some(last) => {
                let elapsed = Utc::now() - last;
                elapsed >= Duration::seconds(min_interval_seconds)
            }
        }
    }
    
    #[test]
    fn test_notification_rate_limiting() {
        // First notification should always be allowed
        assert!(should_send_notification(None, 300));
        
        // Recent notification should be blocked
        let recent = Utc::now() - Duration::seconds(60);
        assert!(!should_send_notification(Some(recent), 300));
        
        // Old notification should be allowed
        let old = Utc::now() - Duration::seconds(600);
        assert!(should_send_notification(Some(old), 300));
    }
}
