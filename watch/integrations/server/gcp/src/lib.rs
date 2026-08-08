//! GCP integrations for Reiver
//!
//! This crate provides server-side integrations for Google Cloud Platform services.
//! Currently supports:
//! - Compute Engine (Cloud Monitoring metrics collection)
//! - Cloud Functions (Cloud Monitoring metrics collection)
//! - Cloud Storage (Cloud Monitoring metrics collection)
//! - CloudSQL (Cloud Monitoring metrics collection for managed instances)
//! - Cloud Spanner (Cloud Monitoring metrics collection)
//! - Cloud Redis (Memorystore) (Cloud Monitoring metrics collection for managed instances)
//! - Cloud Run (Cloud Monitoring metrics collection)
//! - Kubernetes Engine (GKE) (GKE API for cluster info, Cloud Monitoring for control plane metrics)
//! - Pub/Sub (Cloud Monitoring metrics collection for topics and subscriptions)
//! - Load Balancing (Cloud Monitoring metrics collection for HTTP(S), TCP/UDP, and Internal load balancers)
//! - Cloud Monitoring (Generic metrics collection via filters)
//! - API Gateway (Cloud Monitoring metrics collection)
//! - Firestore (Cloud Monitoring metrics collection)
//! - BigQuery (Cloud Monitoring metrics collection)

pub mod compute;
pub mod cloud_functions;
pub mod cloud_storage;
pub mod cloudsql;
pub mod spanner;
pub mod redis;
pub mod cloud_run;
pub mod gke;
pub mod pubsub;
pub mod load_balancing;
pub mod monitoring;
pub mod api_gateway;
pub mod firestore;
pub mod bigquery;
pub mod config;

pub use config::GcpConfig;
pub use compute::{GceCollector, GceInstanceMetrics, GceInstanceId, gce_metrics_to_reiver_format, ReiverMetric as GceReiverMetric};
pub use cloud_functions::{CloudFunctionsCollector, CloudFunctionMetrics, CloudFunctionId, cloud_functions_metrics_to_reiver_format, ReiverMetric as CloudFunctionsReiverMetric};
pub use cloud_storage::{GcsCollector, GcsBucketMetrics, GcsBucketId, gcs_metrics_to_reiver_format, ReiverMetric as GcsReiverMetric};
pub use cloudsql::{CloudSqlCollector, CloudSqlMetrics, CloudSqlInstanceId, cloudsql_metrics_to_reiver_format, ReiverMetric as CloudSqlReiverMetric};
pub use spanner::{SpannerCollector, SpannerMetrics, SpannerInstanceId, spanner_metrics_to_reiver_format, ReiverMetric as SpannerReiverMetric};
pub use redis::{RedisCollector, RedisMetrics, RedisInstanceId, redis_metrics_to_reiver_format, ReiverMetric as RedisReiverMetric};
pub use cloud_run::{CloudRunCollector, CloudRunMetrics, CloudRunServiceId, cloud_run_metrics_to_reiver_format, ReiverMetric as CloudRunReiverMetric};
pub use gke::{GkeCollector, GkeClusterMetrics, GkeClusterId, gke_metrics_to_reiver_format, ReiverMetric as GkeReiverMetric};
pub use pubsub::{PubSubCollector, PubSubMetrics, PubSubTopicId, PubSubSubscriptionId, pubsub_metrics_to_reiver_format, ReiverMetric as PubSubReiverMetric};
pub use load_balancing::{LoadBalancingCollector, LoadBalancingMetrics, LoadBalancerId, load_balancing_metrics_to_reiver_format, ReiverMetric as LoadBalancingReiverMetric};
pub use monitoring::{MonitoringCollector, MonitoringMetrics, MonitoringFilterId, monitoring_metrics_to_reiver_format, ReiverMetric as MonitoringReiverMetric};
pub use api_gateway::{ApiGatewayCollector, ApiGatewayMetrics, ApiGatewayId, api_gateway_metrics_to_reiver_format, ReiverMetric as ApiGatewayReiverMetric};
pub use firestore::{FirestoreCollector, FirestoreMetrics, FirestoreDatabaseId, firestore_metrics_to_reiver_format, ReiverMetric as FirestoreReiverMetric};
pub use bigquery::{BigQueryCollector, BigQueryMetrics, BigQueryProjectId, bigquery_metrics_to_reiver_format, ReiverMetric as BigQueryReiverMetric};
