//! OCI integrations for Reiver
//!
//! This crate provides server-side integrations for Oracle Cloud Infrastructure services.
//! Currently supports:
//! - Compute (OCI Monitoring metrics collection)
//!
//! Note: OCI uses request signing for authentication, which requires proper implementation
//! of RSA-SHA256 signing. For production use, consider using the official OCI SDK
//! or implementing full request signing.

pub mod compute;
pub mod config;
pub mod container_instances;
pub mod database;
pub mod functions;
pub mod load_balancer;
pub mod object_storage;
pub mod oke;
mod signing;

pub use config::OciConfig;
pub use compute::{OciComputeCollector, OciInstanceMetrics, OciInstanceId, oci_compute_metrics_to_reiver_format, ReiverMetric as OciComputeReiverMetric};
pub use container_instances::{OciContainerInstanceCollector, OciContainerInstanceMetrics, OciContainerInstanceId, oci_container_instance_metrics_to_reiver_format, ReiverMetric as OciContainerInstanceReiverMetric};
pub use database::{OciDatabaseCollector, OciDatabaseMetrics, OciDatabaseId, oci_database_metrics_to_reiver_format, ReiverMetric as OciDatabaseReiverMetric};
pub use functions::{OciFunctionCollector, OciFunctionMetrics, OciFunctionAppId, oci_function_metrics_to_reiver_format, ReiverMetric as OciFunctionReiverMetric};
pub use load_balancer::{OciLoadBalancerCollector, OciLoadBalancerMetrics, OciLoadBalancerId, oci_load_balancer_metrics_to_reiver_format, ReiverMetric as OciLoadBalancerReiverMetric};
pub use object_storage::{OciObjectStorageCollector, OciObjectStorageMetrics, OciBucketId, oci_object_storage_metrics_to_reiver_format, ReiverMetric as OciObjectStorageReiverMetric};
pub use oke::{OciOkeCollector, OciOkeClusterMetrics, OciOkeClusterId, oci_oke_metrics_to_reiver_format, ReiverMetric as OciOkeReiverMetric};
