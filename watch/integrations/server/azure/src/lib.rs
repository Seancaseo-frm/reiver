//! Azure integrations for Reiver
//!
//! This crate provides server-side integrations for Azure services.
//! Currently supports:
//! - Virtual Machines (Azure Monitor metrics collection)
//! - Functions (Azure Monitor metrics collection)
//! - Blob Storage (Azure Monitor metrics collection)
//! - SQL Database (Azure Monitor metrics collection)
//! - CosmosDB (Azure Monitor metrics collection)
//! - Redis Cache (Azure Monitor metrics collection)
//! - Container Instances (Azure Monitor metrics collection)
//! - Kubernetes Service (AKS) (cluster-level metrics collection)
//! - App Services (Azure Monitor metrics collection)
//! - Service Bus (Azure Monitor metrics collection)
//! - Event Hub (Azure Monitor metrics collection)
//! - API Management (Azure Monitor metrics collection)
//! - Application Gateway (Azure Monitor metrics collection)
//! - Synapse Analytics (Azure Monitor metrics collection)

pub mod compute;
pub mod functions;
pub mod storage;
pub mod sql_database;
pub mod cosmosdb;
pub mod redis_cache;
pub mod container_instances;
pub mod aks;
pub mod app_services;
pub mod service_bus;
pub mod event_hub;
pub mod api_management;
pub mod application_gateway;
pub mod synapse_analytics;
pub mod config;

pub use config::AzureConfig;
pub use compute::{AzureVmCollector, AzureVmMetrics, AzureVmId, azure_vm_metrics_to_reiver_format, ReiverMetric as AzureVmReiverMetric};
pub use functions::{AzureFunctionsCollector, AzureFunctionsMetrics, AzureFunctionAppId, azure_functions_metrics_to_reiver_format, ReiverMetric as AzureFunctionsReiverMetric};
pub use storage::{AzureBlobStorageCollector, AzureBlobStorageMetrics, AzureStorageAccountId, azure_blob_storage_metrics_to_reiver_format, ReiverMetric as AzureBlobStorageReiverMetric};
pub use sql_database::{AzureSqlDatabaseCollector, AzureSqlDatabaseMetrics, AzureSqlDatabaseId, azure_sql_database_metrics_to_reiver_format, ReiverMetric as AzureSqlDatabaseReiverMetric};
pub use cosmosdb::{AzureCosmosDbCollector, AzureCosmosDbMetrics, AzureCosmosDbAccountId, azure_cosmosdb_metrics_to_reiver_format, ReiverMetric as AzureCosmosDbReiverMetric};
pub use redis_cache::{AzureRedisCacheCollector, AzureRedisCacheMetrics, AzureRedisCacheId, azure_redis_cache_metrics_to_reiver_format, ReiverMetric as AzureRedisCacheReiverMetric};
pub use container_instances::{AzureContainerInstancesCollector, AzureContainerInstancesMetrics, AzureContainerGroupId, azure_container_instances_metrics_to_reiver_format, ReiverMetric as AzureContainerInstancesReiverMetric};
pub use aks::{AzureAksCollector, AzureAksMetrics, AzureAksClusterId, azure_aks_metrics_to_reiver_format, ReiverMetric as AzureAksReiverMetric};
pub use app_services::{AzureAppServicesCollector, AzureAppServicesMetrics, AzureAppServiceId, azure_app_services_metrics_to_reiver_format, ReiverMetric as AzureAppServicesReiverMetric};
pub use service_bus::{AzureServiceBusCollector, AzureServiceBusMetrics, AzureServiceBusNamespaceId, azure_service_bus_metrics_to_reiver_format, ReiverMetric as AzureServiceBusReiverMetric};
pub use event_hub::{AzureEventHubCollector, AzureEventHubMetrics, AzureEventHubNamespaceId, azure_event_hub_metrics_to_reiver_format, ReiverMetric as AzureEventHubReiverMetric};
pub use api_management::{AzureApiManagementCollector, AzureApiManagementMetrics, AzureApiManagementServiceId, azure_api_management_metrics_to_reiver_format, ReiverMetric as AzureApiManagementReiverMetric};
pub use application_gateway::{AzureApplicationGatewayCollector, AzureApplicationGatewayMetrics, AzureApplicationGatewayId, azure_application_gateway_metrics_to_reiver_format, ReiverMetric as AzureApplicationGatewayReiverMetric};
pub use synapse_analytics::{AzureSynapseAnalyticsCollector, AzureSynapseAnalyticsMetrics, AzureSynapseWorkspaceId, azure_synapse_analytics_metrics_to_reiver_format, ReiverMetric as AzureSynapseAnalyticsReiverMetric};
