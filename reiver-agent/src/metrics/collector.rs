use anyhow::Result;

/// Trait for all metric collectors
/// Collectors register observable callbacks with OpenTelemetry Meter
/// The SDK calls these callbacks when metrics need to be exported
pub trait Collector: Send + Sync {
    /// Register observable callbacks with the meter
    /// This is called once during initialization
    fn register_observables(&self, meter: opentelemetry::metrics::Meter) -> Result<()>;
    
    /// Get the name of this collector (for logging)
    fn name(&self) -> &str;
    
    /// Check if this collector is enabled
    fn enabled(&self) -> bool {
        true
    }
}

