use anyhow::Result;
use std::sync::{Arc, Mutex};
use sysinfo::{System, Disks, Networks};
use tracing::{instrument, info};
use opentelemetry::metrics::{Meter, ObservableGauge, ObservableCounter};
use opentelemetry::KeyValue;

use crate::config::Config;
use crate::metrics::Collector;

/// Get the hostname for tagging metrics
/// Returns config hostname if set, otherwise tries to detect system hostname
fn get_hostname(config: &crate::config::SystemMetricsConfig) -> String {
    if let Some(ref hostname) = config.hostname {
        return hostname.clone();
    }
    
    // Try to detect system hostname
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        return hostname;
    }
    
    // Try hostname command as fallback
    #[cfg(unix)]
    {
        use std::process::Command;
        if let Ok(output) = Command::new("hostname").output() {
            if let Ok(hostname) = String::from_utf8(output.stdout) {
                return hostname.trim().to_string();
            }
        }
    }
    
    // Final fallback
    "unknown".to_string()
}

/// Build base tags for system metrics (host and source)
fn build_system_metric_tags(config: &crate::config::SystemMetricsConfig) -> Vec<String> {
    let hostname = get_hostname(config);
    vec![
        format!("host:{}", hostname),
        "source:agent".to_string(),
    ]
}

pub struct SystemMetricsCollector {
    config: Arc<Config>,
    // Use Mutex for interior mutability (observable callbacks need &self)
    system: Arc<Mutex<System>>,
    disks: Arc<Mutex<Disks>>,
    networks: Arc<Mutex<Networks>>,
}

impl SystemMetricsCollector {
    pub fn new(config: Arc<Config>) -> Self {
        let system = System::new_all();
        let disks = Disks::new_with_refreshed_list();
        let networks = Networks::new_with_refreshed_list();
        
        Self { 
            config,
            system: Arc::new(Mutex::new(system)),
            disks: Arc::new(Mutex::new(disks)),
            networks: Arc::new(Mutex::new(networks)),
        }
    }
    
    fn get_hostname(&self) -> String {
        get_hostname(&self.config.system_metrics)
    }
    
    fn register_cpu_metrics(&self, meter: Meter) -> Result<()> {
        if !self.config.system_metrics.cpu.enabled {
            return Ok(());
        }
        
        let hostname = self.get_hostname();
        let system = self.system.clone();
        let config = self.config.clone();
        
        // Create observable CPU gauge with callback - using 0.31 API
        let _cpu_gauge = meter
            .f64_observable_gauge("system.cpu.usage_percent")
            .with_description("CPU usage percentage")
            .with_callback(move |observer| {
                let mut sys = system.lock().unwrap();
                sys.refresh_cpu();
                
                let hostname = hostname.clone();
                
                // Overall CPU usage
                observer.observe(
                    sys.global_cpu_info().cpu_usage() as f64,
                    &[
                        KeyValue::new("host", hostname.clone()),
                        KeyValue::new("source", "agent"),
                        KeyValue::new("cpu", "total"),
                    ],
                );
                
                // Per-core CPU usage if enabled
                if config.system_metrics.cpu.per_core {
                    for (i, cpu) in sys.cpus().iter().enumerate() {
                        observer.observe(
                            cpu.cpu_usage() as f64,
                            &[
                                KeyValue::new("host", hostname.clone()),
                                KeyValue::new("source", "agent"),
                                KeyValue::new("cpu", format!("core_{}", i)),
                            ],
                        );
                    }
                }
            })
            .build();
        
        Ok(())
    }
    
    fn register_memory_metrics(&self, meter: Meter) -> Result<()> {
        if !self.config.system_metrics.memory.enabled {
            return Ok(());
        }
        
        let hostname = self.get_hostname();
        let system = self.system.clone();
        
        // Memory total - using 0.31 API with .with_callback()
        let system_total = system.clone();
        let hostname_total = hostname.clone();
        let _mem_total = meter
            .u64_observable_gauge("system.memory.total")
            .with_description("Total system memory in bytes")
            .with_callback(move |observer| {
                let mut sys = system_total.lock().unwrap();
                sys.refresh_memory();
                observer.observe(
                    sys.total_memory(),
                    &[
                        KeyValue::new("host", hostname_total.clone()),
                        KeyValue::new("source", "agent"),
                    ],
                );
            })
            .build();
        
        // Memory used
        let system_used = system.clone();
        let hostname_used = hostname.clone();
        let _mem_used = meter
            .u64_observable_gauge("system.memory.used")
            .with_description("Used system memory in bytes")
            .with_callback(move |observer| {
                let mut sys = system_used.lock().unwrap();
                sys.refresh_memory();
                observer.observe(
                    sys.used_memory(),
                    &[
                        KeyValue::new("host", hostname_used.clone()),
                        KeyValue::new("source", "agent"),
                    ],
                );
            })
            .build();
        
        // Memory free
        let system_free = system.clone();
        let hostname_free = hostname.clone();
        let _mem_free = meter
            .u64_observable_gauge("system.memory.free")
            .with_description("Free system memory in bytes")
            .with_callback(move |observer| {
                let mut sys = system_free.lock().unwrap();
                sys.refresh_memory();
                observer.observe(
                    sys.free_memory(),
                    &[
                        KeyValue::new("host", hostname_free.clone()),
                        KeyValue::new("source", "agent"),
                    ],
                );
            })
            .build();
        
        // Memory usage percent
        let system_pct = system.clone();
        let hostname_pct = hostname.clone();
        let _mem_usage_pct = meter
            .f64_observable_gauge("system.memory.usage_percent")
            .with_description("Memory usage percentage")
            .with_callback(move |observer| {
                let mut sys = system_pct.lock().unwrap();
                sys.refresh_memory();
                let total = sys.total_memory();
                let used = sys.used_memory();
                if total > 0 {
                    let usage_percent = (used as f64 / total as f64) * 100.0;
                    observer.observe(
                        usage_percent,
                        &[
                            KeyValue::new("host", hostname_pct.clone()),
                            KeyValue::new("source", "agent"),
                        ],
                    );
                }
            })
            .build();
        
        Ok(())
    }
    
    fn register_disk_metrics(&self, meter: Meter) -> Result<()> {
        if !self.config.system_metrics.disk.enabled {
            return Ok(());
        }
        
        let hostname = self.get_hostname();
        let disks = self.disks.clone();
        let config = self.config.clone();
        
        // Disk total
        let disks_total = disks.clone();
        let hostname_total = hostname.clone();
        let config_total = config.clone();
        let _disk_total = meter
            .u64_observable_gauge("system.disk.total")
            .with_description("Total disk space in bytes")
            .with_callback(move |observer| {
                let mut disks = disks_total.lock().unwrap();
                disks.refresh();
                
                for disk in disks.iter() {
                    let mount_point = disk.mount_point().to_string_lossy().to_string();
                    
                    // Check if this mount should be included/excluded
                    if !config_total.system_metrics.disk.include_mounts.is_empty() 
                        && !config_total.system_metrics.disk.include_mounts.contains(&mount_point) {
                        continue;
                    }
                    if config_total.system_metrics.disk.exclude_mounts.contains(&mount_point) {
                        continue;
                    }
                    
                    observer.observe(
                        disk.total_space(),
                        &[
                            KeyValue::new("host", hostname_total.clone()),
                            KeyValue::new("source", "agent"),
                            KeyValue::new("mount", mount_point.clone()),
                        ],
                    );
                }
            })
            .build();
        
        // Disk used
        let disks_used = disks.clone();
        let hostname_used = hostname.clone();
        let config_used = config.clone();
        let _disk_used = meter
            .u64_observable_gauge("system.disk.used")
            .with_description("Used disk space in bytes")
            .with_callback(move |observer| {
                let mut disks = disks_used.lock().unwrap();
                disks.refresh();
                
                for disk in disks.iter() {
                    let mount_point = disk.mount_point().to_string_lossy().to_string();
                    
                    // Check if this mount should be included/excluded
                    if !config_used.system_metrics.disk.include_mounts.is_empty() 
                        && !config_used.system_metrics.disk.include_mounts.contains(&mount_point) {
                        continue;
                    }
                    if config_used.system_metrics.disk.exclude_mounts.contains(&mount_point) {
                        continue;
                    }
                    
                    let total = disk.total_space();
                    let available = disk.available_space();
                    let used = total - available;
                    
                    observer.observe(
                        used,
                        &[
                            KeyValue::new("host", hostname_used.clone()),
                            KeyValue::new("source", "agent"),
                            KeyValue::new("mount", mount_point.clone()),
                        ],
                    );
                }
            })
            .build();
        
        // Disk free
        let disks_free = disks.clone();
        let hostname_free = hostname.clone();
        let config_free = config.clone();
        let _disk_free = meter
            .u64_observable_gauge("system.disk.free")
            .with_description("Free disk space in bytes")
            .with_callback(move |observer| {
                let mut disks = disks_free.lock().unwrap();
                disks.refresh();
                
                for disk in disks.iter() {
                    let mount_point = disk.mount_point().to_string_lossy().to_string();
                    
                    // Check if this mount should be included/excluded
                    if !config_free.system_metrics.disk.include_mounts.is_empty() 
                        && !config_free.system_metrics.disk.include_mounts.contains(&mount_point) {
                        continue;
                    }
                    if config_free.system_metrics.disk.exclude_mounts.contains(&mount_point) {
                        continue;
                    }
                    
                    observer.observe(
                        disk.available_space(),
                        &[
                            KeyValue::new("host", hostname_free.clone()),
                            KeyValue::new("source", "agent"),
                            KeyValue::new("mount", mount_point.clone()),
                        ],
                    );
                }
            })
            .build();
        
        // Disk usage percent
        let disks_pct = disks.clone();
        let hostname_pct = hostname.clone();
        let config_pct = config.clone();
        let _disk_usage_pct = meter
            .f64_observable_gauge("system.disk.usage_percent")
            .with_description("Disk usage percentage")
            .with_callback(move |observer| {
                let mut disks = disks_pct.lock().unwrap();
                disks.refresh();
                
                for disk in disks.iter() {
                    let mount_point = disk.mount_point().to_string_lossy().to_string();
                    
                    // Check if this mount should be included/excluded
                    if !config_pct.system_metrics.disk.include_mounts.is_empty() 
                        && !config_pct.system_metrics.disk.include_mounts.contains(&mount_point) {
                        continue;
                    }
                    if config_pct.system_metrics.disk.exclude_mounts.contains(&mount_point) {
                        continue;
                    }
                    
                    let total = disk.total_space();
                    let available = disk.available_space();
                    let used = total - available;
                    
                    if total > 0 {
                        let usage_percent = (used as f64 / total as f64) * 100.0;
                        observer.observe(
                            usage_percent,
                            &[
                                KeyValue::new("host", hostname_pct.clone()),
                                KeyValue::new("source", "agent"),
                                KeyValue::new("mount", mount_point.clone()),
                            ],
                        );
                    }
                }
            })
            .build();
        
        Ok(())
    }
    
    fn register_network_metrics(&self, meter: Meter) -> Result<()> {
        if !self.config.system_metrics.network.enabled {
            return Ok(());
        }
        
        let hostname = self.get_hostname();
        let networks = self.networks.clone();
        let config = self.config.clone();
        
        // Network bytes received
        let networks_rx = networks.clone();
        let hostname_rx = hostname.clone();
        let config_rx = config.clone();
        let _bytes_received = meter
            .u64_observable_counter("system.network.bytes_received")
            .with_description("Bytes received over network interfaces")
            .with_callback(move |observer| {
                let mut networks = networks_rx.lock().unwrap();
                networks.refresh();
                
                for (interface_name, network) in networks.iter() {
                    // Skip loopback if configured
                    if config_rx.system_metrics.network.exclude_loopback && interface_name == "lo" {
                        continue;
                    }
                    
                    // Check if this interface should be included
                    if !config_rx.system_metrics.network.interfaces.is_empty() 
                        && !config_rx.system_metrics.network.interfaces.contains(&interface_name.to_string()) {
                        continue;
                    }
                    
                    observer.observe(
                        network.received(),
                        &[
                            KeyValue::new("host", hostname_rx.clone()),
                            KeyValue::new("source", "agent"),
                            KeyValue::new("interface", interface_name.clone()),
                        ],
                    );
                }
            })
            .build();
        
        // Network bytes sent
        let networks_tx = networks.clone();
        let hostname_tx = hostname.clone();
        let config_tx = config.clone();
        let _bytes_sent = meter
            .u64_observable_counter("system.network.bytes_sent")
            .with_description("Bytes sent over network interfaces")
            .with_callback(move |observer| {
                let mut networks = networks_tx.lock().unwrap();
                networks.refresh();
                
                for (interface_name, network) in networks.iter() {
                    // Skip loopback if configured
                    if config_tx.system_metrics.network.exclude_loopback && interface_name == "lo" {
                        continue;
                    }
                    
                    // Check if this interface should be included
                    if !config_tx.system_metrics.network.interfaces.is_empty() 
                        && !config_tx.system_metrics.network.interfaces.contains(&interface_name.to_string()) {
                        continue;
                    }
                    
                    observer.observe(
                        network.transmitted(),
                        &[
                            KeyValue::new("host", hostname_tx.clone()),
                            KeyValue::new("source", "agent"),
                            KeyValue::new("interface", interface_name.clone()),
                        ],
                    );
                }
            })
            .build();
        
        Ok(())
    }
}

impl Collector for SystemMetricsCollector {
    fn register_observables(&self, meter: Meter) -> Result<()> {
        self.register_cpu_metrics(meter.clone())?;
        self.register_memory_metrics(meter.clone())?;
        self.register_disk_metrics(meter.clone())?;
        self.register_network_metrics(meter.clone())?;
        Ok(())
    }
    
    fn name(&self) -> &str {
        "system_metrics"
    }
    
    fn enabled(&self) -> bool {
        self.config.system_metrics.enabled
    }
}

