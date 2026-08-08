//! Process memory and allocator metrics.
//!
//! Provides [`observe_memory`] — a single function that registers OpenTelemetry
//! observable instruments for process-level memory stats and, when a supported
//! allocator feature flag is enabled, detailed allocator-internal metrics.
//!
//! ## Process-Level Metrics (always available on Linux and macOS)
//!
//! | Metric                       | OTel Type          | Description                     |
//! |------------------------------|--------------------|---------------------------------|
//! | `process.memory.rss`         | Observable Gauge   | Resident Set Size (bytes)       |
//! | `process.memory.virtual`     | Observable Gauge   | Virtual address space (bytes)   |
//! | `process.memory.page_faults` | Observable Counter | Major page faults               |
//!
//! ## Allocator Metrics (behind feature flags)
//!
//! Enable `"jemalloc"` or `"mimalloc"` to get allocator internals.
//! All carry an `allocator.name` attribute.
//!
//! | Metric                | jemalloc | mimalloc | Description                              |
//! |-----------------------|----------|----------|------------------------------------------|
//! | `allocator.allocated` | ✓        | ✓        | Bytes handed out to the application      |
//! | `allocator.active`    | ✓        |          | Bytes in active allocator pages          |
//! | `allocator.resident`  | ✓        | ✓        | Allocator's view of physical memory      |
//! | `allocator.mapped`    | ✓        |          | Virtual memory mapped by the allocator   |
//! | `allocator.retained`  | ✓        |          | Memory held back from the OS             |
//!
//! ## Example
//!
//! ```no_run
//! reiver_sdk::observe_memory().expect("failed to register memory metrics");
//! ```

#[cfg(all(feature = "jemalloc", feature = "mimalloc"))]
compile_error!("Features 'jemalloc' and 'mimalloc' are mutually exclusive. Enable only one.");

#[cfg(any(feature = "jemalloc", feature = "mimalloc"))]
use opentelemetry::KeyValue;

/// Errors from memory metric registration.
#[derive(Debug, thiserror::Error)]
pub enum MemoryError {
    /// An allocator-specific initialisation step failed.
    #[error("allocator metric registration failed: {0}")]
    AllocatorInit(String),
}

// ---------------------------------------------------------------------------
// Platform: macOS Mach FFI (only what we need)
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
mod mach_ffi {
    use std::ffi::c_int;

    pub const MACH_TASK_BASIC_INFO: u32 = 20;
    pub const KERN_SUCCESS: c_int = 0;

    #[repr(C)]
    pub struct TimeValue {
        pub seconds: c_int,
        pub microseconds: c_int,
    }

    #[repr(C)]
    pub struct MachTaskBasicInfo {
        pub virtual_size: u64,
        pub resident_size: u64,
        pub resident_size_max: u64,
        pub user_time: TimeValue,
        pub system_time: TimeValue,
        pub policy: c_int,
        pub suspend_count: c_int,
    }

    extern "C" {
        pub static mach_task_self_: u32;
        pub fn task_info(
            target_task: u32,
            flavor: u32,
            task_info_out: *mut c_int,
            task_info_out_cnt: *mut u32,
        ) -> c_int;
    }
}

// ---------------------------------------------------------------------------
// Platform-specific: read process memory (RSS + virtual)
// ---------------------------------------------------------------------------

struct ProcessMemory {
    rss_bytes: u64,
    virtual_bytes: u64,
}

#[cfg(target_os = "linux")]
fn read_process_memory() -> Option<ProcessMemory> {
    let content = std::fs::read_to_string("/proc/self/status").ok()?;
    let mut rss_kb: Option<u64> = None;
    let mut vsize_kb: Option<u64> = None;
    for line in content.lines() {
        if let Some(val) = line.strip_prefix("VmRSS:") {
            rss_kb = val
                .trim()
                .strip_suffix(" kB")
                .and_then(|s| s.trim().parse().ok());
        } else if let Some(val) = line.strip_prefix("VmSize:") {
            vsize_kb = val
                .trim()
                .strip_suffix(" kB")
                .and_then(|s| s.trim().parse().ok());
        }
        if rss_kb.is_some() && vsize_kb.is_some() {
            break;
        }
    }
    Some(ProcessMemory {
        rss_bytes: rss_kb? * 1024,
        virtual_bytes: vsize_kb? * 1024,
    })
}

#[cfg(target_os = "macos")]
fn read_process_memory() -> Option<ProcessMemory> {
    use std::ffi::c_int;

    unsafe {
        let mut info: mach_ffi::MachTaskBasicInfo = std::mem::zeroed();
        let mut count = (std::mem::size_of::<mach_ffi::MachTaskBasicInfo>()
            / std::mem::size_of::<u32>()) as u32;
        let kr = mach_ffi::task_info(
            mach_ffi::mach_task_self_,
            mach_ffi::MACH_TASK_BASIC_INFO,
            &mut info as *mut _ as *mut c_int,
            &mut count,
        );
        if kr != mach_ffi::KERN_SUCCESS {
            return None;
        }
        Some(ProcessMemory {
            rss_bytes: info.resident_size,
            virtual_bytes: info.virtual_size,
        })
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_process_memory() -> Option<ProcessMemory> {
    None
}

// ---------------------------------------------------------------------------
// Platform-specific: read major page faults
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn read_major_page_faults() -> Option<u64> {
    let content = std::fs::read_to_string("/proc/self/stat").ok()?;
    let close_paren = content.rfind(')')?;
    let after_comm = content.get(close_paren + 2..)?;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    // After ") state", field indices (0-based from after closing paren):
    // 0=state 1=ppid 2=pgrp 3=session 4=tty_nr 5=tpgid
    // 6=flags 7=minflt 8=cminflt 9=majflt
    fields.get(9)?.parse().ok()
}

#[cfg(target_os = "macos")]
fn read_major_page_faults() -> Option<u64> {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
            Some(usage.ru_majflt as u64)
        } else {
            None
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_major_page_faults() -> Option<u64> {
    None
}

// ---------------------------------------------------------------------------
// OTel registration: process-level metrics
// ---------------------------------------------------------------------------

fn register_process_metrics(meter: &opentelemetry::metrics::Meter) {
    let _ = meter
        .u64_observable_gauge("process.memory.rss")
        .with_description("Resident Set Size: physical memory the process occupies")
        .with_unit("By")
        .with_callback(|obs| {
            if let Some(mem) = read_process_memory() {
                obs.observe(mem.rss_bytes, &[]);
            }
        })
        .build();

    let _ = meter
        .u64_observable_gauge("process.memory.virtual")
        .with_description("Virtual address space size")
        .with_unit("By")
        .with_callback(|obs| {
            if let Some(mem) = read_process_memory() {
                obs.observe(mem.virtual_bytes, &[]);
            }
        })
        .build();

    let _ = meter
        .u64_observable_counter("process.memory.page_faults")
        .with_description("Major page faults requiring physical I/O")
        .with_callback(|obs| {
            if let Some(faults) = read_major_page_faults() {
                obs.observe(faults, &[]);
            }
        })
        .build();
}

// ---------------------------------------------------------------------------
// OTel registration: jemalloc allocator metrics
// ---------------------------------------------------------------------------

#[cfg(feature = "jemalloc")]
fn register_jemalloc_metrics(meter: &opentelemetry::metrics::Meter) -> Result<(), MemoryError> {
    tikv_jemalloc_ctl::epoch::advance()
        .map_err(|e| MemoryError::AllocatorInit(format!("jemalloc epoch advance: {e}")))?;

    let _ = meter
        .u64_observable_gauge("allocator.allocated")
        .with_description("Bytes currently allocated by the application")
        .with_unit("By")
        .with_callback(|obs| {
            let _ = tikv_jemalloc_ctl::epoch::advance();
            if let Ok(v) = tikv_jemalloc_ctl::stats::allocated::read() {
                obs.observe(v as u64, &[KeyValue::new("allocator.name", "jemalloc")]);
            }
        })
        .build();

    let _ = meter
        .u64_observable_gauge("allocator.active")
        .with_description("Bytes in active allocator pages")
        .with_unit("By")
        .with_callback(|obs| {
            let _ = tikv_jemalloc_ctl::epoch::advance();
            if let Ok(v) = tikv_jemalloc_ctl::stats::active::read() {
                obs.observe(v as u64, &[KeyValue::new("allocator.name", "jemalloc")]);
            }
        })
        .build();

    let _ = meter
        .u64_observable_gauge("allocator.resident")
        .with_description("Physical memory used by the allocator")
        .with_unit("By")
        .with_callback(|obs| {
            let _ = tikv_jemalloc_ctl::epoch::advance();
            if let Ok(v) = tikv_jemalloc_ctl::stats::resident::read() {
                obs.observe(v as u64, &[KeyValue::new("allocator.name", "jemalloc")]);
            }
        })
        .build();

    let _ = meter
        .u64_observable_gauge("allocator.mapped")
        .with_description("Virtual memory mapped by the allocator")
        .with_unit("By")
        .with_callback(|obs| {
            let _ = tikv_jemalloc_ctl::epoch::advance();
            if let Ok(v) = tikv_jemalloc_ctl::stats::mapped::read() {
                obs.observe(v as u64, &[KeyValue::new("allocator.name", "jemalloc")]);
            }
        })
        .build();

    let _ = meter
        .u64_observable_gauge("allocator.retained")
        .with_description("Memory held back from the OS for future reuse")
        .with_unit("By")
        .with_callback(|obs| {
            let _ = tikv_jemalloc_ctl::epoch::advance();
            if let Ok(v) = tikv_jemalloc_ctl::stats::retained::read() {
                obs.observe(v as u64, &[KeyValue::new("allocator.name", "jemalloc")]);
            }
        })
        .build();

    tracing::info!(
        allocator = "jemalloc",
        "Registered 5 jemalloc allocator OTel metrics"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// OTel registration: mimalloc allocator metrics
// ---------------------------------------------------------------------------

#[cfg(feature = "mimalloc")]
fn register_mimalloc_metrics(meter: &opentelemetry::metrics::Meter) {
    let _ = meter
        .u64_observable_gauge("allocator.allocated")
        .with_description("Bytes currently allocated by the application")
        .with_unit("By")
        .with_callback(|obs| {
            let info = read_mimalloc_info();
            obs.observe(
                info.current_commit as u64,
                &[KeyValue::new("allocator.name", "mimalloc")],
            );
        })
        .build();

    let _ = meter
        .u64_observable_gauge("allocator.resident")
        .with_description("Physical memory used by the allocator")
        .with_unit("By")
        .with_callback(|obs| {
            let info = read_mimalloc_info();
            obs.observe(
                info.current_rss as u64,
                &[KeyValue::new("allocator.name", "mimalloc")],
            );
        })
        .build();

    tracing::info!(
        allocator = "mimalloc",
        "Registered 2 mimalloc allocator OTel metrics"
    );
}

#[cfg(feature = "mimalloc")]
struct MimallocInfo {
    current_rss: usize,
    current_commit: usize,
}

#[cfg(feature = "mimalloc")]
fn read_mimalloc_info() -> MimallocInfo {
    let mut elapsed_msecs: usize = 0;
    let mut user_msecs: usize = 0;
    let mut system_msecs: usize = 0;
    let mut current_rss: usize = 0;
    let mut peak_rss: usize = 0;
    let mut current_commit: usize = 0;
    let mut peak_commit: usize = 0;
    let mut page_faults: usize = 0;

    unsafe {
        libmimalloc_sys::mi_process_info(
            &mut elapsed_msecs,
            &mut user_msecs,
            &mut system_msecs,
            &mut current_rss,
            &mut peak_rss,
            &mut current_commit,
            &mut peak_commit,
            &mut page_faults,
        );
    }

    MimallocInfo {
        current_rss,
        current_commit,
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Register process-level and allocator memory metrics as OpenTelemetry
/// observable instruments.
///
/// Always registers process-level metrics (RSS, virtual size, page faults).
/// If the `"jemalloc"` or `"mimalloc"` feature is enabled, also registers
/// detailed allocator metrics automatically.
///
/// Call once after setting the global meter provider.
pub fn observe_memory() -> Result<(), MemoryError> {
    let meter = opentelemetry::global::meter_provider().meter("reiver-sdk");

    register_process_metrics(&meter);

    #[cfg(feature = "jemalloc")]
    register_jemalloc_metrics(&meter)?;

    #[cfg(feature = "mimalloc")]
    register_mimalloc_metrics(&meter);

    let allocator_name = if cfg!(feature = "jemalloc") {
        "jemalloc"
    } else if cfg!(feature = "mimalloc") {
        "mimalloc"
    } else {
        "system"
    };

    tracing::info!(
        allocator = allocator_name,
        "Registered memory OTel metrics (3 process-level + allocator-specific)"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_process_memory() {
        let mem = read_process_memory();
        // Should succeed on Linux and macOS
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            let mem = mem.expect("read_process_memory should succeed on this platform");
            assert!(mem.rss_bytes > 0, "RSS should be nonzero");
            assert!(mem.virtual_bytes > 0, "Virtual size should be nonzero");
            assert!(
                mem.virtual_bytes >= mem.rss_bytes,
                "Virtual should be >= RSS"
            );
        }
    }

    #[test]
    fn test_read_major_page_faults() {
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            let faults = read_major_page_faults();
            assert!(faults.is_some(), "page faults should be readable");
        }
    }
}
