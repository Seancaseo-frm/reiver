use anyhow::{Result, Context};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{info, error, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod agent;
mod config;
mod metrics;
mod database;
mod http;
mod daemon;
mod health_checks;

#[derive(Parser)]
#[command(name = "reiver-agent")]
#[command(about = "Reiver Agent - Collects system and database metrics")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the agent (default behavior)
    Run {
        /// Path to configuration file
        #[arg(short, long, default_value = "/etc/reiver/reiver.toml")]
        config: PathBuf,
        
        /// Run in foreground (don't daemonize)
        #[arg(short, long)]
        foreground: bool,
    },
    /// Install the agent as a system service
    Install {
        /// Path to configuration file
        #[arg(short, long, default_value = "/etc/reiver/reiver.toml")]
        config: PathBuf,
    },
    /// Uninstall the agent service
    Uninstall,
    /// Validate configuration file
    Validate {
        /// Path to configuration file
        #[arg(short, long)]
        config: PathBuf,
    },
}

fn main() -> Result<()> {
    // Initialize basic logging first (before daemonization)
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .init();

    let cli = Cli::parse();
    
    // Handle non-async commands first
    match &cli.command {
        Commands::Install { config } => {
            return tokio::runtime::Runtime::new()
                .context("Failed to create tokio runtime")?
                .block_on(install_service(config));
        }
        Commands::Uninstall => {
            return tokio::runtime::Runtime::new()
                .context("Failed to create tokio runtime")?
                .block_on(uninstall_service());
        }
        Commands::Validate { config } => {
            match config::Config::from_file(config) {
                Ok(cfg) => {
                    println!("✅ Configuration file is valid: {}", config.display());
                    println!("   - API URL: {}", cfg.api.url);
                    println!("   - System metrics: {}", if cfg.system_metrics.enabled { "enabled" } else { "disabled" });
                    println!("   - Databases configured: {}", cfg.databases.len());
                    return Ok(());
                }
                Err(e) => {
                    eprintln!("❌ Configuration file is invalid: {}", e);
                    std::process::exit(1);
                }
            }
        }
        _ => {}
    }

    // For Run command, check if daemonization is needed BEFORE initializing tokio
    if let Commands::Run { foreground, .. } = &cli.command {
        if !foreground {
            // Daemonize before initializing tokio runtime
            let daemon_config = daemon::DaemonConfig {
                pid_file: PathBuf::from("/var/run/reiver-agent.pid"),
                log_file: Some(PathBuf::from("/var/log/reiver-agent.log")),
                working_dir: Some(PathBuf::from("/")),
                ..Default::default()
            };
            
            #[cfg(unix)]
            daemon::daemonize(&daemon_config)?;
            
            #[cfg(windows)]
            {
                warn!("Windows daemonization not fully implemented - running in background");
                daemon::daemonize(&daemon_config)?;
            }
        }
    }

    // Now run the async main function
    tokio::runtime::Runtime::new()
        .context("Failed to create tokio runtime")?
        .block_on(async_main(cli))
}

async fn async_main(cli: Cli) -> Result<()> {
    // Initialize OpenTelemetry tracer and meter if configured
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "reiver-agent".to_string());
    
    // Initialize tracing and metrics with OpenTelemetry if endpoint is configured
    let meter = if let Some(endpoint) = otlp_endpoint.as_ref() {
        info!("Initializing OpenTelemetry tracer and meter, sending to: {}", endpoint);
        init_opentelemetry(endpoint, &service_name).await?
    } else {
        info!("OpenTelemetry not configured (set OTEL_EXPORTER_OTLP_ENDPOINT to enable)");
        return Err(anyhow::anyhow!("OTEL_EXPORTER_OTLP_ENDPOINT must be set for metrics export"));
    };

    // Build subscriber with OpenTelemetry layer
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    
    let fmt_layer = tracing_subscriber::fmt::layer();
    let otel_layer = tracing_opentelemetry::layer().with_tracer(opentelemetry::global::tracer("reiver-agent"));
    
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    match cli.command {
        Commands::Run { config, foreground } => {
            info!("Starting Reiver Agent...");
            info!("Loading configuration from: {}", config.display());
            
            // Load configuration
            let agent_config = config::Config::from_file(&config)?;
            info!("Configuration loaded successfully");
            
            // Create and run agent (pass meter for OpenTelemetry metrics)
            let mut agent = agent::Agent::new(agent_config, meter).await?;
            
            // Setup signal handlers for graceful shutdown
            let daemon_config = daemon::DaemonConfig {
                pid_file: PathBuf::from("/var/run/reiver-agent.pid"),
                log_file: Some(PathBuf::from("/var/log/reiver-agent.log")),
                working_dir: Some(PathBuf::from("/")),
                ..Default::default()
            };
            
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate())
                    .context("Failed to create SIGTERM signal handler")?;
                
                // Write PID file (for both foreground and daemon mode)
                if !foreground {
                    let pid = std::process::id();
                    daemon::write_pid_file(&daemon_config.pid_file, pid)?;
                }
                
                let pid_file = daemon_config.pid_file.clone();
                tokio::spawn(async move {
                    sigterm.recv().await;
                    info!("Received SIGTERM, shutting down gracefully...");
                    daemon::remove_pid_file(&pid_file).ok();
                    std::process::exit(0);
                });
                
                // Also handle SIGINT for foreground mode
                if foreground {
                    let mut sigint = signal(SignalKind::interrupt()).unwrap();
                    tokio::spawn(async move {
                        sigint.recv().await;
                        info!("Received SIGINT, shutting down...");
                        std::process::exit(0);
                    });
                }
            }
            #[cfg(windows)]
            {
                tokio::spawn(async {
                    tokio::signal::ctrl_c().await.ok();
                    info!("Received Ctrl+C, shutting down...");
                    std::process::exit(0);
                });
            }
            
            if foreground {
                info!("Running in foreground mode");
            } else {
                info!("Running as daemon (PID file: {})", daemon_config.pid_file.display());
            }
            
            agent.run().await?;
        }
        // These commands are handled in main() before reaching async_main
        Commands::Install { .. } | Commands::Uninstall | Commands::Validate { .. } => {
            unreachable!("Install, Uninstall, and Validate commands are handled in main()");
        }
    }

    // Flush any remaining traces and metrics (0.31 API)
    // Shutdown is handled by dropping the providers at the end of main
    
    Ok(())
}

/// Initialize OpenTelemetry tracer and meter (metrics)
async fn init_opentelemetry(endpoint: &str, service_name: &str) -> Result<opentelemetry::metrics::Meter> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;

    let service_name_string = service_name.to_string();
    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", service_name_string.clone()),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    let endpoint_string = endpoint.to_string();
    
    // Initialize trace exporter - using 0.31 API
    let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint_string.clone())
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build OTLP span exporter: {}", e))?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(trace_exporter)
        .with_resource(resource.clone())
        .build();

    let _tracer = tracer_provider.tracer("reiver-agent");
    opentelemetry::global::set_tracer_provider(tracer_provider);

    // Initialize metrics exporter - using 0.31 API (MetricExporter, not MetricsExporter)
    let metrics_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint_string)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build OTLP metric exporter: {}", e))?;

    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metrics_exporter)
        .with_resource(resource)
        .build();

    let meter = meter_provider.meter("reiver-agent");
    opentelemetry::global::set_meter_provider(meter_provider);

    Ok(meter)
}

/// Install Reiver Agent as a system service
async fn install_service(config_path: &PathBuf) -> Result<()> {
    use std::fs;
    use std::io::Write;
    use std::env;
    
    // Get the current executable path
    let exe_path = env::current_exe()
        .context("Failed to get current executable path")?;
    
    // Get current user
    let user = env::var("USER").unwrap_or_else(|_| "root".to_string());
    
    #[cfg(target_os = "linux")]
    {
        // Create systemd service file
        let service_file = "/etc/systemd/system/reiver-agent.service";
        
        info!("Creating systemd service file: {}", service_file);
        
        let service_content = format!(
            r#"[Unit]
Description=Reiver Agent - Collects system and database metrics
After=network.target

[Service]
Type=simple
User={}
ExecStart={} run --config {} --foreground
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"#,
            user,
            exe_path.display(),
            config_path.display()
        );
        
        // Check if running as root
        #[cfg(unix)]
        {
            if unsafe { libc::getuid() } != 0 {
                return Err(anyhow::anyhow!("Service installation requires root privileges. Please run with sudo."));
            }
        }
        
        fs::write(service_file, service_content)
            .context(format!("Failed to write service file: {}", service_file))?;
        
        info!("Service file created successfully");
        info!("Run 'systemctl daemon-reload' to reload systemd");
        info!("Run 'systemctl enable reiver-agent' to enable the service");
        info!("Run 'systemctl start reiver-agent' to start the service");
    }
    
    #[cfg(target_os = "macos")]
    {
        // Create launchd plist file
        let plist_file = format!("/Library/LaunchDaemons/com.reiver.agent.plist");
        
        info!("Creating launchd plist file: {}", plist_file);
        
        let plist_content = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.reiver.agent</string>
    <key>ProgramArguments</key>
    <array>
        <string>{}</string>
        <string>run</string>
        <string>--config</string>
        <string>{}</string>
        <string>--foreground</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>/var/log/reiver-agent.log</string>
    <key>StandardErrorPath</key>
    <string>/var/log/reiver-agent.error.log</string>
</dict>
</plist>
"#,
            exe_path.display(),
            config_path.display()
        );
        
        // Check if running as root
        #[cfg(unix)]
        {
            if unsafe { libc::getuid() } != 0 {
                return Err(anyhow::anyhow!("Service installation requires root privileges. Please run with sudo."));
            }
        }
        
        fs::write(&plist_file, plist_content)
            .context(format!("Failed to write plist file: {}", plist_file))?;
        
        info!("Plist file created successfully");
        info!("Run 'launchctl load {}' to load the service", plist_file);
        info!("Run 'launchctl start com.reiver.agent' to start the service");
    }
    
    #[cfg(windows)]
    {
        warn!("Windows service installation not yet implemented");
        return Err(anyhow::anyhow!("Windows service installation not yet implemented"));
    }
    
    Ok(())
}

/// Uninstall Reiver Agent service
async fn uninstall_service() -> Result<()> {
    use std::fs;
    
    #[cfg(target_os = "linux")]
    {
        // Stop and disable systemd service
        let service_file = "/etc/systemd/system/reiver-agent.service";
        
        // Check if running as root
        #[cfg(unix)]
        {
            if unsafe { libc::getuid() } != 0 {
                return Err(anyhow::anyhow!("Service uninstallation requires root privileges. Please run with sudo."));
            }
        }
        
        // Stop the service first
        let _ = std::process::Command::new("systemctl")
            .args(&["stop", "reiver-agent"])
            .output();
        
        // Disable the service
        let _ = std::process::Command::new("systemctl")
            .args(&["disable", "reiver-agent"])
            .output();
        
        // Remove service file
        if fs::metadata(service_file).is_ok() {
            fs::remove_file(service_file)
                .context(format!("Failed to remove service file: {}", service_file))?;
            info!("Removed service file: {}", service_file);
        }
        
        info!("Service uninstalled successfully");
        info!("Run 'systemctl daemon-reload' to reload systemd");
    }
    
    #[cfg(target_os = "macos")]
    {
        // Unload and remove launchd plist
        let plist_file = "/Library/LaunchDaemons/com.reiver.agent.plist";
        
        // Check if running as root
        #[cfg(unix)]
        {
            if unsafe { libc::getuid() } != 0 {
                return Err(anyhow::anyhow!("Service uninstallation requires root privileges. Please run with sudo."));
            }
        }
        
        // Unload the service
        let _ = std::process::Command::new("launchctl")
            .args(&["unload", plist_file])
            .output();
        
        // Stop the service
        let _ = std::process::Command::new("launchctl")
            .args(&["stop", "com.reiver.agent"])
            .output();
        
        // Remove plist file
        if fs::metadata(plist_file).is_ok() {
            fs::remove_file(plist_file)
                .context(format!("Failed to remove plist file: {}", plist_file))?;
            info!("Removed plist file: {}", plist_file);
        }
        
        info!("Service uninstalled successfully");
    }
    
    #[cfg(windows)]
    {
        warn!("Windows service uninstallation not yet implemented");
        return Err(anyhow::anyhow!("Windows service uninstallation not yet implemented"));
    }
    
    Ok(())
}

