use anyhow::{Context, Result};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use tracing::{info, warn, error};

/// Daemon configuration
pub struct DaemonConfig {
    pub pid_file: PathBuf,
    pub log_file: Option<PathBuf>,
    pub working_dir: Option<PathBuf>,
    pub user: Option<String>,
    pub group: Option<String>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            pid_file: PathBuf::from("/var/run/reiver-agent.pid"),
            log_file: Some(PathBuf::from("/var/log/reiver-agent.log")),
            working_dir: Some(PathBuf::from("/")),
            user: None,
            group: None,
        }
    }
}

/// Write PID to file
pub fn write_pid_file(pid_file: &Path, pid: u32) -> Result<()> {
    // Create parent directory if it doesn't exist
    if let Some(parent) = pid_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create PID file directory: {}", parent.display()))?;
    }

    let mut file = fs::File::create(pid_file)
        .with_context(|| format!("Failed to create PID file: {}", pid_file.display()))?;
    
    file.write_all(pid.to_string().as_bytes())
        .with_context(|| format!("Failed to write PID to file: {}", pid_file.display()))?;
    
    file.sync_all()
        .with_context(|| format!("Failed to sync PID file: {}", pid_file.display()))?;
    
    info!("Wrote PID {} to {}", pid, pid_file.display());
    Ok(())
}

/// Read PID from file
pub fn read_pid_file(pid_file: &Path) -> Result<Option<u32>> {
    if !pid_file.exists() {
        return Ok(None);
    }

    let pid_str = fs::read_to_string(pid_file)
        .with_context(|| format!("Failed to read PID file: {}", pid_file.display()))?;
    
    let pid = pid_str.trim().parse::<u32>()
        .with_context(|| format!("Invalid PID in file: {}", pid_file.display()))?;
    
    Ok(Some(pid))
}

/// Remove PID file
pub fn remove_pid_file(pid_file: &Path) -> Result<()> {
    if pid_file.exists() {
        fs::remove_file(pid_file)
            .with_context(|| format!("Failed to remove PID file: {}", pid_file.display()))?;
        info!("Removed PID file: {}", pid_file.display());
    }
    Ok(())
}

/// Check if process with given PID is still running
#[cfg(unix)]
pub fn is_process_running(pid: u32) -> bool {
    // Signal 0 doesn't actually send a signal, but checks if process exists
    // Use raw libc call to avoid nix dependency complexity
    unsafe {
        libc::kill(pid as i32, 0) == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
}

#[cfg(windows)]
pub fn is_process_running(pid: u32) -> bool {
    use std::process::Command;
    
    // On Windows, try to query the process
    let output = Command::new("tasklist")
        .arg("/FI")
        .arg(format!("PID eq {}", pid))
        .output();
    
    match output {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout.contains(&pid.to_string())
        }
        Err(_) => false,
    }
}

/// Check if agent is already running based on PID file
pub fn is_already_running(pid_file: &Path) -> Result<bool> {
    if let Some(pid) = read_pid_file(pid_file)? {
        if is_process_running(pid) {
            return Ok(true);
        } else {
            // PID file exists but process is dead, remove stale PID file
            warn!("Stale PID file found (process {} not running), removing it", pid);
            remove_pid_file(pid_file)?;
        }
    }
    Ok(false)
}

/// Setup signal handlers for graceful shutdown
#[cfg(unix)]
pub fn setup_signal_handlers() -> Result<tokio::signal::unix::Signal> {
    use tokio::signal::unix::{signal, SignalKind};
    
                    let sigterm = signal(SignalKind::terminate())
        .context("Failed to create SIGTERM signal handler")?;
    
    // Also handle SIGINT (Ctrl+C) for foreground mode
    tokio::spawn(async {
        if let Ok(mut sigint) = signal(SignalKind::interrupt()) {
            loop {
                if sigint.recv().await.is_some() {
                    info!("Received SIGINT, shutting down...");
                    process::exit(0);
                }
            }
        }
    });
    
    Ok(sigterm)
}

#[cfg(windows)]
pub fn setup_signal_handlers() -> Result<()> {
    // Windows uses Ctrl+C handler
    tokio::spawn(async {
        tokio::signal::ctrl_c().await.ok();
        info!("Received Ctrl+C, shutting down...");
        process::exit(0);
    });
    
    Ok(())
}

/// Daemonize the current process (Unix only)
#[cfg(unix)]
pub fn daemonize(config: &DaemonConfig) -> Result<()> {
    use std::os::unix::io::{IntoRawFd, FromRawFd};
    
    // Check if already running
    if is_already_running(&config.pid_file)? {
        anyhow::bail!("Reiver Agent is already running (check PID file: {})", config.pid_file.display());
    }
    
    // Fork the process
    let pid = unsafe { libc::fork() };
    
    match pid {
        -1 => {
            // Fork failed
            Err(anyhow::anyhow!("Failed to fork process: {}", std::io::Error::last_os_error()))
        }
        0 => {
            // Child process
            
            // Create new session (detach from terminal)
            let sid = unsafe { libc::setsid() };
            if sid == -1 {
                return Err(anyhow::anyhow!("Failed to create new session: {}", std::io::Error::last_os_error()));
            }
            
            // Change to working directory
            if let Some(ref working_dir) = config.working_dir {
                let dir_c_str = std::ffi::CString::new(working_dir.to_string_lossy().as_ref())
                    .context("Failed to convert working directory to CString")?;
                if unsafe { libc::chdir(dir_c_str.as_ptr()) } != 0 {
                    return Err(anyhow::anyhow!("Failed to change to working directory: {}", std::io::Error::last_os_error()));
                }
            }
            
            // Redirect stdin, stdout, stderr
            if let Some(ref log_file) = config.log_file {
                let log_file_path = log_file.as_path();
                
                // Create log file if it doesn't exist
                if let Some(parent) = log_file_path.parent() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("Failed to create log directory: {}", parent.display()))?;
                }
                
                let file = fs::File::create(log_file_path)
                    .with_context(|| format!("Failed to create log file: {}", log_file_path.display()))?;
                
                // Redirect stdout and stderr to log file
                let fd = file.into_raw_fd();
                unsafe {
                    libc::dup2(fd, libc::STDOUT_FILENO);
                    libc::dup2(fd, libc::STDERR_FILENO);
                    libc::close(fd);
                    // Close stdin
                    let dev_null = libc::open(b"/dev/null\0".as_ptr() as *const i8, libc::O_RDONLY);
                    if dev_null >= 0 {
                        libc::dup2(dev_null, libc::STDIN_FILENO);
                        libc::close(dev_null);
                    }
                }
            }
            
            // Write PID file from child (in case parent died before writing it)
            let child_pid = unsafe { libc::getpid() } as u32;
            write_pid_file(&config.pid_file, child_pid)?;
            
            info!("Daemonized successfully, PID: {}", child_pid);
            Ok(())
        }
        child_pid => {
            // Parent process
            info!("Forked child process with PID: {}", child_pid);
            // Write PID file from parent
            write_pid_file(&config.pid_file, child_pid as u32)?;
            // Exit parent process
            process::exit(0);
        }
    }
}

#[cfg(windows)]
pub fn daemonize(_config: &DaemonConfig) -> Result<()> {
    // On Windows, daemonization is different - typically done via service manager
    // For now, just run in background without fork
    warn!("Windows daemonization not fully implemented - consider using service manager");
    Ok(())
}

/// Stop the daemon by reading PID file and sending SIGTERM
#[cfg(unix)]
pub fn stop_daemon(pid_file: &Path) -> Result<()> {
    if let Some(pid) = read_pid_file(pid_file)? {
        if is_process_running(pid) {
            info!("Sending SIGTERM to process {}", pid);
            // Send SIGTERM
            unsafe {
                if libc::kill(pid as i32, libc::SIGTERM) != 0 {
                    return Err(anyhow::anyhow!("Failed to send SIGTERM to process {}: {}", pid, std::io::Error::last_os_error()));
                }
            }
            
            // Wait a bit for graceful shutdown
            std::thread::sleep(std::time::Duration::from_secs(2));
            
            // Check if process is still running
            if is_process_running(pid) {
                warn!("Process {} didn't terminate, sending SIGKILL", pid);
                unsafe {
                    if libc::kill(pid as i32, libc::SIGKILL) != 0 {
                        return Err(anyhow::anyhow!("Failed to send SIGKILL to process {}: {}", pid, std::io::Error::last_os_error()));
                    }
                }
            }
            
            remove_pid_file(pid_file)?;
            info!("Stopped daemon process {}", pid);
        } else {
            warn!("Process {} is not running", pid);
            remove_pid_file(pid_file)?;
        }
    } else {
        warn!("No PID file found at {}", pid_file.display());
    }
    
    Ok(())
}

#[cfg(windows)]
pub fn stop_daemon(pid_file: &Path) -> Result<()> {
    use std::process::Command;
    
    if let Some(pid) = read_pid_file(pid_file)? {
        if is_process_running(pid) {
            info!("Terminating process {}", pid);
            Command::new("taskkill")
                .args(&["/F", "/PID", &pid.to_string()])
                .output()
                .context(format!("Failed to terminate process {}", pid))?;
            
            remove_pid_file(pid_file)?;
            info!("Stopped daemon process {}", pid);
        } else {
            warn!("Process {} is not running", pid);
            remove_pid_file(pid_file)?;
        }
    } else {
        warn!("No PID file found at {}", pid_file.display());
    }
    
    Ok(())
}
