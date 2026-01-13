//! mqlite - A high-performance MQTT broker.

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Configure jemalloc to return memory to OS.
/// - retain:false - Actually unmap pages instead of keeping address space
/// - dirty_decay_ms:0 - Immediately purge dirty pages (keeps active memory low)
/// - muzzy_decay_ms:0 - Immediately purge muzzy pages
/// - background_thread:true - Enable background thread for decay
#[cfg(feature = "jemalloc")]
#[used]
#[export_name = "malloc_conf"]
static MALLOC_CONF: &[u8] =
    b"retain:false,dirty_decay_ms:0,muzzy_decay_ms:0,background_thread:true\0";

mod auth;
mod bridge;
mod cleanup;
mod client;
mod client_handle;
mod config;
mod fanout;
mod handlers;
mod prometheus;
mod proxy;
mod publish_encoder;
mod route_cache;
mod server;
mod shared;
mod subscription;
mod sys_tree;
mod util;
mod websocket;
mod will;
mod worker;
mod write_buffer;

use std::sync::Arc;

use log::{error, info};

use crate::config::Config;
use crate::server::Server;

/// Force jemalloc to release memory back to the OS.
#[cfg(feature = "jemalloc")]
pub fn jemalloc_purge() {
    use tikv_jemalloc_ctl::epoch;

    // Advance epoch to get current stats
    if let Err(e) = epoch::advance() {
        log::debug!("jemalloc epoch advance failed: {}", e);
        return;
    }

    // Purge all arenas using raw mallctl API
    // MALLCTL_ARENAS_ALL = 4096 - purges all arenas
    // The key is "arena.<i>.purge" where <i> is the arena index
    // For void operations, call mallctl with null pointers
    let ret = unsafe {
        tikv_jemalloc_sys::mallctl(
            c"arena.4096.purge".as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    };
    if ret != 0 {
        log::debug!("jemalloc purge failed: error code {}", ret);
    }
}

#[cfg(not(feature = "jemalloc"))]
pub fn jemalloc_purge() {
    // Use glibc's malloc_trim to release memory back to OS
    #[cfg(target_os = "linux")]
    unsafe {
        libc::malloc_trim(0);
    }
}

struct Args {
    config_path: String,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut config_path = "mqlite.toml".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-c" | "--config" => {
                if i + 1 < args.len() {
                    config_path = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: -c requires a file path");
                    std::process::exit(1);
                }
            }
            "-h" | "--help" => {
                println!("mqlite - High-performance MQTT broker");
                println!();
                println!("Usage: mqlite [OPTIONS]");
                println!();
                println!("Options:");
                println!("  -c, --config <FILE>     Config file path (default: mqlite.toml)");
                println!("  -h, --help              Show this help message");
                println!();
                println!("Configuration:");
                println!("  Config file uses TOML format. All settings can be overridden");
                println!("  with environment variables using MQLITE__ prefix:");
                println!();
                println!("  MQLITE__SERVER__BIND=0.0.0.0:1884");
                println!("  MQLITE__SERVER__WORKERS=4");
                println!("  MQLITE__LIMITS__MAX_PACKET_SIZE=2097152");
                println!("  MQLITE__LOG__LEVEL=debug");
                std::process::exit(0);
            }
            arg => {
                eprintln!("Unknown argument: {}", arg);
                eprintln!("Use --help for usage information");
                std::process::exit(1);
            }
        }
    }

    Args { config_path }
}

fn main() {
    // Parse CLI args first (only for config path and help)
    let args = parse_args();

    // Load configuration from file + environment variables
    let config = match Config::load(&args.config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load configuration: {}", e);
            std::process::exit(1);
        }
    };

    // Initialize logger with configured level
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&config.log.level))
        .init();

    info!("Loaded configuration from {}", args.config_path);

    // Determine worker count (0 = auto)
    let num_workers = if config.server.workers == 0 {
        num_cpus::get()
    } else {
        config.server.workers
    };

    // Build ID to verify Docker builds - change this when making fixes
    const BUILD_ID: &str = "2026-01-09-jemalloc-purge";

    info!(
        "Starting mqlite [build={}] with {} worker threads (max_packet_size={}KB, max_inflight={})",
        BUILD_ID,
        num_workers,
        config.limits.max_packet_size / 1024,
        config.limits.max_inflight
    );

    let config = Arc::new(config);

    let mut server = match Server::new(config.server.bind, num_workers, config) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to start server: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(e) = server.run() {
        error!("Server error: {}", e);
        std::process::exit(1);
    }
}
