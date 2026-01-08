//! mqlite - A high-performance MQTT broker.

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Configure jemalloc to return memory to OS faster during idle periods.
/// Default dirty_decay_ms is 10000 (10s). We set it to 1000 (1s) for
/// faster memory release when idle, with negligible performance impact.
#[cfg(feature = "jemalloc")]
fn configure_jemalloc() {
    use tikv_jemalloc_ctl::raw;
    // Set dirty page decay time to 1 second (default 10s)
    // This returns unused memory to OS faster during idle periods
    let decay_ms: isize = 1000;
    // SAFETY: We're passing valid null-terminated strings and a valid isize value.
    // These are standard jemalloc configuration options.
    unsafe {
        if let Err(e) = raw::write(b"arenas.dirty_decay_ms\0", decay_ms) {
            eprintln!("Warning: failed to configure jemalloc dirty_decay_ms: {}", e);
        }
        // Also set muzzy decay (purged but still mapped pages)
        if let Err(e) = raw::write(b"arenas.muzzy_decay_ms\0", decay_ms) {
            eprintln!("Warning: failed to configure jemalloc muzzy_decay_ms: {}", e);
        }
    }
}

#[cfg(not(feature = "jemalloc"))]
fn configure_jemalloc() {}

mod auth;
mod bridge;
mod client;
mod client_handle;
mod config;
mod prometheus;
mod proxy;
mod publish_encoder;
mod server;
mod shared;
mod subscription;
mod sys_tree;
mod util;
mod worker;
mod write_buffer;

use std::sync::Arc;

use log::{error, info};

use crate::config::Config;
use crate::server::Server;

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
    // Configure jemalloc memory decay (returns memory to OS faster when idle)
    configure_jemalloc();

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
    const BUILD_ID: &str = "2026-01-06-read-shrink";

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
