//! mqlite - A high-performance MQTT broker.

mod auth;
mod client;
mod client_handle;
mod config;
mod error;
mod packet;
mod publish_encoder;
mod server;
mod shared;
mod subscription;
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
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(&config.log.level),
    )
    .init();

    // Determine worker count (0 = auto)
    let num_workers = if config.server.workers == 0 {
        num_cpus::get()
    } else {
        config.server.workers
    };

    info!(
        "Starting mqlite with {} worker threads (max_packet_size={}KB, max_inflight={})",
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
