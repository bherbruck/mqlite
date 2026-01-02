//! mqlite - A high-performance MQTT broker.

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

use std::net::SocketAddr;
use std::sync::Arc;

use log::{error, info};

use crate::config::Config;
use crate::server::Server;

struct Args {
    bind_addr: SocketAddr,
    num_threads: usize,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut bind_addr = "0.0.0.0:1883".to_string();
    let mut num_threads = num_cpus::get();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-b" | "--bind" => {
                if i + 1 < args.len() {
                    bind_addr = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: -b requires an address argument");
                    std::process::exit(1);
                }
            }
            "-t" | "--threads" => {
                if i + 1 < args.len() {
                    num_threads = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Error: --threads requires a number");
                        std::process::exit(1);
                    });
                    if num_threads == 0 {
                        eprintln!("Error: --threads must be at least 1");
                        std::process::exit(1);
                    }
                    i += 2;
                } else {
                    eprintln!("Error: --threads requires a number");
                    std::process::exit(1);
                }
            }
            "-h" | "--help" => {
                println!("mqlite - High-performance MQTT broker");
                println!();
                println!("Usage: mqlite [OPTIONS]");
                println!();
                println!("Options:");
                println!("  -b, --bind <ADDR>       Bind address (default: 0.0.0.0:1883)");
                println!("  -t, --threads <NUM>     Number of worker threads (default: num_cpus)");
                println!("  -h, --help              Show this help message");
                std::process::exit(0);
            }
            arg => {
                eprintln!("Unknown argument: {}", arg);
                std::process::exit(1);
            }
        }
    }

    let bind_addr = bind_addr.parse().unwrap_or_else(|e| {
        eprintln!("Invalid bind address '{}': {}", bind_addr, e);
        std::process::exit(1);
    });

    Args {
        bind_addr,
        num_threads,
    }
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = parse_args();
    let config = Arc::new(Config::default());

    // Validate config
    if let Err(e) = config.validate() {
        error!("Invalid configuration: {}", e);
        std::process::exit(1);
    }

    info!(
        "Starting mqlite with {} worker threads (max_packet_size={}KB, max_inflight={})",
        args.num_threads,
        config.max_packet_size / 1024,
        config.max_inflight
    );

    let mut server = match Server::new(args.bind_addr, args.num_threads, config) {
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
