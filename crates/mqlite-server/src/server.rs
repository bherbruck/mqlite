//! MQTT broker server - coordinates workers.
//!
//! The server accepts connections and distributes them to workers.
//! Supports single-threaded (1 worker) or multi-threaded (N workers) modes.

use std::fs::File;
use std::io::{self, BufReader};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Sender};
use log::{debug, error, info};
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;

use mqlite_core::error::{Error, Result};

use crate::bridge::BridgeManager;
use crate::client::Transport;
use crate::config::Config;
use crate::prometheus;
use crate::proxy;
use crate::shared::SharedState;
use crate::sys_tree::SysTreePublisher;
use crate::websocket;
use crate::worker::{Worker, WorkerMsg};

/// Token for the plain TCP listener socket.
const LISTENER: Token = Token(0);

/// Token for the TLS listener socket.
const LISTENER_TLS: Token = Token(1);

/// Token for the WebSocket listener socket.
const LISTENER_WS: Token = Token(2);

/// Token for the secure WebSocket listener socket.
const LISTENER_WSS: Token = Token(3);

/// Channel capacity for worker messages.
const CHANNEL_CAPACITY: usize = 4096;

/// MQTT broker server.
pub struct Server {
    poll: Poll,
    listener: TcpListener,
    /// TLS listener (if TLS is enabled).
    tls_listener: Option<TcpListener>,
    /// TLS server configuration (if TLS is enabled).
    tls_config: Option<Arc<ServerConfig>>,
    /// WebSocket listener (if WebSocket is enabled).
    ws_listener: Option<TcpListener>,
    /// Secure WebSocket listener (if WSS is enabled).
    wss_listener: Option<TcpListener>,
    /// TLS config for WSS (may share with MQTTS).
    wss_tls_config: Option<Arc<ServerConfig>>,
    /// Senders to worker channels.
    worker_senders: Vec<Sender<WorkerMsg>>,
    /// Round-robin counter for connection distribution.
    next_worker: usize,
    /// Number of workers.
    num_workers: usize,
    /// Broker configuration.
    config: Arc<Config>,
}

impl Server {
    /// Create a new server with the specified number of workers and config.
    pub fn new(addr: SocketAddr, num_workers: usize, config: Arc<Config>) -> Result<Self> {
        let poll = Poll::new()?;
        let mut listener = TcpListener::bind(addr)?;

        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)?;

        info!("mqlite listening on {}", addr);

        // Initialize TLS if enabled
        let (tls_listener, tls_config) = if config.tls.enabled {
            let tls_config = Self::load_tls_config(&config)?;
            let mut tls_listener = TcpListener::bind(config.tls.bind)?;

            poll.registry()
                .register(&mut tls_listener, LISTENER_TLS, Interest::READABLE)?;

            info!("TLS listening on {}", config.tls.bind);

            (Some(tls_listener), Some(Arc::new(tls_config)))
        } else {
            (None, None)
        };

        // Initialize WebSocket listener if enabled
        let ws_listener = if config.websocket.enabled {
            let mut ws_listener = TcpListener::bind(config.websocket.bind)?;
            poll.registry()
                .register(&mut ws_listener, LISTENER_WS, Interest::READABLE)?;
            info!("WebSocket listening on {}", config.websocket.bind);
            Some(ws_listener)
        } else {
            None
        };

        // Initialize secure WebSocket listener if enabled
        let (wss_listener, wss_tls_config) = if config.websocket_tls.enabled {
            // Load TLS config for WSS (use own cert/key or fall back to tls config)
            let wss_tls_config = Self::load_wss_tls_config(&config)?;
            let mut wss_listener = TcpListener::bind(config.websocket_tls.bind)?;
            poll.registry()
                .register(&mut wss_listener, LISTENER_WSS, Interest::READABLE)?;
            info!("Secure WebSocket listening on {}", config.websocket_tls.bind);
            (Some(wss_listener), Some(Arc::new(wss_tls_config)))
        } else {
            (None, None)
        };

        Ok(Self {
            poll,
            listener,
            tls_listener,
            tls_config,
            ws_listener,
            wss_listener,
            wss_tls_config,
            worker_senders: Vec::new(),
            next_worker: 0,
            num_workers,
            config,
        })
    }

    /// Load TLS certificates and create server configuration.
    fn load_tls_config(config: &Config) -> Result<ServerConfig> {
        // Load certificate chain
        let cert_file = File::open(&config.tls.cert).map_err(|e| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Failed to open TLS certificate file {:?}: {}",
                    config.tls.cert, e
                ),
            ))
        })?;
        let mut cert_reader = BufReader::new(cert_file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse TLS certificate: {}", e),
                ))
            })?;

        if certs.is_empty() {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "No certificates found in certificate file",
            )));
        }

        // Load private key
        let key_file = File::open(&config.tls.key).map_err(|e| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Failed to open TLS key file {:?}: {}", config.tls.key, e),
            ))
        })?;
        let mut key_reader = BufReader::new(key_file);
        let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|e| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse TLS private key: {}", e),
                ))
            })?
            .ok_or_else(|| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "No private key found in key file",
                ))
            })?;

        // Build TLS config
        let tls_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to build TLS config: {}", e),
                ))
            })?;

        info!("TLS configuration loaded from {:?}", config.tls.cert);
        Ok(tls_config)
    }

    /// Load TLS configuration for secure WebSocket.
    /// Uses websocket_tls cert/key if specified, otherwise falls back to tls config.
    fn load_wss_tls_config(config: &Config) -> Result<ServerConfig> {
        let cert_path = config
            .websocket_tls
            .cert
            .clone()
            .unwrap_or_else(|| config.tls.cert.clone());
        let key_path = config
            .websocket_tls
            .key
            .clone()
            .unwrap_or_else(|| config.tls.key.clone());

        // Load certificate chain
        let cert_file = File::open(&cert_path).map_err(|e| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Failed to open WSS certificate file {:?}: {}",
                    cert_path, e
                ),
            ))
        })?;
        let mut cert_reader = BufReader::new(cert_file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse WSS certificate: {}", e),
                ))
            })?;

        if certs.is_empty() {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "No certificates found in WSS certificate file",
            )));
        }

        // Load private key
        let key_file = File::open(&key_path).map_err(|e| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Failed to open WSS key file {:?}: {}", key_path, e),
            ))
        })?;
        let mut key_reader = BufReader::new(key_file);
        let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|e| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse WSS private key: {}", e),
                ))
            })?
            .ok_or_else(|| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "No private key found in WSS key file",
                ))
            })?;

        // Build TLS config
        let tls_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to build WSS TLS config: {}", e),
                ))
            })?;

        info!("WSS TLS configuration loaded from {:?}", cert_path);
        Ok(tls_config)
    }

    /// Run the server with workers.
    pub fn run(&mut self) -> Result<()> {
        let shared = Arc::new(SharedState::new());
        let start_time = Instant::now();

        // Start Prometheus metrics server if enabled
        if self.config.prometheus.enabled {
            prometheus::start_metrics_server(
                self.config.prometheus.bind,
                Arc::clone(&shared),
                start_time,
            );
        }

        // Create $SYS publisher if enabled
        let sys_interval = self.config.server.sys_interval;
        let mut sys_publisher = if sys_interval > 0 {
            info!(
                "$SYS topic publishing enabled (interval: {}s)",
                sys_interval
            );
            Some(SysTreePublisher::new(Arc::clone(&shared), sys_interval))
        } else {
            None
        };
        let mut last_sys_publish = Instant::now();

        // Memory purge interval (independent of $SYS)
        const MEMORY_PURGE_INTERVAL_SECS: u64 = 30;
        let mut last_memory_purge = Instant::now();

        // Start bridge connections if configured
        let mut _bridge_manager = BridgeManager::new();
        if !self.config.bridge.is_empty() {
            info!("Starting {} bridge connection(s)", self.config.bridge.len());
            _bridge_manager.start_bridges(self.config.bridge.clone(), Arc::clone(&shared));
        }

        // Create channels for all workers
        let mut receivers = Vec::with_capacity(self.num_workers);
        for _ in 0..self.num_workers {
            let (tx, rx) = bounded(CHANNEL_CAPACITY);
            self.worker_senders.push(tx);
            receivers.push(rx);
        }

        if self.num_workers == 1 {
            // Single worker: run in main thread (best latency)
            let rx = receivers.remove(0);
            let mut worker = Worker::new(
                0,
                Arc::clone(&shared),
                rx,
                self.worker_senders.clone(),
                Arc::clone(&self.config),
            )?;

            let mut events = Events::with_capacity(1024);

            loop {
                self.poll
                    .poll(&mut events, Some(Duration::from_millis(1)))?;

                for event in events.iter() {
                    match event.token() {
                        LISTENER => self.accept_connections()?,
                        LISTENER_TLS => self.accept_tls_connections()?,
                        LISTENER_WS => self.accept_websocket_connections()?,
                        LISTENER_WSS => self.accept_websocket_tls_connections()?,
                        _ => {}
                    }
                }

                worker.run_once()?;

                // Publish $SYS topics if interval elapsed
                if let Some(ref mut publisher) = sys_publisher {
                    if last_sys_publish.elapsed().as_secs() >= sys_interval {
                        publisher.publish_if_changed();
                        last_sys_publish = Instant::now();
                    }
                }

                // Periodically purge allocator arenas to return memory to OS
                if last_memory_purge.elapsed().as_secs() >= MEMORY_PURGE_INTERVAL_SECS {
                    crate::jemalloc_purge();
                    last_memory_purge = Instant::now();
                }
            }
        } else {
            // Multi-worker: spawn worker threads
            let mut handles = Vec::with_capacity(self.num_workers);

            for (id, rx) in receivers.into_iter().enumerate() {
                let shared = Arc::clone(&shared);
                let senders = self.worker_senders.clone();
                let config = Arc::clone(&self.config);

                let handle = thread::Builder::new()
                    .name(format!("worker-{}", id))
                    .spawn(move || {
                        let mut worker = Worker::new(id, shared, rx, senders, config)
                            .expect("Failed to create worker");
                        if let Err(e) = worker.run() {
                            error!("Worker {} error: {}", id, e);
                        }
                    })?;

                handles.push(handle);
            }

            info!("Spawned {} worker threads", self.num_workers);

            // Main thread handles accept loop and $SYS publishing
            let mut events = Events::with_capacity(256);

            loop {
                self.poll
                    .poll(&mut events, Some(Duration::from_millis(100)))?;

                for event in events.iter() {
                    match event.token() {
                        LISTENER => self.accept_connections()?,
                        LISTENER_TLS => self.accept_tls_connections()?,
                        LISTENER_WS => self.accept_websocket_connections()?,
                        LISTENER_WSS => self.accept_websocket_tls_connections()?,
                        _ => {}
                    }
                }

                // Publish $SYS topics if interval elapsed
                if let Some(ref mut publisher) = sys_publisher {
                    if last_sys_publish.elapsed().as_secs() >= sys_interval {
                        publisher.publish_if_changed();
                        last_sys_publish = Instant::now();
                    }
                }

                // Periodically purge allocator arenas to return memory to OS
                if last_memory_purge.elapsed().as_secs() >= MEMORY_PURGE_INTERVAL_SECS {
                    crate::jemalloc_purge();
                    last_memory_purge = Instant::now();
                }
            }
        }
    }

    /// Accept new plain TCP connections and distribute to workers.
    fn accept_connections(&mut self) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((socket, mut addr)) => {
                    let mut preamble = Vec::new();

                    // Parse PROXY protocol header if enabled
                    if self.config.server.proxy_protocol.enabled {
                        let timeout =
                            Duration::from_secs(self.config.server.proxy_protocol.timeout_secs);
                        match proxy::parse_proxy_header(&socket, timeout) {
                            Ok((real_addr, remaining)) => {
                                debug!(
                                    "PROXY protocol: {} -> {} (real client IP)",
                                    addr, real_addr
                                );
                                addr = real_addr;
                                preamble = remaining;
                            }
                            Err(e) => {
                                debug!("PROXY header error from {}: {}", addr, e);
                                // Connection rejected - PROXY protocol is required when enabled
                                continue;
                            }
                        }
                    }

                    let worker_id = self.next_worker;
                    self.next_worker = (self.next_worker + 1) % self.num_workers;

                    debug!(
                        "Accepted connection from {}, assigning to worker {}",
                        addr, worker_id
                    );

                    let transport = Transport::plain(socket);
                    let _ = self.worker_senders[worker_id].send(WorkerMsg::NewClient {
                        transport,
                        addr,
                        preamble,
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Accept new TLS connections and distribute to workers.
    fn accept_tls_connections(&mut self) -> Result<()> {
        let tls_listener = match &self.tls_listener {
            Some(l) => l,
            None => return Ok(()),
        };
        let tls_config = match &self.tls_config {
            Some(c) => Arc::clone(c),
            None => return Ok(()),
        };

        loop {
            match tls_listener.accept() {
                Ok((socket, mut addr)) => {
                    let mut preamble = Vec::new();

                    // Parse PROXY protocol header on raw socket BEFORE TLS handshake
                    if self.config.tls.proxy_protocol.enabled {
                        let timeout =
                            Duration::from_secs(self.config.tls.proxy_protocol.timeout_secs);
                        match proxy::parse_proxy_header(&socket, timeout) {
                            Ok((real_addr, remaining)) => {
                                debug!(
                                    "PROXY protocol (TLS): {} -> {} (real client IP)",
                                    addr, real_addr
                                );
                                addr = real_addr;
                                preamble = remaining;
                            }
                            Err(e) => {
                                debug!("PROXY header error from {} (TLS): {}", addr, e);
                                // Connection rejected - PROXY protocol is required when enabled
                                continue;
                            }
                        }
                    }

                    let worker_id = self.next_worker;
                    self.next_worker = (self.next_worker + 1) % self.num_workers;

                    debug!(
                        "Accepted TLS connection from {}, assigning to worker {}",
                        addr, worker_id
                    );

                    // Create TLS server connection
                    let tls_conn = match rustls::ServerConnection::new(Arc::clone(&tls_config)) {
                        Ok(conn) => conn,
                        Err(e) => {
                            error!("Failed to create TLS connection for {}: {}", addr, e);
                            continue;
                        }
                    };

                    let transport = Transport::tls(tls_conn, socket);
                    let _ = self.worker_senders[worker_id].send(WorkerMsg::NewClient {
                        transport,
                        addr,
                        preamble,
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Accept new WebSocket connections and distribute to workers.
    fn accept_websocket_connections(&mut self) -> Result<()> {
        let ws_listener = match &self.ws_listener {
            Some(l) => l,
            None => return Ok(()),
        };

        loop {
            match ws_listener.accept() {
                Ok((socket, addr)) => {
                    // Convert to std socket for blocking handshake
                    let std_socket: std::net::TcpStream = socket.into();

                    // Set to blocking mode for the handshake
                    if let Err(e) = std_socket.set_nonblocking(false) {
                        debug!("Failed to set socket to blocking for {}: {}", addr, e);
                        continue;
                    }

                    // Perform WebSocket handshake (blocking)
                    match websocket::accept_websocket(std_socket, &self.config.websocket.path) {
                        Ok(ws) => {
                            // Wrap in mio-compatible transport
                            match websocket::wrap_websocket(ws) {
                                Ok(ws_transport) => {
                                    let worker_id = self.next_worker;
                                    self.next_worker = (self.next_worker + 1) % self.num_workers;

                                    debug!(
                                        "Accepted WebSocket connection from {}, assigning to worker {}",
                                        addr, worker_id
                                    );

                                    let transport = Transport::websocket(ws_transport);
                                    let _ =
                                        self.worker_senders[worker_id].send(WorkerMsg::NewClient {
                                            transport,
                                            addr,
                                            preamble: Vec::new(),
                                        });
                                }
                                Err(e) => {
                                    debug!("Failed to wrap WebSocket for {}: {}", addr, e);
                                }
                            }
                        }
                        Err(e) => {
                            debug!("WebSocket handshake failed from {}: {}", addr, e);
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Accept new secure WebSocket (WSS) connections and distribute to workers.
    /// TODO: WSS support requires adding Transport::WebSocketTls variant
    fn accept_websocket_tls_connections(&mut self) -> Result<()> {
        let wss_listener = match &self.wss_listener {
            Some(l) => l,
            None => return Ok(()),
        };
        let _wss_tls_config = match &self.wss_tls_config {
            Some(c) => Arc::clone(c),
            None => return Ok(()),
        };

        // For now, accept and immediately close WSS connections with a warning
        // Full WSS support requires Transport::WebSocketTls variant
        loop {
            match wss_listener.accept() {
                Ok((_socket, addr)) => {
                    debug!(
                        "WSS connection from {} rejected - WSS not yet implemented",
                        addr
                    );
                    // Socket is dropped, closing the connection
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}
