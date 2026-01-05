//! MQTT broker server - coordinates workers.
//!
//! The server accepts connections and distributes them to workers.
//! Supports single-threaded (1 worker) or multi-threaded (N workers) modes.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Sender};
use log::{debug, error, info};
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};

use crate::config::Config;
use crate::error::Result;
use crate::prometheus;
use crate::shared::SharedState;
use crate::sys_tree::SysTreePublisher;
use crate::worker::{Worker, WorkerMsg};

/// Token for the listener socket.
const LISTENER: Token = Token(0);

/// Channel capacity for worker messages.
const CHANNEL_CAPACITY: usize = 4096;

/// MQTT broker server.
pub struct Server {
    poll: Poll,
    listener: TcpListener,
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

        Ok(Self {
            poll,
            listener,
            worker_senders: Vec::new(),
            next_worker: 0,
            num_workers,
            config,
        })
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
                    if event.token() == LISTENER {
                        self.accept_connections()?;
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
                    if event.token() == LISTENER {
                        self.accept_connections()?;
                    }
                }

                // Publish $SYS topics if interval elapsed
                if let Some(ref mut publisher) = sys_publisher {
                    if last_sys_publish.elapsed().as_secs() >= sys_interval {
                        publisher.publish_if_changed();
                        last_sys_publish = Instant::now();
                    }
                }
            }
        }
    }

    /// Accept new connections and distribute to workers.
    fn accept_connections(&mut self) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((socket, addr)) => {
                    let worker_id = self.next_worker;
                    self.next_worker = (self.next_worker + 1) % self.num_workers;

                    debug!(
                        "Accepted connection from {}, assigning to worker {}",
                        addr, worker_id
                    );

                    let _ =
                        self.worker_senders[worker_id].send(WorkerMsg::NewClient { socket, addr });
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
