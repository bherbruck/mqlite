//! MQTT broker server - coordinates workers.
//!
//! The server accepts connections and distributes them to workers.
//! Supports single-threaded (1 worker) or multi-threaded (N workers) modes.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Sender};
use log::{debug, error, info};
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};

use crate::error::Result;
use crate::shared::SharedState;
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
}

impl Server {
    /// Create a new server bound to the given address.
    #[allow(dead_code)]
    pub fn new(addr: SocketAddr) -> Result<Self> {
        Self::with_workers(addr, 1)
    }

    /// Create a new server with the specified number of workers.
    pub fn with_workers(addr: SocketAddr, num_workers: usize) -> Result<Self> {
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
        })
    }

    /// Run the server with workers.
    pub fn run(&mut self) -> Result<()> {
        let shared = Arc::new(SharedState::new());

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
            let mut worker = Worker::new(0, shared, rx, self.worker_senders.clone())?;

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
            }
        } else {
            // Multi-worker: spawn worker threads
            let mut handles = Vec::with_capacity(self.num_workers);

            for (id, rx) in receivers.into_iter().enumerate() {
                let shared = Arc::clone(&shared);
                let senders = self.worker_senders.clone();

                let handle = thread::Builder::new()
                    .name(format!("worker-{}", id))
                    .spawn(move || {
                        let mut worker =
                            Worker::new(id, shared, rx, senders).expect("Failed to create worker");
                        if let Err(e) = worker.run() {
                            error!("Worker {} error: {}", id, e);
                        }
                    })?;

                handles.push(handle);
            }

            info!("Spawned {} worker threads", self.num_workers);

            // Main thread handles accept loop only
            let mut events = Events::with_capacity(256);

            loop {
                self.poll
                    .poll(&mut events, Some(Duration::from_millis(100)))?;

                for event in events.iter() {
                    if event.token() == LISTENER {
                        self.accept_connections()?;
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
