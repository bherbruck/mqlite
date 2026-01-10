//! MQTT packet handlers.
//!
//! This module contains extracted handler logic for MQTT control packets.
//! The Worker struct coordinates these handlers, but the heavy lifting
//! is done here to keep worker.rs manageable.

pub mod connect;
pub mod publish;
pub mod subscribe;
