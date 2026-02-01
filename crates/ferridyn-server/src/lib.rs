//! FerridynDB server and client library.
//!
//! Runs a FerridynDB database as a local Unix socket server, allowing multiple
//! clients (MCP server, CLI, hooks) to share one database without file lock
//! conflicts.

pub mod client;
pub mod error;
pub mod protocol;
pub mod server;

pub use client::FerridynClient;
pub use server::FerridynServer;
