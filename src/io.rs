//! I/O primitives for FUSE.

mod conn;
pub mod splice;
mod unite;

pub use conn::{Connection, MountOptions};
pub use unite::{unite, Unite};
