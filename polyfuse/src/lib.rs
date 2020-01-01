#![doc(html_root_url = "https://docs.rs/polyfuse/0.3.1")]

//! A FUSE (Filesystem in userspace) library for Rust.

#![warn(clippy::checked_conversions)]
#![deny(
    missing_docs,
    missing_debug_implementations,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::invalid_upcast_comparisons
)]
#![forbid(clippy::unimplemented)]

pub mod io;
pub mod op;
pub mod reply;

mod common;
mod context;
mod dirent;
mod fs;
mod init;
mod kernel;
mod session;
mod util;

#[doc(inline)]
pub use crate::{
    common::{FileAttr, FileLock, Forget, StatFs},
    context::Context,
    dirent::DirEntry,
    fs::Filesystem,
    init::{CapabilityFlags, ConnectionInfo, SessionInitializer},
    op::Operation,
    session::Session,
};

/// A re-export of [`async_trait`] for implementing `Filesystem`.
///
/// [`async_trait`]: https://docs.rs/async-trait
pub use async_trait::async_trait;

#[test]
fn test_html_root_url() {
    version_sync::assert_html_root_url_updated!("src/lib.rs");
}
