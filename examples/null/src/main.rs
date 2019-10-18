#![warn(clippy::unimplemented)]
#![allow(clippy::needless_lifetimes)]

use polyfuse::{
    reply::{ReplyAttr, ReplyData, ReplyEmpty, ReplyOpen, ReplyWrite},
    AttrSet, Context, FileAttr, Nodeid, Operations,
};
use std::{convert::TryInto, env, future::Future, io, path::PathBuf, pin::Pin};

#[tokio::main]
async fn main() -> io::Result<()> {
    std::env::set_var("RUST_LOG", "fuse_async=debug");
    pretty_env_logger::init();

    let mountpoint = env::args()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, ""))?;
    if !mountpoint.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "the mountpoint must be a regular file",
        ));
    }

    let op = Null;
    polyfuse::tokio::mount(mountpoint, None::<&str>, op).await?;

    Ok(())
}

struct Null;

impl<'d> Operations<&'d [u8]> for Null {
    fn getattr<'a>(
        &mut self,
        _cx: &Context,
        ino: Nodeid,
        _fh: Option<u64>,
        reply: ReplyAttr<'a>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>> {
        match ino {
            Nodeid::ROOT => Box::pin(reply.attr(root_attr())),
            _ => Box::pin(reply.err(libc::ENOENT)),
        }
    }

    fn setattr<'a>(
        &mut self,
        _cx: &Context,
        ino: Nodeid,
        _fh: Option<u64>,
        _attr: AttrSet,
        reply: ReplyAttr<'a>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>> {
        match ino {
            Nodeid::ROOT => Box::pin(reply.attr(root_attr())),
            _ => Box::pin(reply.err(libc::ENOENT)),
        }
    }

    fn open<'a>(
        &mut self,
        _cx: &Context,
        ino: Nodeid,
        _flags: u32,
        reply: ReplyOpen<'a>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>> {
        match ino {
            Nodeid::ROOT => Box::pin(reply.open(0)),
            _ => Box::pin(reply.err(libc::ENOENT)),
        }
    }

    fn read<'a>(
        &mut self,
        _cx: &Context,
        ino: Nodeid,
        _fh: u64,
        _offset: u64,
        _size: u32,
        _flags: u32,
        _lock_owner: Option<u64>,
        reply: ReplyData<'a>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>> {
        match ino {
            Nodeid::ROOT => Box::pin(reply.data(&[])),
            _ => Box::pin(reply.err(libc::ENOENT)),
        }
    }

    fn write<'a>(
        &mut self,
        _cx: &Context,
        ino: Nodeid,
        _fh: u64,
        _offset: u64,
        data: &'d [u8],
        _size: u32,
        _flags: u32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite<'a>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>> {
        match ino {
            Nodeid::ROOT => Box::pin(reply.write(data.len() as u32)),
            _ => Box::pin(reply.err(libc::ENOENT)),
        }
    }

    fn release<'a>(
        &mut self,
        _cx: &Context,
        ino: Nodeid,
        _fh: u64,
        _flags: u32,
        _lock_owner: Option<u64>,
        _flush: bool,
        _flock_release: bool,
        reply: ReplyEmpty<'a>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'a>> {
        match ino {
            Nodeid::ROOT => Box::pin(reply.ok()),
            _ => Box::pin(reply.err(libc::ENOENT)),
        }
    }
}

fn root_attr() -> FileAttr {
    let mut attr: libc::stat = unsafe { std::mem::zeroed() };
    attr.st_mode = libc::S_IFREG | 0o644;
    attr.st_nlink = 1;
    attr.st_uid = unsafe { libc::getuid() };
    attr.st_gid = unsafe { libc::getgid() };
    attr.try_into().unwrap()
}
