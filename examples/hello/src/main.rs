#![allow(clippy::unnecessary_mut_passed)]
#![deny(clippy::unimplemented)]

use polyfuse::{
    op,
    reply::{AttrOut, EntryOut, FileAttr, OpenOut, ReaddirOut},
    KernelConfig, Operation, Request, Session,
};

use anyhow::{ensure, Context as _, Result};
use std::{io, os::unix::prelude::*, path::PathBuf, time::Duration};
use tracing::warn;

const TTL: Duration = Duration::from_secs(60 * 60 * 24 * 365);
const ROOT_INO: u64 = 1;
const HELLO_INO: u64 = 2;
const HELLO_FILENAME: &str = "hello.txt";
const HELLO_CONTENT: &[u8] = b"Hello, world!\n";

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut args = pico_args::Arguments::from_env();

    let mountpoint: PathBuf = args.free_from_str()?.context("missing mountpoint")?;
    ensure!(mountpoint.is_dir(), "mountpoint must be a directory");

    let session = Session::mount(mountpoint, KernelConfig::default())?;

    let fs = Hello::new();

    while let Some(req) = session.next_request()? {
        match req.operation()? {
            Operation::Lookup(op) => fs.lookup(&req, op)?,
            Operation::Getattr(op) => fs.getattr(&req, op)?,
            Operation::Read(op) => fs.read(&req, op)?,
            Operation::Readdir(op) => fs.readdir(&req, op)?,
            Operation::Opendir(op) => fs.opendir(&req, op)?,
            unhandled_op => {
                warn!("Missing handler mapping for {:?} operation", unhandled_op);
                req.reply_error(libc::ENOSYS)?
            }
        }
    }

    Ok(())
}

struct Hello {
    entries: Vec<DirEntry>,
    uid: u32,
    gid: u32,
}

struct DirEntry {
    name: &'static str,
    ino: u64,
    typ: u32,
}

impl Hello {
    fn new() -> Self {
        let mut entries = Vec::with_capacity(3);
        entries.push(DirEntry {
            name: ".",
            ino: ROOT_INO,
            typ: libc::DT_DIR as u32,
        });
        entries.push(DirEntry {
            name: "..",
            ino: ROOT_INO,
            typ: libc::DT_DIR as u32,
        });
        entries.push(DirEntry {
            name: HELLO_FILENAME,
            ino: HELLO_INO,
            typ: libc::DT_REG as u32,
        });

        Self {
            entries,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        }
    }

    fn fill_root_attr(&self, attr: &mut FileAttr) {
        attr.ino(ROOT_INO);
        attr.mode(libc::S_IFDIR as u32 | 0o555);
        attr.nlink(2);
        attr.uid(self.uid);
        attr.gid(self.gid);
    }

    fn fill_hello_attr(&self, attr: &mut FileAttr) {
        attr.ino(HELLO_INO);
        attr.size(HELLO_CONTENT.len() as u64);
        attr.mode(libc::S_IFREG as u32 | 0o444);
        attr.nlink(1);
        attr.uid(self.uid);
        attr.gid(self.gid);
    }

    fn lookup(&self, req: &Request, op: op::Lookup<'_>) -> io::Result<()> {
        match op.parent() {
            ROOT_INO if op.name().as_bytes() == HELLO_FILENAME.as_bytes() => {
                let mut out = EntryOut::default();
                self.fill_hello_attr(out.attr());
                out.ino(HELLO_INO);
                out.ttl_attr(TTL);
                out.ttl_entry(TTL);
                req.reply(out)
            }
            _ => req.reply_error(libc::ENOENT),
        }
    }

    fn getattr(&self, req: &Request, op: op::Getattr<'_>) -> io::Result<()> {
        let fill_attr = match op.ino() {
            ROOT_INO => Self::fill_root_attr,
            HELLO_INO => Self::fill_hello_attr,
            _ => return req.reply_error(libc::ENOENT),
        };

        let mut out = AttrOut::default();
        fill_attr(self, out.attr());
        out.ttl(TTL);

        req.reply(out)
    }

    fn read(&self, req: &Request, op: op::Read<'_>) -> io::Result<()> {
        match op.ino() {
            HELLO_INO => (),
            ROOT_INO => return req.reply_error(libc::EISDIR),
            _ => return req.reply_error(libc::ENOENT),
        }

        let mut data: &[u8] = &[];

        let offset = op.offset() as usize;
        if offset < HELLO_CONTENT.len() {
            let size = op.size() as usize;
            data = &HELLO_CONTENT[offset..];
            data = &data[..std::cmp::min(data.len(), size)];
        }

        req.reply(data)
    }

    fn dir_entries(&self) -> impl Iterator<Item = (u64, &DirEntry)> + '_ {
        self.entries.iter().enumerate().map(|(i, ent)| {
            let offset = (i + 1) as u64;
            (offset, ent)
        })
    }

    fn readdir(&self, req: &Request, op: op::Readdir<'_>) -> io::Result<()> {
        if op.ino() != ROOT_INO {
            return req.reply_error(libc::ENOTDIR);
        }

        let mut out = ReaddirOut::new(op.size() as usize);

        for (i, entry) in self.dir_entries().skip(op.offset() as usize) {
            let full = out.entry(
                entry.name.as_ref(), //
                entry.ino,
                entry.typ,
                i + 1,
            );
            if full {
                break;
            }
        }

        req.reply(out)
    }

    fn opendir(&self, req: &Request, op: op::Opendir<'_>) -> io::Result<()> {
        let mut out = OpenOut::default();
        out.fh(op.ino());
        req.reply(out)
    }
}
