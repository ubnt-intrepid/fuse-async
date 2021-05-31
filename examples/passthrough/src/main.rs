#![allow(clippy::unnecessary_mut_passed)]
#![warn(clippy::unimplemented, clippy::todo)]

cfg_if::cfg_if! {
if #[cfg(target_os = "linux")] {
mod fs;

use polyfuse::{
    op,
    reply::{
        AttrOut, EntryOut, FileAttr, OpenOut, ReaddirOut, Statfs, StatfsOut, WriteOut, XattrOut,
    },
    KernelConfig, Operation, Session,
};

use anyhow::{ensure, Context as _, Result};
use either::Either;
use pico_args::Arguments;
use slab::Slab;
use std::{
    collections::hash_map::{Entry, HashMap},
    ffi::{OsStr, OsString},
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{self, prelude::*, BufRead},
    os::unix::prelude::*,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::fs::{FileDesc, ReadDir};

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut args = Arguments::from_env();

    let source: PathBuf = args
        .opt_value_from_str(["-s", "--source"])?
        .unwrap_or_else(|| std::env::current_dir().unwrap());
    ensure!(source.is_dir(), "the source path must be a directory");

    let timeout = if args.contains("--no-cache") {
        None
    } else {
        Some(Duration::from_secs(60 * 60 * 24)) // one day
    };

    let mountpoint: PathBuf = args.free_from_str()?.context("missing mountpoint")?;
    ensure!(mountpoint.is_dir(), "mountpoint must be a directory");

    // TODO: splice read/write
    let session = Session::mount(mountpoint, {
        let mut config = KernelConfig::default();
        config.mount_option("default_permissions");
        config.mount_option("fsname=passthrough");
        config.export_support(true);
        config.flock_locks(true);
        config.writeback_cache(timeout.is_some());
        config
    })?;

    let fs = Arc::new(Passthrough::new(source, timeout)?);

    while let Some(req) = session.next_request()? {
        let fs = fs.clone();

        std::thread::spawn(move || -> Result<()> {
            let span = tracing::debug_span!("handle_request", unique = req.unique());
            let _enter = span.enter();

            let op = req.operation()?;
            tracing::debug!(?op);

            macro_rules! try_reply {
                ($e:expr) => {
                    match $e {
                        Ok(data) => {
                            tracing::debug!(?data);
                            req.reply(data)?;
                        }
                        Err(err) => {
                            let errno = io_to_errno(err);
                            tracing::debug!(errno = errno);
                            req.reply_error(errno)?;
                        }
                    }
                };
            }

            match op {
                Operation::Lookup(op) => try_reply!(fs.do_lookup(op.parent(), op.name())),
                Operation::Forget(forgets) => {
                    for forget in forgets.as_ref() {
                        fs.forget_one(forget.ino(), forget.nlookup());
                    }
                }
                Operation::Getattr(op) => try_reply!(fs.do_getattr(&op)),
                Operation::Setattr(op) => try_reply!(fs.do_setattr(&op)),
                Operation::Readlink(op) => try_reply!(fs.do_readlink(&op)),
                Operation::Link(op) => try_reply!(fs.do_link(&op)),

                Operation::Mknod(op) => {
                    try_reply!(fs.make_node(
                        op.parent(),
                        op.name(),
                        op.mode(),
                        Some(op.rdev()),
                        None
                    ))
                }
                Operation::Mkdir(op) => try_reply!(fs.make_node(
                    op.parent(),
                    op.name(),
                    libc::S_IFDIR | op.mode(),
                    None,
                    None
                )),
                Operation::Symlink(op) => try_reply!(fs.make_node(
                    op.parent(),
                    op.name(),
                    libc::S_IFLNK,
                    None,
                    Some(op.link())
                )),

                Operation::Unlink(op) => try_reply!(fs.do_unlink(&op)),
                Operation::Rmdir(op) => try_reply!(fs.do_rmdir(&op)),
                Operation::Rename(op) => try_reply!(fs.do_rename(&op)),

                Operation::Opendir(op) => try_reply!(fs.do_opendir(&op)),
                Operation::Readdir(op) => try_reply!(fs.do_readdir(&op)),
                Operation::Fsyncdir(op) => try_reply!(fs.do_fsyncdir(&op)),
                Operation::Releasedir(op) => try_reply!(fs.do_releasedir(&op)),

                Operation::Open(op) => try_reply!(fs.do_open(&op)),
                Operation::Read(op) => try_reply!(fs.do_read(&op)),
                Operation::Write(op, data) => try_reply!(fs.do_write(&op, data)),
                Operation::Flush(op) => try_reply!(fs.do_flush(&op)),
                Operation::Fsync(op) => try_reply!(fs.do_fsync(&op)),
                Operation::Flock(op) => try_reply!(fs.do_flock(&op)),
                Operation::Fallocate(op) => try_reply!(fs.do_fallocate(&op)),
                Operation::Release(op) => try_reply!(fs.do_release(&op)),

                Operation::Getxattr(op) => try_reply!(fs.do_getxattr(&op)),
                Operation::Listxattr(op) => try_reply!(fs.do_listxattr(&op)),
                Operation::Setxattr(op) => try_reply!(fs.do_setxattr(&op)),
                Operation::Removexattr(op) => try_reply!(fs.do_removexattr(&op)),

                Operation::Statfs(op) => try_reply!(fs.do_statfs(&op)),

                _ => req.reply_error(libc::ENOSYS)?,
            }

            Ok(())
        });
    }

    Ok(())
}

type Ino = u64;
type SrcId = (u64, libc::dev_t);

struct Passthrough {
    inodes: Mutex<INodeTable>,
    opened_dirs: HandlePool<Mutex<ReadDir>>,
    opened_files: HandlePool<Mutex<File>>,
    timeout: Option<Duration>,
}

impl Passthrough {
    fn new(source: PathBuf, timeout: Option<Duration>) -> io::Result<Self> {
        let source = source.canonicalize()?;
        tracing::debug!("source={:?}", source);
        let fd = FileDesc::open(&source, libc::O_PATH)?;
        let stat = fd.fstatat("", libc::AT_SYMLINK_NOFOLLOW)?;

        let mut inodes = INodeTable::new();
        let entry = inodes.vacant_entry();
        debug_assert_eq!(entry.ino(), 1);
        entry.insert(INode {
            ino: 1,
            fd,
            refcount: u64::max_value() / 2, // the root node's cache is never removed.
            src_id: (stat.st_ino, stat.st_dev),
            is_symlink: false,
        });

        Ok(Self {
            inodes: Mutex::new(inodes),
            opened_dirs: HandlePool::default(),
            opened_files: HandlePool::default(),
            timeout,
        })
    }

    fn make_entry_param(&self, ino: u64, attr: libc::stat) -> EntryOut {
        let mut reply = EntryOut::default();
        reply.ino(ino);
        fill_attr(reply.attr(), &attr);
        if let Some(timeout) = self.timeout {
            reply.ttl_entry(timeout);
            reply.ttl_attr(timeout);
        };
        reply
    }

    fn do_lookup(&self, parent: Ino, name: &OsStr) -> io::Result<EntryOut> {
        let mut inodes = self.inodes.lock().unwrap();
        let inodes = &mut *inodes;

        let parent = inodes.get(parent).ok_or_else(no_entry)?;
        let parent = parent.lock().unwrap();

        let fd = parent.fd.openat(name, libc::O_PATH | libc::O_NOFOLLOW)?;

        let stat = fd.fstatat("", libc::AT_SYMLINK_NOFOLLOW)?;
        let src_id = (stat.st_ino, stat.st_dev);
        let is_symlink = stat.st_mode & libc::S_IFMT == libc::S_IFLNK;

        let ino;
        match inodes.get_src(src_id) {
            Some(inode) => {
                let mut inode = inode.lock().unwrap();
                ino = inode.ino;
                inode.refcount += 1;
                tracing::debug!(
                    "update the lookup count: ino={}, refcount={}",
                    inode.ino,
                    inode.refcount
                );
            }
            None => {
                let entry = inodes.vacant_entry();
                ino = entry.ino();
                tracing::debug!("create a new inode cache: ino={}", ino);
                entry.insert(INode {
                    ino,
                    fd,
                    refcount: 1,
                    src_id,
                    is_symlink,
                });
            }
        }

        Ok(self.make_entry_param(ino, stat))
    }

    fn forget_one(&self, ino: Ino, nlookup: u64) {
        let mut inodes = self.inodes.lock().unwrap();

        if let Entry::Occupied(mut entry) = inodes.map.entry(ino) {
            let refcount = {
                let mut inode = entry.get_mut().lock().unwrap();
                inode.refcount = inode.refcount.saturating_sub(nlookup);
                inode.refcount
            };

            if refcount == 0 {
                tracing::debug!("remove ino={}", entry.key());
                drop(entry.remove());
            }
        }
    }

    fn do_getattr(&self, op: &op::Getattr<'_>) -> io::Result<AttrOut> {
        let inodes = self.inodes.lock().unwrap();

        let inode = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let inode = inode.lock().unwrap();

        let stat = inode.fd.fstatat("", libc::AT_SYMLINK_NOFOLLOW)?;

        let mut out = AttrOut::default();
        fill_attr(out.attr(), &stat);
        if let Some(timeout) = self.timeout {
            out.ttl(timeout);
        };

        Ok(out)
    }

    #[allow(clippy::cognitive_complexity)]
    fn do_setattr(&self, op: &op::Setattr<'_>) -> io::Result<AttrOut> {
        let inodes = self.inodes.lock().unwrap();
        let inode = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let inode = inode.lock().unwrap();
        let fd = &inode.fd;

        let mut file = if let Some(fh) = op.fh() {
            Some(self.opened_files.get(fh).ok_or_else(no_entry)?)
        } else {
            None
        };
        let mut file = if let Some(ref mut file) = file {
            Some(file.lock().unwrap())
        } else {
            None
        };

        // chmod
        if let Some(mode) = op.mode() {
            if let Some(file) = file.as_mut() {
                fs::fchmod(&**file, mode)?;
            } else {
                fs::chmod(fd.procname(), mode)?;
            }
        }

        // chown
        match (op.uid(), op.gid()) {
            (None, None) => (),
            (uid, gid) => {
                fd.fchownat("", uid, gid, libc::AT_SYMLINK_NOFOLLOW)?;
            }
        }

        // truncate
        if let Some(size) = op.size() {
            if let Some(file) = file.as_mut() {
                fs::ftruncate(&**file, size as libc::off_t)?;
            } else {
                fs::truncate(fd.procname(), size as libc::off_t)?;
            }
        }

        // utimens
        fn make_timespec(t: Option<op::SetAttrTime>) -> libc::timespec {
            match t {
                Some(op::SetAttrTime::Now) => libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_NOW,
                },
                Some(op::SetAttrTime::Timespec(ts)) => libc::timespec {
                    tv_sec: ts.as_secs() as i64,
                    tv_nsec: ts.subsec_nanos() as u64 as i64,
                },
                _ => libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_OMIT,
                },
            }
        }
        match (op.atime(), op.mtime()) {
            (None, None) => (),
            (atime, mtime) => {
                let tv = [make_timespec(atime), make_timespec(mtime)];
                if let Some(file) = file.as_mut() {
                    fs::futimens(&**file, tv)?;
                } else if inode.is_symlink {
                    // According to libfuse/examples/passthrough_hp.cc, it does not work on
                    // the current kernels, but may in the future.
                    fd.futimensat("", tv, libc::AT_SYMLINK_NOFOLLOW)
                        .map_err(|err| match err.raw_os_error() {
                            Some(libc::EINVAL) => io::Error::from_raw_os_error(libc::EPERM),
                            _ => err,
                        })?;
                } else {
                    fs::utimens(fd.procname(), tv)?;
                }
            }
        }

        // finally, acquiring the latest metadata from the source filesystem.
        let stat = fd.fstatat("", libc::AT_SYMLINK_NOFOLLOW)?;

        let mut out = AttrOut::default();
        fill_attr(out.attr(), &stat);
        if let Some(timeout) = self.timeout {
            out.ttl(timeout);
        };

        Ok(out)
    }

    fn do_readlink(&self, op: &op::Readlink<'_>) -> io::Result<OsString> {
        let inodes = self.inodes.lock().unwrap();
        let inode = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let inode = inode.lock().unwrap();
        inode.fd.readlinkat("")
    }

    fn do_link(&self, op: &op::Link<'_>) -> io::Result<EntryOut> {
        let inodes = self.inodes.lock().unwrap();

        let source = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let mut source = source.lock().unwrap();

        let parent = inodes.get(op.newparent()).ok_or_else(no_entry)?;
        let parent = parent.lock().unwrap();

        if source.is_symlink {
            source
                .fd
                .linkat("", &parent.fd, op.newname(), 0)
                .map_err(|err| match err.raw_os_error() {
                    Some(libc::ENOENT) | Some(libc::EINVAL) => {
                        // no race-free way to hard-link a symlink.
                        io::Error::from_raw_os_error(libc::EOPNOTSUPP)
                    }
                    _ => err,
                })?;
        } else {
            fs::link(
                source.fd.procname(),
                &parent.fd,
                op.newname(),
                libc::AT_SYMLINK_FOLLOW,
            )?;
        }

        let stat = source.fd.fstatat("", libc::AT_SYMLINK_NOFOLLOW)?;
        let entry = self.make_entry_param(source.ino, stat);

        source.refcount += 1;

        Ok(entry)
    }

    fn make_node(
        &self,
        parent: Ino,
        name: &OsStr,
        mode: u32,
        rdev: Option<u32>,
        link: Option<&OsStr>,
    ) -> io::Result<EntryOut> {
        {
            let inodes = self.inodes.lock().unwrap();
            let parent = inodes.get(parent).ok_or_else(no_entry)?;
            let parent = parent.lock().unwrap();

            match mode & libc::S_IFMT {
                libc::S_IFDIR => {
                    parent.fd.mkdirat(name, mode)?;
                }
                libc::S_IFLNK => {
                    let link = link.expect("missing 'link'");
                    parent.fd.symlinkat(name, link)?;
                }
                _ => {
                    parent
                        .fd
                        .mknodat(name, mode, rdev.unwrap_or(0) as libc::dev_t)?;
                }
            }
        }
        self.do_lookup(parent, name)
    }

    fn do_unlink(&self, op: &op::Unlink<'_>) -> io::Result<()> {
        let inodes = self.inodes.lock().unwrap();
        let parent = inodes.get(op.parent()).ok_or_else(no_entry)?;
        let parent = parent.lock().unwrap();
        parent.fd.unlinkat(op.name(), 0)?;
        Ok(())
    }

    fn do_rmdir(&self, op: &op::Rmdir<'_>) -> io::Result<()> {
        let inodes = self.inodes.lock().unwrap();
        let parent = inodes.get(op.parent()).ok_or_else(no_entry)?;
        let parent = parent.lock().unwrap();
        parent.fd.unlinkat(op.name(), libc::AT_REMOVEDIR)?;
        Ok(())
    }

    fn do_rename(&self, op: &op::Rename<'_>) -> io::Result<()> {
        if op.flags() != 0 {
            // rename2 is not supported.
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }

        let inodes = self.inodes.lock().unwrap();

        let parent = inodes.get(op.parent()).ok_or_else(no_entry)?;
        let newparent = inodes.get(op.newparent()).ok_or_else(no_entry)?;

        let parent = parent.lock().unwrap();
        if op.parent() == op.newparent() {
            parent
                .fd
                .renameat(op.name(), None::<&FileDesc>, op.newname())?;
        } else {
            let newparent = newparent.lock().unwrap();
            parent
                .fd
                .renameat(op.name(), Some(&newparent.fd), op.newname())?;
        }

        Ok(())
    }

    fn do_opendir(&self, op: &op::Opendir<'_>) -> io::Result<OpenOut> {
        let inodes = self.inodes.lock().unwrap();
        let inode = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let inode = inode.lock().unwrap();
        let dir = inode.fd.read_dir()?;
        let fh = self.opened_dirs.insert(Mutex::new(dir));

        let mut out = OpenOut::default();
        out.fh(fh);

        Ok(out)
    }

    fn do_readdir(&self, op: &op::Readdir<'_>) -> io::Result<ReaddirOut> {
        if op.mode() == op::ReaddirMode::Plus {
            return Err(io::Error::from_raw_os_error(libc::ENOSYS));
        }

        let read_dir = self
            .opened_dirs
            .get(op.fh())
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
        let read_dir = &mut *read_dir.lock().unwrap();
        read_dir.seek(op.offset());

        let mut out = ReaddirOut::new(op.size() as usize);
        for entry in read_dir {
            let entry = entry?;
            if out.entry(&entry.name, entry.ino, entry.typ, entry.off) {
                break;
            }
        }

        Ok(out)
    }

    fn do_fsyncdir(&self, op: &op::Fsyncdir<'_>) -> io::Result<()> {
        let read_dir = self.opened_dirs.get(op.fh()).ok_or_else(no_entry)?;
        let read_dir = read_dir.lock().unwrap();

        if op.datasync() {
            read_dir.sync_data()?;
        } else {
            read_dir.sync_all()?;
        }

        Ok(())
    }

    fn do_releasedir(&self, op: &op::Releasedir<'_>) -> io::Result<()> {
        let _dir = self.opened_dirs.remove(op.fh());
        Ok(())
    }

    fn do_open(&self, op: &op::Open<'_>) -> io::Result<OpenOut> {
        let inodes = self.inodes.lock().unwrap();
        let inode = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let inode = inode.lock().unwrap();

        let mut options = OpenOptions::new();
        match (op.flags() & 0x03) as i32 {
            libc::O_RDONLY => {
                options.read(true);
            }
            libc::O_WRONLY => {
                options.write(true);
            }
            libc::O_RDWR => {
                options.read(true).write(true);
            }
            _ => (),
        }
        options.custom_flags(op.flags() as i32 & !libc::O_NOFOLLOW);

        let file = options.open(&inode.fd.procname())?;
        let fh = self.opened_files.insert(Mutex::new(file));

        let mut out = OpenOut::default();
        out.fh(fh);

        Ok(out)
    }

    fn do_read(&self, op: &op::Read<'_>) -> io::Result<Vec<u8>> {
        let file = self.opened_files.get(op.fh()).ok_or_else(no_entry)?;
        let mut file = file.lock().unwrap();
        let file = &mut *file;

        file.seek(io::SeekFrom::Start(op.offset()))?;

        let mut buf = Vec::<u8>::with_capacity(op.size() as usize);
        file.take(op.size() as u64).read_to_end(&mut buf)?;

        Ok(buf)
    }

    fn do_write<T>(&self, op: &op::Write<'_>, mut data: T) -> io::Result<WriteOut>
    where
        T: BufRead + Unpin,
    {
        let file = self.opened_files.get(op.fh()).ok_or_else(no_entry)?;
        let mut file = file.lock().unwrap();
        let file = &mut *file;

        file.seek(io::SeekFrom::Start(op.offset()))?;

        // At here, the data is transferred via the temporary buffer due to
        // the incompatibility between the I/O abstraction in `futures` and
        // `tokio`.
        //
        // In order to efficiently transfer the large files, both of zero
        // copying support in `polyfuse` and resolution of impedance mismatch
        // between `futures::io` and `tokio::io` are required.
        let mut buf = Vec::with_capacity(op.size() as usize);
        data.read_to_end(&mut buf)?;

        let mut buf = &buf[..];
        let mut buf = (&mut buf).take(op.size() as u64);
        let written = std::io::copy(&mut buf, &mut *file)?;

        let mut out = WriteOut::default();
        out.size(written as u32);

        Ok(out)
    }

    fn do_flush(&self, op: &op::Flush<'_>) -> io::Result<()> {
        let file = self.opened_files.get(op.fh()).ok_or_else(no_entry)?;
        let file = file.lock().unwrap();

        file.sync_all()?;

        Ok(())
    }

    fn do_fsync(&self, op: &op::Fsync<'_>) -> io::Result<()> {
        let file = self.opened_files.get(op.fh()).ok_or_else(no_entry)?;
        let file = file.lock().unwrap();

        if op.datasync() {
            file.sync_data()?;
        } else {
            file.sync_all()?;
        }

        Ok(())
    }

    fn do_flock(&self, op: &op::Flock<'_>) -> io::Result<()> {
        let file = self.opened_files.get(op.fh()).ok_or_else(no_entry)?;
        let file = file.lock().unwrap();

        let op = op.op().expect("invalid lock operation") as i32;

        fs::flock(&*file, op)?;

        Ok(())
    }

    fn do_fallocate(&self, op: &op::Fallocate<'_>) -> io::Result<()> {
        if op.mode() != 0 {
            return Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP));
        }

        let file = self.opened_files.get(op.fh()).ok_or_else(no_entry)?;
        let file = file.lock().unwrap();

        fs::posix_fallocate(&*file, op.offset() as i64, op.length() as i64)?;

        Ok(())
    }

    fn do_release(&self, op: &op::Release<'_>) -> io::Result<()> {
        let _file = self.opened_files.remove(op.fh());
        Ok(())
    }

    fn do_getxattr(
        &self,
        op: &op::Getxattr<'_>,
    ) -> io::Result<impl polyfuse::bytes::Bytes + Debug> {
        let inodes = self.inodes.lock().unwrap();
        let inode = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let inode = inode.lock().unwrap();

        if inode.is_symlink {
            // no race-free way to getxattr on symlink.
            return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
        }

        match op.size() {
            0 => {
                let size = fs::getxattr(inode.fd.procname(), op.name(), None)?;
                let mut out = XattrOut::default();
                out.size(size as u32);
                Ok(Either::Left(out))
            }
            size => {
                let mut value = vec![0u8; size as usize];
                let n = fs::getxattr(inode.fd.procname(), op.name(), Some(&mut value[..]))?;
                value.resize(n as usize, 0);
                Ok(Either::Right(value))
            }
        }
    }

    fn do_listxattr(
        &self,
        op: &op::Listxattr<'_>,
    ) -> io::Result<impl polyfuse::bytes::Bytes + Debug> {
        let inodes = self.inodes.lock().unwrap();
        let inode = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let inode = inode.lock().unwrap();

        if inode.is_symlink {
            // no race-free way to getxattr on symlink.
            return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
        }

        match op.size() {
            0 => {
                let size = fs::listxattr(inode.fd.procname(), None)?;
                let mut out = XattrOut::default();
                out.size(size as u32);
                Ok(Either::Left(out))
            }
            size => {
                let mut value = vec![0u8; size as usize];
                let n = fs::listxattr(inode.fd.procname(), Some(&mut value[..]))?;
                value.resize(n as usize, 0);
                Ok(Either::Right(value))
            }
        }
    }

    fn do_setxattr(&self, op: &op::Setxattr<'_>) -> io::Result<()> {
        let inodes = self.inodes.lock().unwrap();
        let inode = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let inode = inode.lock().unwrap();

        if inode.is_symlink {
            // no race-free way to getxattr on symlink.
            return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
        }

        fs::setxattr(
            inode.fd.procname(),
            op.name(),
            op.value(),
            op.flags() as libc::c_int,
        )?;

        Ok(())
    }

    fn do_removexattr(&self, op: &op::Removexattr<'_>) -> io::Result<()> {
        let inodes = self.inodes.lock().unwrap();
        let inode = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let inode = inode.lock().unwrap();

        if inode.is_symlink {
            // no race-free way to getxattr on symlink.
            return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
        }

        fs::removexattr(inode.fd.procname(), op.name())?;

        Ok(())
    }

    fn do_statfs(&self, op: &op::Statfs<'_>) -> io::Result<StatfsOut> {
        let inodes = self.inodes.lock().unwrap();
        let inode = inodes.get(op.ino()).ok_or_else(no_entry)?;
        let inode = inode.lock().unwrap();

        let st = fs::fstatvfs(&inode.fd)?;

        let mut out = StatfsOut::default();
        fill_statfs(out.statfs(), &st);

        Ok(out)
    }
}

fn fill_attr(attr: &mut FileAttr, st: &libc::stat) {
    attr.ino(st.st_ino);
    attr.size(st.st_size as u64);
    attr.mode(st.st_mode);
    attr.nlink(st.st_nlink as u32);
    attr.uid(st.st_uid);
    attr.gid(st.st_gid);
    attr.rdev(st.st_rdev as u32);
    attr.blksize(st.st_blksize as u32);
    attr.blocks(st.st_blocks as u64);
    attr.atime(Duration::new(st.st_atime as u64, st.st_atime_nsec as u32));
    attr.mtime(Duration::new(st.st_mtime as u64, st.st_mtime_nsec as u32));
    attr.ctime(Duration::new(st.st_ctime as u64, st.st_ctime_nsec as u32));
}

fn fill_statfs(statfs: &mut Statfs, st: &libc::statvfs) {
    statfs.bsize(st.f_bsize as u32);
    statfs.frsize(st.f_frsize as u32);
    statfs.blocks(st.f_blocks);
    statfs.bfree(st.f_bfree);
    statfs.bavail(st.f_bavail);
    statfs.files(st.f_files);
    statfs.ffree(st.f_ffree);
    statfs.namelen(st.f_namemax as u32);
}

// ==== HandlePool ====

struct HandlePool<T>(Mutex<Slab<Arc<T>>>);

impl<T> Default for HandlePool<T> {
    fn default() -> Self {
        Self(Mutex::default())
    }
}

impl<T> HandlePool<T> {
    fn get(&self, fh: u64) -> Option<Arc<T>> {
        self.0.lock().unwrap().get(fh as usize).cloned()
    }

    fn remove(&self, fh: u64) -> Arc<T> {
        self.0.lock().unwrap().remove(fh as usize)
    }

    fn insert(&self, entry: T) -> u64 {
        self.0.lock().unwrap().insert(Arc::new(entry)) as u64
    }
}

// ==== INode ====

struct INode {
    ino: Ino,
    src_id: SrcId,
    is_symlink: bool,
    fd: FileDesc,
    refcount: u64,
}

// ==== INodeTable ====

struct INodeTable {
    map: HashMap<Ino, Arc<Mutex<INode>>>,
    src_to_ino: HashMap<SrcId, Ino>,
    next_ino: u64,
}

impl INodeTable {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            src_to_ino: HashMap::new(),
            next_ino: 1, // the ino is started with 1 and the first entry is mapped to the root.
        }
    }

    fn get(&self, ino: Ino) -> Option<Arc<Mutex<INode>>> {
        self.map.get(&ino).cloned()
    }

    fn get_src(&self, src_id: SrcId) -> Option<Arc<Mutex<INode>>> {
        let ino = self.src_to_ino.get(&src_id)?;
        self.map.get(ino).cloned()
    }

    fn vacant_entry(&mut self) -> VacantEntry<'_> {
        let ino = self.next_ino;
        VacantEntry { table: self, ino }
    }
}

struct VacantEntry<'a> {
    table: &'a mut INodeTable,
    ino: Ino,
}

impl VacantEntry<'_> {
    fn ino(&self) -> Ino {
        self.ino
    }

    fn insert(self, inode: INode) {
        let src_id = inode.src_id;
        self.table.map.insert(self.ino, Arc::new(Mutex::new(inode)));
        self.table.src_to_ino.insert(src_id, self.ino);
        self.table.next_ino += 1;
    }
}

#[inline]
fn no_entry() -> io::Error {
    io::Error::from_raw_os_error(libc::ENOENT)
}

#[inline]
fn io_to_errno(err: io::Error) -> i32 {
    err.raw_os_error().unwrap_or(libc::EIO)
}

} else {
    // This example needs some work to build on FreeBSD
    fn main() {}
}
}
