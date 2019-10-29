use crate::{
    buf::{Buffer, MAX_WRITE_SIZE},
    fs::{FileLock, Filesystem, Operation},
    parse::{Arg, Request},
    reply::{Payload, ReplyData},
};
use futures_channel::oneshot;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{io::AsyncWriteExt, lock::Mutex};
use polyfuse_sys::abi::{fuse_in_header, fuse_init_out, fuse_out_header};
use smallvec::SmallVec;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt,
    future::Future,
    io::{self, IoSlice},
};

/// A FUSE filesystem driver.
#[derive(Debug)]
pub struct Session {
    proto_major: u32,
    proto_minor: u32,
    max_readahead: u32,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    exited: bool,
    remains: HashMap<u64, oneshot::Sender<()>>,
}

impl Session {
    /// Create a new session initializer.
    pub fn initializer() -> InitSession {
        InitSession::default()
    }

    /// Dispatch an incoming request to the provided operations.
    #[allow(clippy::cognitive_complexity)]
    pub async fn dispatch<F, T, W>(
        &self,
        fs: &F,
        request: Request<'_>,
        data: Option<T>,
        writer: &mut W,
    ) -> io::Result<()>
    where
        F: Filesystem<T>,
        W: AsyncWrite + Unpin + 'static,
    {
        if self.inner.lock().await.exited {
            log::warn!("The sesson has already been exited");
            return Ok(());
        }

        let Request { header, arg, .. } = request;
        let ino = header.nodeid;

        let mut cx = Context {
            header,
            writer: &mut *writer,
            session: &*self,
        };

        match arg {
            Arg::Init { .. } => {
                log::warn!("");
                cx.reply_err(libc::EIO).await?;
            }
            Arg::Destroy => {
                self.inner.lock().await.exited = true;
                cx.send_reply(0, &[]).await?;
            }
            Arg::Lookup { name } => {
                fs.call(
                    &mut cx,
                    Operation::Lookup {
                        parent: ino,
                        name,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Forget { arg } => {
                // no reply.
                fs.call(
                    &mut cx,
                    Operation::Forget {
                        nlookups: &[(ino, arg.nlookup)],
                    },
                )
                .await?;
            }
            Arg::Getattr { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Getattr {
                        ino,
                        fh: arg.fh(),
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Setattr { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Setattr {
                        ino,
                        fh: arg.fh(),
                        mode: arg.mode(),
                        uid: arg.uid(),
                        gid: arg.gid(),
                        size: arg.size(),
                        atime: arg.atime(),
                        mtime: arg.mtime(),
                        ctime: arg.ctime(),
                        lock_owner: arg.lock_owner(),
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Readlink => {
                fs.call(
                    &mut cx,
                    Operation::Readlink {
                        ino,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Symlink { name, link } => {
                fs.call(
                    &mut cx,
                    Operation::Symlink {
                        parent: ino,
                        name,
                        link,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Mknod { arg, name } => {
                fs.call(
                    &mut cx,
                    Operation::Mknod {
                        parent: ino,
                        name,
                        mode: arg.mode,
                        rdev: arg.rdev,
                        umask: Some(arg.umask),
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Mkdir { arg, name } => {
                fs.call(
                    &mut cx,
                    Operation::Mkdir {
                        parent: ino,
                        name,
                        mode: arg.mode,
                        umask: Some(arg.umask),
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Unlink { name } => {
                fs.call(
                    &mut cx,
                    Operation::Unlink {
                        parent: ino,
                        name,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Rmdir { name } => {
                fs.call(
                    &mut cx,
                    Operation::Rmdir {
                        parent: ino,
                        name,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Rename { arg, name, newname } => {
                fs.call(
                    &mut cx,
                    Operation::Rename {
                        parent: ino,
                        name,
                        newparent: arg.newdir,
                        newname,
                        flags: 0,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Link { arg, newname } => {
                fs.call(
                    &mut cx,
                    Operation::Link {
                        ino: arg.oldnodeid,
                        newparent: ino,
                        newname,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Open { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Open {
                        ino,
                        flags: arg.flags,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Read { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Read {
                        ino,
                        fh: arg.fh,
                        offset: arg.offset,
                        flags: arg.flags,
                        lock_owner: arg.lock_owner(),
                        reply: ReplyData::new(arg.size),
                    },
                )
                .await?;
            }
            Arg::Write { arg } => match data {
                Some(data) => {
                    fs.call(
                        &mut cx,
                        Operation::Write {
                            ino,
                            fh: arg.fh,
                            offset: arg.offset,
                            data,
                            size: arg.size,
                            flags: arg.flags,
                            lock_owner: arg.lock_owner(),
                            reply: Default::default(),
                        },
                    )
                    .await?;
                }
                None => panic!("unexpected condition"),
            },
            Arg::Release { arg } => {
                let mut flush = false;
                let mut flock_release = false;
                let mut lock_owner = None;
                if self.proto_minor >= 8 {
                    flush = arg.release_flags & polyfuse_sys::abi::FUSE_RELEASE_FLUSH != 0;
                    lock_owner.get_or_insert_with(|| arg.lock_owner);
                }
                if arg.release_flags & polyfuse_sys::abi::FUSE_RELEASE_FLOCK_UNLOCK != 0 {
                    flock_release = true;
                    lock_owner.get_or_insert_with(|| arg.lock_owner);
                }
                fs.call(
                    &mut cx,
                    Operation::Release {
                        ino,
                        fh: arg.fh,
                        flags: arg.flags,
                        lock_owner,
                        flush,
                        flock_release,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Statfs => {
                fs.call(
                    &mut cx,
                    Operation::Statfs {
                        ino,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Fsync { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Fsync {
                        ino,
                        fh: arg.fh,
                        datasync: arg.datasync(),
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Setxattr { arg, name, value } => {
                fs.call(
                    &mut cx,
                    Operation::Setxattr {
                        ino,
                        name,
                        value,
                        flags: arg.flags,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Getxattr { arg, name } => {
                fs.call(
                    &mut cx,
                    Operation::Getxattr {
                        ino,
                        name,
                        size: arg.size,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Listxattr { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Listxattr {
                        ino,
                        size: arg.size,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Removexattr { name } => {
                fs.call(
                    &mut cx,
                    Operation::Removexattr {
                        ino,
                        name,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Flush { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Flush {
                        ino,
                        fh: arg.fh,
                        lock_owner: arg.lock_owner,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Opendir { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Opendir {
                        ino,
                        flags: arg.flags,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Readdir { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Readdir {
                        ino,
                        fh: arg.fh,
                        offset: arg.offset,
                        reply: ReplyData::new(arg.size),
                    },
                )
                .await?;
            }
            Arg::Releasedir { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Releasedir {
                        ino,
                        fh: arg.fh,
                        flags: arg.flags,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Fsyncdir { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Fsyncdir {
                        ino,
                        fh: arg.fh,
                        datasync: arg.datasync(),
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Getlk { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Getlk {
                        ino,
                        fh: arg.fh,
                        owner: arg.owner,
                        lk: FileLock::new(&arg.lk),
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Setlk { arg, sleep } => {
                if arg.lk_flags & polyfuse_sys::abi::FUSE_LK_FLOCK != 0 {
                    const F_RDLCK: u32 = libc::F_RDLCK as u32;
                    const F_WRLCK: u32 = libc::F_WRLCK as u32;
                    const F_UNLCK: u32 = libc::F_UNLCK as u32;
                    #[allow(clippy::cast_possible_wrap)]
                    let mut op = match arg.lk.typ {
                        F_RDLCK => libc::LOCK_SH as u32,
                        F_WRLCK => libc::LOCK_EX as u32,
                        F_UNLCK => libc::LOCK_UN as u32,
                        _ => return cx.reply_err(libc::EIO).await,
                    };
                    if !sleep {
                        op |= libc::LOCK_NB as u32;
                    }
                    fs.call(
                        &mut cx,
                        Operation::Flock {
                            ino,
                            fh: arg.fh,
                            owner: arg.owner,
                            op,
                            reply: Default::default(),
                        },
                    )
                    .await?;
                } else {
                    fs.call(
                        &mut cx,
                        Operation::Setlk {
                            ino,
                            fh: arg.fh,
                            owner: arg.owner,
                            lk: FileLock::new(&arg.lk),
                            sleep,
                            reply: Default::default(),
                        },
                    )
                    .await?;
                }
            }
            Arg::Access { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Access {
                        ino,
                        mask: arg.mask,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Create { arg, name } => {
                fs.call(
                    &mut cx,
                    Operation::Create {
                        parent: ino,
                        name,
                        mode: arg.mode,
                        umask: Some(arg.umask),
                        open_flags: arg.flags,
                        reply: Default::default(),
                    },
                )
                .await?;
            }
            Arg::Interrupt { arg } => {
                log::debug!("INTERRUPT (unique = {:?})", arg.unique);
                let mut inner = self.inner.lock().await;
                if let Some(tx) = inner.remains.remove(&header.unique) {
                    let _ = tx.send(());
                }
            }
            Arg::Bmap { arg } => {
                fs.call(
                    &mut cx,
                    Operation::Bmap {
                        ino,
                        block: arg.block,
                        blocksize: arg.blocksize,
                        reply: Default::default(),
                    },
                )
                .await?;
            }

            // Ioctl,
            // Poll,
            // NotifyReply,
            // BatchForget,
            // Fallocate,
            // Readdirplus,
            // Rename2,
            // Lseek,
            // CopyFileRange,
            Arg::Unknown => {
                log::warn!("unsupported opcode: {:?}", header.opcode);
                cx.reply_err(libc::ENOSYS).await?;
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn register(&self, unique: u64) -> impl Future<Output = ()> {
        let mut inner = self.inner.lock().await;
        let (tx, rx) = oneshot::channel();
        inner.remains.insert(unique, tx);
        async move {
            let _ = rx.await;
        }
    }
}

/// Session initializer.
#[derive(Debug, Default)]
pub struct InitSession {
    _p: (),
}

impl InitSession {
    /// Start a new FUSE session.
    ///
    /// This function receives an INIT request from the kernel and replies
    /// after initializing the connection parameters.
    pub async fn start<I>(self, io: &mut I) -> io::Result<Session>
    where
        I: AsyncRead + AsyncWrite + Unpin,
    {
        let mut buf = Buffer::default();

        loop {
            let terminated = buf.receive(io).await?;
            if terminated {
                log::warn!("the connection is closed");
                return Err(io::Error::from_raw_os_error(libc::ENODEV));
            }

            let (Request { header, arg, .. }, _data) = buf.extract()?;

            let (proto_major, proto_minor, max_readahead);
            match arg {
                Arg::Init { arg } => {
                    let mut init_out = fuse_init_out::default();

                    if arg.major > 7 {
                        log::debug!("wait for a second INIT request with a 7.X version.");
                        send_reply(&mut *io, header.unique, 0, &[init_out.as_bytes()]).await?;
                        continue;
                    }

                    if arg.major < 7 || (arg.major == 7 && arg.minor < 6) {
                        log::warn!("unsupported protocol version: {}.{}", arg.major, arg.minor);
                        send_reply(&mut *io, header.unique, libc::EPROTO, &[]).await?;
                        return Err(io::Error::from_raw_os_error(libc::EPROTO));
                    }

                    // remember the kernel parameters.
                    proto_major = arg.major;
                    proto_minor = arg.minor;
                    max_readahead = arg.max_readahead;

                    // TODO: max_background, congestion_threshold, time_gran, max_pages
                    init_out.max_readahead = arg.max_readahead;
                    init_out.max_write = MAX_WRITE_SIZE;

                    send_reply(&mut *io, header.unique, 0, &[init_out.as_bytes()]).await?;
                }
                _ => {
                    log::warn!(
                        "ignoring an operation before init (opcode={:?})",
                        header.opcode
                    );
                    send_reply(&mut *io, header.unique, libc::EIO, &[]).await?;
                    continue;
                }
            }

            return Ok(Session {
                proto_major,
                proto_minor,
                max_readahead,
                inner: Mutex::new(Inner {
                    exited: false,
                    remains: HashMap::new(),
                }),
            });
        }
    }
}

/// Contextural information about an incoming request.
pub struct Context<'a> {
    header: &'a fuse_in_header,
    writer: &'a mut (dyn AsyncWrite + Unpin),
    #[allow(dead_code)]
    session: &'a Session,
}

impl fmt::Debug for Context<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Context").finish()
    }
}

impl<'a> Context<'a> {
    /// Return the user ID of the calling process.
    pub fn uid(&self) -> u32 {
        self.header.uid
    }

    /// Return the group ID of the calling process.
    pub fn gid(&self) -> u32 {
        self.header.gid
    }

    /// Return the process ID of the calling process.
    pub fn pid(&self) -> u32 {
        self.header.pid
    }

    /// Reply to the kernel with an error code.
    pub async fn reply_err(&mut self, error: i32) -> io::Result<()> {
        self.send_reply(error, &[]).await
    }

    /// Reply to the kernel with the specified data.
    #[inline]
    pub(crate) async fn send_reply(&mut self, error: i32, data: &[&[u8]]) -> io::Result<()> {
        send_reply(&mut *self.writer, self.header.unique, error, data).await
    }
}

async fn send_reply(
    writer: &mut (dyn AsyncWrite + Unpin),
    unique: u64,
    error: i32,
    data: &[&[u8]],
) -> io::Result<()> {
    let data_len: usize = data.iter().map(|t| t.len()).sum();

    let out_header = fuse_out_header {
        unique: unique,
        error: -error,
        len: u32::try_from(std::mem::size_of::<fuse_out_header>() + data_len).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the total length of data is too long: {}", e),
            )
        })?,
    };

    let vec: SmallVec<[_; 4]> = Some(IoSlice::new(out_header.as_bytes()))
        .into_iter()
        .chain(data.iter().map(|t| IoSlice::new(&*t)))
        .collect();

    writer.write_vectored(&*vec).await?;

    Ok(())
}
