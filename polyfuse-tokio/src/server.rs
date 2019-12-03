//! Serve FUSE filesystem.

use crate::channel::Channel;
use bytes::Bytes;
use futures::{
    future::{Future, FutureExt},
    select,
};
use libc::c_int;
use polyfuse::{request::BytesBuffer, Filesystem, Session, SessionInitializer};
use std::{ffi::OsStr, io, path::Path, sync::Arc};
use tokio::signal::unix::{signal, SignalKind};

/// A FUSE filesystem server running on Tokio runtime.
#[derive(Debug)]
pub struct Server {
    session: Arc<Session>,
    notifier: Arc<polyfuse::Notifier<Bytes>>,
    channel: Channel,
}

impl Server {
    /// Create a FUSE server mounted on the specified path.
    pub async fn mount(mountpoint: impl AsRef<Path>, mountopts: &[&OsStr]) -> io::Result<Self> {
        let mut channel = Channel::open(mountpoint.as_ref(), mountopts)?;
        let session = SessionInitializer::default() //
            .init(&mut channel)
            .await?;
        Ok(Server {
            session: Arc::new(session),
            notifier: Arc::new(polyfuse::Notifier::new()),
            channel,
        })
    }

    /// Create an instance of `Notifier` associated with this server.
    pub fn notifier(&mut self) -> io::Result<Notifier> {
        let channel = self.channel.try_clone()?;
        Ok(Notifier {
            session: self.session.clone(),
            notifier: self.notifier.clone(),
            channel,
        })
    }

    /// Run a FUSE filesystem daemon.
    pub async fn run<F>(self, fs: F) -> io::Result<()>
    where
        F: Filesystem<Bytes> + Send + 'static,
    {
        let sig = default_shutdown_signal()?;
        let _sig = self.run_until(fs, sig).await?;
        Ok(())
    }

    /// Run a FUSE filesystem until the specified signal is received.
    #[allow(clippy::unnecessary_mut_passed)]
    pub async fn run_until<F, S>(self, fs: F, sig: S) -> io::Result<Option<S::Output>>
    where
        F: Filesystem<Bytes> + Send + 'static,
        S: Future + Unpin,
    {
        let Self {
            session,
            notifier,
            mut channel,
        } = self;
        let fs = Arc::new(fs);
        let mut sig = sig.fuse();

        let mut main_loop = Box::pin(async move {
            loop {
                let mut buf = BytesBuffer::new(session.buffer_size());
                if let Err(err) = session.receive(&mut channel, &mut buf, &notifier).await {
                    match err.raw_os_error() {
                        Some(libc::ENODEV) => {
                            tracing::debug!("connection was closed by the kernel");
                            return Ok(());
                        }
                        _ => return Err(err),
                    }
                }

                let session = session.clone();
                let fs = fs.clone();
                let mut writer = channel.try_clone()?;
                tokio::spawn(async move {
                    if let Err(e) = session.process(&*fs, &mut buf, &mut writer).await {
                        tracing::error!("error during handling a request: {}", e);
                    }
                });
            }
        })
        .fuse();

        select! {
            _ = main_loop => Ok(None),
            sig = sig => Ok(Some(sig)),
        }
    }
}

/// Notification sender to the kernel.
#[derive(Debug)]
pub struct Notifier {
    session: Arc<Session>,
    notifier: Arc<polyfuse::Notifier<Bytes>>,
    channel: Channel,
}

impl Notifier {
    /// Attempt to make a clone of this instance.
    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            session: self.session.clone(),
            notifier: self.notifier.clone(),
            channel: self.channel.try_clone()?,
        })
    }

    /// Invalidate the specified range of cache data for an inode.
    ///
    /// When the kernel receives this notification, some requests are queued to read
    /// the updated data.
    pub async fn inval_inode(&mut self, ino: u64, off: i64, len: i64) -> io::Result<()> {
        self.notifier
            .inval_inode(&mut self.channel, &*self.session, ino, off, len)
            .await
    }

    /// Invalidate an entry with the specified name in the directory.
    pub async fn inval_entry(&mut self, parent: u64, name: impl AsRef<OsStr>) -> io::Result<()> {
        self.notifier
            .inval_entry(&mut self.channel, &*self.session, parent, name)
            .await
    }

    /// Notify that an entry with the specified name has been deleted from the directory.
    pub async fn delete(
        &mut self,
        parent: u64,
        child: u64,
        name: impl AsRef<OsStr>,
    ) -> io::Result<()> {
        self.notifier
            .delete(&mut self.channel, &*self.session, parent, child, name)
            .await
    }

    /// Replace the specified range of cache data with a new value.
    pub async fn store(&mut self, ino: u64, offset: u64, data: &[&[u8]]) -> io::Result<()> {
        self.notifier
            .store(&mut self.channel, &*self.session, ino, offset, data)
            .await
    }

    /// Retrieve the value of the cache data with the specified range.
    pub async fn retrieve(&mut self, ino: u64, offset: u64, size: u32) -> io::Result<Bytes> {
        let handle = self
            .notifier
            .retrieve(&mut self.channel, &*self.session, ino, offset, size)
            .await?;
        let (in_offset, data) = handle.await;
        debug_assert_eq!(offset, in_offset);
        Ok(data)
    }

    /// Notify an I/O readiness.
    pub async fn poll_wakeup(&mut self, kh: u64) -> io::Result<()> {
        self.notifier
            .poll_wakeup(&mut self.channel, &*self.session, kh)
            .await
    }
}

#[allow(clippy::unnecessary_mut_passed)]
fn default_shutdown_signal() -> io::Result<impl Future<Output = c_int> + Unpin> {
    let mut sighup = signal(SignalKind::hangup())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigpipe = signal(SignalKind::pipe())?;

    Ok(Box::pin(async move {
        // TODO: use stabilized API.
        let mut sighup = Box::pin(sighup.recv()).fuse();
        let mut sigint = Box::pin(sigint.recv()).fuse();
        let mut sigterm = Box::pin(sigterm.recv()).fuse();
        let mut sigpipe = Box::pin(sigpipe.recv()).fuse();

        loop {
            select! {
                _ = sighup => {
                    tracing::debug!("Got SIGHUP");
                    return libc::SIGHUP;
                },
                _ = sigint => {
                    tracing::debug!("Got SIGINT");
                    return libc::SIGINT;
                },
                _ = sigterm => {
                    tracing::debug!("Got SIGTERM");
                    return libc::SIGTERM;
                },
                _ = sigpipe => {
                    tracing::debug!("Got SIGPIPE (and ignored)");
                    continue
                }
            }
        }
    }))
}
