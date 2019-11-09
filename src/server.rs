//! Serve FUSE filesystem.

use crate::{
    channel::{Channel, MountOptions},
    lock::Lock,
    session::{Filesystem, NotifyRetrieve, Session},
};
use futures::{
    future::{Future, FutureExt},
    lock::Mutex,
    select,
    stream::StreamExt,
};
use libc::c_int;
use std::{ffi::OsStr, io, path::Path, sync::Arc};
use tokio::signal::unix::{signal, SignalKind};

/// FUSE filesystem server.
#[derive(Debug)]
pub struct Server {
    session: Arc<Session>,
    channel: Channel,
    notify_writer: Option<Arc<Mutex<Channel>>>,
}

impl Server {
    /// Create a FUSE server mounted on the specified path.
    pub async fn mount(mointpoint: impl AsRef<Path>, mountopts: MountOptions) -> io::Result<Self> {
        let mut channel = Channel::open(mointpoint.as_ref(), &mountopts)?;
        let session = Session::start(
            &mut channel, //
            Default::default(),
        )
        .await?;
        Ok(Server {
            session: Arc::new(session),
            channel,
            notify_writer: None,
        })
    }

    pub fn notifier(&mut self) -> io::Result<Notifier> {
        let writer = match self.notify_writer {
            Some(ref writer) => writer,
            None => {
                let writer = self.channel.try_clone(false)?;
                self.notify_writer
                    .get_or_insert(Arc::new(Mutex::new(writer)))
            }
        };

        Ok(Notifier {
            session: self.session.clone(),
            writer: writer.clone(),
        })
    }

    /// Run a FUSE filesystem.
    pub async fn run<F>(self, fs: F) -> io::Result<()>
    where
        F: Filesystem + 'static,
    {
        let sig = default_shutdown_signal()?;
        let _sig = self.run_until(fs, sig).await?;
        Ok(())
    }

    /// Run a FUSE filesystem until the specified signal is received.
    pub async fn run_until<F, S>(self, fs: F, sig: S) -> io::Result<Option<S::Output>>
    where
        F: Filesystem + 'static,
        S: Future + Unpin,
    {
        let session = self.session;
        let fs = Arc::new(fs);
        let mut channel = self.channel;
        let writer = Lock::new(channel.try_clone(false)?);
        let mut sig = sig.fuse();

        let mut main_loop = Box::pin(async move {
            loop {
                let req = match session.receive(&mut channel).await? {
                    Some(req) => req,
                    None => {
                        log::debug!("connection was closed by the kernel");
                        return Ok::<_, io::Error>(());
                    }
                };

                let session = session.clone();
                let fs = fs.clone();
                let mut writer = writer.clone();
                tokio::spawn(async move {
                    if let Err(e) = session.process(&*fs, req, &mut writer).await {
                        log::error!("error during handling a request: {}", e);
                    }
                });
            }
        })
        .fuse();

        // FIXME: graceful shutdown the background tasks.
        select! {
            _ = main_loop => Ok(None),
            sig = sig => Ok(Some(sig)),
        }
    }
}

/// Notification sender to the kernel.
#[derive(Debug, Clone)]
pub struct Notifier {
    session: Arc<Session>,
    writer: Arc<Mutex<Channel>>,
}

impl Notifier {
    pub async fn inval_inode(&self, ino: u64, off: i64, len: i64) -> io::Result<()> {
        let mut writer = self.writer.lock().await;
        self.session
            .notify_inval_inode(&mut *writer, ino, off, len)
            .await
    }

    pub async fn inval_entry(&self, parent: u64, name: impl AsRef<OsStr>) -> io::Result<()> {
        let mut writer = self.writer.lock().await;
        self.session
            .notify_inval_entry(&mut *writer, parent, name)
            .await
    }

    pub async fn delete(&self, parent: u64, child: u64, name: impl AsRef<OsStr>) -> io::Result<()> {
        let mut writer = self.writer.lock().await;
        self.session
            .notify_delete(&mut *writer, parent, child, name)
            .await
    }

    pub async fn store(&self, ino: u64, offset: u64, data: &[&[u8]]) -> io::Result<()> {
        let mut writer = self.writer.lock().await;
        self.session
            .notify_store(&mut *writer, ino, offset, data)
            .await
    }

    pub async fn retrieve(&self, ino: u64, offset: u64, size: u32) -> io::Result<NotifyRetrieve> {
        let mut writer = self.writer.lock().await;
        self.session
            .notify_retrieve(&mut *writer, ino, offset, size)
            .await
    }
}

fn default_shutdown_signal() -> io::Result<impl Future<Output = c_int> + Unpin> {
    let mut sighup = signal(SignalKind::hangup())?.into_future();
    let mut sigint = signal(SignalKind::interrupt())?.into_future();
    let mut sigterm = signal(SignalKind::terminate())?.into_future();
    let mut sigpipe = signal(SignalKind::pipe())?.into_future();

    Ok(Box::pin(async move {
        loop {
            select! {
                _ = sighup => {
                    log::debug!("Got SIGHUP");
                    return libc::SIGHUP;
                },
                _ = sigint => {
                    log::debug!("Got SIGINT");
                    return libc::SIGINT;
                },
                _ = sigterm => {
                    log::debug!("Got SIGTERM");
                    return libc::SIGTERM;
                },
                _ = sigpipe => {
                    log::debug!("Got SIGPIPE (and ignored)");
                    continue
                }
            }
        }
    }))
}
