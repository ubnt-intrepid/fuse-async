use polyfuse::{
    reply::{AttrOut, OpenOut, PollOut},
    Notifier, Operation, Request, Session,
};

use anyhow::{ensure, Context as _, Result};
use dashmap::DashMap;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

const CONTENT: &str = "Hello, world!\n";

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut args = pico_args::Arguments::from_env();

    let wakeup_interval = Duration::from_secs(
        args //
            .opt_value_from_str("--interval")?
            .unwrap_or(5),
    );

    let mountpoint: PathBuf = args.free_from_str()?.context("missing mountpoint")?;
    ensure!(mountpoint.is_file(), "mountpoint must be a regular file");

    let session = Session::mount(mountpoint, Default::default())?;

    let fs = Arc::new(PollFS::new(session.notifier(), wakeup_interval));

    while let Some(req) = session.next_request()? {
        let fs = fs.clone();
        std::thread::spawn(move || -> Result<()> {
            fs.handle_request(&req)?;
            Ok(())
        });
    }

    Ok(())
}

struct PollFS {
    handles: DashMap<u64, Arc<FileHandle>>,
    next_fh: AtomicU64,

    notifier: Notifier,
    wakeup_interval: Duration,
}

impl PollFS {
    fn new(notifier: Notifier, wakeup_interval: Duration) -> Self {
        Self {
            handles: DashMap::new(),
            next_fh: AtomicU64::new(0),
            notifier,
            wakeup_interval,
        }
    }

    fn handle_request(&self, req: &Request) -> Result<()> {
        let span = tracing::debug_span!("handle_request", unique = req.unique());
        let _enter = span.enter();

        let op = req.operation()?;
        tracing::debug!(?op);

        match op {
            Operation::Getattr(..) => {
                let mut out = AttrOut::default();
                out.attr().ino(1);
                out.attr().nlink(1);
                out.attr().mode((libc::S_IFREG | 0o444).into());
                out.attr().uid(unsafe { libc::getuid() });
                out.attr().gid(unsafe { libc::getgid() });
                out.ttl(Duration::from_secs(u64::max_value() / 2));

                req.reply(out)?;
            }

            Operation::Open(op) => {
                if op.flags() as i32 & libc::O_ACCMODE != libc::O_RDONLY {
                    return req.reply_error(libc::EACCES).map_err(Into::into);
                }

                let is_nonblock = op.flags() as i32 & libc::O_NONBLOCK != 0;

                let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
                let handle = Arc::new(FileHandle {
                    is_nonblock,
                    state: Mutex::default(),
                    condvar: Condvar::new(),
                });

                tracing::info!("spawn reading task");
                std::thread::spawn({
                    let handle = Arc::downgrade(&handle);
                    let notifier = self.notifier.clone();
                    let wakeup_interval = self.wakeup_interval;

                    move || -> Result<()> {
                        let span = tracing::debug_span!("reading_task", fh=?fh);
                        let _enter = span.enter();

                        tracing::info!("start reading");
                        std::thread::sleep(wakeup_interval);

                        tracing::info!("reading completed");

                        // Do nothing when the handle is already closed.
                        if let Some(handle) = handle.upgrade() {
                            let state = &mut *handle.state.lock().unwrap();

                            if let Some(kh) = state.kh {
                                tracing::info!("send wakeup notification, kh={}", kh);
                                notifier.poll_wakeup(kh)?;
                            }

                            state.is_ready = true;
                            handle.condvar.notify_one();
                        }

                        Ok(())
                    }
                });

                self.handles.insert(fh, handle);

                let mut out = OpenOut::default();
                out.fh(fh);
                out.direct_io(true);
                out.nonseekable(true);

                req.reply(out)?;
            }

            Operation::Read(op) => {
                let handle = match self.handles.get(&op.fh()) {
                    Some(h) => h,
                    None => return req.reply_error(libc::EINVAL).map_err(Into::into),
                };

                let mut state = handle.state.lock().unwrap();
                if handle.is_nonblock {
                    if !state.is_ready {
                        tracing::info!("send EAGAIN immediately");
                        return req.reply_error(libc::EAGAIN).map_err(Into::into);
                    }
                } else {
                    tracing::info!("wait for the completion of background task");
                    state = handle
                        .condvar
                        .wait_while(state, |state| !state.is_ready)
                        .unwrap();
                }

                debug_assert!(state.is_ready);

                tracing::info!("complete reading");

                let offset = op.offset() as usize;
                let bufsize = op.size() as usize;
                let content = CONTENT.as_bytes().get(offset..).unwrap_or(&[]);

                req.reply(&content[..std::cmp::min(content.len(), bufsize)])?;
            }

            Operation::Poll(op) => {
                let handle = match self.handles.get(&op.fh()) {
                    Some(h) => h,
                    None => return req.reply_error(libc::EINVAL).map_err(Into::into),
                };
                let state = &mut *handle.state.lock().unwrap();

                let mut out = PollOut::default();

                if state.is_ready {
                    tracing::info!("file is ready to read");
                    out.revents(op.events() & libc::POLLIN as u32);
                } else if let Some(kh) = op.kh() {
                    tracing::info!("register the poll handle for notification: kh={}", kh);
                    state.kh = Some(kh);
                }

                req.reply(out)?;
            }

            Operation::Release(op) => {
                drop(self.handles.remove(&op.fh()));
                req.reply(&[])?;
            }

            _ => req.reply_error(libc::ENOSYS)?,
        }

        Ok(())
    }
}

struct FileHandle {
    is_nonblock: bool,
    state: Mutex<FileHandleState>,
    condvar: Condvar,
}

#[derive(Default)]
struct FileHandleState {
    is_ready: bool,
    kh: Option<u64>,
}
