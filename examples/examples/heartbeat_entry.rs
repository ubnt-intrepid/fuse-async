#![allow(clippy::unnecessary_mut_passed)]
#![deny(clippy::unimplemented)]

use polyfuse_examples::prelude::*;

use chrono::Local;
use futures::lock::Mutex;
use polyfuse::{
    reply::{ReplyAttr, ReplyEntry},
    DirEntry, FileAttr,
};
use polyfuse_tokio::Server;
use std::{io, mem, sync::Arc};

const ROOT_INO: u64 = 1;
const FILE_INO: u64 = 2;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let mountpoint = examples::get_mountpoint()?;
    ensure!(mountpoint.is_dir(), "the mountpoint must be a directory");

    let mut args = pico_args::Arguments::from_vec(std::env::args_os().skip(2).collect());

    let no_notify = args.contains("--no-notify");
    let timeout: u64 = args.value_from_str("--timeout")?;
    let update_interval: u64 = args.value_from_str("--update-interval")?;

    let heartbeat = Arc::new(Heartbeat::new(timeout));

    // It is necessary to use the primitive server APIs in order to obtain
    // the instance of `Notifier` associated with the server.
    let mut server = Server::mount(mountpoint, &[]).await?;

    // Spawn a task that beats the heart.
    {
        let heartbeat = heartbeat.clone();
        let mut server = if !no_notify {
            Some(server.try_clone()?)
        } else {
            None
        };

        let _: tokio::task::JoinHandle<io::Result<()>> = tokio::task::spawn(async move {
            loop {
                tracing::info!("heartbeat");
                heartbeat.rename_file(server.as_mut()).await?;
                tokio::time::delay_for(std::time::Duration::from_secs(update_interval)).await;
            }
        });
    }

    // Run the filesystem daemon on the foreground.
    server.run(heartbeat).await?;
    Ok(())
}

fn generate_filename() -> String {
    Local::now().format("Time_is_%Hh_%Mm_%Ss").to_string()
}

struct Heartbeat {
    root_attr: FileAttr,
    file_attr: FileAttr,
    timeout: u64,
    current: Mutex<CurrentFile>,
}

struct CurrentFile {
    filename: String,
    nlookup: u64,
}

impl Heartbeat {
    fn new(timeout: u64) -> Self {
        let mut root_attr = FileAttr::default();
        root_attr.set_ino(ROOT_INO);
        root_attr.set_mode(libc::S_IFDIR | 0o555);
        root_attr.set_nlink(1);

        let mut file_attr = FileAttr::default();
        file_attr.set_ino(FILE_INO);
        file_attr.set_mode(libc::S_IFREG | 0o444);
        file_attr.set_nlink(1);
        file_attr.set_size(0);

        Self {
            root_attr,
            file_attr,
            timeout,
            current: Mutex::new(CurrentFile {
                filename: generate_filename(),
                nlookup: 0,
            }),
        }
    }

    async fn rename_file(&self, server: Option<&mut Server>) -> io::Result<()> {
        let mut current = self.current.lock().await;
        let old_filename = mem::replace(&mut current.filename, generate_filename());

        match (server, current.nlookup) {
            (Some(server), n) if n > 0 => {
                tracing::info!("send notify_inval_entry");
                server.notify_inval_entry(ROOT_INO, old_filename).await?;
            }
            _ => (),
        }

        Ok(())
    }
}

#[async_trait]
impl Filesystem for Heartbeat {
    #[allow(clippy::cognitive_complexity)]
    async fn call<'a, 'cx, T: ?Sized>(
        &'a self,
        cx: &'a mut Context<'cx, T>,
        op: Operation<'cx>,
    ) -> io::Result<()>
    where
        T: Reader + Writer + Unpin + Send,
    {
        match op {
            Operation::Lookup(op) => match op.parent() {
                ROOT_INO => {
                    let mut current = self.current.lock().await;
                    if op.name().as_bytes() == current.filename.as_bytes() {
                        op.reply(cx, {
                            ReplyEntry::new(self.file_attr)
                                .entry_valid(self.timeout, 0)
                                .attr_valid(self.timeout, 0)
                        })
                        .await?;
                        current.nlookup += 1;
                    } else {
                        cx.reply_err(libc::ENOENT).await?;
                    }
                }
                _ => cx.reply_err(libc::ENOTDIR).await?,
            },

            Operation::Forget(forgets) => {
                let mut current = self.current.lock().await;
                for forget in forgets.as_ref() {
                    if forget.ino() == FILE_INO {
                        current.nlookup -= forget.nlookup();
                    }
                }
            }

            Operation::Getattr(op) => {
                let attr = match op.ino() {
                    ROOT_INO => self.root_attr,
                    FILE_INO => self.file_attr,
                    _ => return cx.reply_err(libc::ENOENT).await,
                };
                op.reply(cx, {
                    ReplyAttr::new(attr) //
                        .attr_valid(self.timeout, 0)
                })
                .await?
            }

            Operation::Read(op) => match op.ino() {
                ROOT_INO => cx.reply_err(libc::EISDIR).await?,
                FILE_INO => op.reply(cx, &[]).await?,
                _ => cx.reply_err(libc::ENOENT).await?,
            },

            Operation::Readdir(op) => match op.ino() {
                ROOT_INO => {
                    if op.offset() == 0 {
                        let current = self.current.lock().await;
                        let dirent = DirEntry::file(&current.filename, FILE_INO, 1);
                        op.reply(cx, dirent).await?;
                    } else {
                        op.reply(cx, &[]).await?;
                    }
                }
                _ => cx.reply_err(libc::ENOTDIR).await?,
            },

            _ => (),
        }

        Ok(())
    }
}
