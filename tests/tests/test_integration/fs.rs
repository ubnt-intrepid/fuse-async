use polyfuse::Filesystem;
use std::{io, path::Path};
use tokio::{sync::oneshot, task};

struct TestFs;

impl Filesystem for TestFs {}

pub(crate) fn start_filesystem(mountpoint: &Path) -> Handle {
    let (tx, rx) = oneshot::channel::<()>();
    let mountpoint = mountpoint.to_owned();
    let handle = tokio::spawn(async move {
        let mut server = polyfuse_tokio::Server::mount(&mountpoint, &[]).await?;
        server.run_until(TestFs, rx).await?;
        Ok(())
    });
    Handle {
        handle,
        shutdown: tx,
    }
}

pub(crate) struct Handle {
    handle: task::JoinHandle<io::Result<()>>,
    shutdown: oneshot::Sender<()>,
}

impl Handle {
    pub(crate) async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.shutdown.send(());
        self.handle.await??;
        Ok(())
    }
}
