use futures::future::Future;
use maybe_unwind::FutureMaybeUnwindExt as _;
use mimicaw::{Args, Outcome, Test};
use polyfuse::Filesystem;
use std::{io, panic::UnwindSafe, path::PathBuf};
use tokio::task;

fn wrap_test_case<F>(fut: F) -> Box<dyn FnMut() -> task::JoinHandle<Outcome>>
where
    F: Future<Output = ()> + UnwindSafe + Send + 'static,
{
    let mut fut = Some(fut.maybe_unwind());
    Box::new(move || {
        let fut = fut.take().unwrap();
        task::spawn(async move {
            match fut.await {
                Ok(()) => Outcome::passed(),
                Err(unwind) => {
                    let msg = match unwind.location() {
                        Some(loc) => format!("[{}] {}", loc, unwind.payload_str()),
                        None => unwind.payload_str().to_string(),
                    };
                    Outcome::failed().error_message(msg)
                }
            }
        })
    })
}

struct TestFs;

impl Filesystem for TestFs {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    maybe_unwind::set_hook();

    let args = Args::from_env().unwrap_or_else(|st| st.exit());

    let mountpoint = tempfile::tempdir()?;
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let fs: task::JoinHandle<io::Result<()>> = tokio::spawn({
        let mountpoint = mountpoint.path().to_owned();
        async move {
            let mut server = polyfuse_tokio::Server::mount(&mountpoint, &[]).await?;
            server.run_until(TestFs, rx).await?;
            Ok(())
        }
    });

    let tests = vec![Test::test(
        "case1",
        wrap_test_case(case1(mountpoint.path().to_owned())),
    )];

    let status = mimicaw::run_tests(
        &args,
        tests,
        |_, mut test_fn: Box<dyn FnMut() -> task::JoinHandle<Outcome>>| {
            let handle = test_fn();
            async move { handle.await.unwrap() }
        },
    )
    .await;

    let _ = tx.send(());
    fs.await??;

    status.exit()
}

async fn case1(_mountpoint: PathBuf) {
    assert!(false, "explicit assertion fail");
}
