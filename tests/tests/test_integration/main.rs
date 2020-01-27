use futures::future::Future;
use maybe_unwind::FutureMaybeUnwindExt as _;
use mimicaw::{Args, Outcome, Test};
use polyfuse::Filesystem;
use std::{
    io,
    panic::{AssertUnwindSafe, UnwindSafe},
};
use tokio::task;

mod root;

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
                Err(unwind) => Outcome::failed().error_message(unwind.to_string()),
            }
        })
    })
}

struct TestFs;

impl Filesystem for TestFs {}

#[tokio::main(basic_scheduler)]
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

    macro_rules! test_suite {
        ($($path:path),*$(,)?) => {
            vec![$(
                Test::test(
                    stringify!($path),
                    wrap_test_case(AssertUnwindSafe($path(mountpoint.path().to_owned()))),
                ),
            )*]
        }
    }

    let tests = test_suite![root::stat,];

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
