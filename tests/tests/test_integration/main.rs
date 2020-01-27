use futures::future::Future;
use mimicaw::{Args, Outcome, Test};
use std::panic::UnwindSafe;
use tokio::task;

mod fs;
mod root;

#[tokio::main(basic_scheduler)]
async fn main() -> anyhow::Result<()> {
    maybe_unwind::set_hook();

    let args = Args::from_env().unwrap_or_else(|st| st.exit());

    let mountpoint = tempfile::tempdir()?;
    let fs = crate::fs::start_filesystem(mountpoint.path());

    macro_rules! test_suite {
        ($($path:path),*$(,)?) => {
            vec![$(
                Test::test(
                    stringify!($path), {
                        let mountpoint = mountpoint.path().to_owned();
                        wrap_test_case($path(mountpoint))
                    }
                ),
            )*]
        }
    }

    let tests = test_suite![root::stat];

    let status = mimicaw::run_tests(
        &args,
        tests,
        |_, mut test_fn: Box<dyn FnMut() -> task::JoinHandle<Outcome>>| {
            let handle = test_fn();
            async move { handle.await.unwrap() }
        },
    )
    .await;

    fs.shutdown().await?;

    status.exit()
}

fn wrap_test_case<F>(fut: F) -> Box<dyn FnMut() -> task::JoinHandle<Outcome>>
where
    F: Future<Output = ()> + UnwindSafe + Send + 'static,
{
    use maybe_unwind::FutureMaybeUnwindExt as _;

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
