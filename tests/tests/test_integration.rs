use mimicaw::{Args, Outcome, Test};
use polyfuse::Filesystem;
use std::io;
use tokio::task;

type TestFn = fn() -> task::JoinHandle<Outcome>;

struct TestFs;

impl Filesystem for TestFs {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    let tests: Vec<Test<TestFn>> = vec![
        Test::test("case1", || task::spawn(async { Outcome::passed() })),
        Test::test("case2", || {
            task::spawn(async { Outcome::failed().error_message("foo") })
        }),
        Test::<TestFn>::test("case3_a_should_be_zero", || {
            task::spawn(async move { Outcome::passed() })
        })
        .ignore(true),
    ];

    let status = mimicaw::run_tests(&args, tests, |_, test_fn: TestFn| {
        let handle = test_fn();
        async move { handle.await.unwrap() }
    })
    .await;

    let _ = tx.send(());
    fs.await??;

    status.exit()
}
