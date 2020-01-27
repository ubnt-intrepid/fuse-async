use std::panic::AssertUnwindSafe;
use std::path::PathBuf;

pub(crate) async fn stat(mountpoint: PathBuf) {
    let fut = AssertUnwindSafe(tokio::fs::symlink_metadata(mountpoint));
    let err = fut.await.unwrap_err();
    match err.raw_os_error() {
        Some(libc::ENOSYS) => (),
        _ => panic!("incorrect error number"),
    }
}
