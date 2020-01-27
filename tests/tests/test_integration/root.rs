use std::panic::AssertUnwindSafe;
use std::path::PathBuf;

pub(crate) async fn stat(mountpoint: PathBuf) {
    let err = e!(AssertUnwindSafe(tokio::fs::symlink_metadata(mountpoint)))
        .await
        .unwrap_err();
    match err.raw_os_error() {
        Some(libc::ENOSYS) => (),
        _ => panic!("incorrect error number"),
    }
}
