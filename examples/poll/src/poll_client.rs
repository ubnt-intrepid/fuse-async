use async_io::Async;
use cfg_if::cfg_if;
use futures::prelude::*;
use std::{
    ffi::{CString, OsStr},
    io::{self, IoSliceMut, Read},
    os::unix::prelude::*,
    path::PathBuf,
};

cfg_if! {
    if #[cfg(target_os = "linux")] {
        const AT_EMPTY_PATH: i32 = libc::AT_EMPTY_PATH;
    } else {
        const AT_EMPTY_PATH: i32 = 0;
    }
}

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let mut args = pico_args::Arguments::from_env();
    let is_nonblocking = args.contains("--nonblock");
    let path: PathBuf = args
        .free_from_str()?
        .ok_or_else(|| anyhow::anyhow!("missing path"))?;

    let content = if is_nonblocking {
        let fd = async_std::task::spawn_blocking(move || {
            FileDesc::open(&path, libc::O_RDONLY | libc::O_NONBLOCK)
        })
        .await?;
        let mut fd = Async::new(fd)?;

        let mut buf = String::new();
        fd.read_to_string(&mut buf).await?;
        buf
    } else {
        async_std::fs::read_to_string(&path).await?
    };

    println!("read: {:?}", content);

    Ok(())
}

// copied from https://github.com/tokio-rs/mio/blob/master/mio/src/sys/unix/mod.rs
macro_rules! syscall {
    ($name:ident ( $($args:expr),* $(,)? )) => {
        match unsafe { libc::$name($($args),*) } {
            -1 => Err(io::Error::last_os_error()),
            ret => Ok(ret),
        }
    }
}

struct FileDesc(RawFd);

impl Drop for FileDesc {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.0);
        }
    }
}

impl AsRawFd for FileDesc {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl FileDesc {
    fn open(path: impl AsRef<OsStr>, mut flags: libc::c_int) -> io::Result<Self> {
        let path = path.as_ref();
        if path.is_empty() {
            flags |= AT_EMPTY_PATH;
        }
        let c_path = CString::new(path.as_bytes())?;
        let fd = syscall!(open(c_path.as_ptr(), flags))?;
        Ok(Self(fd))
    }
}

impl Read for FileDesc {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let len = syscall!(read(self.0, dst.as_mut_ptr().cast(), dst.len()))?;
        Ok(len as usize)
    }

    fn read_vectored(&mut self, dst: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        let len = syscall!(readv(
            self.0,
            dst.as_mut_ptr().cast(),
            dst.len() as libc::c_int
        ))?;
        Ok(len as usize)
    }
}
