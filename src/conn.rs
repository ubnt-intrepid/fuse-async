// FIXME: re-enable lint rules
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use libc::{c_char, c_int, c_void, iovec};
use mio::{unix::EventedFd, Evented, PollOpt, Ready, Token};
use polyfuse_sys::{
    abi::FUSE_DEV_IOC_CLONE,
    v2::{
        fuse_args, //
        fuse_mount_compat25,
        fuse_opt_free_args,
        fuse_unmount_compat22,
    },
};
use std::{
    env,
    ffi::{CStr, CString, OsString}, //
    io::{self, IoSlice, IoSliceMut, Read, Write},
    os::unix::{
        ffi::OsStrExt,
        io::{AsRawFd, RawFd},
    },
    path::Path,
};

#[derive(Debug, Default)]
pub struct MountOptions {
    args: Vec<OsString>,
}

impl MountOptions {
    pub fn from_env() -> Self {
        Self {
            args: env::args_os().collect(),
        }
    }
}

/// A connection with the FUSE kernel driver.
#[derive(Debug)]
pub struct Connection {
    fd: RawFd,
    mountpoint: Option<CString>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _e = self.unmount();
    }
}

impl Connection {
    /// Establish a new connection with the FUSE kernel driver.
    pub fn open(mountpoint: impl AsRef<Path>, mountopts: MountOptions) -> io::Result<Self> {
        let mountpoint = mountpoint.as_ref();
        let c_mountpoint = CString::new(mountpoint.as_os_str().as_bytes())?;

        let args: Vec<CString> = mountopts
            .args
            .into_iter()
            .map(|arg| CString::new(arg.as_bytes()))
            .collect::<Result<_, _>>()?;
        let c_args: Vec<*const c_char> = args.iter().map(|arg| arg.as_ptr()).collect();

        let mut f_args = fuse_args {
            argc: c_args.len() as c_int,
            argv: c_args.as_ptr(),
            allocated: 0,
        };

        let fd = unsafe { fuse_mount_compat25(c_mountpoint.as_ptr(), &mut f_args) };
        unsafe {
            fuse_opt_free_args(&mut f_args);
        }
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }

        set_nonblocking(fd)?;

        Ok(Connection {
            fd,
            mountpoint: Some(c_mountpoint),
        })
    }

    #[allow(dead_code)]
    pub fn duplicate(&self, ioc_clone: bool) -> io::Result<Self> {
        let clonefd;
        unsafe {
            if ioc_clone {
                let devname = CStr::from_bytes_with_nul_unchecked(b"/dev/fuse\0");

                clonefd = libc::open(devname.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC);
                if clonefd == -1 {
                    return Err(io::Error::last_os_error());
                }

                let mut sourcefd = self.fd;
                let res = libc::ioctl(clonefd, FUSE_DEV_IOC_CLONE.into(), &mut sourcefd);
                if res == -1 {
                    let err = io::Error::last_os_error();
                    libc::close(clonefd);
                    return Err(err);
                }
            } else {
                clonefd = libc::dup(self.fd);
                if clonefd == -1 {
                    return Err(io::Error::last_os_error());
                }
            }
        }

        Ok(Self {
            fd: clonefd,
            mountpoint: None,
        })
    }

    pub fn unmount(&mut self) -> io::Result<()> {
        if let Some(mountpoint) = self.mountpoint.take() {
            unsafe {
                fuse_unmount_compat22(mountpoint.as_ptr());
            }
        }
        Ok(())
    }
}

impl AsRawFd for Connection {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Read for Connection {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let res = unsafe {
            libc::read(
                self.fd, //
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
            )
        };
        if res < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(res as usize)
    }

    fn read_vectored(&mut self, dst: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        let res = unsafe {
            libc::readv(
                self.fd, //
                dst.as_mut_ptr() as *mut iovec,
                dst.len() as c_int,
            )
        };
        if res < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(res as usize)
    }
}

impl Write for Connection {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        let res = unsafe {
            libc::write(
                self.fd, //
                src.as_ptr() as *const c_void,
                src.len(),
            )
        };
        if res < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(res as usize)
    }

    fn write_vectored(&mut self, src: &[IoSlice]) -> io::Result<usize> {
        let res = unsafe {
            libc::writev(
                self.fd, //
                src.as_ptr() as *const iovec,
                src.len() as c_int,
            )
        };
        if res < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(res as usize)
    }

    fn flush(&mut self) -> io::Result<()> {
        let res = unsafe { libc::fsync(self.fd) };
        if res < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Evented for Connection {
    fn register(
        &self,
        poll: &mio::Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        EventedFd(&self.fd).register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &mio::Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        EventedFd(&self.fd).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
        EventedFd(&self.fd).deregister(poll)
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }

    let res = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}
