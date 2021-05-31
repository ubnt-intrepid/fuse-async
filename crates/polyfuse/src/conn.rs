use libc::{c_int, c_void, iovec};
use std::{
    cmp,
    ffi::{OsStr, OsString},
    io,
    mem::{self, MaybeUninit},
    os::unix::{net::UnixStream, prelude::*},
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    ptr,
};

const FUSERMOUNT_PROG: &str = "/usr/bin/fusermount";
const FUSE_COMMFD_ENV: &str = "_FUSE_COMMFD";

macro_rules! syscall {
    ($fn:ident ( $($arg:expr),* $(,)* ) ) => {{
        #[allow(unused_unsafe)]
        let res = unsafe { libc::$fn($($arg),*) };
        if res == -1 {
            return Err(std::io::Error::last_os_error());
        }
        res
    }};
}

/// A connection with the FUSE kernel driver.
#[derive(Debug)]
pub struct Connection {
    fd: RawFd,
    child: Option<Fusermount>,
    mountpoint: PathBuf,
    mountopts: MountOptions,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.unmount();
    }
}

impl Connection {
    /// Establish a connection with the FUSE kernel driver.
    pub(crate) fn open(mountpoint: PathBuf, mountopts: MountOptions) -> io::Result<Self> {
        let (fd, child) = mount(&mountpoint, &mountopts)?;
        Ok(Self {
            fd,
            child,
            mountpoint,
            mountopts,
        })
    }

    fn read(&self, dst: &mut [u8]) -> io::Result<usize> {
        let len = syscall! {
            read(
                self.fd, //
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
            )
        };
        Ok(len as usize)
    }

    fn read_vectored(&self, dst: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        let len = syscall! {
            readv(
                self.fd, //
                dst.as_mut_ptr() as *mut iovec,
                cmp::min(dst.len(), c_int::max_value() as usize) as c_int,
            )
        };
        Ok(len as usize)
    }

    fn write(&self, src: &[u8]) -> io::Result<usize> {
        let res = syscall! {
            write(
                self.fd, //
                src.as_ptr() as *const c_void,
                src.len(),
            )
        };
        Ok(res as usize)
    }

    fn write_vectored(&self, src: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let res = syscall! {
            writev(
                self.fd, //
                src.as_ptr() as *const iovec,
                cmp::min(src.len(), c_int::max_value() as usize) as c_int,
            )
        };
        Ok(res as usize)
    }

    fn unmount(&mut self) {
        unsafe {
            libc::close(self.fd);
        }

        if let Some(child) = self.child.take() {
            let _ = child.wait();
        }

        unmount(&self.mountpoint);
    }
}

impl AsRawFd for Connection {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl io::Read for Connection {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (*self).read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        (*self).read_vectored(bufs)
    }
}

impl io::Read for &Connection {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (**self).read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        (**self).read_vectored(bufs)
    }
}

impl io::Write for Connection {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (*self).write(buf)
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        (*self).write_vectored(bufs)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl io::Write for &Connection {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (**self).write(buf)
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        (**self).write_vectored(bufs)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ==== mount ====

#[derive(Debug)]
pub(crate) struct MountOptions {
    pub(crate) options: Vec<String>,
    pub(crate) auto_unmount: bool,
    pub(crate) fusermount_path: Option<PathBuf>,
    pub(crate) fuse_comm_fd: Option<OsString>,
}

impl Default for MountOptions {
    fn default() -> Self {
        Self {
            options: vec![],
            auto_unmount: true,
            fusermount_path: None,
            fuse_comm_fd: None,
        }
    }
}

#[derive(Debug)]
struct Fusermount {
    pid: c_int,
    input: UnixStream,
}

impl Fusermount {
    fn wait(self) -> io::Result<ExitStatus> {
        drop(self.input);
        let mut status = 0;
        syscall! { waitpid(self.pid, &mut status, 0) };
        Ok(ExitStatus::from_raw(status))
    }
}

fn mount(mountpoint: &Path, mountopts: &MountOptions) -> io::Result<(RawFd, Option<Fusermount>)> {
    let (input, output) = UnixStream::pair()?;

    let mut fusermount = Command::new(
        mountopts
            .fusermount_path
            .as_deref()
            .unwrap_or_else(|| Path::new(FUSERMOUNT_PROG)),
    );

    let opts = mountopts
        .options
        .iter()
        .map(|opt| opt.as_str())
        .chain(if mountopts.auto_unmount {
            Some("auto_unmount")
        } else {
            None
        })
        .fold(String::new(), |mut opts, opt| {
            if !opts.is_empty() {
                opts.push(',');
            }
            opts.push_str(&opt);
            opts
        });
    if !opts.is_empty() {
        fusermount.arg("-o").arg(opts);
    }

    fusermount.arg("--").arg(mountpoint);

    fusermount.env(
        mountopts
            .fuse_comm_fd
            .as_deref()
            .unwrap_or_else(|| OsStr::new(FUSE_COMMFD_ENV)),
        output.as_raw_fd().to_string(),
    );

    match unsafe { fork()? } {
        ForkResult::Child => {
            // Only async-signal-safe functions are allowed to call here.
            // in a multi threaded situation.

            let output = output.into_raw_fd();
            unsafe { libc::fcntl(output, libc::F_SETFD, 0) };

            // Assumes that the UnixStream destructor only calls close(2).
            drop(input);

            let _err = fusermount.exec();

            // Exit immediately since the process may be in a "broken state".
            // https://doc.rust-lang.org/stable/std/os/unix/process/trait.CommandExt.html#notes
            unsafe {
                libc::_exit(1);
            }
        }

        ForkResult::Parent { child_pid, .. } => {
            drop(output);

            let fd = receive_fd(&input)?;

            let mut child = Some(Fusermount {
                pid: child_pid,
                input,
            });

            if !mountopts.auto_unmount {
                // When auto_unmount is not specified, `fusermount` exits immediately
                // after sending the file descriptor and thus we need to wait until
                // the command is exited.
                let child = child.take().unwrap();
                let _st = child.wait()?;
            }

            Ok((fd, child))
        }
    }
}

fn unmount(mountpoint: &Path) {
    let _ = Command::new(FUSERMOUNT_PROG)
        .args(&["-u", "-q", "-z", "--"])
        .arg(&mountpoint)
        .status();
}

fn receive_fd(reader: &UnixStream) -> io::Result<RawFd> {
    let mut buf = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut c_void,
        iov_len: 1,
    };

    #[repr(C)]
    struct Cmsg {
        header: libc::cmsghdr,
        fd: c_int,
    }
    let mut cmsg = MaybeUninit::<Cmsg>::uninit();

    let mut msg = libc::msghdr {
        msg_name: ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: &mut iov,
        msg_iovlen: 1,
        msg_control: cmsg.as_mut_ptr() as *mut c_void,
        msg_controllen: mem::size_of_val(&cmsg) as _,
        msg_flags: 0,
    };

    syscall! { recvmsg(reader.as_raw_fd(), &mut msg, 0) };

    if msg.msg_controllen < mem::size_of_val(&cmsg) as _ {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "too short control message length",
        ));
    }
    let cmsg = unsafe { cmsg.assume_init() };

    if cmsg.header.cmsg_type != libc::SCM_RIGHTS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "got control message with unknown type",
        ));
    }

    let fd = cmsg.fd;
    syscall! { fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };

    Ok(fd)
}

// ==== util ====

enum ForkResult {
    Parent { child_pid: c_int },
    Child,
}

unsafe fn fork() -> io::Result<ForkResult> {
    let pid = syscall! { fork() };
    match pid {
        -1 => Err(io::Error::last_os_error()),
        0 => Ok(ForkResult::Child),
        pid => Ok(ForkResult::Parent { child_pid: pid }),
    }
}
