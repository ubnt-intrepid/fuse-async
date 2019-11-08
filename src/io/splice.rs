use libc::{c_int, c_void, size_t};
use mio::{unix::EventedFd, Evented, Poll, PollOpt, Ready, Token};
use std::{
    io::{self, IoSlice, IoSliceMut, Read, Write},
    os::unix::io::{AsRawFd, RawFd},
    ptr,
};

pub fn pipe() -> io::Result<(Reader, Writer)> {
    let mut fds = [0; 2];

    let ret = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }

    let reader = Reader(fds[0]);
    let writer = Writer(fds[1]);

    Ok((reader, writer))
}

#[derive(Debug)]
pub struct Reader(RawFd);

impl AsRawFd for Reader {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.0);
        }
    }
}

impl Read for Reader {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let len = unsafe {
            libc::read(
                self.0,
                dst.as_mut_ptr() as *mut c_void, //
                dst.len() as size_t,
            )
        };
        if len < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(len as usize)
    }

    fn read_vectored(&mut self, dst: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        let len = unsafe {
            libc::readv(
                self.0,
                dst.as_mut_ptr() as *mut libc::iovec, //
                dst.len() as c_int,
            )
        };
        if len < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(len as usize)
    }
}

impl Reader {
    /// Read the specified length of data *without* copying between the kernel and user spaces.
    pub fn splice_read<W: ?Sized>(&mut self, writer: &mut W, len: usize) -> io::Result<usize>
    where
        W: Write + AsRawFd,
    {
        let ret = unsafe {
            libc::splice(
                self.as_raw_fd(),
                ptr::null_mut(),
                writer.as_raw_fd(),
                ptr::null_mut(),
                len,
                libc::SPLICE_F_NONBLOCK,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(ret as usize)
    }
}

impl Evented for Reader {
    fn register(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        EventedFd(&self.0).register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        EventedFd(&self.0).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        EventedFd(&self.0).deregister(poll)
    }
}

#[derive(Debug)]
pub struct Writer(RawFd);

impl AsRawFd for Writer {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.0);
        }
    }
}

impl Write for Writer {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        let len = unsafe {
            libc::write(
                self.0,
                src.as_ptr() as *const c_void, //
                src.len() as size_t,
            )
        };
        if len < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(len as usize)
    }

    fn write_vectored(&mut self, src: &[IoSlice<'_>]) -> io::Result<usize> {
        let len = unsafe {
            libc::writev(
                self.0,
                src.as_ptr() as *const libc::iovec, //
                src.len() as c_int,
            )
        };
        if len < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(len as usize)
    }

    fn flush(&mut self) -> io::Result<()> {
        let ret = unsafe { libc::fsync(self.0) };
        if ret == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Writer {
    /// Write the specified length of data *without* copying between the kernel and user spaces.
    pub fn splice_write<R: ?Sized>(&mut self, reader: &mut R, len: usize) -> io::Result<usize>
    where
        R: Read + AsRawFd,
    {
        let ret = unsafe {
            libc::splice(
                reader.as_raw_fd(),
                ptr::null_mut(),
                self.as_raw_fd(),
                ptr::null_mut(),
                len,
                libc::SPLICE_F_NONBLOCK,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(ret as usize)
    }
}

impl Evented for Writer {
    fn register(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        EventedFd(&self.0).register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        EventedFd(&self.0).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        EventedFd(&self.0).deregister(poll)
    }
}
