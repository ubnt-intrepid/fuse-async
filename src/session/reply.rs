//! Replies to the kernel.

#![allow(clippy::needless_update)]

use super::{Context, FileAttr, FileLock, FsStatistics};
use futures::{future::poll_fn, io::AsyncWrite};
use polyfuse_sys::kernel::{
    fuse_attr_out, //
    fuse_bmap_out,
    fuse_entry_out,
    fuse_getxattr_out,
    fuse_init_out,
    fuse_lk_out,
    fuse_notify_delete_out,
    fuse_notify_inval_entry_out,
    fuse_notify_inval_inode_out,
    fuse_notify_poll_wakeup_out,
    fuse_notify_retrieve_out,
    fuse_notify_store_out,
    fuse_open_out,
    fuse_out_header,
    fuse_poll_out,
    fuse_statfs_out,
    fuse_write_out,
};
use smallvec::SmallVec;
use std::{
    convert::{TryFrom, TryInto},
    ffi::OsStr,
    io::{self, IoSlice},
    mem,
    os::unix::ffi::OsStrExt,
    pin::Pin,
};

pub(crate) trait Payload {
    fn as_bytes(&self) -> &[u8];
}

macro_rules! impl_as_ref_for_abi {
    ($($t:ty,)*) => {$(
        impl Payload for $t {
            #[allow(unsafe_code)]
            fn as_bytes(&self) -> &[u8] {
                unsafe {
                    std::slice::from_raw_parts(
                        self as *const Self as *const u8,
                        std::mem::size_of::<Self>(),
                    )
                }
            }
        }
    )*}
}

impl_as_ref_for_abi! {
    fuse_out_header,
    fuse_init_out,
    fuse_open_out,
    fuse_attr_out,
    fuse_entry_out,
    fuse_getxattr_out,
    fuse_write_out,
    fuse_statfs_out,
    fuse_lk_out,
    fuse_bmap_out,
    fuse_poll_out,
    fuse_notify_inval_inode_out,
    fuse_notify_inval_entry_out,
    fuse_notify_delete_out,
    fuse_notify_store_out,
    fuse_notify_retrieve_out,
    fuse_notify_poll_wakeup_out,
}

/// Reply with an empty output.
#[derive(Debug)]
#[must_use]
pub struct ReplyEmpty {
    _p: (),
}

impl ReplyEmpty {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { _p: () }
    }

    #[inline]
    pub async fn ok(self, cx: &mut Context<'_>) -> io::Result<()> {
        cx.reply(&[]).await
    }
}

/// Reply with arbitrary binary data.
#[derive(Debug)]
#[must_use]
pub struct ReplyData {
    size: u32,
}

impl ReplyData {
    #[inline]
    pub(crate) const fn new(size: u32) -> Self {
        Self { size }
    }

    /// Return the maximum size of data provided by the kernel.
    pub fn size(&self) -> u32 {
        self.size
    }

    /// Reply to the kernel with a data.
    ///
    /// If the data size is larger than the maximum size provided by the kernel,
    /// this method replies with the error number ERANGE.
    pub async fn data(self, cx: &mut Context<'_>, data: impl AsRef<[u8]>) -> io::Result<()> {
        self.data_vectored(cx, &[data.as_ref()]).await
    }

    /// Reply to the kernel with a *split* data.
    ///
    /// If the data size is larger than the maximum size provided by the kernel,
    /// this method replies with the error number ERANGE.
    #[allow(clippy::cast_possible_truncation)]
    pub async fn data_vectored(self, cx: &mut Context<'_>, data: &[&[u8]]) -> io::Result<()> {
        let len: u32 = data.iter().map(|t| t.len() as u32).sum();
        if len <= self.size {
            cx.reply_vectored(data).await
        } else {
            cx.reply_err(libc::ERANGE).await
        }
    }

    // TODO: async fn reader(self, impl AsyncRead) -> io::Result<()>
}

/// Reply with the inode attributes.
#[derive(Debug)]
#[must_use]
pub struct ReplyAttr {
    attr_valid: (u64, u32),
}

impl ReplyAttr {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { attr_valid: (0, 0) }
    }

    /// Set the validity timeout for attributes.
    pub fn attr_valid(&mut self, secs: u64, nsecs: u32) {
        self.attr_valid = (secs, nsecs);
    }

    /// Reply to the kernel with the specified attributes.
    pub async fn attr<T>(self, cx: &mut Context<'_>, attr: T) -> io::Result<()>
    where
        T: TryInto<FileAttr>,
        T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let attr_out = fuse_attr_out {
            attr: attr
                .try_into()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                .into_inner(),
            attr_valid: self.attr_valid.0,
            ..Default::default()
        };
        cx.reply(attr_out.as_bytes()).await
    }
}

/// Reply with entry params.
#[derive(Debug)]
#[must_use]
pub struct ReplyEntry {
    entry_valid: (u64, u32),
    attr_valid: (u64, u32),
}

impl ReplyEntry {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self {
            entry_valid: (0, 0),
            attr_valid: (0, 0),
        }
    }

    /// Set the validity timeout for inode attributes.
    ///
    /// The operations should set this value to very large
    /// when the changes of inode attributes are caused
    /// only by FUSE requests.
    pub fn attr_valid(&mut self, sec: u64, nsec: u32) {
        self.attr_valid = (sec, nsec);
    }

    /// Set the validity timeout for the name.
    ///
    /// The operations should set this value to very large
    /// when the changes/deletions of directory entries are
    /// caused only by FUSE requests.
    pub fn entry_valid(&mut self, sec: u64, nsec: u32) {
        self.entry_valid = (sec, nsec);
    }

    /// Reply to the kernel with the specified entry parameters.
    ///
    /// The parameter `generation` is used to distinguish the inode
    /// from the past one when the filesystem reuse inode numbers.
    /// That is, the operations must ensure that the pair of
    /// entry's inode number and `generation` is unique for
    /// the lifetime of filesystem.
    pub async fn entry<T>(self, cx: &mut Context<'_>, attr: T, generation: u64) -> io::Result<()>
    where
        T: TryInto<FileAttr>,
        T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let attr = attr
            .try_into()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .into_inner();
        let entry_out = fuse_entry_out {
            nodeid: attr.ino,
            generation,
            entry_valid: self.entry_valid.0,
            entry_valid_nsec: self.entry_valid.1,
            attr_valid: self.attr_valid.0,
            attr_valid_nsec: self.attr_valid.1,
            attr,
            ..Default::default()
        };
        cx.reply(entry_out.as_bytes()).await
    }
}

/// Reply with the read link value.
#[derive(Debug)]
#[must_use]
pub struct ReplyReadlink {
    _p: (),
}

impl ReplyReadlink {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { _p: () }
    }

    /// Reply to the kernel with the specified link value.
    pub async fn link(self, cx: &mut Context<'_>, value: impl AsRef<OsStr>) -> io::Result<()> {
        cx.reply(value.as_ref().as_bytes()).await
    }
}

/// Reply with an opened file.
#[derive(Debug)]
#[must_use]
pub struct ReplyOpen {
    open_flags: u32,
}

impl ReplyOpen {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { open_flags: 0 }
    }

    fn set_flag(&mut self, flag: u32, enabled: bool) {
        if enabled {
            self.open_flags |= flag;
        } else {
            self.open_flags &= !flag;
        }
    }

    /// Indicates that the direct I/O is used on this file.
    pub fn direct_io(&mut self, enabled: bool) {
        self.set_flag(polyfuse_sys::kernel::FOPEN_DIRECT_IO, enabled);
    }

    /// Indicates that the currently cached file data in the kernel
    /// need not be invalidated.
    pub fn keep_cache(&mut self, enabled: bool) {
        self.set_flag(polyfuse_sys::kernel::FOPEN_KEEP_CACHE, enabled);
    }

    /// Indicates that the opened file is not seekable.
    pub fn nonseekable(&mut self, enabled: bool) {
        self.set_flag(polyfuse_sys::kernel::FOPEN_NONSEEKABLE, enabled);
    }

    /// Reply to the kernel with the specified file handle and flags.
    pub async fn open(self, cx: &mut Context<'_>, fh: u64) -> io::Result<()> {
        let out = fuse_open_out {
            fh,
            open_flags: self.open_flags,
            ..Default::default()
        };
        cx.reply(out.as_bytes()).await
    }
}

/// Reply with the information about written data.
#[derive(Debug)]
#[must_use]
pub struct ReplyWrite {
    _p: (),
}

impl ReplyWrite {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { _p: () }
    }

    /// Reply to the kernel with the total length of written data.
    pub async fn write(self, cx: &mut Context<'_>, size: u32) -> io::Result<()> {
        let out = fuse_write_out {
            size,
            ..Default::default()
        };
        cx.reply(out.as_bytes()).await
    }
}

/// Reply with an opened directory.
#[derive(Debug)]
#[must_use]
pub struct ReplyOpendir {
    open_flags: u32,
}

impl ReplyOpendir {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { open_flags: 0 }
    }

    fn set_flag(&mut self, flag: u32, enabled: bool) {
        if enabled {
            self.open_flags |= flag;
        } else {
            self.open_flags &= !flag;
        }
    }

    // MEMO: should we add direct_io()?

    /// Enable caching of entries returned by `readdir`.
    pub fn cache_dir(&mut self, enabled: bool) {
        self.set_flag(polyfuse_sys::kernel::FOPEN_CACHE_DIR, enabled);
    }

    /// Reply to the kernel with the specified file handle and flags.
    pub async fn open(self, cx: &mut Context<'_>, fh: u64) -> io::Result<()> {
        let out = fuse_open_out {
            fh,
            open_flags: self.open_flags,
            ..Default::default()
        };
        cx.reply(out.as_bytes()).await
    }
}

/// Reply to a request about extended attributes.
#[derive(Debug)]
#[must_use]
pub struct ReplyXattr {
    _p: (),
}

impl ReplyXattr {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { _p: () }
    }

    /// Reply to the kernel with the specified size value.
    pub async fn size(self, cx: &mut Context<'_>, size: u32) -> io::Result<()> {
        let out = fuse_getxattr_out {
            size,
            ..Default::default()
        };
        cx.reply(out.as_bytes()).await
    }

    /// Reply to the kernel with the specified value.
    pub async fn value(self, cx: &mut Context<'_>, value: impl AsRef<[u8]>) -> io::Result<()> {
        cx.reply(value.as_ref()).await
    }
}

/// Reply with the filesystem staticstics.
#[derive(Debug)]
#[must_use]
pub struct ReplyStatfs {
    _p: (),
}

impl ReplyStatfs {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { _p: () }
    }

    /// Reply to the kernel with the specified statistics.
    pub async fn stat<T>(self, cx: &mut Context<'_>, st: T) -> io::Result<()>
    where
        T: TryInto<FsStatistics>,
        T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let out = fuse_statfs_out {
            st: st
                .try_into()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                .into_inner(),
            ..Default::default()
        };
        cx.reply(out.as_bytes()).await
    }
}

/// Reply with a file lock.
#[derive(Debug)]
#[must_use]
pub struct ReplyLk {
    _p: (),
}

impl ReplyLk {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { _p: () }
    }

    /// Reply to the kernel with the specified file lock.
    pub async fn lock<T>(self, cx: &mut Context<'_>, lk: T) -> io::Result<()>
    where
        T: TryInto<FileLock>,
        T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let out = fuse_lk_out {
            lk: lk
                .try_into()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                .into_inner(),
            ..Default::default()
        };
        cx.reply(out.as_bytes()).await
    }
}

#[derive(Debug)]
#[must_use]
pub struct ReplyCreate {
    entry_valid: (u64, u32),
    attr_valid: (u64, u32),
    open_flags: u32,
}

impl ReplyCreate {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self {
            entry_valid: (0, 0),
            attr_valid: (0, 0),
            open_flags: 0,
        }
    }

    /// Set the validity timeout for inode attributes.
    ///
    /// The operations should set this value to very large
    /// when the changes of inode attributes are caused
    /// only by FUSE requests.
    pub fn attr_valid(&mut self, sec: u64, nsec: u32) {
        self.attr_valid = (sec, nsec);
    }

    /// Set the validity timeout for the name.
    ///
    /// The operations should set this value to very large
    /// when the changes/deletions of directory entries are
    /// caused only by FUSE requests.
    pub fn entry_valid(&mut self, sec: u64, nsec: u32) {
        self.entry_valid = (sec, nsec);
    }

    fn set_flag(&mut self, flag: u32, enabled: bool) {
        if enabled {
            self.open_flags |= flag;
        } else {
            self.open_flags &= !flag;
        }
    }

    /// Indicates that the direct I/O is used on this file.
    pub fn direct_io(&mut self, enabled: bool) {
        self.set_flag(polyfuse_sys::kernel::FOPEN_DIRECT_IO, enabled);
    }

    /// Indicates that the currently cached file data in the kernel
    /// need not be invalidated.
    pub fn keep_cache(&mut self, enabled: bool) {
        self.set_flag(polyfuse_sys::kernel::FOPEN_KEEP_CACHE, enabled);
    }

    /// Indicates that the opened file is not seekable.
    pub fn nonseekable(&mut self, enabled: bool) {
        self.set_flag(polyfuse_sys::kernel::FOPEN_NONSEEKABLE, enabled);
    }

    /// Reply to the kernel with the specified entry parameters and file handle.
    pub async fn create<T>(
        self,
        cx: &mut Context<'_>,
        attr: T,
        generation: u64,
        fh: u64,
    ) -> io::Result<()>
    where
        T: TryInto<FileAttr>,
        T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let attr = attr
            .try_into()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .into_inner();

        let entry_out = fuse_entry_out {
            nodeid: attr.ino,
            generation,
            entry_valid: self.entry_valid.0,
            entry_valid_nsec: self.entry_valid.1,
            attr_valid: self.attr_valid.0,
            attr_valid_nsec: self.attr_valid.1,
            attr,
            ..Default::default()
        };

        let open_out = fuse_open_out {
            fh,
            open_flags: self.open_flags,
            ..Default::default()
        };

        cx.reply_vectored(&[entry_out.as_bytes(), open_out.as_bytes()])
            .await
    }
}

/// Reply with the mapped block index.
#[derive(Debug)]
#[must_use]
pub struct ReplyBmap {
    _p: (),
}

impl ReplyBmap {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { _p: () }
    }

    /// Reply to the kernel with a mapped block index.
    pub async fn block(self, cx: &mut Context<'_>, block: u64) -> io::Result<()> {
        let out = fuse_bmap_out {
            block,
            ..Default::default()
        };
        cx.reply(out.as_bytes()).await
    }
}

/// Reply with the poll result.
#[derive(Debug)]
#[must_use]
pub struct ReplyPoll {
    _p: (),
}

impl ReplyPoll {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { _p: () }
    }

    /// Reply to the kernel with a poll event mask.
    pub async fn events(self, cx: &mut Context<'_>, revents: u32) -> io::Result<()> {
        let out = fuse_poll_out {
            revents,
            ..Default::default()
        };
        cx.reply(out.as_bytes()).await
    }
}

pub(crate) async fn send_msg<W: ?Sized>(
    writer: &mut W,
    unique: u64,
    error: i32,
    data: &[&[u8]],
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let data_len: usize = data.iter().map(|t| t.len()).sum();
    let len = u32::try_from(mem::size_of::<fuse_out_header>() + data_len) //
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "the total length of data is too long: {}",
            )
        })?;

    let out_header = fuse_out_header { unique, error, len };

    // Unfortunately, IoSlice<'_> does not implement Send and
    // the data vector must be created in `poll` function.
    poll_fn(move |cx| {
        let vec: SmallVec<[_; 4]> = Some(IoSlice::new(out_header.as_bytes()))
            .into_iter()
            .chain(data.iter().map(|t| IoSlice::new(&*t)))
            .collect();
        Pin::new(&mut *writer).poll_write_vectored(cx, &*vec)
    })
    .await?;

    log::debug!("Reply to kernel: unique={}: error={}", unique, error);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[inline]
    fn bytes(bytes: &[u8]) -> &[u8] {
        bytes
    }
    macro_rules! b {
        ($($b:expr),*$(,)?) => ( *bytes(&[$($b),*]) );
    }

    #[test]
    fn send_msg_empty() {
        let mut dest = Vec::<u8>::new();
        block_on(send_msg(&mut dest, 42, 4, &[])).unwrap();
        assert_eq!(dest[0..4], b![0x10, 0x00, 0x00, 0x00], "header.len");
        assert_eq!(dest[4..8], b![0x04, 0x00, 0x00, 0x00], "header.error");
        assert_eq!(
            dest[8..16],
            b![0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            "header.unique"
        );
    }

    #[test]
    fn send_msg_single_data() {
        let mut dest = Vec::<u8>::new();
        block_on(send_msg(&mut dest, 42, 0, &["hello".as_ref()])).unwrap();
        assert_eq!(dest[0..4], b![0x15, 0x00, 0x00, 0x00], "header.len");
        assert_eq!(dest[4..8], b![0x00, 0x00, 0x00, 0x00], "header.error");
        assert_eq!(
            dest[8..16],
            b![0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            "header.unique"
        );
        assert_eq!(dest[16..], b![0x68, 0x65, 0x6c, 0x6c, 0x6f], "payload");
    }

    #[test]
    fn send_msg_chunked_data() {
        let payload: &[&[u8]] = &[
            "hello, ".as_ref(), //
            "this ".as_ref(),
            "is a ".as_ref(),
            "message.".as_ref(),
        ];
        let mut dest = Vec::<u8>::new();
        block_on(send_msg(&mut dest, 26, 0, payload)).unwrap();
        assert_eq!(dest[0..4], b![0x29, 0x00, 0x00, 0x00], "header.len");
        assert_eq!(dest[4..8], b![0x00, 0x00, 0x00, 0x00], "header.error");
        assert_eq!(
            dest[8..16],
            b![0x1a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            "header.unique"
        );
        assert_eq!(dest[16..], *b"hello, this is a message.", "payload");
    }

    #[test]
    fn smoke_debug() {
        let _ = dbg!(ReplyEmpty::new());
        let _ = dbg!(ReplyData::new(42));
        let _ = dbg!(ReplyAttr::new());
        let _ = dbg!(ReplyEntry::new());
        let _ = dbg!(ReplyReadlink::new());
        let _ = dbg!(ReplyOpen::new());
        let _ = dbg!(ReplyWrite::new());
        let _ = dbg!(ReplyOpendir::new());
        let _ = dbg!(ReplyXattr::new());
        let _ = dbg!(ReplyStatfs::new());
        let _ = dbg!(ReplyLk::new());
        let _ = dbg!(ReplyCreate::new());
        let _ = dbg!(ReplyBmap::new());
    }
}
