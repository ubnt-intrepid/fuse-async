use crate::{
    bytes::{Bytes, FillBytes},
    conn::{Connection, MountOptions},
    decoder::Decoder,
    op::{DecodeError, Operation},
};
use polyfuse_kernel::*;
use std::{
    cmp,
    convert::{TryFrom, TryInto as _},
    ffi::OsStr,
    fmt,
    io::{self, prelude::*, IoSlice, IoSliceMut},
    mem::{self, MaybeUninit},
    os::unix::prelude::*,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use zerocopy::AsBytes as _;

// The minimum supported ABI minor version by polyfuse.
const MINIMUM_SUPPORTED_MINOR_VERSION: u32 = 23;

const DEFAULT_MAX_WRITE: u32 = 16 * 1024 * 1024;
const MIN_MAX_WRITE: u32 = FUSE_MIN_READ_BUFFER - BUFFER_HEADER_SIZE as u32;

// copied from fuse_i.h
const MAX_MAX_PAGES: usize = 256;
//const DEFAULT_MAX_PAGES_PER_REQ: usize = 32;
const BUFFER_HEADER_SIZE: usize = 0x1000;

// TODO: add FUSE_IOCTL_DIR
const DEFAULT_INIT_FLAGS: u32 = FUSE_ASYNC_READ
    | FUSE_PARALLEL_DIROPS
    | FUSE_AUTO_INVAL_DATA
    | FUSE_HANDLE_KILLPRIV
    | FUSE_ASYNC_DIO
    | FUSE_ATOMIC_O_TRUNC;

const INIT_FLAGS_MASK: u32 = FUSE_ASYNC_READ
    | FUSE_ATOMIC_O_TRUNC
    | FUSE_AUTO_INVAL_DATA
    | FUSE_ASYNC_DIO
    | FUSE_PARALLEL_DIROPS
    | FUSE_HANDLE_KILLPRIV
    | FUSE_POSIX_LOCKS
    | FUSE_FLOCK_LOCKS
    | FUSE_EXPORT_SUPPORT
    | FUSE_DONT_MASK
    | FUSE_WRITEBACK_CACHE
    | FUSE_POSIX_ACL
    | FUSE_DO_READDIRPLUS
    | FUSE_READDIRPLUS_AUTO;

// ==== KernelConfig ====

/// Parameters for setting up the connection with FUSE driver
/// and the kernel side behavior.
pub struct KernelConfig {
    mountopts: MountOptions,
    init_out: fuse_init_out,
}

impl Default for KernelConfig {
    fn default() -> Self {
        Self {
            mountopts: MountOptions::default(),
            init_out: default_init_out(),
        }
    }
}

impl KernelConfig {
    #[doc(hidden)] // TODO: dox
    pub fn auto_unmount(&mut self, enabled: bool) -> &mut Self {
        self.mountopts.auto_unmount = enabled;
        self
    }

    #[doc(hidden)] // TODO: dox
    pub fn mount_option(&mut self, option: &str) -> &mut Self {
        for option in option.split(',').map(|s| s.trim()) {
            match option {
                "auto_unmount" => {
                    self.auto_unmount(true);
                }
                option => self.mountopts.options.push(option.to_owned()),
            }
        }
        self
    }

    #[doc(hidden)] // TODO: dox
    pub fn fusermount_path(&mut self, program: impl AsRef<OsStr>) -> &mut Self {
        let program = Path::new(program.as_ref());
        assert!(
            program.is_absolute(),
            "the binary path to `fusermount` must be absolute."
        );
        self.mountopts.fusermount_path = Some(program.to_owned());
        self
    }

    #[doc(hidden)] // TODO: dox
    pub fn fuse_comm_fd(&mut self, name: impl AsRef<OsStr>) -> &mut Self {
        self.mountopts.fuse_comm_fd = Some(name.as_ref().to_owned());
        self
    }

    #[inline]
    fn set_init_flag(&mut self, flag: u32, enabled: bool) {
        if enabled {
            self.init_out.flags |= flag;
        } else {
            self.init_out.flags &= !flag;
        }
    }

    /// Specify that the filesystem supports asynchronous read requests.
    ///
    /// Enabled by default.
    pub fn async_read(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_ASYNC_READ, enabled);
        self
    }

    /// Specify that the filesystem supports the `O_TRUNC` open flag.
    ///
    /// Enabled by default.
    pub fn atomic_o_trunc(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_ATOMIC_O_TRUNC, enabled);
        self
    }

    /// Specify that the kernel check the validity of attributes on every read.
    ///
    /// Enabled by default.
    pub fn auto_inval_data(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_AUTO_INVAL_DATA, enabled);
        self
    }

    /// Specify that the filesystem supports asynchronous direct I/O submission.
    ///
    /// Enabled by default.
    pub fn async_dio(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_ASYNC_DIO, enabled);
        self
    }

    /// Specify that the kernel supports parallel directory operations.
    ///
    /// Enabled by default.
    pub fn parallel_dirops(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_PARALLEL_DIROPS, enabled);
        self
    }

    /// Specify that the filesystem is responsible for unsetting setuid and setgid bits
    /// when a file is written, truncated, or its owner is changed.
    ///
    /// Enabled by default.
    pub fn handle_killpriv(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_HANDLE_KILLPRIV, enabled);
        self
    }

    /// The filesystem supports the POSIX-style file lock.
    pub fn posix_locks(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_POSIX_LOCKS, enabled);
        self
    }

    /// Specify that the filesystem supports the `flock` handling.
    pub fn flock_locks(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_FLOCK_LOCKS, enabled);
        self
    }

    /// Specify that the filesystem supports lookups of `"."` and `".."`.
    pub fn export_support(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_EXPORT_SUPPORT, enabled);
        self
    }

    /// Specify that the kernel should not apply the umask to the file mode
    /// on `create` operations.
    pub fn dont_mask(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_DONT_MASK, enabled);
        self
    }

    /// Specify that the kernel should enable writeback caching.
    pub fn writeback_cache(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_WRITEBACK_CACHE, enabled);
        self
    }

    /// Specify that the filesystem supports POSIX access control lists.
    pub fn posix_acl(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_POSIX_ACL, enabled);
        self
    }

     /// Specify that the value of max_pages should be derived from max_write.
    pub fn max_pages(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_MAX_PAGES, enabled);
        self
    }

    /// Specify that the filesystem supports `readdirplus` operations.
    pub fn readdirplus(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_DO_READDIRPLUS, enabled);
        self
    }

    /// Indicates that the kernel uses the adaptive readdirplus.
    ///
    /// This option is meaningful only if `readdirplus` is enabled.
    pub fn readdirplus_auto(&mut self, enabled: bool) -> &mut Self {
        self.set_init_flag(FUSE_READDIRPLUS_AUTO, enabled);
        self
    }

    /// Set the maximum readahead.
    pub fn max_readahead(&mut self, value: u32) -> &mut Self {
        self.init_out.max_readahead = value;
        self
    }

    /// Set the maximum size of the write buffer.
    ///
    /// # Panic
    /// It causes an assertion panic if the setting value is less than the absolute minimum.
    pub fn max_write(&mut self, value: u32) -> &mut Self {
        assert!(
            value >= MIN_MAX_WRITE,
            "max_write must be greater or equal to {}",
            MIN_MAX_WRITE,
        );
        self.init_out.max_write = value;
        self
    }

    /// Return the maximum number of pending *background* requests.
    pub fn max_background(&mut self, max_background: u16) -> &mut Self {
        self.init_out.max_background = max_background;
        self
    }

    /// Set the threshold number of pending background requests that the kernel marks
    /// the filesystem as *congested*.
    ///
    /// If the setting value is 0, the value is automatically calculated by using max_background.
    ///
    /// # Panics
    /// It cause a panic if the setting value is greater than `max_background`.
    pub fn congestion_threshold(&mut self, mut threshold: u16) -> &mut Self {
        assert!(
            threshold <= self.init_out.max_background,
            "The congestion_threshold must be less or equal to max_background"
        );
        if threshold == 0 {
            threshold = self.init_out.max_background * 3 / 4;
            tracing::debug!(congestion_threshold = threshold);
        }
        self.init_out.congestion_threshold = threshold;
        self
    }

    /// Set the timestamp resolution supported by the filesystem.
    ///
    /// The setting value has the nanosecond unit and should be a power of 10.
    ///
    /// The default value is 1.
    pub fn time_gran(&mut self, time_gran: u32) -> &mut Self {
        self.init_out.time_gran = time_gran;
        self
    }
}

// ==== Session ====

/// The object containing the contextrual information about a FUSE session.
pub struct Session {
    inner: Arc<SessionInner>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session").finish()
    }
}

struct SessionInner {
    conn: Connection,
    init_out: fuse_init_out,
    bufsize: usize,
    exited: AtomicBool,
    notify_unique: AtomicU64,
}

impl SessionInner {
    #[inline]
    fn exited(&self) -> bool {
        // FIXME: choose appropriate atomic ordering.
        self.exited.load(Ordering::SeqCst)
    }

    #[inline]
    fn exit(&self) {
        // FIXME: choose appropriate atomic ordering.
        self.exited.store(true, Ordering::SeqCst)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.inner.exit();
    }
}

impl AsRawFd for Session {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.conn.as_raw_fd()
    }
}

impl Session {
    /// Start a FUSE daemon mount on the specified path.
    pub fn mount(mountpoint: PathBuf, config: KernelConfig) -> io::Result<Self> {
        let KernelConfig {
            mountopts,
            mut init_out,
        } = config;

        let conn = Connection::open(mountpoint, mountopts)?;

        init_session(&mut init_out, &conn, &conn)?;
        let bufsize = BUFFER_HEADER_SIZE + init_out.max_write as usize;

        Ok(Self {
            inner: Arc::new(SessionInner {
                conn,
                init_out,
                bufsize,
                exited: AtomicBool::new(false),
                notify_unique: AtomicU64::new(0),
            }),
        })
    }

    /// Return whether the kernel supports for zero-message opens.
    ///
    /// When the returned value is `true`, the kernel treat an `ENOSYS`
    /// error for a `FUSE_OPEN` request as successful and does not send
    /// subsequent `open` requests.  Otherwise, the filesystem should
    /// implement the handler for `open` requests appropriately.
    pub fn no_open_support(&self) -> bool {
        self.inner.init_out.flags & FUSE_NO_OPEN_SUPPORT != 0
    }

    /// Return whether the kernel supports for zero-message opendirs.
    ///
    /// See the documentation of `no_open_support` for details.
    pub fn no_opendir_support(&self) -> bool {
        self.inner.init_out.flags & FUSE_NO_OPENDIR_SUPPORT != 0
    }

    /// Receive an incoming FUSE request from the kernel.
    pub fn next_request(&self) -> io::Result<Option<Request>> {
        let mut conn = &self.inner.conn;

        // FIXME: Align the allocated region in `arg` with the FUSE argument types.
        let mut header = fuse_in_header::default();
        let mut arg = vec![0u8; self.inner.bufsize - mem::size_of::<fuse_in_header>()];

        loop {
            match conn.read_vectored(&mut [
                io::IoSliceMut::new(header.as_bytes_mut()),
                io::IoSliceMut::new(&mut arg[..]),
            ]) {
                Ok(len) => {
                    if len < mem::size_of::<fuse_in_header>() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "dequeued request message is too short",
                        ));
                    }
                    unsafe {
                        arg.set_len(len - mem::size_of::<fuse_in_header>());
                    }

                    break;
                }

                Err(err) => match err.raw_os_error() {
                    Some(libc::ENODEV) => {
                        tracing::debug!("ENODEV");
                        return Ok(None);
                    }
                    Some(libc::ENOENT) => {
                        tracing::debug!("ENOENT");
                        continue;
                    }
                    _ => return Err(err),
                },
            }
        }

        Ok(Some(Request {
            session: self.inner.clone(),
            header,
            arg,
        }))
    }

    /// Create an instance of `Notifier` corresponding to this session.
    pub fn notifier(&self) -> Notifier {
        Notifier {
            session: self.inner.clone(),
        }
    }
}

fn init_session<R, W>(init_out: &mut fuse_init_out, mut reader: R, mut writer: W) -> io::Result<()>
where
    R: io::Read,
    W: io::Write,
{
    // FIXME: align the allocated buffer in `buf` with FUSE argument types.
    let mut header = fuse_in_header::default();
    let mut arg = vec![0u8; pagesize() * MAX_MAX_PAGES];

    for _ in 0..10 {
        let len = reader.read_vectored(&mut [
            io::IoSliceMut::new(header.as_bytes_mut()),
            io::IoSliceMut::new(&mut arg[..]),
        ])?;
        if len < mem::size_of::<fuse_in_header>() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request message is too short",
            ));
        }

        let mut decoder = Decoder::new(&arg[..]);

        match fuse_opcode::try_from(header.opcode) {
            Ok(fuse_opcode::FUSE_INIT) => {
                let init_in = decoder
                    .fetch::<fuse_init_in>() //
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "failed to decode fuse_init_in")
                    })?;

                let capable = init_in.flags & INIT_FLAGS_MASK;
                let readonly_flags = init_in.flags & !INIT_FLAGS_MASK;

                tracing::debug!("INIT request:");
                tracing::debug!("  proto = {}.{}:", init_in.major, init_in.minor);
                tracing::debug!("  flags = 0x{:08x} ({:?})", init_in.flags, capable);
                tracing::debug!("  max_readahead = 0x{:08X}", init_in.max_readahead);
                tracing::debug!("  max_pages = {}", readonly_flags & FUSE_MAX_PAGES != 0);
                tracing::debug!(
                    "  no_open_support = {}",
                    readonly_flags & FUSE_NO_OPEN_SUPPORT != 0
                );
                tracing::debug!(
                    "  no_opendir_support = {}",
                    readonly_flags & FUSE_NO_OPENDIR_SUPPORT != 0
                );

                if init_in.major > 7 {
                    tracing::debug!("wait for a second INIT request with an older version.");
                    let init_out = fuse_init_out {
                        major: FUSE_KERNEL_VERSION,
                        minor: FUSE_KERNEL_MINOR_VERSION,
                        ..Default::default()
                    };
                    write_bytes(
                        &mut writer,
                        Reply::new(header.unique, 0, init_out.as_bytes()),
                    )?;
                    continue;
                }

                if init_in.major < 7 || init_in.minor < MINIMUM_SUPPORTED_MINOR_VERSION {
                    tracing::warn!(
                        "polyfuse supports only ABI 7.{} or later. {}.{} is not supported",
                        MINIMUM_SUPPORTED_MINOR_VERSION,
                        init_in.major,
                        init_in.minor
                    );
                    write_bytes(&mut writer, Reply::new(header.unique, libc::EPROTO, ()))?;
                    continue;
                }

                init_out.minor = cmp::min(init_out.minor, init_in.minor);

                init_out.max_readahead = cmp::min(init_out.max_readahead, init_in.max_readahead);

                init_out.flags &= capable;
                init_out.flags |= FUSE_BIG_WRITES; // the flag was superseded by `max_write`.

                if init_in.flags & FUSE_MAX_PAGES != 0 {
                    init_out.flags |= FUSE_MAX_PAGES;
                    init_out.max_pages = cmp::min(
                        (init_out.max_write - 1) / (pagesize() as u32) + 1,
                        u16::max_value() as u32,
                    ) as u16;
                }

                debug_assert_eq!(init_out.major, FUSE_KERNEL_VERSION);
                debug_assert!(init_out.minor >= MINIMUM_SUPPORTED_MINOR_VERSION);

                tracing::debug!("Reply to INIT:");
                tracing::debug!("  proto = {}.{}:", init_out.major, init_out.minor);
                tracing::debug!("  flags = 0x{:08x}", init_out.flags);
                tracing::debug!("  max_readahead = 0x{:08X}", init_out.max_readahead);
                tracing::debug!("  max_write = 0x{:08X}", init_out.max_write);
                tracing::debug!("  max_background = 0x{:04X}", init_out.max_background);
                tracing::debug!(
                    "  congestion_threshold = 0x{:04X}",
                    init_out.congestion_threshold
                );
                tracing::debug!("  time_gran = {}", init_out.time_gran);
                write_bytes(writer, Reply::new(header.unique, 0, init_out.as_bytes()))?;

                init_out.flags |= readonly_flags;

                return Ok(());
            }

            _ => {
                tracing::warn!(
                    "ignoring an operation before init (opcode={:?})",
                    header.opcode
                );
                write_bytes(&mut writer, Reply::new(header.unique, libc::EIO, ()))?;
                continue;
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        "session initialization is aborted",
    ))
}

// ==== Request ====

/// Context about an incoming FUSE request.
pub struct Request {
    session: Arc<SessionInner>,
    header: fuse_in_header,
    arg: Vec<u8>,
}

impl Request {
    /// Return the unique ID of the request.
    #[inline]
    pub fn unique(&self) -> u64 {
        self.header.unique
    }

    /// Return the user ID of the calling process.
    #[inline]
    pub fn uid(&self) -> u32 {
        self.header.uid
    }

    /// Return the group ID of the calling process.
    #[inline]
    pub fn gid(&self) -> u32 {
        self.header.gid
    }

    /// Return the process ID of the calling process.
    #[inline]
    pub fn pid(&self) -> u32 {
        self.header.pid
    }

    /// Decode the argument of this request.
    pub fn operation(&self) -> Result<Operation<'_, Data<'_>>, DecodeError> {
        if self.session.exited() {
            return Ok(Operation::unknown());
        }

        let (arg, data) = match fuse_opcode::try_from(self.header.opcode).ok() {
            Some(fuse_opcode::FUSE_WRITE) | Some(fuse_opcode::FUSE_NOTIFY_REPLY) => {
                self.arg.split_at(mem::size_of::<fuse_write_in>())
            }
            _ => (&self.arg[..], &[] as &[_]),
        };

        Operation::decode(&self.header, arg, Data { data })
    }

    pub fn reply<T>(&self, arg: T) -> io::Result<()>
    where
        T: Bytes,
    {
        write_bytes(&self.session.conn, Reply::new(self.unique(), 0, arg))
    }

    pub fn reply_error(&self, code: i32) -> io::Result<()> {
        write_bytes(&self.session.conn, Reply::new(self.unique(), code, ()))
    }
}

/// The remaining part of request message.
pub struct Data<'op> {
    data: &'op [u8],
}

impl fmt::Debug for Data<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Data").finish()
    }
}

impl<'op> io::Read for Data<'op> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        io::Read::read(&mut self.data, buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        io::Read::read_vectored(&mut self.data, bufs)
    }
}

impl<'op> BufRead for Data<'op> {
    #[inline]
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        io::BufRead::fill_buf(&mut self.data)
    }

    #[inline]
    fn consume(&mut self, amt: usize) {
        io::BufRead::consume(&mut self.data, amt)
    }
}

// ==== Notifier ====

#[derive(Clone)]
pub struct Notifier {
    session: Arc<SessionInner>,
}

impl Notifier {
    /// Notify the cache invalidation about an inode to the kernel.
    pub fn inval_inode(&self, ino: u64, off: i64, len: i64) -> io::Result<()> {
        let total_len = u32::try_from(
            mem::size_of::<fuse_out_header>() + mem::size_of::<fuse_notify_inval_inode_out>(),
        )
        .unwrap();

        return write_bytes(
            &self.session.conn,
            InvalInode {
                header: fuse_out_header {
                    len: total_len,
                    error: fuse_notify_code::FUSE_NOTIFY_INVAL_INODE as i32,
                    unique: 0,
                },
                arg: fuse_notify_inval_inode_out { ino, off, len },
            },
        );

        struct InvalInode {
            header: fuse_out_header,
            arg: fuse_notify_inval_inode_out,
        }
        impl Bytes for InvalInode {
            fn size(&self) -> usize {
                self.header.len as usize
            }

            fn count(&self) -> usize {
                2
            }

            fn fill_bytes<'a>(&'a self, dst: &mut dyn FillBytes<'a>) {
                dst.put(self.header.as_bytes());
                dst.put(self.arg.as_bytes());
            }
        }
    }

    /// Notify the invalidation about a directory entry to the kernel.
    pub fn inval_entry<T>(&self, parent: u64, name: T) -> io::Result<()>
    where
        T: AsRef<OsStr>,
    {
        let namelen = u32::try_from(name.as_ref().len()).expect("provided name is too long");

        let total_len = u32::try_from(
            mem::size_of::<fuse_out_header>()
                + mem::size_of::<fuse_notify_inval_entry_out>()
                + name.as_ref().len()
                + 1,
        )
        .unwrap();

        return write_bytes(
            &self.session.conn,
            InvalEntry {
                header: fuse_out_header {
                    len: total_len,
                    error: fuse_notify_code::FUSE_NOTIFY_INVAL_ENTRY as i32,
                    unique: 0,
                },
                arg: fuse_notify_inval_entry_out {
                    parent,
                    namelen,
                    padding: 0,
                },
                name,
            },
        );

        struct InvalEntry<T>
        where
            T: AsRef<OsStr>,
        {
            header: fuse_out_header,
            arg: fuse_notify_inval_entry_out,
            name: T,
        }
        impl<T> Bytes for InvalEntry<T>
        where
            T: AsRef<OsStr>,
        {
            fn size(&self) -> usize {
                self.header.len as usize
            }

            fn count(&self) -> usize {
                4
            }

            fn fill_bytes<'a>(&'a self, dst: &mut dyn FillBytes<'a>) {
                dst.put(self.header.as_bytes());
                dst.put(self.arg.as_bytes());
                dst.put(self.name.as_ref().as_bytes());
                dst.put(b"\0"); // null terminator
            }
        }
    }

    /// Notify the invalidation about a directory entry to the kernel.
    ///
    /// The role of this notification is similar to `notify_inval_entry`.
    /// Additionally, when the provided `child` inode matches the inode
    /// in the dentry cache, the inotify will inform the deletion to
    /// watchers if exists.
    pub fn delete<T>(&self, parent: u64, child: u64, name: T) -> io::Result<()>
    where
        T: AsRef<OsStr>,
    {
        let namelen = u32::try_from(name.as_ref().len()).expect("provided name is too long");

        let total_len = u32::try_from(
            mem::size_of::<fuse_out_header>()
                + mem::size_of::<fuse_notify_delete_out>()
                + name.as_ref().len()
                + 1,
        )
        .expect("payload is too long");

        return write_bytes(
            &self.session.conn,
            Delete {
                header: fuse_out_header {
                    len: total_len,
                    error: fuse_notify_code::FUSE_NOTIFY_DELETE as i32,
                    unique: 0,
                },
                arg: fuse_notify_delete_out {
                    parent,
                    child,
                    namelen,
                    padding: 0,
                },
                name,
            },
        );

        struct Delete<T>
        where
            T: AsRef<OsStr>,
        {
            header: fuse_out_header,
            arg: fuse_notify_delete_out,
            name: T,
        }
        impl<T> Bytes for Delete<T>
        where
            T: AsRef<OsStr>,
        {
            fn size(&self) -> usize {
                self.header.len as usize
            }

            fn count(&self) -> usize {
                4
            }

            fn fill_bytes<'a>(&'a self, dst: &mut dyn FillBytes<'a>) {
                dst.put(self.header.as_bytes());
                dst.put(self.arg.as_bytes());
                dst.put(self.name.as_ref().as_bytes());
                dst.put(b"\0"); // null terminator
            }
        }
    }

    /// Push the data in an inode for updating the kernel cache.
    pub fn store<T>(&self, ino: u64, offset: u64, data: T) -> io::Result<()>
    where
        T: Bytes,
    {
        let size = u32::try_from(data.size()).expect("provided data is too large");

        let total_len = u32::try_from(
            mem::size_of::<fuse_out_header>()
                + mem::size_of::<fuse_notify_store_out>()
                + data.size(),
        )
        .expect("payload is too long");

        return write_bytes(
            &self.session.conn,
            Store {
                header: fuse_out_header {
                    len: total_len,
                    error: fuse_notify_code::FUSE_NOTIFY_STORE as i32,
                    unique: 0,
                },
                arg: fuse_notify_store_out {
                    nodeid: ino,
                    offset,
                    size,
                    padding: 0,
                },
                data,
            },
        );

        struct Store<T>
        where
            T: Bytes,
        {
            header: fuse_out_header,
            arg: fuse_notify_store_out,
            data: T,
        }
        impl<T> Bytes for Store<T>
        where
            T: Bytes,
        {
            fn size(&self) -> usize {
                self.header.len as usize
            }

            fn count(&self) -> usize {
                2 + self.data.count()
            }

            fn fill_bytes<'a>(&'a self, dst: &mut dyn FillBytes<'a>) {
                dst.put(self.header.as_bytes());
                dst.put(self.arg.as_bytes());
                self.data.fill_bytes(dst);
            }
        }
    }

    /// Retrieve data in an inode from the kernel cache.
    pub fn retrieve(&self, ino: u64, offset: u64, size: u32) -> io::Result<u64> {
        let total_len = u32::try_from(
            mem::size_of::<fuse_out_header>() + mem::size_of::<fuse_notify_retrieve_out>(),
        )
        .unwrap();

        // FIXME: choose appropriate memory ordering.
        let notify_unique = self.session.notify_unique.fetch_add(1, Ordering::SeqCst);

        write_bytes(
            &self.session.conn,
            Retrieve {
                header: fuse_out_header {
                    len: total_len,
                    error: fuse_notify_code::FUSE_NOTIFY_RETRIEVE as i32,
                    unique: 0,
                },
                arg: fuse_notify_retrieve_out {
                    nodeid: ino,
                    offset,
                    size,
                    notify_unique,
                    padding: 0,
                },
            },
        )?;

        return Ok(notify_unique);

        struct Retrieve {
            header: fuse_out_header,
            arg: fuse_notify_retrieve_out,
        }
        impl Bytes for Retrieve {
            fn size(&self) -> usize {
                self.header.len as usize
            }

            fn count(&self) -> usize {
                2
            }

            fn fill_bytes<'a>(&'a self, dst: &mut dyn FillBytes<'a>) {
                dst.put(self.header.as_bytes());
                dst.put(self.arg.as_bytes());
            }
        }
    }

    /// Send I/O readiness to the kernel.
    pub fn poll_wakeup(&self, kh: u64) -> io::Result<()> {
        let total_len = u32::try_from(
            mem::size_of::<fuse_out_header>() + mem::size_of::<fuse_notify_poll_wakeup_out>(),
        )
        .unwrap();

        return write_bytes(
            &self.session.conn,
            PollWakeup {
                header: fuse_out_header {
                    len: total_len,
                    error: fuse_notify_code::FUSE_NOTIFY_POLL as i32,
                    unique: 0,
                },
                arg: fuse_notify_poll_wakeup_out { kh },
            },
        );

        struct PollWakeup {
            header: fuse_out_header,
            arg: fuse_notify_poll_wakeup_out,
        }
        impl Bytes for PollWakeup {
            fn size(&self) -> usize {
                self.header.len as usize
            }

            fn count(&self) -> usize {
                2
            }

            fn fill_bytes<'a>(&'a self, dst: &mut dyn FillBytes<'a>) {
                dst.put(self.header.as_bytes());
                dst.put(self.arg.as_bytes());
            }
        }
    }
}

// ==== utils ====

struct Reply<T> {
    header: fuse_out_header,
    arg: T,
}
impl<T> Reply<T>
where
    T: Bytes,
{
    #[inline]
    fn new(unique: u64, error: i32, arg: T) -> Self {
        let len = (mem::size_of::<fuse_out_header>() + arg.size())
            .try_into()
            .expect("Argument size is too large");
        Self {
            header: fuse_out_header {
                len,
                error: -error,
                unique,
            },
            arg,
        }
    }
}
impl<T> Bytes for Reply<T>
where
    T: Bytes,
{
    #[inline]
    fn size(&self) -> usize {
        self.header.len as usize
    }

    #[inline]
    fn count(&self) -> usize {
        self.arg.count() + 1
    }

    fn fill_bytes<'a>(&'a self, dst: &mut dyn FillBytes<'a>) {
        dst.put(self.header.as_bytes());
        self.arg.fill_bytes(dst);
    }
}

#[inline]
fn write_bytes<W, T>(mut writer: W, bytes: T) -> io::Result<()>
where
    W: io::Write,
    T: Bytes,
{
    let size = bytes.size();
    let count = bytes.count();

    let written;

    macro_rules! small_write {
        ($n:expr) => {{
            let mut vec: [MaybeUninit<IoSlice<'_>>; $n] =
                unsafe { MaybeUninit::uninit().assume_init() };
            bytes.fill_bytes(&mut FillWriteBytes {
                vec: &mut vec[..],
                offset: 0,
            });
            let vec = unsafe { slice_assume_init_ref(&vec[..]) };

            written = writer.write_vectored(vec)?;
        }};
    }

    match count {
        // Skip writing.
        0 => return Ok(()),

        // Avoid heap allocation if count is small.
        1 => small_write!(1),
        2 => small_write!(2),
        3 => small_write!(3),
        4 => small_write!(4),

        count => {
            let mut vec: Vec<IoSlice<'_>> = Vec::with_capacity(count);
            unsafe {
                let dst = std::slice::from_raw_parts_mut(
                    vec.as_mut_ptr().cast(), //
                    count,
                );
                bytes.fill_bytes(&mut FillWriteBytes {
                    vec: dst,
                    offset: 0,
                });
                vec.set_len(count);
            }

            written = writer.write_vectored(&*vec)?;
        }
    }

    if written < size {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "written data is too short",
        ));
    }

    Ok(())
}

struct FillWriteBytes<'a, 'vec> {
    vec: &'vec mut [MaybeUninit<IoSlice<'a>>],
    offset: usize,
}

impl<'a, 'vec> FillBytes<'a> for FillWriteBytes<'a, 'vec> {
    fn put(&mut self, chunk: &'a [u8]) {
        self.vec[self.offset] = MaybeUninit::new(IoSlice::new(chunk));
        self.offset += 1;
    }
}

// FIXME: replace with stabilized MaybeUninit::slice_assume_init_ref.
#[inline(always)]
unsafe fn slice_assume_init_ref<T>(slice: &[MaybeUninit<T>]) -> &[T] {
    #[allow(unused_unsafe)]
    unsafe {
        &*(slice as *const [MaybeUninit<T>] as *const [T])
    }
}

#[inline]
fn pagesize() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

#[inline]
const fn default_init_out() -> fuse_init_out {
    fuse_init_out {
        major: FUSE_KERNEL_VERSION,
        minor: FUSE_KERNEL_MINOR_VERSION,
        max_readahead: u32::MAX,
        flags: DEFAULT_INIT_FLAGS,
        max_background: 0,
        congestion_threshold: 0,
        max_write: DEFAULT_MAX_WRITE,
        time_gran: 1,
        max_pages: 0,
        padding: 0,
        unused: [0; 8],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn init_default() {
        let input_len = mem::size_of::<fuse_in_header>() + mem::size_of::<fuse_init_in>();
        let in_header = fuse_in_header {
            len: input_len as u32,
            opcode: fuse_opcode::FUSE_INIT as u32,
            unique: 2,
            nodeid: 0,
            uid: 100,
            gid: 100,
            pid: 12,
            padding: 0,
        };
        let init_in = fuse_init_in {
            major: 7,
            minor: 23,
            max_readahead: 40,
            flags: INIT_FLAGS_MASK
                | FUSE_MAX_PAGES
                | FUSE_NO_OPEN_SUPPORT
                | FUSE_NO_OPENDIR_SUPPORT,
        };

        let mut input = Vec::with_capacity(input_len);
        input.extend_from_slice(in_header.as_bytes());
        input.extend_from_slice(init_in.as_bytes());
        assert_eq!(input.len(), input_len);

        let mut output = Vec::<u8>::new();

        let mut init_out = default_init_out();
        init_session(&mut init_out, &input[..], &mut output).expect("initialization failed");

        let expected_max_pages = (DEFAULT_MAX_WRITE / (pagesize() as u32)) as u16;

        assert_eq!(init_out.major, 7);
        assert_eq!(init_out.minor, 23);
        assert_eq!(init_out.max_readahead, 40);
        assert_eq!(init_out.max_background, 0);
        assert_eq!(init_out.congestion_threshold, 0);
        assert_eq!(init_out.max_write, DEFAULT_MAX_WRITE);
        assert_eq!(init_out.max_pages, expected_max_pages);
        assert_eq!(init_out.time_gran, 1);
        assert!(init_out.flags & FUSE_NO_OPEN_SUPPORT != 0);
        assert!(init_out.flags & FUSE_NO_OPENDIR_SUPPORT != 0);

        let output_len = mem::size_of::<fuse_out_header>() + mem::size_of::<fuse_init_out>();
        let out_header = fuse_out_header {
            len: output_len as u32,
            error: 0,
            unique: 2,
        };
        let init_out = fuse_init_out {
            major: 7,
            minor: 23,
            max_readahead: 40,
            flags: DEFAULT_INIT_FLAGS | FUSE_MAX_PAGES | FUSE_BIG_WRITES,
            max_background: 0,
            congestion_threshold: 0,
            max_write: DEFAULT_MAX_WRITE,
            time_gran: 1,
            max_pages: expected_max_pages,
            padding: 0,
            unused: [0; 8],
        };

        let mut expected = Vec::with_capacity(output_len);
        expected.extend_from_slice(out_header.as_bytes());
        expected.extend_from_slice(init_out.as_bytes());
        assert_eq!(output.len(), output_len);

        assert_eq!(expected[0..4], output[0..4], "out_header.len");
        assert_eq!(expected[4..8], output[4..8], "out_header.error");
        assert_eq!(expected[8..16], output[8..16], "out_header.unique");

        let expected = &expected[mem::size_of::<fuse_out_header>()..];
        let output = &output[mem::size_of::<fuse_out_header>()..];
        assert_eq!(expected[0..4], output[0..4], "init_out.major");
        assert_eq!(expected[4..8], output[4..8], "init_out.minor");
        assert_eq!(expected[8..12], output[8..12], "init_out.max_readahead");
        assert_eq!(expected[12..16], output[12..16], "init_out.flags");
        assert_eq!(expected[16..18], output[16..18], "init_out.max_background");
        assert_eq!(
            expected[18..20],
            output[18..20],
            "init_out.congestion_threshold"
        );
        assert_eq!(expected[20..24], output[20..24], "init_out.max_write");
        assert_eq!(expected[24..28], output[24..28], "init_out.time_gran");
        assert_eq!(expected[28..30], output[28..30], "init_out.max_pages");
        assert!(
            output[30..30 + 2 + 4 * 8].iter().all(|&b| b == 0x00),
            "init_out.paddings"
        );
    }

    #[inline]
    fn bytes(bytes: &[u8]) -> &[u8] {
        bytes
    }
    macro_rules! b {
        ($($b:expr),*$(,)?) => ( *bytes(&[$($b),*]) );
    }

    #[test]
    fn send_msg_empty() {
        let mut buf = vec![0u8; 0];
        write_bytes(&mut buf, Reply::new(42, -4, &[])).unwrap();
        assert_eq!(buf[0..4], b![0x10, 0x00, 0x00, 0x00], "header.len");
        assert_eq!(buf[4..8], b![0x04, 0x00, 0x00, 0x00], "header.error");
        assert_eq!(
            buf[8..16],
            b![0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            "header.unique"
        );
    }

    #[test]
    fn send_msg_single_data() {
        let mut buf = vec![0u8; 0];
        write_bytes(&mut buf, Reply::new(42, 0, "hello")).unwrap();
        assert_eq!(buf[0..4], b![0x15, 0x00, 0x00, 0x00], "header.len");
        assert_eq!(buf[4..8], b![0x00, 0x00, 0x00, 0x00], "header.error");
        assert_eq!(
            buf[8..16],
            b![0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            "header.unique"
        );
        assert_eq!(buf[16..], b![0x68, 0x65, 0x6c, 0x6c, 0x6f], "payload");
    }

    #[test]
    fn send_msg_chunked_data() {
        let payload: &[&[u8]] = &[
            "hello, ".as_ref(), //
            "this ".as_ref(),
            "is a ".as_ref(),
            "message.".as_ref(),
        ];
        let mut buf = vec![0u8; 0];
        write_bytes(&mut buf, Reply::new(26, 0, payload)).unwrap();
        assert_eq!(buf[0..4], b![0x29, 0x00, 0x00, 0x00], "header.len");
        assert_eq!(buf[4..8], b![0x00, 0x00, 0x00, 0x00], "header.error");
        assert_eq!(
            buf[8..16],
            b![0x1a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            "header.unique"
        );
        assert_eq!(buf[16..], *b"hello, this is a message.", "payload");
    }
}
