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
