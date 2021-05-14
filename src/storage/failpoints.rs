#[macro_export]
#[cfg(test)]
macro_rules! fail {
    ($name:expr, io) => {
        fail::fail_point!($name, |_| Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            "write"
        ))));
    };
}

#[macro_export]
#[cfg(not(test))]
macro_rules! fail {
    ($name:expr, io) => {
        {};
    };
}

pub use fail;