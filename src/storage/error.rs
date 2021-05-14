use std::{error, array, io, fmt};

#[derive(Debug)]
pub enum Error {
    Crc16Mismatch,
    UnknownCompression,
    Io(io::Error),
    Slice(array::TryFromSliceError),
    VarIntError,
    ArgTooSmall,
    TooManyEntries,
    DataFileTooBig,
    InvalidOffset,
    IndexFileTooBig,
    OffsetOutsideTheRange,
    OffsetIsNotAligned,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<array::TryFromSliceError> for Error {
    fn from(err: array::TryFromSliceError) -> Error {
        Error::Slice(err)
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, format!("{:?}", err))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            Error::Slice(err) => Some(err),
            _ => None
        }
    }
}