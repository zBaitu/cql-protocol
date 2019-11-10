use uuid;

use std::error::Error;
use std::{fmt::{self, Display, Formatter}, io, string};

#[derive(Debug)]
pub enum ProtError {
    IoErr(io::Error),
    Utf8Err(string::FromUtf8Error),
    AsciiErr(ascii::FromAsciiError<Vec<u8>>),
    UuidErr(uuid::Error),
}

impl From<io::Error> for ProtError {
    fn from(e: io::Error) -> ProtError {
        ProtError::IoErr(e)
    }
}

impl From<string::FromUtf8Error> for ProtError {
    fn from(e: string::FromUtf8Error) -> ProtError {
        ProtError::Utf8Err(e)
    }
}

impl From<ascii::FromAsciiError<Vec<u8>>> for ProtError {
    fn from(e: ascii::FromAsciiError<Vec<u8>>) -> ProtError {
        ProtError::AsciiErr(e)
    }
}

impl From<uuid::Error> for ProtError {
    fn from(e: uuid::Error) -> ProtError {
        ProtError::UuidErr(e)
    }
}

impl Error for ProtError {
    fn description(&self) -> &str {
        match self {
            Self::IoErr(e) => Error::description(e),
            Self::Utf8Err(e) => Error::description(e),
            Self::AsciiErr(e) => Error::description(e),
            Self::UuidErr(e) => Error::description(e),
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            _ => Some(self as &dyn Error),
        }
    }
}

impl Display for ProtError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            _ => write!(f, "{}", Error::description(self)),
        }
    }
}

pub type ProtResult<T> = Result<T, ProtError>;

pub const OK: ProtResult<()> = Ok(());
