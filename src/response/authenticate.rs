use crate::codec::*;
use crate::def::*;
use crate::message::*;
use crate::result::*;
use crate::types::*;

use std::io;

#[derive(Debug, PartialEq)]
pub struct Authenticate {
    authenticator: String,
}

impl Authenticate {
    pub fn from(authenticator: &str) -> Self {
        Authenticate {
            authenticator: authenticator.to_string(),
        }
    }

    pub fn set_authenticator(&mut self, authenticator: &str) {
        self.authenticator = authenticator.to_string();
    }

    pub fn authenticator(&self) -> &str {
        &self.authenticator
    }
}

impl Message for Authenticate {
    fn new() -> Self {
        Authenticate {
            authenticator: Default::default(),
        }
    }

    fn opcode(&self) -> Opcode { Opcode::Authenticate }
}

impl Serializable for Authenticate {
    fn length(&self) -> u32 {
        self.authenticator.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_string(&self.authenticator)
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        Ok(Authenticate {
            authenticator: codec.read_string()?,
        })
    }
}
