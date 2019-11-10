use crate::codec::*;
use crate::def::*;
use crate::message::*;
use crate::result::*;
use crate::types::*;

use std::io;

#[derive(Debug, PartialEq)]
pub struct Supported {
    options: StringMultimap,
}

impl Supported {
    pub fn from(options: StringMultimap) -> Self {
        Supported {
            options,
        }
    }

    pub fn add_option(&mut self, k: String, v: StringList) {
        self.options.insert(k, v);
    }

    pub fn options(&self) -> &StringMultimap {
        &self.options
    }
}

impl Message for Supported {
    fn new() -> Self {
        Supported {
            options: Default::default(),
        }
    }

    fn opcode(&self) -> Opcode { Opcode::Supported }
}

impl Serializable for Supported {
    fn length(&self) -> u32 {
        self.options.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_sting_multimap(&self.options)
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        Ok(Supported {
            options: codec.read_string_multimap()?,
        })
    }
}
