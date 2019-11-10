use crate::codec::*;
use crate::compression::Compression;
use crate::def::*;
use crate::message::*;
use crate::result::*;
use crate::types::*;

use std::{io, str::FromStr};

#[derive(Debug, PartialEq)]
pub struct Startup {
    options: StringMap,
}

impl Startup {
    pub fn set_compression(&mut self, compression: Compression) {
        self.options.insert(OptionKeys::Compression.to_string(), compression.to_string());
    }

    pub fn compression(&self) -> Option<Compression> {
        self.options.get(&OptionKeys::Compression.to_string()).map(|c| FromStr::from_str(c.as_str()).unwrap())
    }
}

impl Message for Startup {
    fn new() -> Self {
        Startup {
            options: hashmap!{OptionKeys::CqlVersion.to_string() => CQL_VERSION.to_string()},
        }
    }

    fn opcode(&self) -> Opcode { Opcode::Startup }
}

impl Serializable for Startup {
    fn length(&self) -> u32 {
        self.options.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_sting_map(&self.options)
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        Ok(Startup {
            options: codec.read_string_map()?,
        })
    }
}
