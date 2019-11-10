use crate::codec::*;
use crate::def::*;
use crate::message::*;
use crate::result::*;
use crate::types::*;

use std::io;

#[derive(Debug, Default, PartialEq)]
pub struct Prepare {
    query: LongString,
    ks: Option<String>,
}

impl Prepare {
    pub fn from(query: &str) -> Self {
        Prepare {
            query: LongString::new(query),
            ..Default::default()
        }
    }

    pub fn set_query(&mut self, query: &str) {
        self.query = LongString::new(query)
    }

    pub fn set_keyspace(&mut self, ks: &str) {
        self.ks = Some(ks.to_string());
    }

    pub fn query(&self) -> &str {
        &self.query.0
    }

    pub fn keyspace(&self) -> &Option<String> {
        &self.ks
    }
}

impl Message for Prepare {
    fn opcode(&self) -> Opcode { Opcode::Prepare }
}

impl Serializable for Prepare {
    fn length(&self) -> u32 {
        self.query.length() + len::INT + self.ks.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_long_string(&self.query)?;
        if let Some(ref ks) = self.ks {
            codec.write_int(PrepareFlags::Keyspace as Int)?;
            codec.write_string(ks)?;
        } else {
            codec.write_int(0)?;
        }
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        let query = codec.read_long_string()?;
        let flags = codec.read_int()?;
        let ks = if PrepareFlags::Keyspace.is_set(flags) {
            Some(codec.read_string()?)
        } else {
            None
        };

        Ok(Prepare {
            query,
            ks,
        })
    }
}
