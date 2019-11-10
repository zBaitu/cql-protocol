use crate::codec::*;
use crate::def::*;
use crate::message::*;
use crate::request::query::QueryParams;
use crate::result::*;
use crate::types::*;

use std::io;

#[derive(Debug, PartialEq)]
pub struct Execute {
    id: ShortBytes,
    params: QueryParams,
}

impl Execute {
    pub fn from(id: ShortBytes, params: QueryParams) -> Self {
        Execute {
            id,
            params,
        }
    }

    pub fn id(&self) -> &ShortBytes {
        &self.id
    }

    pub fn params(&self) -> &QueryParams {
        &self.params
    }
}

impl Message for Execute {
    fn opcode(&self) -> Opcode { Opcode::Execute }
}

impl Serializable for Execute {
    fn length(&self) -> u32 {
        self.id.length() + self.params.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_short_bytes(&self.id)?;
        self.params.encode(codec)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        Ok(Execute {
            id: codec.read_short_bytes()?,
            params: QueryParams::decode(codec)?,
        })
    }
}
