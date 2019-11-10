use crate::codec::*;
use crate::def::*;
use crate::message::*;
use crate::result::*;
use crate::types::*;

use std::{io, str::FromStr};

#[derive(Debug, PartialEq)]
pub struct Register {
    events: StringList,
}

impl Register {
    pub fn from(events: &[EventType]) -> Register {
        Register {
            events: events.into_iter().map(ToString::to_string).collect(),
        }
    }

    pub fn set_events(&mut self, events: &[EventType]) {
        self.events = events.into_iter().map(ToString::to_string).collect();
    }

    pub fn events(&self) -> Vec<EventType> {
        self.events.iter().map(|e| FromStr::from_str(e.as_str()).unwrap()).collect()
    }
}

impl Message for Register {
    fn new() -> Self {
        Register {
            events: Default::default(),
        }
    }

    fn opcode(&self) -> Opcode { Opcode::Register }
}

impl Serializable for Register {
    fn length(&self) -> u32 {
        self.events.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_string_list(&self.events)
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        Ok(Register {
            events: codec.read_string_list()?,
        })
    }
}
