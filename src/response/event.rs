use crate::codec::*;
use crate::def::*;
use crate::message::*;
use crate::result::*;
use crate::types::*;

use std::{
    fmt::{self, Debug, Display, Formatter},
    io,
    str::FromStr
};

macro_rules! change_event {
    ($C:ident, $T:ident) => (

#[derive(Debug, PartialEq)]
pub struct $C {
    change: String,
    node: Inet,
}

impl $C {
    pub fn new(change: $T, node: Inet) -> Self {
        $C {
            change: change.to_string(),
            node,
        }
    }

    pub fn change(&self) -> $T {
        FromStr::from_str(&self.change).unwrap()
    }

    pub fn node(&self) -> &Inet {
        &self.node
    }
}

impl Serializable for $C {
    fn length(&self) -> u32 {
        self.change.length() + self.node.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_string(&self.change)?;
        codec.write_inet(&self.node)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok($C {
            change: codec.read_string()?,
            node: codec.read_inet()?,
        })
    }
}

    );
}

change_event!(TopologyChange, TopologyChangeType);
change_event!(StatusChange, StatusChangeType);

#[derive(Debug, Default, PartialEq)]
pub struct SchemaChange {
    change: String,
    target: String,
    ks: String,
    name: Option<String>,
    args: Option<StringList>,
}

impl SchemaChange {
    pub fn new(change: SchemaChangeType, target: SchemaChangeTarget, ks: &str) -> Self {
        SchemaChange {
            change: change.to_string(),
            target: target.to_string(),
            ks: ks.to_string(),
            ..Default::default()
        }
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = Some(name.to_string());
    }

    pub fn set_args(&mut self, args: StringList) {
        self.args = Some(args);
    }

    pub fn change(&self) -> SchemaChangeType {
        FromStr::from_str(&self.change).unwrap()
    }

    pub fn target(&self) -> SchemaChangeTarget {
        FromStr::from_str(&self.change).unwrap()
    }

    pub fn keyspace(&self) -> &str {
        &self.ks
    }

    pub fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }

    pub fn args(&self) -> Option<&StringList> {
        self.args.as_ref()
    }
}

impl Serializable for SchemaChange {
    fn length(&self) -> u32 {
        self.change.length() + self.target.length() + self.ks.length() + self.name.length() + self.args.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_string(&self.change)?;
        codec.write_string(&self.target)?;
        codec.write_string(&self.ks)?;
        if let Some(ref name) = self.name {
            codec.write_string(name)?;
        }
        if let Some(ref args) = self.args {
            codec.write_string_list(args)?;
        }
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        let change = codec.read_string()?;
        let target = codec.read_string()?;
        let ks = codec.read_string()?;
        let mut name = None;
        let mut args = None;

        let target_type = FromStr::from_str(&target).unwrap();
        match target_type {
            SchemaChangeTarget::Keyspace => (),
            SchemaChangeTarget::Table | SchemaChangeTarget::Type => {
                name = Some(codec.read_string()?);
            },
            SchemaChangeTarget::Function | SchemaChangeTarget::Aggregate => {
                name = Some(codec.read_string()?);
                args = Some(codec.read_string_list()?);
            },
        }

        Ok(SchemaChange {
            change,
            target,
            ks,
            name,
            args,
        })
    }
}

impl_display!(TopologyChange);
impl_display!(StatusChange);
impl_display!(SchemaChange);

macro_rules! match_event_type {
    ($sf:expr, $($E:ident),+) => (
        match $sf {
            $(Self::$E(_) => EventType::$E,)*
        }
    );
}

macro_rules! match_length {
    ($sf:expr, $($E:ident),+) => (
        match $sf {
            $(Self::$E(ref e) => e.length(),)*
        }
    );
}

macro_rules! match_encode {
    ($sf:expr, $codec:expr, $($E:ident),+) => (
        match $sf {
            $(Self::$E(ref e) => e.encode($codec),)*
        }
    );
}

macro_rules! match_decode {
    ($event_type:expr, $codec:expr, $($E:ident),+) => (
        match $event_type {
            $(EventType::$E => Event::$E($E::decode($codec)?),)*
        }
    );
}

#[derive(Debug, PartialEq)]
pub enum Event {
    TopologyChange(TopologyChange),
    StatusChange(StatusChange),
    SchemaChange(SchemaChange),
}

impl Message for Event {
    fn opcode(&self) -> Opcode { Opcode::Event }
}

impl Serializable for Event {
    fn length(&self) -> u32 {
        let event_type = match_event_type!(self, TopologyChange, StatusChange, SchemaChange);
        event_type.to_string().length() + match_length!(self, TopologyChange, StatusChange, SchemaChange)
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        let event_type = match_event_type!(self, TopologyChange, StatusChange, SchemaChange);
        codec.write_string(&event_type.to_string())?;
        match_encode!(self, codec, TopologyChange, StatusChange, SchemaChange)
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        let event_type = FromStr::from_str(&codec.read_string()?).unwrap();
        let event = match_decode!(event_type, codec, TopologyChange, StatusChange, SchemaChange);
        Ok(event)
    }
}
