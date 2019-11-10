use crate::codec::*;
use crate::def::*;
use crate::message::*;
use crate::result::*;
use crate::types::*;

use num_traits::FromPrimitive;

use std::{
    fmt::{self, Debug, Display, Formatter},
    io
};

#[derive(Debug, PartialEq)]
pub enum BatchQueryKind {
    Query(LongString),
    Execute(ShortBytes),
}

#[derive(Debug, PartialEq)]
pub struct BatchQuery {
    kind: BatchQueryKind,
    values: Vec<Value>,
}

impl BatchQuery {
    pub fn new(kind: BatchQueryKind) -> Self {
        BatchQuery {
            kind,
            values: Vec::new(),
        }
    }

    pub fn from(kind: BatchQueryKind, values: Vec<Value>) -> Self {
        BatchQuery {
            kind,
            values,
        }
    }

    pub fn set_values(&mut self, values: Vec<Value>) {
        self.values = values;
    }

    pub fn kind(&self) -> &BatchQueryKind {
        &self.kind
    }

    pub fn values(&self) -> &Vec<Value> {
        &self.values
    }
}

impl Serializable for BatchQuery {
    fn length(&self) -> u32 {
        let mut len = len::BYTE;
        match self.kind {
            BatchQueryKind::Query(ref s) => len += s.length(),
            BatchQueryKind::Execute(ref b) => len += b.length(),
        };

        len += self.values.iter().fold(len::SHORT, |len, e| len + e.length());
        len
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        match self.kind {
            BatchQueryKind::Query(ref s) => {
                codec.write_byte(0)?;
                codec.write_long_string(s)?;
            },
            BatchQueryKind::Execute(ref b) => {
                codec.write_byte(1)?;
                codec.write_short_bytes(b)?;
            },
        };

        codec.write_short(self.values.len() as Short)?;
        for i in 0..self.values.len() {
            codec.write_value(&self.values[i])?;
        }

        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        let kind_num = codec.read_byte()?;
        let kind = if kind_num == 0 {
            BatchQueryKind::Query(codec.read_long_string()?)
        } else {
            BatchQueryKind::Execute(codec.read_short_bytes()?)
        };

        let mut values = Vec::new();
        let len = codec.read_short()?;
        for _ in 1..=len {
            values.push(codec.read_value()?);
        }

        Ok(BatchQuery {
            kind,
            values,
        })
    }
}

impl_display!(BatchQuery);

#[derive(Debug, Default, PartialEq)]
pub struct Batch {
    ty: BatchType,
    querys: Vec<BatchQuery>,
    consistency: Consistency,
    serial_consistency: Option<Consistency>,
    timestamp: Option<Long>,
    ks: Option<String>,
}

impl Batch {
    pub fn from(ty: BatchType, querys: Vec<BatchQuery>) -> Self {
        Batch {
            ty,
            querys,
            ..Default::default()
        }
    }

    pub fn set_consistency(&mut self, consistency: Consistency) {
        self.consistency = consistency;
    }

    pub fn set_serial_consistency(&mut self, serial_consistency: Consistency) {
        self.serial_consistency = Some(serial_consistency);
    }

    pub fn set_timestamp(&mut self, timestamp: Long) {
        self.timestamp = Some(timestamp);
    }

    pub fn set_keyspace(&mut self, ks: &str) {
        self.ks = Some(ks.to_string());
    }

    pub fn ty(&self) -> &BatchType {
        &self.ty
    }

    pub fn querys(&self) -> &Vec<BatchQuery> {
        &self.querys
    }

    pub fn consistency(&self) -> &Consistency {
        &self.consistency
    }

    pub fn serial_consistency(&self) -> &Option<Consistency> {
        &self.serial_consistency
    }

    pub fn timestamp(&self) -> &Option<Long> {
        &self.timestamp
    }

    pub fn keyspace(&self) -> &Option<String> {
        &self.ks
    }

    fn flags(&self) -> Int {
        let mut flags = 0;
        if self.serial_consistency.is_some() {
            flags |= QueryFlags::SerialConsistency;
        }
        if self.timestamp.is_some() {
            flags |= QueryFlags::DefaultTimestamp;
        }
        if self.ks.is_some() {
            flags |= QueryFlags::Keyspace;
        }
        flags
    }
}

impl Message for Batch {
    fn new() -> Self {
        Batch::default()
    }

    fn opcode(&self) -> Opcode { Opcode::Batch }
}

impl Serializable for Batch {
    fn length(&self) -> u32 {
        let mut len = len::BYTE + len::SHORT + len::INT;
        len += self.querys.iter().fold(len::SHORT, |len, e| len + e.length());

        if self.serial_consistency.is_some() {
            len += len::SHORT;
        }
        if self.timestamp.is_some() {
            len += len::LONG;
        }
        if self.ks.is_some() {
            len += self.ks.length();
        }

        len
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_byte(self.ty as Byte)?;
        codec.write_short(self.querys.len() as Short)?;
        for query in &self.querys {
            query.encode(codec)?;
        }
        codec.write_consistency(self.consistency)?;
        codec.write_int(self.flags())?;

        if let Some(serial_consistency) = self.serial_consistency {
            codec.write_consistency(serial_consistency)?;
        }
        if let Some(timestamp) = self.timestamp {
            codec.write_long(timestamp)?;
        }
        if let Some(ref ks) = self.ks {
            codec.write_string(ks)?;
        }

        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        let ty = FromPrimitive::from_u8(codec.read_byte()?).unwrap();
        let mut querys = Vec::new();
        let len = codec.read_short()?;
        for _ in 1..=len {
            querys.push(BatchQuery::decode(codec)?);
        }
        let consistency = codec.read_consistency()?;

        let flags = codec.read_int()?;
        let serial_consistency = if QueryFlags::SerialConsistency.is_set(flags) {
            Some(codec.read_consistency()?)
        } else {
            None
        };
        let timestamp = if QueryFlags::DefaultTimestamp.is_set(flags) {
            Some(codec.read_long()?)
        } else {
            None
        };
        let ks = if QueryFlags::Keyspace.is_set(flags) {
            Some(codec.read_string()?)
        } else {
            None
        };

        Ok(Batch {
            ty,
            querys,
            consistency,
            serial_consistency,
            timestamp,
            ks,
        })
    }
}
