use crate::codec::*;
use crate::def::*;
use crate::message::*;
use crate::result::*;
use crate::types::*;

use std::{
    fmt::{self, Debug, Display, Formatter},
    io
};

#[derive(Debug, Default, PartialEq)]
pub struct QueryParams {
    consistency: Consistency,
    names: Vec<String>,
    values: Vec<Value>,
    result_page_size: Option<Int>,
    paging_state: Bytes,
    serial_consistency: Option<Consistency>,
    timestamp: Option<Long>,
    ks: Option<String>,
}

impl QueryParams {
    pub fn set_consistency(&mut self, consistency: Consistency) {
        self.consistency = consistency;
    }

    pub fn set_names(&mut self, names: Vec<String>) {
        self.names = names;
    }

    pub fn set_values(&mut self, values: Vec<Value>) {
        self.values = values;
    }

    pub fn set_result_page_size(&mut self, size: Int) {
        self.result_page_size = Some(size);
    }

    pub fn set_paging_state(&mut self, paging_state: Bytes) {
        self.paging_state = paging_state;
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

    pub fn consistency(&self) -> &Consistency {
        &self.consistency
    }

    pub fn names(&self) -> &Vec<String> {
        &self.names
    }

    pub fn values(&self) -> &Vec<Value> {
        &self.values
    }

    pub fn result_page_size(&self) -> &Option<Int> {
        &self.result_page_size
    }

    pub fn paging_state(&self) -> &Bytes {
        &self.paging_state
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
        if !self.values.is_empty() {
            flags |= QueryFlags::Values;

            if !self.names.is_empty() {
                flags |= QueryFlags::NamesForValues;
            }
        }

        if self.result_page_size.is_some() {
            flags |= QueryFlags::PageSize;
        }
        if self.paging_state.is_some() {
            flags |= QueryFlags::PagingState;
        }
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

impl Serializable for QueryParams {
    fn length(&self) -> u32 {
        let mut len = len::SHORT + len::INT;
        if !self.values.is_empty() {
            len += self.values.iter().fold(len::SHORT, |len, e| len + e.length());
            len += self.names.iter().map(|e| e.length()).sum::<u32>();
        }
        if self.result_page_size.is_some() {
            len += len::INT;
        }
        if self.paging_state.is_some() {
            len += self.paging_state.length();
        }
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
        codec.write_consistency(self.consistency)?;
        codec.write_int(self.flags())?;

        if !self.values.is_empty() {
            codec.write_short(self.values.len() as Short)?;
            let has_names = !self.names.is_empty();
            for i in 0..self.values.len() {
                if has_names {
                    codec.write_string(&self.names[i])?;
                }
                codec.write_value(&self.values[i])?;
            }
        }

        if let Some(size) = self.result_page_size {
            codec.write_int(size)?;
        }
        if self.paging_state.is_some() {
            codec.write_bytes(&self.paging_state)?;
        }
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
        let consistency = codec.read_consistency()?;
        let flags = codec.read_int()?;

        let mut names = Vec::new();
        let mut values = Vec::new();
        if QueryFlags::Values.is_set(flags) {
            let len = codec.read_short()?;
            let has_names = QueryFlags::NamesForValues.is_set(flags);
            for _ in 1..=len {
                if has_names {
                    names.push(codec.read_string()?);
                }
                values.push(codec.read_value()?);
            }
        }

        let result_page_size = if QueryFlags::PageSize.is_set(flags) {
            Some(codec.read_int()?)
        } else {
            None
        };
        let paging_state = if QueryFlags::PagingState.is_set(flags) {
            codec.read_bytes()?
        } else {
            None
        };
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

        Ok(QueryParams {
            consistency,
            names,
            values,
            result_page_size,
            paging_state,
            serial_consistency,
            timestamp,
            ks,
        })
    }
}

impl_display!(QueryParams);

#[derive(Debug, Default, PartialEq)]
pub struct Query {
    query: LongString,
    params: QueryParams,
    tracing: bool,
}

impl Query {
    pub fn from(query: &str) -> Self {
        Query {
            query: LongString::new(query),
            ..Default::default()
        }
    }

    pub fn set_query(&mut self, query: &str) {
        self.query = LongString::new(query)
    }

    pub fn set_params(&mut self, params: QueryParams) {
        self.params = params;
    }

    pub fn tracing_on(&mut self) {
        self.tracing = true;
    }

    pub fn tracing_off(&mut self) {
        self.tracing = false;
    }

    pub fn query(&self) -> &str {
        &self.query.0
    }

    pub fn params(&self) -> &QueryParams {
        &self.params
    }

    pub fn params_mut(&mut self) -> &mut QueryParams {
        &mut self.params
    }
}

impl Message for Query {
    fn tracing(&self) -> bool {
        self.tracing
    }

    fn opcode(&self) -> Opcode { Opcode::Query }
}

impl Serializable for Query {
    fn length(&self) -> u32 {
        self.query.length() + self.params.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_long_string(&self.query)?;
        self.params.encode(codec)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        Ok(Query {
            query: codec.read_long_string()?,
            params: QueryParams::decode(codec)?,
            tracing: false,
        })
    }
}
