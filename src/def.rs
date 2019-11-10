use crate::types::*;

use std::ops::{BitAndAssign, BitOrAssign, Not};

macro_rules! impl_flags {
    ($F:ident, $T:ident) => (

impl $F {
    pub fn is_set(&self, flags: $T) -> bool {
        (*self as $T) & flags != 0
    }
}

impl Not for $F {
    type Output = $T;

    fn not(self) -> $T {
        !(self as $T)
    }
}

impl BitOrAssign<$F> for $T {
    fn bitor_assign(&mut self, rhs: $F) {
        *self |= rhs as $T
    }
}

impl BitAndAssign<$F> for $T {
    fn bitand_assign(&mut self, rhs: $F) {
        *self &= rhs as $T
    }
}

    );
}

#[derive(Clone, Copy, FromPrimitive)]
pub enum Version {
    V3 = 3,
    V4 = 4,
    V5 = 5,
}

impl Version {
    pub fn is_beta(&self) -> bool {
        match self {
            Self::V5 => true,
            _ => false,
        }
    }
}

impl Default for Version {
    fn default() -> Version { Version::V4 }
}

#[derive(Clone, Copy)]
pub enum Flags {
    Compression = 0x01,
    Tracing = 0x02,
    CustomPayload = 0x04,
    Warning = 0x08,
    Beta = 0x10,
}

impl_flags!(Flags, Byte);

#[derive(Debug, FromPrimitive, PartialEq)]
pub enum Opcode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}

#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq)]
pub enum ErrorCode {
    ServerError = 0x0000,
    ProtocolError = 0x000A,
    AuthenticationError = 0x0100,
    UnavailableException = 0x1000,
    Overloaded = 0x1001,
    IsBootstrapping = 0x1002,
    TruncateError = 0x1003,
    WriteTimeout = 0x1100,
    ReadTimeout = 0x1200,
    ReadFailure = 0x1300,
    FunctionFailure = 0x1400,
    WriteFailure = 0x1500,
    SyntaxError = 0x2000,
    Unauthorized = 0x2100,
    Invalid = 0x2200,
    ConfigError = 0x2300,
    AlreadyExists = 0x2400,
    Unprepared = 0x2500,
}

#[derive(Debug, Display, EnumIter, EnumString, PartialEq)]
pub enum EventType {
    #[strum(serialize = "TOPOLOGY_CHANGE")]
    TopologyChange,
    #[strum(serialize = "STATUS_CHANGE")]
    StatusChange,
    #[strum(serialize = "SCHEMA_CHANGE")]
    SchemaChange,
}

#[derive(Debug, Display, EnumIter, EnumString, PartialEq)]
pub enum TopologyChangeType {
    #[strum(serialize = "NEW_NODE")]
    NewNode,
    #[strum(serialize = "REMOVED_NODE")]
    RemovedNode,
    #[strum(serialize = "MOVED_NODE")]
    MovedNode,
}

#[derive(Debug, Display, EnumIter, EnumString, PartialEq)]
pub enum StatusChangeType {
    #[strum(serialize = "UP")]
    Up,
    #[strum(serialize = "DOWN")]
    Down,
}

#[derive(Debug, Display, EnumIter, EnumString, PartialEq)]
pub enum SchemaChangeType {
    #[strum(serialize = "CREATED")]
    Created,
    #[strum(serialize = "UPDATED")]
    Updated,
    #[strum(serialize = "DROPPED")]
    Dropped,
}

#[derive(Debug, Display, EnumIter, EnumString, PartialEq)]
pub enum SchemaChangeTarget {
    #[strum(serialize = "KEYSPACE")]
    Keyspace,
    #[strum(serialize = "TABLE")]
    Table,
    #[strum(serialize = "TYPE")]
    Type,
    #[strum(serialize = "FUNCTION")]
    Function,
    #[strum(serialize = "AGGREGATE")]
    Aggregate,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum QueryFlags {
    Values = 0x01,
    SkipMetadata = 0x02,
    PageSize = 0x04,
    PagingState = 0x08,
    SerialConsistency = 0x10,
    DefaultTimestamp = 0x20,
    NamesForValues = 0x40,
    Keyspace = 0x80,
}

impl_flags!(QueryFlags, Int);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PrepareFlags {
    Keyspace = 0x01,
}

impl_flags!(PrepareFlags, Int);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BatchFlags {
    SerialConsistency = 0x10,
    DefaultTimestamp = 0x20,
    NamesForValues = 0x40,
    Keyspace = 0x80,
}

impl_flags!(BatchFlags, Int);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RowsFlags {
    GlobalTablesSpec = 0x0001,
    HasMorePages = 0x0002,
    NoMetadata = 0x0004,
    MetadataChanged = 0x0008,
}

impl_flags!(RowsFlags, Int);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PreparedFlags {
    GlobalTablesSpec = 0x0001,
}

impl_flags!(PreparedFlags, Int);

#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq)]
pub enum BatchType {
    Logged = 0,
    Unlogged = 1,
    Counter = 3,
}

impl Default for BatchType {
    fn default() -> BatchType { BatchType::Logged }
}

#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq)]
pub enum ResultKind {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    SchemaChange = 0x0005,
    Prepared = 0x0004,
}

#[derive(Display, EnumIter, EnumString)]
pub enum OptionKeys {
    #[strum(serialize = "CQL_VERSION")]
    CqlVersion,
    #[strum(serialize = "COMPRESSION")]
    Compression,
}

pub const CQL_VERSION: &str = "3.0.0";

#[derive(Display, EnumIter, EnumString)]
pub enum WriteType {
    #[strum(serialize = "SIMPLE")]
    Simple,
    #[strum(serialize = "BATCH")]
    Batch,
    #[strum(serialize = "UNLOGGED_BATCH")]
    UnloggedBatch,
    #[strum(serialize = "COUNTER")]
    Counter,
    #[strum(serialize = "BATCH_LOG")]
    BatchLog,
}

