use crate::codec::*;
use crate::def::*;
use crate::message::*;
use crate::result::*;
use crate::types::*;

use num_traits::FromPrimitive;

use std::{
    fmt::{self, Debug, Display, Formatter},
    io,
    str::FromStr
};

#[derive(Debug, PartialEq)]
pub struct UnavailableException {
    cl: Consistency,
    required: Int,
    alive: Int,
}

impl UnavailableException {
    pub fn new(cl: Consistency, required: Int, alive: Int) -> Self {
        UnavailableException {
            cl,
            required,
            alive,
        }
    }

    pub fn consistency(&self) -> Consistency {
        self.cl
    }

    pub fn required(&self) -> Int {
        self.required
    }

    pub fn alive(&self) -> Int {
        self.alive
    }
}

impl Serializable for UnavailableException {
    fn length(&self) -> u32 {
        len::SHORT + len::INT * 2
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_consistency(self.consistency())?;
        codec.write_int(self.required)?;
        codec.write_int(self.alive)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok(UnavailableException {
            cl: codec.read_consistency()?,
            required: codec.read_int()?,
            alive: codec.read_int()?,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct WriteTimeout {
    cl: Consistency,
    received: Int,
    blockfor: Int,
    write_type: String,
}

impl WriteTimeout {
    pub fn new(cl: Consistency, received: Int, blockfor: Int, write_type: WriteType) -> Self {
        WriteTimeout {
            cl,
            received,
            blockfor,
            write_type: write_type.to_string(),
        }
    }

    pub fn consistency(&self) -> Consistency {
        self.cl
    }

    pub fn received(&self) -> Int {
        self.received
    }

    pub fn blockfor(&self) -> Int {
        self.blockfor
    }

    pub fn write_type(&self) -> WriteType {
        FromStr::from_str(&self.write_type).unwrap()
    }
}

impl Serializable for WriteTimeout {
    fn length(&self) -> u32 {
        len::SHORT + len::INT * 2 + self.write_type.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_consistency(self.consistency())?;
        codec.write_int(self.received)?;
        codec.write_int(self.blockfor)?;
        codec.write_string(&self.write_type)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok(WriteTimeout {
            cl: codec.read_consistency()?,
            received: codec.read_int()?,
            blockfor: codec.read_int()?,
            write_type: codec.read_string()?,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct ReadTimeout {
    cl: Consistency,
    received: Int,
    blockfor: Int,
    data_present: Byte,
}

impl ReadTimeout {
    pub fn new(cl: Consistency, received: Int, blockfor: Int, data_present: Byte) -> Self {
        ReadTimeout {
            cl,
            received,
            blockfor,
            data_present,
        }
    }

    pub fn consistency(&self) -> Consistency {
        self.cl
    }

    pub fn received(&self) -> Int {
        self.received
    }

    pub fn blockfor(&self) -> Int {
        self.blockfor
    }

    pub fn data_present(&self) -> Byte {
        self.data_present
    }
}

impl Serializable for ReadTimeout {
    fn length(&self) -> u32 {
        len::SHORT + len::INT * 2 + len::BYTE
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_consistency(self.consistency())?;
        codec.write_int(self.received)?;
        codec.write_int(self.blockfor)?;
        codec.write_byte(self.data_present)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok(ReadTimeout {
            cl: codec.read_consistency()?,
            received: codec.read_int()?,
            blockfor: codec.read_int()?,
            data_present: codec.read_byte()?,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct ReadFailure {
    cl: Consistency,
    received: Int,
    blockfor: Int,
    reasonmap: Vec<(InetAddr, Short)>,
    data_present: Byte,
}

impl ReadFailure {
    pub fn new(cl: Consistency, received: Int, blockfor: Int, reasonmap: Vec<(InetAddr, Short)>, data_present: Byte)
    -> Self {
        ReadFailure {
            cl,
            received,
            blockfor,
            reasonmap,
            data_present,
        }
    }

    pub fn consistency(&self) -> Consistency {
        self.cl
    }

    pub fn received(&self) -> Int {
        self.received
    }

    pub fn blockfor(&self) -> Int {
        self.blockfor
    }

    pub fn reasonmap(&self) -> &Vec<(InetAddr, Short)> {
        &self.reasonmap
    }

    pub fn data_present(&self) -> Byte {
        self.data_present
    }
}

impl Serializable for ReadFailure {
    fn length(&self) -> u32 {
        len::SHORT + len::INT * 2
                + len::INT + self.reasonmap.iter().map(|e| e.0.length() + len::SHORT).sum::<u32>()
                + len::BYTE
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_consistency(self.consistency())?;
        codec.write_int(self.received)?;
        codec.write_int(self.blockfor)?;

        codec.write_int(self.reasonmap.len() as Int)?;
        for e in &self.reasonmap {
            codec.write_inetaddr(&e.0)?;
            codec.write_short(e.1)?;
        }

        codec.write_byte(self.data_present)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        let cl = codec.read_consistency()?;
        let received = codec.read_int()?;
        let blockfor = codec.read_int()?;

        let mut reasonmap = Vec::new();
        let len = codec.read_int()?;
        for _ in 1..=len {
            reasonmap.push((codec.read_inetaddr()?, codec.read_short()?));
        }

        let data_present = codec.read_byte()?;

        Ok(ReadFailure {
            cl,
            received,
            blockfor,
            reasonmap,
            data_present,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct FunctionFailure {
    ks: String,
    function: String,
    arg_types: StringList,
}

impl FunctionFailure {
    pub fn new(ks: &str, table: &str, arg_types: StringList) -> Self {
        FunctionFailure {
            ks: ks.to_string(),
            function: table.to_string(),
            arg_types,
        }
    }

    pub fn keyspace(&self) -> &str {
        &self.ks
    }

    pub fn function(&self) -> &str {
        &self.function
    }

    pub fn arg_types(&self) -> &StringList {
        &self.arg_types
    }
}

impl Serializable for FunctionFailure {
    fn length(&self) -> u32 {
        self.ks.length() + self.function.length() + self.arg_types.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_string(&self.ks)?;
        codec.write_string(&self.function)?;
        codec.write_string_list(&self.arg_types)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok(FunctionFailure {
            ks: codec.read_string()?,
            function: codec.read_string()?,
            arg_types: codec.read_string_list()?,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct WriteFailure {
    cl: Consistency,
    received: Int,
    blockfor: Int,
    reasonmap: Vec<(InetAddr, Short)>,
    write_type: String,
}

impl WriteFailure {
    pub fn new(cl: Consistency, received: Int, blockfor: Int, reasonmap: Vec<(InetAddr, Short)>, write_type: WriteType)
    -> Self {
        WriteFailure {
            cl,
            received,
            blockfor,
            reasonmap,
            write_type: write_type.to_string(),
        }
    }

    pub fn consistency(&self) -> Consistency {
        self.cl
    }

    pub fn received(&self) -> Int {
        self.received
    }

    pub fn blockfor(&self) -> Int {
        self.blockfor
    }

    pub fn reasonmap(&self) -> &Vec<(InetAddr, Short)> {
        &self.reasonmap
    }

    pub fn write_type(&self) -> WriteType {
        FromStr::from_str(&self.write_type).unwrap()
    }
}

impl Serializable for WriteFailure {
    fn length(&self) -> u32 {
        len::SHORT + len::INT * 2
                + len::INT + self.reasonmap.iter().map(|e| e.0.length() + len::SHORT).sum::<u32>()
                + self.write_type.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_consistency(self.consistency())?;
        codec.write_int(self.received)?;
        codec.write_int(self.blockfor)?;

        codec.write_int(self.reasonmap.len() as Int)?;
        for e in &self.reasonmap {
            codec.write_inetaddr(&e.0)?;
            codec.write_short(e.1)?;
        }

        codec.write_string(&self.write_type)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        let cl = codec.read_consistency()?;
        let received = codec.read_int()?;
        let blockfor = codec.read_int()?;

        let mut reasonmap = Vec::new();
        let len = codec.read_int()?;
        for _ in 1..=len {
            reasonmap.push((codec.read_inetaddr()?, codec.read_short()?));
        }

        let write_type = codec.read_string()?;

        Ok(WriteFailure {
            cl,
            received,
            blockfor,
            reasonmap,
            write_type,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct AlreadyExists {
    ks: String,
    table: String,
}

impl AlreadyExists {
    pub fn new(ks: &str, table: &str) -> Self {
        AlreadyExists {
            ks: ks.to_string(),
            table: table.to_string(),
        }
    }

    pub fn keyspace(&self) -> &str {
        &self.ks
    }

    pub fn table(&self) -> &str {
        &self.table
    }
}

impl Serializable for AlreadyExists {
    fn length(&self) -> u32 {
        self.ks.length() + self.table.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_string(&self.ks)?;
        codec.write_string(&self.table)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok(AlreadyExists {
            ks: codec.read_string()?,
            table: codec.read_string()?,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct Unprepared {
    id: ShortBytes,
}

impl Unprepared {
    pub fn new(id: ShortBytes) -> Self {
        Unprepared {
            id,
        }
    }

    pub fn id(&self) -> &ShortBytes {
        &self.id
    }
}

impl Serializable for Unprepared {
    fn length(&self) -> u32 {
        self.id.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_short_bytes(&self.id)
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok(Unprepared {
            id: codec.read_short_bytes()?,
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum ExceptionKind {
    None,
    UnavailableException(UnavailableException),
    WriteTimeout(WriteTimeout),
    ReadTimeout(ReadTimeout),
    ReadFailure(ReadFailure),
    FunctionFailure(FunctionFailure),
    WriteFailure(WriteFailure),
    AlreadyExists(AlreadyExists),
    Unprepared(Unprepared),
}

macro_rules! match_error_code {
    ($sf:expr, $($M:ident),+) => (
        match $sf {
            $(ExceptionKind::$M(_) => ErrorCode::$M,)*
            Self::None => unreachable!("{:?}", $sf),
        }
    );
}

macro_rules! match_length {
    ($sf:expr, $($M:ident),+) => (
        match $sf {
            $(ExceptionKind::$M(ref e) => e.length(),)*
            Self::None => 0,
        }
    );
}

macro_rules! match_encode {
    ($sf:expr, $codec:expr, $($M:ident),+) => (
        match $sf {
            $(ExceptionKind::$M(ref e) => e.encode($codec),)*
            Self::None => OK,
        }
    );
}

macro_rules! match_decode {
    ($code:expr, $codec:expr, $($M:ident),+) => (
        match $code {
            $(ErrorCode::$M => ExceptionKind::$M($M::decode($codec)?),)*
            _ => ExceptionKind::None,
        }
    );
}

impl ExceptionKind {
    fn error_code(&self) -> ErrorCode {
        match_error_code!(self, UnavailableException, WriteTimeout, ReadTimeout, ReadFailure,
                          FunctionFailure, WriteFailure, AlreadyExists, Unprepared)
    }

    fn length(&self) -> u32 {
        match_length!(self, UnavailableException, WriteTimeout, ReadTimeout, ReadFailure,
                      FunctionFailure, WriteFailure, AlreadyExists, Unprepared)
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        match_encode!(self, codec, UnavailableException, WriteTimeout, ReadTimeout, ReadFailure,
                      FunctionFailure, WriteFailure, AlreadyExists, Unprepared)
    }

    fn decode<B: io::Read + io::Write>(code: ErrorCode, codec: &mut Codec<B>) -> ProtResult<Self> {
        let e = match_decode!(code, codec, UnavailableException, WriteTimeout, ReadTimeout, ReadFailure,
                              FunctionFailure, WriteFailure, AlreadyExists, Unprepared);
        Ok(e)
    }
}

impl_display!(ExceptionKind);
impl_display!(UnavailableException);
impl_display!(WriteTimeout);
impl_display!(ReadTimeout);
impl_display!(ReadFailure);
impl_display!(FunctionFailure);
impl_display!(WriteFailure);
impl_display!(AlreadyExists);
impl_display!(Unprepared);

#[derive(Debug, PartialEq)]
pub struct Error {
    code: ErrorCode,
    msg: String,
    e: ExceptionKind,
}

impl Error {
    pub fn from(code: ErrorCode, msg: &str) -> Self {
        Error {
            code,
            msg: msg.to_string(),
            e: ExceptionKind::None,
        }
    }

    pub fn from_exception(e: ExceptionKind, msg: &str) -> Error {
        assert_ne!(e, ExceptionKind::None);
        Error {
            code: e.error_code(),
            msg: msg.to_string(),
            e,
        }
    }

    pub fn set_error(&mut self, code: ErrorCode, msg: &str) {
        self.code = code;
        self.msg = msg.to_string();
    }

    pub fn set_exception(&mut self, e: ExceptionKind, msg: &str) {
        assert_ne!(e, ExceptionKind::None);
        self.code = e.error_code();
        self.msg = msg.to_string();
        self.e = e;
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn exception(&self) -> &ExceptionKind {
        &self.e
    }
}

impl Message for Error {
    fn new() -> Self {
        Error {
            code: ErrorCode::ServerError,
            msg: Default::default(),
            e: ExceptionKind::None,
        }
    }

    fn opcode(&self) -> Opcode { Opcode::Error }
}

impl Serializable for Error {
    fn length(&self) -> u32 {
        len::INT + self.msg.length() + self.e.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_int(self.code as Int)?;
        codec.write_string(&self.msg)?;
        self.e.encode(codec)?;
        OK
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        let code = FromPrimitive::from_i32(codec.read_int()?).unwrap();
        Ok(Error {
            code,
            msg: codec.read_string()?,
            e: ExceptionKind::decode(code, codec)?,
        })
    }
}
