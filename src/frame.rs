use crate::codec::*;
use crate::compression::*;
use crate::def::*;
use crate::message::*;
use crate::request::*;
use crate::response::*;
use crate::result::*;
use crate::types::*;

use num_traits::FromPrimitive;

use std::io::{self, Cursor};

const VERSION_MASK: Byte = 0x80;

macro_rules! match_decode {
    ($codec:expr, $header:expr, $($M:ident),+) => (
        match $header.opcode {
            $(Opcode::$M => MessageKind::$M($M::decode(&mut $codec)?),)*
        }
    );
}

macro_rules! decode_msg {
    ($codec:expr, $header:expr) => (
        match_decode!($codec, $header,
                      Startup, AuthResponse, Options, Query, Prepare, Execute, Batch, Register,
                      Error, Ready, Authenticate, Supported, Result, Event, AuthChallenge, AuthSuccess);
    );
}

fn decode_msg<B: io::Read + io::Write>(mut codec: &mut Codec<B>, header: &Header) -> ProtResult<MessageKind> {
    let tracing_id = if Flags::Tracing.is_set(header.flags) {
        Some(codec.read_uuid()?)
    } else {
        None
    };

    if Flags::Warning.is_set(header.flags) {
        codec.read_string_list()?;
    }

    let mut m = decode_msg!(&mut codec, header);
    if let MessageKind::Result(ref mut result) = m {
        result.set_tracing_id(tracing_id);
    }
    Ok(m)
}

pub struct Frame<B: io::Read + io::Write> {
    req_version: Byte,
    rsp_version: Byte,
    flags: Byte,
    stream_id: i16,
    codec: Codec<B>,
    compressor: Option<Box<dyn Compressor>>,
}

struct Header {
    _version: Version,
    flags: Byte,
    stream_id: i16,
    opcode: Opcode,
}

impl<B> Frame<B> where B: io::Read + io::Write {
    pub fn new(version: Version, stream_id: i16, io: B, compressor: Option<Box<dyn Compressor>>) -> Frame<B> {
        let v = version as Byte;

        let mut flags = 0;
        if compressor.is_some() {
            flags |= Flags::Compression;
        }
        if version.is_beta() {
            flags |= Flags::Beta;
        }

        Frame {
            req_version: v,
            rsp_version: v | VERSION_MASK,
            flags,
            stream_id,
            codec: Codec::new(io),
            compressor,
        }
    }

    pub fn encode<M: Message>(&mut self, m: &M) -> ProtResult<()> {
        self.encode_header(m)?;
        if self.compressor.is_some() && m.opcode() != Opcode::Startup {
            self.compress(m)
        } else {
            self.encode_length(m)?;
            self.encode_body(m)
        }
    }

    pub fn decode(&mut self) -> ProtResult<(i16, MessageKind)> {
        let header = self.decode_header()?;
        let len = self.decode_length()?;
        let m = if self.compressor.is_some() && Flags::Compression.is_set(header.flags) {
            self.decompress(&header, len)?
        } else {
            self.decode_body(&header)?
        };
        Ok((header.stream_id, m))
    }

    pub fn io_mut(&mut self) -> &mut B {
        self.codec.io()
    }

    fn encode_header<M: Message>(&mut self, m: &M) -> ProtResult<()> {
        self.encode_version(m)?;
        self.encode_flags(m)?;
        self.encode_stream_id()?;
        self.encode_opcode(m)
    }

    fn encode_version<M: Message>(&mut self, m: &M) -> ProtResult<()> {
        let version = if m.is_response() {
            self.rsp_version
        } else {
            self.req_version
        };
        self.codec.write_byte(version)
    }

    fn encode_flags<M: Message>(&mut self, m: &M) -> ProtResult<()> {
        let mut flags = self.flags;
        if let Opcode::Startup = m.opcode() {
            flags &= !Flags::Compression;
        }

        if m.tracing() {
            flags |= Flags::Tracing;
        }
        if m.has_custom_payload() {
            flags |= Flags::CustomPayload;
        }
        if m.warning() {
            flags |= Flags::Warning;
        }

        self.codec.write_byte(flags)
    }

    fn encode_stream_id(&mut self) -> ProtResult<()> {
        self.codec.write_i16(self.stream_id)
    }

    fn encode_opcode<M: Message>(&mut self, m: &M) -> ProtResult<()> {
        self.codec.write_byte(m.opcode() as Byte)
    }

    fn encode_length<M: Message>(&mut self, m: &M) -> ProtResult<()> {
        self.codec.write_u32(m.length())
    }

    fn encode_body<M: Message>(&mut self, m: &M) -> ProtResult<()> {
        m.encode(&mut self.codec)
    }

    fn compress<M: Message>(&mut self, m: &M) -> ProtResult<()> {
        let mut codec = Codec::new(Cursor::new(Vec::new()));
        m.encode(&mut codec)?;

        let v = self.compressor.as_mut().unwrap().compress(codec.io().get_ref())?;
        self.codec.write_u32(v.len() as u32)?;
        self.codec.write_raw_bytes(v.as_slice())
    }

    fn decode_header(&mut self) -> ProtResult<Header> {
        Ok(Header {
            _version: self.decode_version()?,
            flags: self.decode_flags()?,
            stream_id: self.decode_stream_id()?,
            opcode: self.decode_opcode()?,
        })
    }

    fn decode_version(&mut self) -> ProtResult<Version> {
        let v: Byte = self.codec.read_byte()?;
        let version: Version = FromPrimitive::from_u8(v & !VERSION_MASK).unwrap();
        Ok(version)
    }

    fn decode_flags(&mut self) -> ProtResult<Byte> {
        self.codec.read_byte()
    }

    fn decode_stream_id(&mut self) -> ProtResult<i16> {
        self.codec.read_i16()
    }

    fn decode_opcode(&mut self) -> ProtResult<Opcode> {
        let v: Byte = self.codec.read_byte()?;
        let opcode: Opcode = FromPrimitive::from_u8(v).unwrap();
        Ok(opcode)
    }

    fn decode_length(&mut self) -> ProtResult<u32> {
        self.codec.read_u32()
    }

    fn decode_body(&mut self, header: &Header) -> ProtResult<MessageKind> {
        decode_msg(&mut self.codec, header)
    }

    fn decompress(&mut self, header: &Header, len: u32) -> ProtResult<MessageKind> {
        let v = self.codec.read_raw_bytes(len)?;
        let v = self.compressor.as_mut().unwrap().decompress(v.as_slice())?;

        let mut codec = Codec::new(Cursor::new(v));
        decode_msg(&mut codec, header)
    }
}

