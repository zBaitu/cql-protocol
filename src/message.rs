use crate::codec::*;
use crate::def::*;
use crate::request::*;
use crate::response::*;
use crate::result::*;

use std::{fmt::{self, Debug, Display, Formatter}, io};

pub trait Serializable: Display {
    fn length(&self) -> u32;
    fn encode<B: io::Read + io::Write>(&self, _codec: &mut Codec<B>) -> ProtResult<()>;
    fn decode<B: io::Read + io::Write>(_codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized;
}

pub trait Message: Serializable {
    fn new() -> Self where Self: Sized {
        unimplemented!("Message::new()")
    }

    fn is_response(&self) -> bool { false }
    fn tracing(&self) -> bool { false }
    fn has_custom_payload(&self) -> bool { false }
    fn warning(&self) -> bool { false }
    fn opcode(&self) -> Opcode;
}

default impl<T: Response> Message for T {
    fn is_response(&self) -> bool { true }
}

#[derive(Debug)]
pub enum MessageKind {
    Startup(Startup),
    AuthResponse(AuthResponse),
    Options(Options),
    Query(Query),
    Prepare(Prepare),
    Execute(Execute),
    Batch(Batch),
    Register(Register),

    Error(Error),
    Ready(Ready),
    Authenticate(Authenticate),
    Supported(Supported),
    Result(Result),
    Event(Event),
    AuthChallenge(AuthChallenge),
    AuthSuccess(AuthSuccess),
}

macro_rules! impl_display {
    ($M:ident) => (
        impl Display for $M {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                Debug::fmt(self, f)
            }
        }
    );
}

impl_display!(Startup);
impl_display!(AuthResponse);
impl_display!(Options);
impl_display!(Query);
impl_display!(Prepare);
impl_display!(Execute);
impl_display!(Batch);
impl_display!(Register);

impl_display!(Error);
impl_display!(Ready);
impl_display!(Authenticate);
impl_display!(Supported);
impl_display!(Result);
impl_display!(Event);
impl_display!(AuthChallenge);
impl_display!(AuthSuccess);

macro_rules! impl_request {
    ($($M:ident),+) => (
        $(impl Request for $M {})*
    );
}

impl_request!(Startup, AuthResponse, Options, Query, Prepare, Execute, Batch, Register);

macro_rules! impl_response {
    ($($M:ident),+) => (
        $(impl Response for $M {})*
    );
}

impl_response!(Error, Ready, Authenticate, Supported, Result, Event, AuthChallenge, AuthSuccess);

macro_rules! empty_msg {
    ($M:ident) => (

#[derive(Debug, PartialEq)]
pub struct $M;

const INSTANCE: $M = $M {};

impl Message for $M {
    fn new() -> Self { INSTANCE }

    fn opcode(&self) -> Opcode { Opcode::$M }
}

impl Serializable for $M {
    fn length(&self) -> u32 { 0 }

    fn encode<B: io::Read + io::Write>(&self, _codec: &mut Codec<B>) -> ProtResult<()> { OK }

    fn decode<B: io::Read + io::Write>(_codec: &mut Codec<B>) -> ProtResult<Self> where Self: Sized {
        Ok(Message::new())
    }
}

    );
}

macro_rules! token_msg {
    ($M:ident) => (

#[derive(Debug, PartialEq)]
pub struct $M {
    token: Bytes,
}

impl $M {
    pub fn from(token: Vec<u8>) -> Self {
        $M {
            token: Some(token),
        }
    }

    pub fn set_token(&mut self, token: Vec<u8>) {
        self.token = Some(token);
    }

    pub fn token(&self) -> &Bytes {
        &self.token
    }
}

impl Message for $M {
    fn new() -> Self {
        $M {
            token: Default::default(),
        }
    }

    fn opcode(&self) -> Opcode { Opcode::$M }
}

impl Serializable for $M {
    fn length(&self) -> u32 {
        self.token.length()
    }

    fn encode<B: io::Read + io::Write>(&self, codec: &mut Codec<B>) -> ProtResult<()> {
        codec.write_bytes(&self.token)
    }

    fn decode<B: io::Read + io::Write>(codec: &mut Codec<B>) -> ProtResult<Self> {
        Ok($M {
            token: codec.read_bytes()?,
        })
    }
}

    );
}
