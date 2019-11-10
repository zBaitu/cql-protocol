use crate::codec::*;
use crate::result::*;
use crate::vint;

use ascii::AsciiString;
use bigdecimal::BigDecimal;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use chrono::prelude::*;
use num::BigInt;
use time::Duration as StdDuration;

use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Debug, Display, Formatter},
    hash::{Hash, Hasher},
    io::{Cursor, Read},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    ops::Add,
    string::FromUtf8Error
};

pub mod len {
    pub const BYTE: u32 = 1;
    pub const SHORT: u32 = 2;
    pub const INT: u32 = 4;
    pub const LONG: u32 = 8;
    pub const UUID: u32 = 16;
    pub const IPV4: u8 = 4;
    pub const IPV6: u8 = 16;
}

pub type Byte = u8;
pub type Short = u16;
pub type Int = i32;
pub type Long = i64;
pub struct LongString(pub String);
pub type Uuid = uuid::Uuid;
pub type StringList = Vec<String>;
pub type Bytes = Option<Vec<u8>>;

#[derive(Debug, PartialEq)]
pub enum Value {
    Some(Vec<u8>),
    None,
    NotSet,
}

pub type ShortBytes = Vec<u8>;

#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq)]
pub enum OptIds {
    Custom = 0x0000,
    Ascii = 0x0001,
    Bigint = 0x0002,
    Blob = 0x0003,
    Boolean = 0x0004,
    Counter = 0x0005,
    Decimal = 0x0006,
    Double = 0x0007,
    Float = 0x0008,
    Int = 0x0009,
    Timestamp = 0x000B,
    Uuid = 0x000C,
    Varchar = 0x000D,
    Varint = 0x000E,
    Timeuuid = 0x000F,
    Inet = 0x0010,
    Date = 0x0011,
    Time = 0x0012,
    Smallint = 0x0013,
    Tinyint = 0x0014,
    Duration = 0x0015,
    List = 0x0020,
    Map = 0x0021,
    Set = 0x0022,
    Udt = 0x0030,
    Tuple = 0x0031,
}

#[derive(Debug, PartialEq)]
pub struct Opt {
    pub id: OptIds,
    pub value: OptValue,
}

impl Opt {
    pub fn new(id: OptIds) -> Opt {
        Opt {
            id,
            value: OptValue::None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum OptValue {
    None,
    Custom(String),
    List(Box<Opt>),
    Map(Box<Opt>, Box<Opt>),
    Set(Box<Opt>),
    Udt(OptUdt),
    Tuple(Vec<Opt>),
}

#[derive(Debug, Default, PartialEq)]
pub struct OptUdt {
    pub ks: String,
    pub name: String,
    pub fields: Vec<(String, Opt)>,
}

impl OptUdt {
    pub fn from_types(types: Vec<Opt>) -> OptUdt {
        OptUdt {
            fields: types.into_iter().map(|ty| (String::default(), ty)).collect(),
            ..Default::default()
        }
    }
}

pub type Inet = SocketAddr;
pub type InetAddr = IpAddr;

#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq)]
pub enum Consistency {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Serial = 0x0008,
    LocalSerial = 0x0009,
    LocalOne = 0x000A,
}

impl Default for Consistency {
    fn default() -> Self { Consistency::One }
}

pub type StringMap = HashMap<String, String>;
pub type StringMultimap = HashMap<String, StringList>;

pub trait Type {
    fn length(&self) -> u32;
}

impl<T: Type> Type for Option<T> {
    default fn length(&self) -> u32 {
        match self {
            Some(ref v) => v.length(),
            None => 0,
        }
    }
}

impl Type for String {
    fn length(&self) -> u32 {
        len::SHORT + self.len() as u32
    }
}

impl LongString {
    pub fn new(s: &str) -> Self {
        LongString(s.to_string())
    }

    pub fn from_utf8(vec: Vec<u8>) -> Result<LongString, FromUtf8Error> {
        Ok(LongString(String::from_utf8(vec)?))
    }
}

impl Type for LongString {
    fn length(&self) -> u32 {
        len::INT + self.0.len() as u32
    }
}

impl Debug for LongString {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Default for LongString {
    fn default() -> Self {
        LongString(String::default())
    }
}

impl PartialEq for LongString {
    fn eq(&self, other: &LongString) -> bool {
        self.0 == other.0
    }
}

impl Type for StringList {
    fn length(&self) -> u32 {
        self.iter().fold(len::SHORT, |len, e| len + e.length())
    }
}

impl Type for Bytes {
    default fn length(&self) -> u32 {
        let mut len = len::INT;
        if let Some(v) = self {
            len += v.len() as u32;
        }
        len
    }
}

impl Type for Value {
    fn length(&self) -> u32 {
        let mut len = len::INT;
        if let Value::Some(v) = self {
            len += v.len() as u32;
        }
        len
    }
}

impl Type for ShortBytes {
    fn length(&self) -> u32 {
        len::SHORT + self.len() as u32
    }
}

impl Type for Opt {
    fn length(&self) -> u32 {
        len::SHORT + self.value.length()
    }
}

impl Type for OptValue {
    fn length(&self) -> u32 {
        match self {
            Self::None => 0,
            Self::Custom(ref s) => s.length(),
            Self::List(ref t) => t.length(),
            Self::Map(ref k, ref v) => k.length() + v.length(),
            Self::Set(ref t) => t.length(),
            Self::Udt(ref u) => u.length(),
            Self::Tuple(ref t) => t.iter().fold(len::SHORT, |len, t| len + t.length()),
        }
    }
}

impl Type for OptUdt {
    fn length(&self) -> u32 {
        let len = self.ks.length() + self.name.length() + len::SHORT;
        self.fields.iter().fold(len, |len, f| len + f.0.length() + f.1.length())
    }
}

impl Type for Inet {
    fn length(&self) -> u32 {
        self.ip().length() + len::INT
    }
}

impl Type for InetAddr {
    fn length(&self) -> u32 {
        len::BYTE + match self {
            Self::V4(_) => len::IPV4,
            Self::V6(_) => len::IPV6,
        } as u32
    }
}

impl Type for StringMap {
    fn length(&self) -> u32 {
        self.iter().fold(len::SHORT, |len, (k, v)| len + k.length() + v.length())
    }
}

impl Type for StringMultimap {
    fn length(&self) -> u32 {
        self.iter().fold(len::SHORT, |len, (k, v)| len + k.length() + v.length())
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Duration {
    pub months: i32,
    pub days: i32,
    pub nanoseconds: i64,
}

impl Duration {
    pub fn new(months: i32, days: i32, nanoseconds: i64) -> Duration {
        Duration {
            months,
            days,
            nanoseconds,
        }
    }
}

impl Display for Duration {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "({}, {}, {})", self.months, self.days, self.nanoseconds)
    }
}

#[derive(PartialEq)]
pub enum DataTypes {
    Null,
    Ascii(AsciiString),
    Bigint(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(i64),
    Decimal(BigDecimal),
    Double(f64),
    Float(f32),
    Int(i32),
    Timestamp(DateTime<Utc>),
    Uuid(Uuid),
    Varchar(String),
    Varint(BigInt),
    Timeuuid(Uuid),
    Inet(IpAddr),
    Date(Date<Utc>),
    Time(NaiveTime),
    Smallint(i16),
    Tinyint(i8),
    Duration(Duration),
    List(Vec<DataTypes>),
    Map(HashMap<DataTypes, DataTypes>),
    Set(HashSet<DataTypes>),
    Udt(Vec<DataTypes>),
    Tuple(Vec<DataTypes>),
}

impl Hash for DataTypes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => Hash::hash(&Option::<DataTypes>::None, state),
            Self::Float(ref v) => {
                let v = marshal_float(v).unwrap().unwrap();
                Hash::hash(&v, state);
            },
            Self::Double(ref v) => {
                let v = marshal_double(v).unwrap().unwrap();
                Hash::hash(&v, state);
            },
            Self::Map(ref v) => {
                let v = marshal_map(v).unwrap().unwrap();
                Hash::hash(&v, state);
            },
            Self::Set(ref v) => {
                let v = marshal_set(v).unwrap().unwrap();
                Hash::hash(&v, state);
            },
            Self::Ascii(ref v) => Hash::hash(v, state),
            Self::Bigint(ref v) => Hash::hash(v, state),
            Self::Blob(ref v) => Hash::hash(v, state),
            Self::Boolean(ref v) => Hash::hash(v, state),
            Self::Counter(ref v) => Hash::hash(v, state),
            Self::Decimal(ref v) => Hash::hash(v, state),
            Self::Uuid(ref v) => Hash::hash(v, state),
            Self::Int(ref v) => Hash::hash(v, state),
            Self::Timestamp(ref v) => Hash::hash(v, state),
            Self::Varchar(ref v) => Hash::hash(v, state),
            Self::Varint(ref v) => Hash::hash(v, state),
            Self::Timeuuid(ref v) => Hash::hash(v, state),
            Self::Inet(ref v) => Hash::hash(v, state),
            Self::Date(ref v) => Hash::hash(v, state),
            Self::Time(ref v) => Hash::hash(v, state),
            Self::Smallint(ref v) => Hash::hash(v, state),
            Self::Tinyint(ref v) => Hash::hash(v, state),
            Self::Duration(ref v) => Hash::hash(v, state),
            Self::List(ref v) => Hash::hash(v, state),
            Self::Udt(ref v) => Hash::hash(v, state),
            Self::Tuple(ref v) => Hash::hash(v, state),
        }
    }
}

impl Eq for DataTypes {}

impl Debug for DataTypes {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Ascii(ref v) => write!(f, "{}", v),
            Self::Bigint(ref v) => write!(f, "{}", v),
            Self::Blob(ref v) => write!(f, "{:?}", v),
            Self::Boolean(ref v) => write!(f, "{}", v),
            Self::Counter(ref v) => write!(f, "{}", v),
            Self::Decimal(ref v) => write!(f, "{}", v),
            Self::Uuid(ref v) => write!(f, "{}", v),
            Self::Double(ref v) => write!(f, "{}", v),
            Self::Float(ref v) => write!(f, "{}", v),
            Self::Int(ref v) => write!(f, "{}", v),
            Self::Timestamp(ref v) => write!(f, "{}", v),
            Self::Varchar(ref v) => write!(f, "{}", v),
            Self::Varint(ref v) => write!(f, "{}", v),
            Self::Timeuuid(ref v) => write!(f, "{}", v),
            Self::Inet(ref v) => write!(f, "{}", v),
            Self::Date(ref v) => write!(f, "{}", v),
            Self::Time(ref v) => write!(f, "{}", v),
            Self::Smallint(ref v) => write!(f, "{}", v),
            Self::Tinyint(ref v) => write!(f, "{}", v),
            Self::Duration(ref v) => write!(f, "{}", v),
            Self::List(ref v) => write!(f, "{:?}", v),
            Self::Map(ref v) => write!(f, "{:?}", v),
            Self::Set(ref v) => write!(f, "{:?}", v),
            Self::Udt(ref v) => write!(f, "{:?}", v),
            Self::Tuple(ref v) => write!(f, "{:?}", v),
        }
    }
}

macro_rules! match_marshal {
    ($v:expr, $($data_type:ident, $ty:tt),+) => (
        match $v {
            $(DataTypes::$data_type(ref v) => {
                concat_idents!(marshal_, $ty)(v)
            })*
            _ => unreachable!("{:?}", $v),
        }
    );
}

pub fn marshal(v: &DataTypes) -> ProtResult<Bytes> {
    match_marshal!(v,
                   Ascii, ascii,
                   Bigint, bigint,
                   Blob, blob,
                   Boolean, boolean,
                   Counter, counter,
                   Decimal, decimal,
                   Double, double,
                   Float, float,
                   Int, int,
                   Timestamp, timestamp,
                   Uuid, uuid,
                   Varchar, varchar,
                   Varint, varint,
                   Timeuuid, timeuuid,
                   Inet, inet,
                   Date, date,
                   Time, time,
                   Smallint, smallint,
                   Tinyint, tinyint,
                   Duration, duration)
}

pub fn unmarshal(ty: &Opt, bytes: &Vec<u8>) -> ProtResult<DataTypes> {
    match ty.value {
        OptValue::None => unmarshal_simple(ty.id, bytes),
        _ => unmarshal_complex(ty, bytes),
    }
}

macro_rules! match_unmarshal_simple {
    ($id:expr, $bytes:expr, $($opt_id:ident, $ty:tt),+) => (
        match $id {
            $(OptIds::$opt_id => {
                let v = concat_idents!(unmarshal_, $ty)($bytes)?;
                Ok(DataTypes::$opt_id(v))
            })*
            _ => unreachable!("{:?}", $id),
        }
    );
}

pub fn unmarshal_simple(id: OptIds, bytes: &Vec<u8>) -> ProtResult<DataTypes> {
    match_unmarshal_simple!(id, bytes,
                            Ascii, ascii,
                            Bigint, bigint,
                            Blob, blob,
                            Boolean, boolean,
                            Counter, counter,
                            Decimal, decimal,
                            Double, double,
                            Float, float,
                            Int, int,
                            Timestamp, timestamp,
                            Uuid, uuid,
                            Varchar, varchar,
                            Varint, varint,
                            Timeuuid, timeuuid,
                            Inet, inet,
                            Date, date,
                            Time, time,
                            Smallint, smallint,
                            Tinyint, tinyint,
                            Duration, duration)
}

pub fn unmarshal_complex(ty: &Opt, bytes: &Vec<u8>) -> ProtResult<DataTypes> {
    match ty.id {
        OptIds::List => {
            if let OptValue::List(ref ty) = ty.value {
                let v = unmarshal_list(ty, bytes)?;
                Ok(DataTypes::List(v))
            } else {
                unreachable!("{:?}", ty.value)
            }
        },
        OptIds::Map => {
            if let OptValue::Map(ref k, ref v) = ty.value {
                let v = unmarshal_map(k, v, bytes)?;
                Ok(DataTypes::Map(v))
            } else {
                unreachable!("{:?}", ty.value)
            }
        },
        OptIds::Set => {
            if let OptValue::Set(ref ty) = ty.value {
                let v = unmarshal_set(ty, bytes)?;
                Ok(DataTypes::Set(v))
            } else {
                unreachable!("{:?}", ty.value)
            }
        },
        OptIds::Udt => {
            if let OptValue::Udt(ref ty) = ty.value {
                let v = unmarshal_udt(&ty.fields, bytes)?;
                Ok(DataTypes::Udt(v))
            } else {
                unreachable!("{:?}", ty.value)
            }
        },
        OptIds::Tuple => {
            if let OptValue::Tuple(ref types) = ty.value {
                let v = unmarshal_tuple(types, bytes)?;
                Ok(DataTypes::Tuple(v))
            } else {
                unreachable!("{:?}", ty.value)
            }
        },
        _ => unreachable!("{:?}", ty.id),
    }
}

pub fn marshal_ascii(v: &AsciiString) -> ProtResult<Bytes> {
    let bytes = v.as_bytes().to_vec();
    Ok(Some(bytes))
}

pub fn unmarshal_ascii(bytes: &Vec<u8>) -> ProtResult<AsciiString> {
    let v = AsciiString::from_ascii(bytes.clone())?;
    Ok(v)
}

pub fn marshal_bigint(v: &i64) -> ProtResult<Bytes> {
    let mut bytes = Vec::with_capacity(4);
    bytes.write_i64::<BigEndian>(*v)?;
    Ok(Some(bytes))
}

pub fn unmarshal_bigint(bytes: &Vec<u8>) -> ProtResult<i64> {
    let v = bytes.as_slice().read_i64::<BigEndian>()?;
    Ok(v)
}

pub fn marshal_blob(v: &Vec<u8>) -> ProtResult<Bytes> {
    Ok(Some(v.to_owned()))
}

pub fn unmarshal_blob(bytes: &Vec<u8>) -> ProtResult<Vec<u8>> {
    Ok(bytes.to_owned())
}

pub fn marshal_boolean(v: &bool) -> ProtResult<Bytes> {
    Ok(Some(vec![*v as u8]))
}

pub fn unmarshal_boolean(bytes: &Vec<u8>) -> ProtResult<bool> {
    Ok(bytes[0] != 0)
}

pub fn marshal_counter(v: &i64) -> ProtResult<Bytes> {
    let mut bytes = Vec::with_capacity(4);
    bytes.write_i64::<BigEndian>(*v)?;
    Ok(Some(bytes))
}

pub fn unmarshal_counter(bytes: &Vec<u8>) -> ProtResult<i64> {
    let v = bytes.as_slice().read_i64::<BigEndian>()?;
    Ok(v)
}

pub fn marshal_decimal(v: &BigDecimal) -> ProtResult<Bytes> {
    let (bigint, scale) = v.as_bigint_and_exponent();
    let mut unscale = bigint.to_signed_bytes_be();
    let mut bytes = Vec::with_capacity(4 + unscale.len());
    bytes.write_i32::<BigEndian>(scale as i32)?;
    bytes.append(&mut unscale);
    Ok(Some(bytes))
}

pub fn unmarshal_decimal(bytes: &Vec<u8>) -> ProtResult<BigDecimal> {
    let scale = bytes.as_slice().read_i32::<BigEndian>()?;
    let bigint = BigInt::from_signed_bytes_be(&bytes[4..]);
    Ok(BigDecimal::new(bigint, scale as i64))
}

pub fn marshal_double(v: &f64) -> ProtResult<Bytes> {
    let mut bytes = Vec::with_capacity(8);
    bytes.write_f64::<BigEndian>(*v)?;
    Ok(Some(bytes))
}

pub fn unmarshal_double(bytes: &Vec<u8>) -> ProtResult<f64> {
    let v = bytes.as_slice().read_f64::<BigEndian>()?;
    Ok(v)
}

pub fn marshal_float(v: &f32) -> ProtResult<Bytes> {
    let mut bytes = Vec::with_capacity(4);
    bytes.write_f32::<BigEndian>(*v)?;
    Ok(Some(bytes))
}

pub fn unmarshal_float(bytes: &Vec<u8>) -> ProtResult<f32> {
    let v = bytes.as_slice().read_f32::<BigEndian>()?;
    Ok(v)
}

pub fn marshal_int(v: &i32) -> ProtResult<Bytes> {
    let mut bytes = Vec::with_capacity(4);
    bytes.write_i32::<BigEndian>(*v)?;
    Ok(Some(bytes))
}

pub fn unmarshal_int(bytes: &Vec<u8>) -> ProtResult<i32> {
    let v = bytes.as_slice().read_i32::<BigEndian>()?;
    Ok(v)
}

pub fn marshal_timestamp(v: &DateTime<Utc>) -> ProtResult<Bytes> {
    let mut bytes = Vec::with_capacity(8);
    bytes.write_i64::<BigEndian>(v.timestamp_millis())?;
    Ok(Some(bytes))
}

pub fn unmarshal_timestamp(bytes: &Vec<u8>) -> ProtResult<DateTime<Utc>> {
    let v = bytes.as_slice().read_i64::<BigEndian>()?;
    Ok(Utc.timestamp_millis(v))
}

pub fn marshal_uuid(v: &Uuid) -> ProtResult<Bytes> {
    Ok(Some(v.as_bytes().to_vec()))
}

pub fn unmarshal_uuid(bytes: &Vec<u8>) -> ProtResult<Uuid> {
    let v = Uuid::from_slice(bytes.as_slice())?;
    Ok(v)
}

pub fn marshal_varchar(v: &str) -> ProtResult<Bytes> {
    Ok(Some(v.to_string().into_bytes()))
}

pub fn unmarshal_varchar(bytes: &Vec<u8>) -> ProtResult<String> {
    let v = String::from_utf8(bytes.to_vec())?;
    Ok(v)
}

pub fn marshal_varint(v: &BigInt) -> ProtResult<Bytes> {
    Ok(Some(v.to_signed_bytes_be()))
}

pub fn unmarshal_varint(bytes: &Vec<u8>) -> ProtResult<BigInt> {
    Ok(BigInt::from_signed_bytes_be(bytes))
}

pub fn marshal_timeuuid(v: &Uuid) -> ProtResult<Bytes> {
    Ok(Some(v.as_bytes().to_vec()))
}

pub fn unmarshal_timeuuid(bytes: &Vec<u8>) -> ProtResult<Uuid> {
    let v = Uuid::from_slice(bytes.as_slice())?;
    Ok(v)
}

pub fn marshal_inet(v: &IpAddr) -> ProtResult<Bytes> {
    let bytes = match v {
        IpAddr::V4(ref ip) => ip.octets().to_vec(),
        IpAddr::V6(ref ip) => ip.octets().to_vec(),
    };
    Ok(Some(bytes))
}

pub fn unmarshal_inet(bytes: &Vec<u8>) -> ProtResult<IpAddr> {
    let v = match bytes.len() as u8 {
        len::IPV4 => {
            let mut octets = [0; len::IPV4 as usize];
            bytes.as_slice().read_exact(&mut octets)?;
            IpAddr::V4(Ipv4Addr::from(octets))
        },
        len::IPV6 => {
            let mut octets = [0; len::IPV6 as usize];
            bytes.as_slice().read_exact(&mut octets)?;
            IpAddr::V6(Ipv6Addr::from(octets))
        },
        _ => unreachable!(bytes.len()),
    };
    Ok(v)
}

pub fn marshal_date(v: &Date<Utc>) -> ProtResult<Bytes> {
    let mut bytes = Vec::with_capacity(4);
    let days = v.signed_duration_since(Utc.ymd(1970, 1, 1)).num_days();
    bytes.write_u32::<BigEndian>(days as u32 + std::i32::MIN as u32)?;
    Ok(Some(bytes))
}

pub fn unmarshal_date(bytes: &Vec<u8>) -> ProtResult<Date<Utc>> {
    let days = bytes.as_slice().read_u32::<BigEndian>()? - std::i32::MIN as u32;
    Ok(Utc.ymd(1970, 1, 1).add(StdDuration::days(days as i64)))
}

pub fn marshal_time(v: &NaiveTime) -> ProtResult<Bytes> {
    let nanoseconds = v.num_seconds_from_midnight() as i64 * 1_000_000_000 + v.nanosecond() as i64;
    let mut bytes = Vec::with_capacity(4);
    bytes.write_i64::<BigEndian>(nanoseconds)?;
    Ok(Some(bytes))
}

pub fn unmarshal_time(bytes: &Vec<u8>) -> ProtResult<NaiveTime> {
    let nanoseconds = bytes.as_slice().read_i64::<BigEndian>()?;
    let nano = nanoseconds % 1_000_000_000;
    let seconds = (nanoseconds - nano) / 1_000_000_000;
    Ok(NaiveTime::from_num_seconds_from_midnight(seconds as u32, nano as u32))
}

pub fn marshal_smallint(v: &i16) -> ProtResult<Bytes> {
    let mut bytes = Vec::with_capacity(2);
    bytes.write_i16::<BigEndian>(*v)?;
    Ok(Some(bytes))
}

pub fn unmarshal_smallint(bytes: &Vec<u8>) -> ProtResult<i16> {
    let v = bytes.as_slice().read_i16::<BigEndian>()?;
    Ok(v)
}

pub fn marshal_tinyint(v: &i8) -> ProtResult<Bytes> {
    let mut bytes = Vec::with_capacity(1);
    bytes.write_i8(*v)?;
    Ok(Some(bytes))
}

pub fn unmarshal_tinyint(bytes: &Vec<u8>) -> ProtResult<i8> {
    let v = bytes.as_slice().read_i8()?;
    Ok(v)
}

pub fn marshal_duration(v: &Duration) -> ProtResult<Bytes> {
    let len = vint::length(v.months as u64) + vint::length(v.days as u64) + vint::length(v.nanoseconds as u64);
    let mut bytes = Vec::with_capacity(len);
    bytes.resize(len, 0);

    let a = vint::encode_i32(v.months, &mut bytes);
    let b = vint::encode_i32(v.days, &mut bytes[a..]);
    vint::encode_i64(v.nanoseconds, &mut bytes[a + b..]);
    Ok(Some(bytes))
}

pub fn unmarshal_duration(bytes: &Vec<u8>) -> ProtResult<Duration> {
    let (months, a) = vint::decode_i32(bytes);
    let (days, b) = vint::decode_i32(&bytes[a..]);
    let (nanoseconds, _) = vint::decode_i64(&bytes[a + b..]);
    Ok(Duration::new(months, days, nanoseconds))
}

pub fn marshal_list(v: &Vec<DataTypes>) -> ProtResult<Bytes> {
    let mut encoder = Encoder::new(Cursor::new(Vec::new()));
    encoder.write_int(v.len() as Int)?;
    for e in v {
        encoder.write_bytes(&marshal(&e)?)?;
    }
    Ok(Some(encoder.into_io().into_inner()))
}

pub fn unmarshal_list(ty: &Opt, bytes: &Vec<u8>) -> ProtResult<Vec<DataTypes>> {
    let mut decoder = Decoder::new(Cursor::new(bytes));
    let len = decoder.read_int()?;

    let mut v = Vec::new();
    for _ in 1..=len {
        let bytes = decoder.read_bytes()?;
        let e = match bytes {
            Some(ref bytes) => unmarshal(ty, bytes)?,
            None => DataTypes::Null,
        };
        v.push(e);
    }
    Ok(v)
}

pub fn marshal_map(v: &HashMap<DataTypes, DataTypes>) -> ProtResult<Bytes> {
    let mut encoder = Encoder::new(Cursor::new(Vec::new()));
    encoder.write_int(v.len() as Int)?;
    for (k, v) in v {
        encoder.write_bytes(&marshal(&k)?)?;
        encoder.write_bytes(&marshal(&v)?)?;
    }
    Ok(Some(encoder.into_io().into_inner()))
}

pub fn unmarshal_map(key_type: &Opt, value_type: &Opt, bytes: &Vec<u8>) -> ProtResult<HashMap<DataTypes, DataTypes>> {
    let mut decoder = Decoder::new(Cursor::new(bytes));
    let len = decoder.read_int()?;

    let mut map = HashMap::new();
    for _ in 1..=len {
        let bytes = decoder.read_bytes()?;
        let k = match bytes {
            Some(ref bytes) => unmarshal(key_type, bytes)?,
            None => DataTypes::Null,
        };

        let bytes = decoder.read_bytes()?;
        let v = match bytes {
            Some(ref bytes) => unmarshal(value_type, bytes)?,
            None => DataTypes::Null,
        };

        map.insert(k, v);
    }
    Ok(map)
}

pub fn marshal_set(v: &HashSet<DataTypes>) -> ProtResult<Bytes> {
    let mut encoder = Encoder::new(Cursor::new(Vec::new()));
    encoder.write_int(v.len() as Int)?;
    for e in v {
        encoder.write_bytes(&marshal(&e)?)?;
    }
    Ok(Some(encoder.into_io().into_inner()))
}

pub fn unmarshal_set(ty: &Opt, bytes: &Vec<u8>) -> ProtResult<HashSet<DataTypes>> {
    let mut decoder = Decoder::new(Cursor::new(bytes));
    let len = decoder.read_int()?;

    let mut v = HashSet::new();
    for _ in 1..=len {
        let bytes = decoder.read_bytes()?;
        let e = match bytes {
            Some(ref bytes) => unmarshal(ty, bytes)?,
            None => DataTypes::Null,
        };
        v.insert(e);
    }
    Ok(v)
}

pub fn marshal_udt(v: &Vec<DataTypes>) -> ProtResult<Bytes> {
    let mut encoder = Encoder::new(Cursor::new(Vec::new()));
    for e in v {
        encoder.write_bytes(&marshal(&e)?)?;
    }
    Ok(Some(encoder.into_io().into_inner()))
}

pub fn unmarshal_udt(types: &Vec<(String, Opt)>, bytes: &Vec<u8>) -> ProtResult<Vec<DataTypes>> {
    let mut decoder = Decoder::new(Cursor::new(bytes));
    let mut v = Vec::new();
    for ty in types {
        let bytes = decoder.read_bytes()?;
        let e = match bytes {
            Some(ref bytes) => unmarshal(&ty.1, bytes)?,
            None => DataTypes::Null,
        };
        v.push(e);
    }
    Ok(v)
}

pub fn marshal_tuple(v: &Vec<DataTypes>) -> ProtResult<Bytes> {
    let mut encoder = Encoder::new(Cursor::new(Vec::new()));
    for e in v {
        encoder.write_bytes(&marshal(&e)?)?;
    }
    Ok(Some(encoder.into_io().into_inner()))
}

pub fn unmarshal_tuple(types: &Vec<Opt>, bytes: &Vec<u8>) -> ProtResult<Vec<DataTypes>> {
    let mut decoder = Decoder::new(Cursor::new(bytes));
    let mut v = Vec::new();
    for ty in types {
        let bytes = decoder.read_bytes()?;
        let e = match bytes {
            Some(ref bytes) => unmarshal(&ty, bytes)?,
            None => DataTypes::Null,
        };
        v.push(e);
    }
    Ok(v)
}

