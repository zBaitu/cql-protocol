use crate::result::*;
use crate::types::*;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use num_traits::FromPrimitive;

use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr}
};

pub struct Encoder<B: io::Write> {
    writer: B,
}

pub struct Decoder<B: io::Read> {
    reader: B,
}

pub struct Codec<B: io::Write + io::Read> {
    io: B,
}

impl<B: io::Write> Encoder<B> {
    pub fn new(writer: B) -> Encoder<B> {
        Encoder {
            writer,
        }
    }

    pub fn into_io(self) -> B {
        self.writer
    }
}

impl<B: io::Read> Decoder<B> {
    pub fn new(reader: B) -> Decoder<B> {
        Decoder {
            reader,
        }
    }
}

impl<B: io::Write + io::Read> Codec<B> {
    pub fn new(io: B) -> Codec<B> {
        Codec {
            io,
        }
    }

    pub fn io(&mut self) -> &mut B {
        &mut self.io
    }
}

pub trait Encodable<B: io::Write> {
    fn io(&mut self) -> &mut B;

    fn write_i16(&mut self, v: i16) -> ProtResult<()> {
        self.io().write_i16::<BigEndian>(v)?;
        OK
    }

    fn write_u32(&mut self, v: u32) -> ProtResult<()> {
        self.io().write_u32::<BigEndian>(v)?;
        OK
    }

    fn write_byte(&mut self, v: Byte) -> ProtResult<()> {
        self.io().write_u8(v)?;
        OK
    }

    fn write_short(&mut self, v: Short) -> ProtResult<()> {
        self.io().write_u16::<BigEndian>(v)?;
        OK
    }

    fn write_int(&mut self, v: Int) -> ProtResult<()> {
        self.io().write_i32::<BigEndian>(v)?;
        OK
    }

    fn write_long(&mut self, v: Long) -> ProtResult<()> {
        self.io().write_i64::<BigEndian>(v)?;
        OK
    }

    fn write_string(&mut self, v: &String) -> ProtResult<()> {
        self.write_short(v.len() as Short)?;
        self.write_raw_bytes(v.as_bytes())?;
        OK
    }

    fn write_long_string(&mut self, v: &LongString) -> ProtResult<()> {
        self.write_int(v.0.len() as Int)?;
        self.write_raw_bytes(v.0.as_bytes())?;
        OK
    }

    fn write_uuid(&mut self, v: &Uuid) -> ProtResult<()> {
        self.write_raw_bytes(v.as_bytes())
    }

    fn write_string_list(&mut self, v: &StringList) -> ProtResult<()> {
        self.write_short(v.len() as Short)?;
        for s in v {
            self.write_string(&s)?;
        }
        OK
    }

    fn write_bytes(&mut self, v: &Bytes) -> ProtResult<()> {
        match v {
            Some(ref v) => {
                self.write_int(v.len() as Int)?;
                self.write_raw_bytes(v)?;
            },
            None => self.write_int(-1)?,
        }
        OK
    }

    fn write_value(&mut self, v: &Value) -> ProtResult<()> {
        match v {
            Value::Some(ref v) => {
                self.write_int(v.len() as Int)?;
                self.write_raw_bytes(v)?;
            },
            Value::None => self.write_int(-1)?,
            Value::NotSet => self.write_int(-2)?,
        }
        OK
    }

    fn write_short_bytes(&mut self, v: &ShortBytes) -> ProtResult<()> {
        self.write_short(v.len() as Short)?;
        self.write_raw_bytes(v)?;
        OK
    }

    fn write_option(&mut self, v: &Opt) -> ProtResult<()> {
        self.write_short(v.id as Short)?;
        self.write_option_value(&v.value)?;
        OK
    }

    fn write_option_value(&mut self, v: &OptValue) -> ProtResult<()> {
        match v {
            OptValue::None => (),
            OptValue::Custom(ref s) => self.write_string(s)?,
            OptValue::List(ref t) => self.write_option(t)?,
            OptValue::Map(ref k, ref v) => {
                self.write_option(k)?;
                self.write_option(v)?;
            },
            OptValue::Set(ref t) => self.write_option(t)?,
            OptValue::Udt(ref u) => self.write_option_udt(u)?,
            OptValue::Tuple(ref t) => self.write_option_tuple(t)?,
        }
        OK
    }

    fn write_option_udt(&mut self, v: &OptUdt) -> ProtResult<()> {
        self.write_string(&v.ks)?;
        self.write_string(&v.name)?;
        self.write_short(v.fields.len() as Short)?;
        for f in &v.fields {
            self.write_string(&f.0)?;
            self.write_option(&f.1)?;
        }
        OK
    }

    fn write_option_tuple(&mut self, v: &Vec<Opt>) -> ProtResult<()> {
        self.write_short(v.len() as Short)?;
        for t in v {
            self.write_option(t)?;
        }
        OK
    }

    fn write_inet(&mut self, v: &Inet) -> ProtResult<()> {
        self.write_inetaddr(&v.ip())?;
        self.write_int(v.port() as Int)?;
        OK
    }

    fn write_inetaddr(&mut self, v: &InetAddr) -> ProtResult<()> {
        match v {
            IpAddr::V4(ref ip) => {
                self.write_byte(len::IPV4)?;
                self.write_raw_bytes(&ip.octets())?
            },
            IpAddr::V6(ref ip) => {
                self.write_byte(len::IPV6)?;
                self.write_raw_bytes(&ip.octets())?
            },
        }
        OK
    }

    fn write_consistency(&mut self, v: Consistency) -> ProtResult<()> {
        self.write_short(v as Short)
    }

    fn write_sting_map(&mut self, v: &StringMap) -> ProtResult<()> {
        self.write_short(v.len() as Short)?;
        for (k, v) in v {
            self.write_string(&k)?;
            self.write_string(&v)?;
        }
        OK
    }

    fn write_sting_multimap(&mut self, v: &StringMultimap) -> ProtResult<()> {
        self.write_short(v.len() as Short)?;
        for (k, v) in v {
            self.write_string(&k)?;
            self.write_string_list(&v)?;
        }
        OK
    }

    fn write_raw_bytes(&mut self, v: &[u8]) -> ProtResult<()> {
        self.io().write_all(v)?;
        OK
    }
}

impl<B: io::Write> Encodable<B> for Encoder<B> {
    fn io(&mut self) -> &mut B {
        &mut self.writer
    }
}

pub trait Decodable<B: io::Read> {
    fn io(&mut self) -> &mut B;

    fn read_i16(&mut self) -> ProtResult<i16> {
        Ok(self.io().read_i16::<BigEndian>()?)
    }

    fn read_u32(&mut self) -> ProtResult<u32> {
        Ok(self.io().read_u32::<BigEndian>()?)
    }

    fn read_byte(&mut self) -> ProtResult<Byte> {
        Ok(self.io().read_u8()?)
    }

    fn read_short(&mut self) -> ProtResult<Short> {
        Ok(self.io().read_u16::<BigEndian>()?)
    }

    fn read_int(&mut self) -> ProtResult<Int> {
        Ok(self.io().read_i32::<BigEndian>()?)
    }

    fn read_long(&mut self) -> ProtResult<Long> {
        Ok(self.io().read_i64::<BigEndian>()?)
    }

    fn read_string(&mut self) -> ProtResult<String> {
        let len = self.read_short()?;
        let bytes = self.read_raw_bytes(len as u32)?;
        Ok(String::from_utf8(bytes)?)
    }

    fn read_long_string(&mut self) -> ProtResult<LongString> {
        let len = self.read_int()?;
        let bytes = self.read_raw_bytes(len as u32)?;
        Ok(LongString::from_utf8(bytes)?)
    }

    fn read_uuid(&mut self) -> ProtResult<Uuid> {
        let bytes = self.read_raw_bytes(len::UUID)?;
        Ok(Uuid::from_slice(&bytes)?)
    }

    fn read_string_list(&mut self) -> ProtResult<StringList> {
        let len = self.read_short()?;
        let mut list = StringList::new();
        for _ in 1..=len {
            list.push(self.read_string()?);
        }
        Ok(list)
    }

    fn read_bytes(&mut self) -> ProtResult<Bytes> {
        let len = self.read_int()?;
        let bytes = if len >= 0 {
            Some(self.read_raw_bytes(len as u32)?)
        } else {
            None
        };
        Ok(bytes)
    }

    fn read_value(&mut self) -> ProtResult<Value> {
        let len = self.read_int()?;
        let value = if len >= 0 {
            Value::Some(self.read_raw_bytes(len as u32)?)
        } else if len == -1 {
            Value::None
        } else if len == -2 {
            Value::NotSet
        } else {
            unreachable!(len)
        };
        Ok(value)
    }

    fn read_short_bytes(&mut self) -> ProtResult<ShortBytes> {
        let len = self.read_short()?;
        Ok(self.read_raw_bytes(len as u32)?)
    }

    fn read_option(&mut self) -> ProtResult<Opt> {
        let id = FromPrimitive::from_u16(self.read_short()?).unwrap();
        let value = self.read_option_value(id)?;
        Ok(Opt {
            id,
            value,
        })
    }

    fn read_option_value(&mut self, id: OptIds) -> ProtResult<OptValue> {
        let value = match id {
            OptIds::Custom => OptValue::Custom(self.read_string()?),
            OptIds::List => OptValue::List(Box::new(self.read_option()?)),
            OptIds::Map => OptValue::Map(Box::new(self.read_option()?), Box::new(self.read_option()?)),
            OptIds::Set => OptValue::Set(Box::new(self.read_option()?)),
            OptIds::Udt => OptValue::Udt(self.read_option_udt()?),
            OptIds::Tuple => OptValue::Tuple(self.read_option_tuple()?),
            _ => OptValue::None,
        };
        Ok(value)
    }

    fn read_option_udt(&mut self) -> ProtResult<OptUdt> {
        let ks = self.read_string()?;
        let name = self.read_string()?;
        let len = self.read_short()?;
        let mut fields = Vec::with_capacity(len as usize);
        for _ in 1..=len {
            fields.push((self.read_string()?, self.read_option()?));
        }
        Ok(OptUdt {
            ks,
            name,
            fields,
        })
    }

    fn read_option_tuple(&mut self) -> ProtResult<Vec<Opt>> {
        let len = self.read_short()?;
        let mut v = Vec::with_capacity(len as usize);
        for _ in 1..=len {
            v.push(self.read_option()?);
        }
        Ok(v)
    }

    fn read_inet(&mut self) -> ProtResult<Inet> {
        let ip = self.read_inetaddr()?;
        let port = self.read_int()? as u16;
        Ok(Inet::new(ip, port))
    }

    fn read_inetaddr(&mut self) -> ProtResult<InetAddr> {
        let len = self.read_byte()?;
        match len {
            len::IPV4 => {
                let mut v = [0; len::IPV4 as usize];
                self.io().read_exact(&mut v)?;
                Ok(IpAddr::V4(Ipv4Addr::from(v)))
            },
            len::IPV6 => {
                let mut v = [0; len::IPV6 as usize];
                self.io().read_exact(&mut v)?;
                Ok(IpAddr::V6(Ipv6Addr::from(v)))
            },
            _ => unreachable!(len),
        }
    }

    fn read_consistency(&mut self) -> ProtResult<Consistency> {
        Ok(FromPrimitive::from_u16(self.read_short()?).unwrap())
    }

    fn read_string_map(&mut self) -> ProtResult<StringMap> {
        let len = self.read_short()?;
        let mut map = StringMap::new();
        for _ in 1..=len {
            map.insert(self.read_string()?, self.read_string()?);
        }
        Ok(map)
    }

    fn read_string_multimap(&mut self) -> ProtResult<StringMultimap> {
        let len = self.read_short()?;
        let mut map = StringMultimap::new();
        for _ in 1..=len {
            map.insert(self.read_string()?, self.read_string_list()?);
        }
        Ok(map)
    }

    fn read_raw_bytes(&mut self, len: u32) -> ProtResult<Vec<u8>> {
        let mut v = vec![0; len as usize];
        self.io().read_exact(&mut v)?;
        Ok(v)
    }
}

impl<B: io::Read> Decodable<B> for Decoder<B> {
    fn io(&mut self) -> &mut B {
        &mut self.reader
    }
}

impl<B: io::Write + io::Read> Encodable<B> for Codec<B> {
    fn io(&mut self) -> &mut B {
        &mut self.io
    }
}

impl<B: io::Write + io::Read> Decodable<B> for Codec<B> {
    fn io(&mut self) -> &mut B {
        &mut self.io
    }
}
