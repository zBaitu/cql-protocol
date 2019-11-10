#![feature(concat_idents)]

#[macro_use]
extern crate maplit;

use cql::types::*;

use ascii::AsciiString;
use chrono::prelude::*;
use num::BigInt;
use uuid::{Uuid, v1::Timestamp};

use bigdecimal::BigDecimal;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

#[test]
fn string() {
    let t = "".to_string();
    assert_eq!(t.length(), 2);

    let t = "a".to_string();
    assert_eq!(t.length(), 3);

    let t = "白兔".to_string();
    assert_eq!(t.length(), 8);
}

#[test]
fn long_string() {
    let t = LongString::new("a");
    assert_eq!(t.length(), 5);

    let t = LongString::new("白兔");
    assert_eq!(t.length(), 10);
}

#[test]
fn string_list() {
    let t = vec!["a".to_string(), "b".to_string()];
    assert_eq!(t.length(), 8);
}

#[test]
fn inet() {
    let t = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 80);
    assert_eq!(t.length(), 9);

    let t = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 80);
    assert_eq!(t.length(), 21);
}

#[test]
fn string_map() {
    let t = hashmap!{"a".to_string() => "b".to_string()};
    assert_eq!(t.length(), 8);
}

#[test]
fn string_multimap() {
    let t = hashmap!{"a".to_string() => vec!["b".to_string(), "c".to_string()]};
    assert_eq!(t.length(), 13);
}

#[test]
fn option() {
    let t = Some("a".to_string());
    assert_eq!(t.length(), 3);

    let t: Option<String> = None;
    assert_eq!(t.length(), 0);
}

macro_rules! test_marshal {
    ($a:expr, $opt_id:ident, $ty:tt) => (
        let v = concat_idents!(marshal_, $ty)(&$a).unwrap();
        if let DataTypes::$opt_id(b) = unmarshal_simple(OptIds::$opt_id, v.as_ref().unwrap()).unwrap() {
            assert_eq!($a, b);
        } else {
            unreachable!("{:?}", OptIds::$opt_id);
        }
    );

    ($a:expr, $opt_id:ident, $data_type:ident, $ty:tt) => (
        let v = concat_idents!(marshal_, $ty)(&$a).unwrap();
        let ty = Opt {
            id: OptIds::$opt_id,
            value: OptValue::$opt_id(Box::new(Opt::new(OptIds::$data_type))),
        };
        if let DataTypes::$opt_id(b) = unmarshal_complex(&ty, v.as_ref().unwrap()).unwrap() {
            assert_eq!($a, b);
        } else {
            unreachable!("{:?}", ty);
        }
    );

    ($a:expr, $opt_id:ident, $key_type:ident, $value_type:ident, $ty:tt) => (
        let v = concat_idents!(marshal_, $ty)(&$a).unwrap();
        let ty = Opt {
            id: OptIds::$opt_id,
            value: OptValue::$opt_id(Box::new(Opt::new(OptIds::$key_type)), Box::new(Opt::new(OptIds::$value_type))),
        };
        if let DataTypes::$opt_id(b) = unmarshal_complex(&ty, v.as_ref().unwrap()).unwrap() {
            assert_eq!($a, b);
        } else {
            unreachable!("{:?}", ty);
        }
    );
}

mod data_types {
    use super::*;

    #[test]
    fn ascii() {
        let a = AsciiString::from_ascii("abcdefg").unwrap();
        test_marshal!(a, Ascii, ascii);
    }

    #[test]
    fn bigint() {
        let a = std::i64::MAX;
        test_marshal!(a, Bigint, bigint);
    }

    #[test]
    fn blob() {
        let a = vec![0, 1, 2, 3];
        test_marshal!(a, Blob, blob);
    }

    #[test]
    fn boolean() {
        let a = true;
        test_marshal!(a, Boolean, boolean);
        let a = false;
        test_marshal!(a, Boolean, boolean);
    }

    #[test]
    fn counter() {
        let a = std::i64::MIN;
        test_marshal!(a, Counter, counter);
    }

    #[test]
    fn decimal() {
        let a = BigDecimal::parse_bytes(b"123456789.987654321", 10).unwrap();
        test_marshal!(a, Decimal, decimal);
    }

    #[test]
    fn double() {
        let a = std::f64::MAX;
        test_marshal!(a, Double, double);
    }

    #[test]
    fn float() {
        let a = std::f32::MIN;
        test_marshal!(a, Float, float);
    }

    #[test]
    fn int() {
        let a = std::i32::MAX;
        test_marshal!(a, Int, int);
    }

    #[test]
    fn timestamp() {
        let a = Utc.timestamp_millis(Utc::now().timestamp_millis());
        test_marshal!(a, Timestamp, timestamp);
    }

    #[test]
    fn uuid() {
        let a = Uuid::new_v4();
        test_marshal!(a, Uuid, uuid);
    }

    #[test]
    fn varchar() {
        let a = "abcdefg".to_string();
        test_marshal!(a, Varchar, varchar);
    }

    #[test]
    fn varint() {
        let a = BigInt::parse_bytes(b"123456789987654321123456789987654321", 10).unwrap();
        test_marshal!(a, Varint, varint);
    }

    #[test]
    fn timeuuid() {
        let a = Uuid::new_v1(Timestamp::from_rfc4122(1497624119, 0), &[1, 2, 3, 4, 5, 6]).unwrap();
        test_marshal!(a, Timeuuid, timeuuid);
    }

    #[test]
    fn inet() {
        let a = IpAddr::V4(Ipv4Addr::LOCALHOST);
        test_marshal!(a, Inet, inet);

        let a = IpAddr::V6(Ipv6Addr::LOCALHOST);
        test_marshal!(a, Inet, inet);
    }

    #[test]
    fn date() {
        let a = Utc::today();
        test_marshal!(a, Date, date);
    }

    #[test]
    fn time() {
        let a = Utc::now().time();
        test_marshal!(a, Time, time);
    }

    #[test]
    fn smallint() {
        let a = std::i16::MAX;
        test_marshal!(a, Smallint, smallint);
    }

    #[test]
    fn tinyint() {
        let a = std::i8::MIN;
        test_marshal!(a, Tinyint, tinyint);
    }

    #[test]
    fn duration() {
        let a = Duration::new(1, 2, 1_000_000_000);
        test_marshal!(a, Duration, duration);
    }

    #[test]
    fn list() {
        let a = vec![DataTypes::Int(0), DataTypes::Int(1), DataTypes::Int(2)];
        test_marshal!(a, List, Int, list);
    }

    #[test]
    fn map() {
        let a = hashmap!{DataTypes::Varchar("a".to_string()) => DataTypes::Int(1),
                         DataTypes::Varchar("b".to_string()) => DataTypes::Int(2)};
        test_marshal!(a, Map, Varchar, Int, map);
    }

    #[test]
    fn set() {
        let a = hashset![DataTypes::Float(0.1), DataTypes::Float(1.2), DataTypes::Float(2.3)];
        test_marshal!(a, Set, Float, set);

        let a = hashset![DataTypes::Double(0.1), DataTypes::Double(1.2), DataTypes::Double(2.3)];
        test_marshal!(a, Set, Double, set);
    }

    #[test]
    fn udt() {
        let a = vec![DataTypes::Boolean(true), DataTypes::Double(0.1), DataTypes::Varchar("a".to_string())];
        let v = marshal_udt(&a).unwrap();
        let ty = Opt {
            id: OptIds::Udt,
            value: OptValue::Udt(OptUdt::from_types(vec![Opt::new(OptIds::Boolean),
                    Opt::new(OptIds::Double),
                    Opt::new(OptIds::Varchar)])),
        };
        if let DataTypes::Udt(b) = unmarshal_complex(&ty, v.as_ref().unwrap()).unwrap() {
            assert_eq!(a, b);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn tuple() {
        let a = vec![DataTypes::Boolean(true), DataTypes::Double(0.1), DataTypes::Varchar("a".to_string())];
        let v = marshal_tuple(&a).unwrap();
        let ty = Opt {
            id: OptIds::Tuple,
            value: OptValue::Tuple(vec![Opt::new(OptIds::Boolean),
                                        Opt::new(OptIds::Double),
                                        Opt::new(OptIds::Varchar)]),
        };
        if let DataTypes::Tuple(b) = unmarshal_complex(&ty, v.as_ref().unwrap()).unwrap() {
            assert_eq!(a, b);
        } else {
            unreachable!();
        }
    }
}

