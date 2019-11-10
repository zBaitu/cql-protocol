use cql::codec::*;
use cql::def::*;
use cql::message::*;
use cql::request::batch::*;
use cql::types::*;

use std::io::Cursor;

fn default() -> Batch {
    Batch::default()
}

#[test]
fn message() {
    let a = default();
    assert_eq!(a.is_response(), false);

    println!("{}", a);
}

#[test]
fn eq() {
    let a = default();
    let b = default();
    assert_eq!(a, b);

    let b = Batch::from(BatchType::Counter, Vec::new());
    assert_ne!(a, b);
}

#[test]
fn length() {
    let m = default();
    assert_eq!(m.length(), 9);

    let m = Batch::from(BatchType::Logged,
                        vec![BatchQuery::from(BatchQueryKind::Query(LongString::new("a")), Vec::new())]);
    assert_eq!(m.length(), 17);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = default();
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Batch::decode(&mut codec).unwrap();
    println!("{}", b);
    assert_eq!(a, b);

    codec.io().set_position(0);
}
