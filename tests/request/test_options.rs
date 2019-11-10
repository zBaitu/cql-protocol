use cql::codec::*;
use cql::message::*;
use cql::request::options::Options;

use std::io::Cursor;

#[test]
fn message() {
    let a = Options::new();
    assert_eq!(a.is_response(), false);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = Options::new();
    a.encode(&mut codec).unwrap();
    assert_eq!(codec.io().get_ref().len(), 0);

    let b = Options::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}
