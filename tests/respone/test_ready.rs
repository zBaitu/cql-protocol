use cql::codec::*;
use cql::message::*;
use cql::response::ready::Ready;

use std::io::Cursor;

#[test]
fn message() {
    let a = Ready::new();
    assert_eq!(a.is_response(), true);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = Ready::new();
    a.encode(&mut codec).unwrap();
    assert_eq!(codec.io().get_ref().len(), 0);

    let b = Ready::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}
