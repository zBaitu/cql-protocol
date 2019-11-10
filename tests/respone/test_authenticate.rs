use cql::codec::*;
use cql::message::*;
use cql::response::authenticate::Authenticate;

use std::io::Cursor;

#[test]
fn message() {
    let mut a = Authenticate::new();
    assert_eq!(a.is_response(), true);

    a.set_authenticator("AllowAllAuthenticator");
    println!("{}", a);
}

#[test]
fn eq() {
    let mut a = Authenticate::new();
    let b = Authenticate::new();
    assert_eq!(a, b);

    a.set_authenticator("AllowAllAuthenticator");
    assert_ne!(a, b);
}

#[test]
fn length() {
    let mut m = Authenticate::new();
    assert_eq!(m.length(), 2);

    m.set_authenticator("AllowAllAuthenticator");
    assert_eq!(m.length(), 23);
}

#[test]
fn serde() {
    let mut codec = Codec::new(Cursor::new(Vec::new()));

    let a = Authenticate::from("AllowAllAuthenticator");
    a.encode(&mut codec).unwrap();
    codec.io().set_position(0);
    let b = Authenticate::decode(&mut codec).unwrap();
    assert_eq!(a, b);
}
