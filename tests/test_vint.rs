#![feature(concat_idents)]

use cql::vint::{self, *};

#[test]
fn length() {
    assert_eq!(vint::length(0), 1);
    assert_eq!(vint::length(256000), 3);
    assert_eq!(vint::length(1000000000), 5);
    assert_eq!(vint::length(std::u64::MAX), 9);
}

#[test]
fn zig_zag() {
    fn zig_zag_pair_32(a: i32, b: u32) {
        assert_eq!(b, vint::encode_zig_zag_32(a));
        assert_eq!(a, vint::decode_zig_zag_32(b));
    }

    fn zig_zag_pair_64(a: i64, b: u64) {
        assert_eq!(b, vint::encode_zig_zag_64(a));
        assert_eq!(a, vint::decode_zig_zag_64(b));
    }

    zig_zag_pair_32(0, 0);
    zig_zag_pair_32(-1, 1);
    zig_zag_pair_32(1, 2);
    zig_zag_pair_32(-2, 3);
    zig_zag_pair_32(2, 4);
    zig_zag_pair_32(-3, 5);
    zig_zag_pair_32(3, 6);
    zig_zag_pair_32(2147483647, 4294967294);
    zig_zag_pair_32(-2147483648, 4294967295);

    zig_zag_pair_64(9223372036854775807, 18446744073709551614);
    zig_zag_pair_64(-9223372036854775808, 18446744073709551615);
}

macro_rules! serde {
    ($T:tt) => (
        fn serde_pair(a: $T, v: Vec<u8>) {
            assert_eq!(v, concat_idents!(encode_, $T, _vec)(a));
            let (b, len) = concat_idents!(decode_, $T)(&v);
            assert_eq!(a, b);
            assert_eq!(v.len() as usize, len);
        }
    );
}

#[test]
fn serde_i32() {
    serde!(i32);

    serde_pair(128000, vec![0b11000011, 0b11101000, 0b00000000]);
}

#[test]
fn serde_u32() {
    serde!(u32);

    serde_pair(256000, vec![0b11000011, 0b11101000, 0b00000000]);
}

#[test]
fn serde_i64() {
    serde!(i64);

    serde_pair(3600 * 1000000000, vec![252, 6, 140, 97, 113, 64, 0]);
}

#[test]
fn serde_u64() {
    serde!(u64);

    serde_pair(7200 * 1000000000, vec![252, 6, 140, 97, 113, 64, 0]);
}
