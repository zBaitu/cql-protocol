pub fn length(v: u64) -> usize {
    let mut len = 9 - (v.leading_zeros() as i32 - 1) / 7;
    if len == 0 {
        len = 1;
    }
    len as usize
}

pub fn encode_zig_zag_32(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

pub fn decode_zig_zag_32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

pub fn decode_zig_zag_64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

pub fn encode_zig_zag_64(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

pub fn encode_i32_vec(v: i32) -> Vec<u8> {
    encode_u64_vec(encode_zig_zag_32(v) as u64)
}

pub fn encode_i32(v: i32, bytes: &mut [u8]) -> usize {
    encode_u64(encode_zig_zag_32(v) as u64, bytes)
}

pub fn decode_i32(bytes: &[u8]) -> (i32, usize) {
    let (v, len) = decode(bytes);
    (decode_zig_zag_32(v as u32), len)
}

pub fn encode_u32_vec(v: u32) -> Vec<u8> {
    encode_u64_vec(v as u64)
}

pub fn encode_u32(v: u32, bytes: &mut [u8]) -> usize {
    encode_u64(v as u64, bytes)
}

pub fn decode_u32(bytes: &[u8]) -> (u32, usize) {
    let (v, len) = decode(bytes);
    (v as u32, len)
}

pub fn encode_i64_vec(v: i64) -> Vec<u8> {
    encode_u64_vec(encode_zig_zag_64(v))
}

pub fn encode_i64(v: i64, bytes: &mut [u8]) -> usize {
    encode_u64(encode_zig_zag_64(v), bytes)
}

pub fn decode_i64(bytes: &[u8]) -> (i64, usize) {
    let (v, len) = decode(bytes);
    (decode_zig_zag_64(v), len)
}

pub fn encode_u64_vec(v: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.resize(length(v), 0);
    encode_u64(v, &mut bytes);
    bytes
}

pub fn encode_u64(v: u64, bytes: &mut [u8]) -> usize {
    let mut v = v;
    let len = length(v);
    for i in (0..len).rev() {
        bytes[i] = v as u8;
        v >>= 8;
    }
    bytes[0] |= msb(len - 1);
    len
}

pub fn decode_u64(bytes: &[u8]) -> (u64, usize) {
    decode(bytes)
}

fn decode(bytes: &[u8]) -> (u64, usize) {
    let first = bytes[0];
    if first as i8 > 0 {
        return (first as u64, 1);
    }

    let len = (!first).leading_zeros();
    let mut v: u64 = (first as u8 & first_byte_mask(len as usize)) as u64;
    for i in 1..=len {
        v <<= 8;
        v |= bytes[i as usize] as u64;
    }
    (v, (len + 1) as usize)
}

fn first_byte_mask(len: usize) -> u8 {
    0xff >> len
}

fn msb(len: usize) -> u8 {
    !first_byte_mask(len)
}
