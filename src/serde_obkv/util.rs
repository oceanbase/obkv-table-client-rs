// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{f32, f64, io::Cursor, str};

use bytes::{Buf, BufMut, BytesMut, IntoBuf};

use super::error::{Error, Result};

pub const OB_MAX_V1B: u64 = (1 << 7) - 1;
pub const OB_MAX_V2B: u64 = (1 << 14) - 1;
pub const OB_MAX_V3B: u64 = (1 << 21) - 1;
pub const OB_MAX_V4B: u64 = (1 << 28) - 1;
pub const OB_MAX_V5B: u64 = (1 << 35) - 1;
pub const OB_MAX_V6B: u64 = (1 << 42) - 1;
pub const OB_MAX_V7B: u64 = (1 << 49) - 1;
pub const OB_MAX_V8B: u64 = (1 << 56) - 1;
pub const OB_MAX_V9B: u64 = (1 << 63) - 1;

pub static OB_MAX: &[u64] = &[
    OB_MAX_V1B, OB_MAX_V2B, OB_MAX_V3B, OB_MAX_V4B, OB_MAX_V5B, OB_MAX_V6B, OB_MAX_V7B, OB_MAX_V8B,
    OB_MAX_V9B,
];

//TODO refactor
pub fn encoded_length_vi8(v: i8) -> usize {
    let v = v as u64;
    if v <= OB_MAX_V1B {
        1
    } else {
        2
    }
}

//TODO refactor
pub fn encoded_length_vi32(v: i32) -> usize {
    let v = v as u64;
    if v <= OB_MAX_V1B {
        1
    } else if v <= OB_MAX_V2B {
        2
    } else if v <= OB_MAX_V3B {
        3
    } else if v <= OB_MAX_V4B {
        4
    } else {
        5
    }
}

pub fn encode_vi32(v: i32, buf: &mut BytesMut) -> Result<()> {
    buf.reserve(encoded_length_vi32(v));

    let mut v = v as u32;
    while u64::from(v) > OB_MAX_V1B {
        buf.put_i8((v | 0x80) as i8);
        v >>= 7;
    }
    if u64::from(v) <= OB_MAX_V1B {
        buf.put_i8((v & 0x7f) as i8);
    }
    Ok(())
}

#[inline]
pub fn advance_buf(buf: &mut BytesMut, skip: usize) -> Result<()> {
    if buf.len() < skip {
        return Err(Error::Custom(
            format!(
                "util::advance_buf can't advance buf, len: {}, try to skip: {}",
                buf.len(),
                skip
            )
            .into(),
        ));
    }
    buf.advance(skip);

    Ok(())
}

#[inline]
pub fn split_buf_to(buf: &mut BytesMut, n: usize) -> Result<BytesMut> {
    if buf.len() < n {
        return Err(Error::Custom(
            format!(
                "util::split_buf_to can't split buf, len: {}, try to split: {}",
                buf.len(),
                n,
            )
            .into(),
        ));
    }

    Ok(buf.split_to(n))
}

pub fn decode_vi32(buf: &mut BytesMut) -> Result<i32> {
    let mut ret: u32 = 0;
    let mut shift: u32 = 0;
    {
        let mut src = Cursor::new(&mut *buf);
        loop {
            let b = src.get_i8();
            ret |= (b as u32 & 0x7f) << shift;
            shift += 7;
            if b as u8 & 0x80 == 0 {
                break;
            }
        }
    }
    let skip = (shift / 7) as usize;
    advance_buf(buf, skip)?;

    Ok(ret as i32)
}

//TODO refactor
pub fn encoded_length_vi64(v: i64) -> usize {
    let v = v as u64;
    if v <= OB_MAX_V1B {
        1
    } else if v <= OB_MAX_V2B {
        2
    } else if v <= OB_MAX_V3B {
        3
    } else if v <= OB_MAX_V4B {
        4
    } else if v <= OB_MAX_V5B {
        5
    } else if v <= OB_MAX_V6B {
        6
    } else if v <= OB_MAX_V7B {
        7
    } else if v <= OB_MAX_V8B {
        8
    } else if v <= OB_MAX_V9B {
        9
    } else {
        10
    }
}

pub fn encode_vi64(v: i64, buf: &mut BytesMut) -> Result<()> {
    buf.reserve(encoded_length_vi64(v));

    let mut v = v as u64;
    while v > OB_MAX_V1B {
        buf.put_i8((v | 0x80) as i8);
        v >>= 7;
    }
    if v <= OB_MAX_V1B {
        buf.put_i8((v & 0x7f) as i8);
    }
    Ok(())
}

pub fn decode_vi64(buf: &mut BytesMut) -> Result<i64> {
    let mut ret: u64 = 0;
    let mut shift: u64 = 0;
    {
        let mut src = Cursor::new(&mut *buf);
        loop {
            let b = src.get_i8();
            ret |= (b as u64 & 0x7f) << shift;
            shift += 7;
            if b as u8 & 0x80 == 0 {
                break;
            }
        }
    }
    let skip = (shift / 7) as usize;
    advance_buf(buf, skip)?;

    Ok(ret as i64)
}

fn utf8(buf: &[u8]) -> Result<&str> {
    str::from_utf8(buf).map_err(|_| Error::Custom("Unable to decode input as UTF8".into()))
}

pub fn encoded_length_vstring(s: &str) -> usize {
    encoded_length_vi32(s.len() as i32) as usize + s.len() + 1
}

const END: u8 = 0;

pub fn encode_vstring(s: &str, buf: &mut BytesMut) -> Result<()> {
    let bs = s.as_bytes();
    buf.reserve(encoded_length_vi32(bs.len() as i32) as usize + bs.len() + 1);
    encode_vi32(bs.len() as i32, buf)?;
    buf.put_slice(bs);
    buf.put_u8(END);

    Ok(())
}

pub fn encoded_length_bytes_string(v: &[u8]) -> usize {
    encoded_length_vi32(v.len() as i32) as usize + v.len() + 1
}

pub fn encode_bytes_string(v: &[u8], buf: &mut BytesMut) -> Result<()> {
    buf.reserve(encoded_length_bytes_string(v));
    encode_vi32(v.len() as i32, buf)?;
    buf.put_slice(v);
    buf.put(END);
    Ok(())
}

pub fn decode_bytes(buf: &mut BytesMut) -> Result<Vec<u8>> {
    let len = decode_vi32(buf)? as usize;
    if len == 0 {
        return Ok(vec![]);
    }
    let v = split_buf_to(buf, len)?;

    Ok(v.to_vec())
}

pub fn decode_vstring(buf: &mut BytesMut) -> Result<String> {
    let len = decode_vi32(buf)? as usize;
    let s = if len == 0 {
        "".to_owned()
    } else {
        let bs = split_buf_to(buf, len)?;
        utf8(&bs[..len])?.to_owned()
    };
    // Skip END byte
    advance_buf(buf, 1)?;

    Ok(s)
}

pub fn decode_bytes_string(buf: &mut BytesMut) -> Result<Vec<u8>> {
    let len = decode_vi32(buf)? as usize;
    let res = if len == 0 {
        vec![]
    } else {
        split_buf_to(buf, len)?.to_vec()
    };
    // Skip END byte
    advance_buf(buf, 1)?;

    Ok(res)
}

pub fn encode_f64(f: f64, buf: &mut BytesMut) -> Result<()> {
    encode_vi64(f64::to_bits(f) as i64, buf)
}

pub fn decode_f64(buf: &mut BytesMut) -> Result<f64> {
    Ok(f64::from_bits(decode_vi64(buf)? as u64))
}

pub fn encode_f32(f: f32, buf: &mut BytesMut) -> Result<()> {
    encode_vi32(f32::to_bits(f) as i32, buf)
}

pub fn decode_f32(buf: &mut BytesMut) -> Result<f32> {
    Ok(f32::from_bits(decode_vi32(buf)? as u32))
}

pub fn decode_i8(buf: &mut BytesMut) -> Result<i8> {
    if buf.is_empty() {
        return Err(Error::Custom("buf EOF".into()));
    }
    Ok(split_buf_to(buf, 1)?.into_buf().get_i8())
}

pub fn decode_u8(buf: &mut BytesMut) -> Result<u8> {
    if buf.is_empty() {
        return Err(Error::Custom("buf EOF".into()));
    }
    Ok(split_buf_to(buf, 1)?.into_buf().get_u8())
}

#[cfg(test)]
mod test {
    use std::{i32, i64};

    use super::*;

    #[test]
    fn encode_decode_vi32() {
        let nums = vec![i32::MIN, -100, -1, 0, 1, 100, i32::MAX];
        for i in nums {
            let a = i as i32;
            let mut buf = BytesMut::new();

            let ret = encode_vi32(a, &mut buf);
            assert!(ret.is_ok());
            assert!(buf.len() > 0);
            assert!(buf.len() <= 5);

            let mut buf = buf.clone();
            let ret = decode_vi32(&mut buf);
            assert!(ret.is_ok());
            assert_eq!(a, ret.unwrap());
        }
    }

    #[test]
    fn encode_decode_vi64() {
        let nums = vec![i64::MIN, -100, -1, 0, 1, 100, i64::MAX];
        for i in nums {
            let a = i as i64;
            let mut buf = BytesMut::new();

            let ret = encode_vi64(a, &mut buf);
            assert!(ret.is_ok());
            assert!(buf.len() > 0);
            assert!(buf.len() <= 10);

            let mut buf = buf.clone();
            let ret = decode_vi64(&mut buf);
            assert!(ret.is_ok());
            assert_eq!(a, ret.unwrap());
        }
    }

    #[test]
    fn test_encode_decode_bytes_string() {
        let bs: Vec<u8> = vec![];
        let mut buf = BytesMut::new();
        let ret = encode_bytes_string(&bs, &mut buf);
        assert!(ret.is_ok());
        assert_eq!(2, buf.len());
        let mut buf = buf.clone();
        let ret = decode_bytes_string(&mut buf);
        assert!(ret.is_ok());
        let ret = ret.unwrap();
        assert_eq!(bs, ret);

        let bs: Vec<u8> = vec![1, 2, 3, 4, 5];
        let mut buf = BytesMut::new();
        let ret = encode_bytes_string(&bs, &mut buf);
        assert!(ret.is_ok());
        assert_eq!(7, buf.len());
        let mut buf = buf.clone();
        let ret = decode_bytes_string(&mut buf);
        assert!(ret.is_ok());
        let ret = ret.unwrap();
        assert_eq!(bs, ret);
    }

    #[test]
    fn encode_decode_vstring() {
        let s = "hello";
        let mut buf = BytesMut::new();
        let ret = encode_vstring(s, &mut buf);
        assert!(ret.is_ok());
        assert_eq!(7, buf.len());

        let mut buf = buf.clone();
        let ret = decode_vstring(&mut buf);
        assert!(ret.is_ok());
        let ret = ret.unwrap();
        assert_eq!(s, &ret[..]);

        // test empty string
        let s = "";
        let mut buf = BytesMut::new();
        let ret = encode_vstring(s, &mut buf);
        assert!(ret.is_ok());
        assert_eq!(2, buf.len());
        let mut buf = buf.clone();
        let ret = decode_vstring(&mut buf);
        assert!(ret.is_ok());
        let ret = ret.unwrap();
        assert_eq!(s, &ret[..]);
    }

    #[test]
    fn encode_decode_f32() {
        let f: f32 = 99.99999;
        let mut buf = BytesMut::new();
        let ret = encode_f32(f, &mut buf);
        assert!(ret.is_ok());
        assert_eq!(5, buf.len());

        let mut buf = buf.clone();
        let ret = decode_f32(&mut buf);
        assert!(ret.is_ok());
        let ret = ret.unwrap();
        assert_eq!(f, ret);
    }

    #[test]
    fn encode_decode_f64() {
        let f: f64 = -99.99999;
        let mut buf = BytesMut::new();
        let ret = encode_f64(f, &mut buf);
        assert!(ret.is_ok());
        assert_eq!(10, buf.len());

        let mut buf = buf.clone();
        let ret = decode_f64(&mut buf);
        assert!(ret.is_ok());
        let ret = ret.unwrap();
        assert_eq!(f, ret);
    }
}
