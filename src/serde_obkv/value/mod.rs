/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the
 * Mulan PSL v2. You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::derived_hash_with_manual_eq)]
#![allow(clippy::wrong_self_convention)]

pub mod from;
use std::hash::{Hash, Hasher};

use bytes::{Buf, BufMut, BytesMut};
use serde::ser::{Serialize, Serializer};

use super::{
    error::{Error, Result},
    util::{
        self, decode_bytes_string, decode_f32, decode_f64, decode_i8, decode_u8, decode_vi32,
        decode_vi64, decode_vstring, encode_f32, encode_f64, encode_vi32, encode_vi64,
        encode_vstring, split_buf_to,
    },
};

const VALUE_MAX: i64 = -2i64;
const VALUE_MIN: i64 = -3i64;

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub enum CollationType {
    Invalid = 0,
    UTF8MB4GeneralCi = 45, // default, case-insensitive,
    UTF8MB4Bin = 46,       // case sensitive
    Binary = 63,
    CollationFree = 100, // not supported in mysql
    Max = 101,
}

impl CollationType {
    pub fn from_u8(v: u8) -> Result<CollationType> {
        match v {
            0 => Ok(CollationType::Invalid),
            45 => Ok(CollationType::UTF8MB4GeneralCi),
            46 => Ok(CollationType::UTF8MB4Bin),
            63 => Ok(CollationType::Binary),
            100 => Ok(CollationType::CollationFree),
            101 => Ok(CollationType::Max),
            _ => Err(Error::Custom(
                format!("CollationType::from_u8 invalid collation type, v={v}").into(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub enum CollationLevel {
    Explicit = 0,
    None = 1,
    Implicit = 2,
    Sysconst = 3,
    Coercible = 4,
    Numeric = 5,
    Ignorable = 6,
    Invalid = 7,
}

impl CollationLevel {
    pub fn from_u8(v: u8) -> Result<CollationLevel> {
        match v {
            0 => Ok(CollationLevel::Explicit),
            1 => Ok(CollationLevel::None),
            2 => Ok(CollationLevel::Implicit),
            3 => Ok(CollationLevel::Sysconst),
            4 => Ok(CollationLevel::Coercible),
            5 => Ok(CollationLevel::Numeric),
            6 => Ok(CollationLevel::Ignorable),
            7 => Ok(CollationLevel::Invalid),
            _ => Err(Error::Custom(
                format!("CollationLevel::from_u8 invalid collation level, v={v}").into(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub struct ObjMeta {
    obj_type: ObjType,
    cs_level: CollationLevel,
    cs_type: CollationType,
    scale: i8, // when the type is ObBitType, this value keeps the bit's length.
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, PartialOrd)]
pub enum ObjType {
    Null = 0,
    TinyInt = 1,
    SmallInt = 2,
    Int32 = 4,
    Int64 = 5,
    UTinyInt = 6,
    USmallInt = 7,
    UMediumInt = 8,
    UInt32 = 9,
    UInt64 = 10,
    Float = 11,
    Double = 12,
    UFloat = 13,
    UDouble = 14,
    Number = 15,
    UNumber = 16,
    DateTime = 17,
    Timestamp = 18,
    Date = 19,
    Time = 20,
    Year = 21,
    Varchar = 22,
    Char = 23,
    HexString = 24,
    Extend = 25,
    TinyText = 27,
    Text = 28,
    MediumText = 29,
    LongText = 30,
    Bit = 31, //TODO
}

impl ObjType {
    pub fn from_u8(v: u8) -> Result<ObjType> {
        match v {
            0 => Ok(ObjType::Null),
            1 => Ok(ObjType::TinyInt),
            2 => Ok(ObjType::SmallInt),
            4 => Ok(ObjType::Int32),
            5 => Ok(ObjType::Int64),
            6 => Ok(ObjType::UTinyInt),
            7 => Ok(ObjType::USmallInt),
            8 => Ok(ObjType::UMediumInt),
            9 => Ok(ObjType::UInt32),
            10 => Ok(ObjType::UInt64),
            11 => Ok(ObjType::Float),
            12 => Ok(ObjType::Double),
            13 => Ok(ObjType::UFloat),
            14 => Ok(ObjType::UDouble),
            15 => Ok(ObjType::Number),
            16 => Ok(ObjType::UNumber),
            17 => Ok(ObjType::DateTime),
            18 => Ok(ObjType::Timestamp),
            19 => Ok(ObjType::Date),
            20 => Ok(ObjType::Time),
            21 => Ok(ObjType::Year),
            22 => Ok(ObjType::Varchar),
            23 => Ok(ObjType::Char),
            25 => Ok(ObjType::Extend),
            27 => Ok(ObjType::TinyText),
            28 => Ok(ObjType::Text),
            29 => Ok(ObjType::MediumText),
            30 => Ok(ObjType::LongText),
            31 => Ok(ObjType::Bit),
            _ => Err(Error::Custom(
                format!("ObjType::from_u8 invalid ob obj type, v={v}").into(),
            )),
        }
    }
}

//Represent any valid OBKV value.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Value {
    Null(ObjMeta),
    Bool(bool, ObjMeta),     //tinyint
    Int8(i8, ObjMeta),       //tinyint
    UInt8(u8, ObjMeta),      //utinyint
    Int32(i32, ObjMeta),     //i8,i16,i24,i32
    Int64(i64, ObjMeta),     //i64
    UInt32(u32, ObjMeta),    //u8,u16,u24,u23
    UInt64(u64, ObjMeta),    //u64
    Float(f32, ObjMeta),     //f32, uf32
    Double(f64, ObjMeta),    //f64,uf64
    Date(i32, ObjMeta),      //date, in seconds
    Time(i64, ObjMeta),      //datetime, timestamp, in millseconds
    Bytes(Vec<u8>, ObjMeta), //varchar
    String(String, ObjMeta), //text,char
}

//TODO refactor, introduce trait for encoder
impl ObjMeta {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        buf.reserve(4);
        buf.put_i8(self.obj_type.to_owned() as i8);
        buf.put_i8(self.cs_level.to_owned() as i8);
        buf.put_i8(self.cs_type.to_owned() as i8);
        buf.put_i8(self.scale.to_owned());
        Ok(())
    }

    pub fn len(&self) -> usize {
        // obj_type(1 byte) + cs_level(1 byte)
        // cs_type( 1 byte) + scale (1 byte)
        4
    }

    pub fn decode(buf: &mut BytesMut) -> Result<ObjMeta> {
        if buf.len() >= 4 {
            let mut buf = split_buf_to(buf, 4)?;
            let obj_type = ObjType::from_u8(buf.get_u8())?;
            let cs_level = CollationLevel::from_u8(buf.get_u8())?;
            let cs_type = CollationType::from_u8(buf.get_u8())?;

            Ok(ObjMeta {
                obj_type,
                cs_level,
                cs_type,
                scale: buf.get_i8(),
            })
        } else {
            Err(Error::Custom("Fail to decode obj meta".into()))
        }
    }

    pub fn new(
        obj_type: ObjType,
        cs_level: CollationLevel,
        cs_type: CollationType,
        scale: i8,
    ) -> ObjMeta {
        ObjMeta {
            obj_type,
            cs_level,
            cs_type,
            scale,
        }
    }

    pub fn new_numeric_meta(obj_type: ObjType) -> ObjMeta {
        ObjMeta::new(obj_type, CollationLevel::Numeric, CollationType::Binary, -1)
    }

    fn default_obj_meta(t: ObjType) -> ObjMeta {
        match t {
            ObjType::Null => ObjMeta::new(t, CollationLevel::Ignorable, CollationType::Binary, 10),
            ObjType::TinyInt => ObjMeta::new_numeric_meta(t),
            ObjType::SmallInt => ObjMeta::new_numeric_meta(t),
            ObjType::Int32 => ObjMeta::new_numeric_meta(t),
            ObjType::Int64 => ObjMeta::new_numeric_meta(t),
            ObjType::UTinyInt => ObjMeta::new_numeric_meta(t),
            ObjType::USmallInt => ObjMeta::new_numeric_meta(t),
            ObjType::UMediumInt => ObjMeta::new_numeric_meta(t),
            ObjType::UInt32 => ObjMeta::new_numeric_meta(t),
            ObjType::UInt64 => ObjMeta::new_numeric_meta(t),
            ObjType::Float => ObjMeta::new_numeric_meta(t),
            ObjType::Double => ObjMeta::new_numeric_meta(t),
            ObjType::UFloat => ObjMeta::new_numeric_meta(t),
            ObjType::UDouble => ObjMeta::new_numeric_meta(t),
            ObjType::Number => ObjMeta::new_numeric_meta(t),
            ObjType::UNumber => ObjMeta::new_numeric_meta(t),
            ObjType::DateTime => ObjMeta::new_numeric_meta(t),
            ObjType::Timestamp => ObjMeta::new_numeric_meta(t),
            ObjType::Date => ObjMeta::new_numeric_meta(t),
            ObjType::Time => ObjMeta::new_numeric_meta(t),
            ObjType::Year => ObjMeta::new_numeric_meta(t),
            ObjType::Varchar => ObjMeta::new(
                t,
                CollationLevel::Explicit,
                CollationType::UTF8MB4GeneralCi,
                10,
            ),
            ObjType::Char => ObjMeta::new(
                t,
                CollationLevel::Explicit,
                CollationType::UTF8MB4GeneralCi,
                10,
            ),
            ObjType::HexString => {
                unimplemented!();
            }
            ObjType::Extend => ObjMeta::new_numeric_meta(t),
            ObjType::TinyText => ObjMeta::new_numeric_meta(t),
            ObjType::Text => ObjMeta::new_numeric_meta(t),
            ObjType::MediumText => ObjMeta::new_numeric_meta(t),
            ObjType::LongText => ObjMeta::new_numeric_meta(t),
            ObjType::Bit => ObjMeta::new_numeric_meta(t),
        }
    }
}

impl Value {
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Value::Int8(_, _)
                | Value::UInt8(_, _)
                | Value::Int32(_, _)
                | Value::Int64(_, _)
                | Value::UInt32(_, _)
                | Value::UInt64(_, _)
        )
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Value::Null(_))
    }

    pub fn is_i32(&self) -> bool {
        matches!(self, Value::Int32(_, _))
    }

    pub fn is_u32(&self) -> bool {
        matches!(self, Value::UInt32(_, _))
    }

    pub fn is_i8(&self) -> bool {
        matches!(self, Value::Int8(_, _))
    }

    pub fn is_u8(&self) -> bool {
        matches!(self, Value::UInt8(_, _))
    }

    pub fn is_i64(&self) -> bool {
        matches!(self, Value::Int64(_, _))
    }

    pub fn is_u64(&self) -> bool {
        matches!(self, Value::UInt64(_, _))
    }

    pub fn is_f32(&self) -> bool {
        matches!(self, Value::Float(_, _))
    }

    pub fn is_f64(&self) -> bool {
        matches!(self, Value::Double(_, _))
    }

    pub fn is_bytes(&self) -> bool {
        matches!(self, Value::Bytes(_, _))
    }

    pub fn as_bytes(self) -> Vec<u8> {
        match self {
            Value::Bytes(v, _) => v,
            _ => panic!("Fail to cast: {self:?}"),
        }
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_, _))
    }

    pub fn as_string(self) -> String {
        match self {
            Value::String(s, _) => s,
            _ => panic!("Fail to cast: {self:?}"),
        }
    }

    pub fn as_i64(&self) -> i64 {
        match self {
            Value::Int64(i, _) => *i,
            Value::UInt64(i, _) => *i as i64,
            _ => panic!("Fail to cast: {self:?}"),
        }
    }

    pub fn as_u64(&self) -> u64 {
        self.as_i64() as u64
    }

    pub fn as_f64(&self) -> f64 {
        match self {
            Value::Double(i, _) => *i,
            _ => panic!("Fail to cast: {self:?}"),
        }
    }

    pub fn as_i32(&self) -> i32 {
        match self {
            Value::Int32(i, _) => *i,
            Value::UInt32(i, _) => *i as i32,
            _ => panic!("Fail to cast: {self:?}"),
        }
    }

    pub fn as_u32(&self) -> u32 {
        self.as_i32() as u32
    }

    pub fn as_f32(&self) -> f32 {
        match self {
            Value::Float(i, _) => *i,
            _ => panic!("Fail to cast: {self:?}"),
        }
    }

    pub fn as_i16(&self) -> i16 {
        self.as_i32() as i16
    }

    pub fn as_u16(&self) -> u16 {
        self.as_i16() as u16
    }

    pub fn as_i8(&self) -> i8 {
        match self {
            Value::Int8(i, _) => *i,
            Value::UInt8(i, _) => *i as i8,
            _ => panic!("Fail to cast: {self:?}"),
        }
    }

    pub fn as_u8(&self) -> u8 {
        self.as_i8() as u8
    }

    fn decode_binary(buf: &mut BytesMut, meta: ObjMeta) -> Result<Value> {
        let v = if meta.cs_type == CollationType::Binary {
            let bs = decode_bytes_string(buf)?;
            Value::Bytes(bs, meta)
        } else {
            let bs = decode_vstring(buf)?;
            Value::String(bs, meta)
        };

        Ok(v)
    }

    /// Max value object
    pub fn get_max() -> Value {
        Value::Int64(VALUE_MAX, ObjMeta::default_obj_meta(ObjType::Extend))
    }

    // Min value object
    pub fn get_min() -> Value {
        Value::Int64(VALUE_MIN, ObjMeta::default_obj_meta(ObjType::Extend))
    }

    pub fn is_max(&self) -> bool {
        match self {
            Value::Int64(VALUE_MAX, meta) => meta.obj_type == ObjType::Extend,
            _ => false,
        }
    }

    pub fn is_min(&self) -> bool {
        match self {
            Value::Int64(VALUE_MIN, meta) => meta.obj_type == ObjType::Extend,
            _ => false,
        }
    }

    pub fn is_extend(&self) -> bool {
        self.is_max() || self.is_min()
    }

    pub fn len(&self) -> usize {
        match *self {
            Value::Null(ref meta) => meta.len(),
            Value::Bool(_, ref meta) => meta.len() + 1,
            Value::Int8(_, ref meta) => meta.len() + 1,
            Value::UInt8(_, ref meta) => meta.len() + 1,
            Value::Int32(v, ref meta) => meta.len() + util::encoded_length_vi32(v),
            Value::Int64(v, ref meta) => meta.len() + util::encoded_length_vi64(v),
            Value::UInt32(v, ref meta) => meta.len() + util::encoded_length_vi32(v as i32),
            Value::UInt64(v, ref meta) => meta.len() + util::encoded_length_vi64(v as i64),
            Value::Float(f, ref meta) => meta.len() + util::encoded_length_vi32(f.to_bits() as i32),
            Value::Double(f, ref meta) => {
                meta.len() + util::encoded_length_vi64(f.to_bits() as i64)
            }
            Value::Date(d, ref meta) => meta.len() + util::encoded_length_vi32(d),
            Value::Time(d, ref meta) => meta.len() + util::encoded_length_vi64(d),
            Value::Bytes(ref vc, ref meta) => {
                meta.len() + util::encoded_length_vi32(vc.len() as i32) + vc.len() + 1
            }
            Value::String(ref s, ref meta) => {
                meta.len() + util::encoded_length_vi32(s.len() as i32) + s.len() + 1
            }
        }
    }

    pub fn decode(buf: &mut BytesMut, obj_type: ObjType) -> Result<Value> {
        let meta = ObjMeta::decode(buf)?;
        assert_eq!(meta.obj_type, obj_type);

        match obj_type {
            ObjType::Null => Ok(Value::default()),
            ObjType::TinyInt => Ok(Value::Int8(decode_i8(buf)?, meta)),
            ObjType::SmallInt => Ok(Value::Int32(decode_vi32(buf)?, meta)),
            ObjType::Int32 => Ok(Value::Int32(decode_vi32(buf)?, meta)),
            ObjType::Int64 => Ok(Value::Int64(decode_vi64(buf)?, meta)),
            ObjType::UTinyInt => Ok(Value::UInt8(decode_u8(buf)?, meta)),
            ObjType::USmallInt => Ok(Value::UInt32(decode_vi32(buf)? as u32, meta)),
            ObjType::UMediumInt => Ok(Value::UInt32(decode_vi32(buf)? as u32, meta)),
            ObjType::UInt32 => Ok(Value::UInt32(decode_vi32(buf)? as u32, meta)),
            ObjType::UInt64 => Ok(Value::UInt64(decode_vi64(buf)? as u64, meta)),
            ObjType::Float => Ok(Value::Float(decode_f32(buf)?, meta)),
            ObjType::Double => Ok(Value::Double(decode_f64(buf)?, meta)),
            ObjType::UFloat => Ok(Value::Float(decode_f32(buf)?, meta)),
            ObjType::UDouble => Ok(Value::Double(decode_f64(buf)?, meta)),
            //FIXME date and time
            ObjType::DateTime => unimplemented!(),
            ObjType::Timestamp => unimplemented!(),
            ObjType::Date => unimplemented!(),
            ObjType::Time => unimplemented!(),
            ObjType::Year => unimplemented!(),
            ObjType::Varchar => Self::decode_binary(buf, meta),
            ObjType::Char => Self::decode_binary(buf, meta),
            // TODO: ObjType::HexString
            ObjType::Extend => Ok(Value::Int64(decode_vi64(buf)?, meta)),
            ObjType::TinyText => Self::decode_binary(buf, meta),
            ObjType::Text => Self::decode_binary(buf, meta),
            ObjType::MediumText => Self::decode_binary(buf, meta),
            ObjType::LongText => Self::decode_binary(buf, meta),
            ObjType::Bit => Ok(Value::Int64(decode_vi64(buf)?, meta)),
            _ => Err(Error::Custom("Unsupported obj type.".into())),
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match *self {
            Value::Null(ref meta) => meta.encode(buf),
            Value::Bool(b, ref meta) => {
                meta.encode(buf)?;
                if b {
                    buf.put_u8(1);
                } else {
                    buf.put_u8(0);
                }
                Ok(())
            }
            Value::Int8(v, ref meta) => {
                meta.encode(buf)?;
                buf.put_i8(v);
                Ok(())
            }
            Value::UInt8(v, ref meta) => {
                meta.encode(buf)?;
                buf.put_u8(v);
                Ok(())
            }
            Value::Int32(v, ref meta) => {
                meta.encode(buf)?;
                encode_vi32(v, buf)
            }
            Value::Int64(v, ref meta) => {
                meta.encode(buf)?;
                encode_vi64(v, buf)
            }
            Value::UInt32(v, ref meta) => {
                meta.encode(buf)?;
                encode_vi32(v as i32, buf)
            }
            Value::UInt64(v, ref meta) => {
                meta.encode(buf)?;
                encode_vi64(v as i64, buf)
            }
            Value::Float(f, ref meta) => {
                meta.encode(buf)?;
                encode_f32(f, buf)
            }
            Value::Double(f, ref meta) => {
                meta.encode(buf)?;
                encode_f64(f, buf)
            }
            Value::Date(d, ref meta) => {
                meta.encode(buf)?;
                encode_vi32(d, buf)
            }
            Value::Time(d, ref meta) => {
                meta.encode(buf)?;
                encode_vi64(d * 1000, buf)
            }
            Value::Bytes(ref vc, ref meta) => {
                meta.encode(buf)?;
                //refactor encode binary
                encode_vi32(vc.len() as i32, buf)?;
                buf.put_slice(&vc[..]);
                buf.put_i8(0);
                Ok(())
            }
            Value::String(ref s, ref meta) => {
                meta.encode(buf)?;
                encode_vstring(s, buf)
            }
        }
    }
}

impl Default for Value {
    fn default() -> Value {
        Value::Null(ObjMeta::default_obj_meta(ObjType::Null))
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match *self {
            Value::Null(_) => 0.hash(state),
            Value::Bool(b, _) => (1, b).hash(state),
            Value::Int8(i, _) => (2, i).hash(state),
            Value::UInt8(i, _) => (2, i).hash(state),
            Value::Int32(i, _) => (2, i).hash(state),
            Value::Int64(i, _) => (2, i).hash(state),
            Value::UInt32(i, _) => (2, i).hash(state),
            Value::UInt64(i, _) => (2, i).hash(state),
            Value::Float(f, _) => f.to_bits().hash(state),
            Value::Double(f, _) => f.to_bits().hash(state),
            Value::Date(d, _) => (5, d).hash(state),
            Value::Time(t, _) => (6, t).hash(state),
            Value::Bytes(ref vc, _) => (7, vc).hash(state),
            Value::String(ref s, _) => (8, s).hash(state),
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Value::Null(_) => serializer.serialize_none(),
            Value::Bool(b, _) => serializer.serialize_bool(b),
            Value::Int8(i, _) => serializer.serialize_i8(i),
            Value::UInt8(i, _) => serializer.serialize_u8(i),
            Value::Int32(i, _) => serializer.serialize_i32(i),
            Value::Int64(i, _) => serializer.serialize_i64(i),
            Value::UInt32(i, _) => serializer.serialize_u32(i),
            Value::UInt64(i, _) => serializer.serialize_u64(i),
            Value::Float(f, _) => serializer.serialize_f32(f),
            Value::Double(f, _) => serializer.serialize_f64(f),
            Value::Date(d, _) => serializer.serialize_i32(d),
            Value::Time(t, _) => serializer.serialize_i64(t),
            Value::Bytes(ref vc, _) => serializer.serialize_bytes(vc),
            Value::String(ref s, _) => serializer.serialize_str(s),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::ser::{serialize_len, to_bytes_mut},
        *,
    };

    #[test]
    fn test_value_struct() {
        #[derive(Serialize, PartialEq)]
        struct Test {
            int: Value,
            #[serde(with = "serde_bytes")]
            seq: Vec<u8>,
        }

        let test = Test {
            int: Value::from(1),
            seq: b"hello".to_vec(),
        };

        let size = serialize_len(&test).unwrap();
        println!("size={size}");
        let ret = to_bytes_mut(&test, size);
        assert!(ret.is_ok());
        assert_eq!(35, ret.unwrap().len());
    }
}
