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

use std::borrow::Cow;

use super::{ObjMeta, ObjType, Value};

macro_rules! from_i32 {
    ($($tx:ident,$ty:ident)*) => {
        $(
            impl From<$tx> for Value {
                fn from(n: $tx) -> Self {
                    Value::Int32(n.into(),ObjMeta::default_obj_meta(ObjType::$ty))
                }
            }
        )*
    };
}

macro_rules! from_u32 {
    ($($tx:ident,$ty:ident)*) => {
        $(
            impl From<$tx> for Value {
                fn from(n: $tx) -> Self {
                    Value::UInt32(n.into(), ObjMeta::default_obj_meta(ObjType::$ty))
                }
            }
        )*
    };
}

from_i32! {
    i16,SmallInt
    i32,Int32
}
from_u32! {
    u16,USmallInt
    u32,UInt32
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(option: Option<T>) -> Self {
        match option {
            None => Value::Null(ObjMeta::default_obj_meta(ObjType::Null)),
            Some(inner) => inner.into(),
        }
    }
}

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Value::Null(ObjMeta::default_obj_meta(ObjType::Null))
    }
}

impl From<i8> for Value {
    fn from(i: i8) -> Self {
        Value::Int8(i, ObjMeta::default_obj_meta(ObjType::TinyInt))
    }
}

impl From<u8> for Value {
    fn from(i: u8) -> Self {
        Value::UInt8(i, ObjMeta::default_obj_meta(ObjType::UTinyInt))
    }
}

impl From<i64> for Value {
    fn from(f: i64) -> Self {
        Value::Int64(f, ObjMeta::default_obj_meta(ObjType::Int64))
    }
}

impl From<u64> for Value {
    fn from(f: u64) -> Self {
        Value::UInt64(f, ObjMeta::default_obj_meta(ObjType::UInt64))
    }
}

impl From<f32> for Value {
    fn from(f: f32) -> Self {
        Value::Float(f, ObjMeta::default_obj_meta(ObjType::Float))
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Double(f, ObjMeta::default_obj_meta(ObjType::Double))
    }
}

impl From<bool> for Value {
    fn from(f: bool) -> Self {
        Value::Bool(f, ObjMeta::default_obj_meta(ObjType::TinyInt))
    }
}

impl From<String> for Value {
    fn from(f: String) -> Self {
        Value::String(f, ObjMeta::default_obj_meta(ObjType::Varchar))
    }
}

impl<'a> From<&'a str> for Value {
    fn from(f: &str) -> Self {
        Value::String(f.to_string(), ObjMeta::default_obj_meta(ObjType::Varchar))
    }
}

impl<'a> From<Cow<'a, str>> for Value {
    fn from(f: Cow<'a, str>) -> Self {
        Value::String(f.to_string(), ObjMeta::default_obj_meta(ObjType::Varchar))
    }
}

impl From<Vec<u8>> for Value {
    fn from(f: Vec<u8>) -> Self {
        Value::Bytes(f, ObjMeta::default_obj_meta(ObjType::Varchar))
    }
}

impl<'a> From<&'a Vec<u8>> for Value {
    fn from(f: &'a Vec<u8>) -> Self {
        Value::Bytes(f.to_vec(), ObjMeta::default_obj_meta(ObjType::Varchar))
    }
}

impl<'a> From<&'a [u8]> for Value {
    fn from(f: &'a [u8]) -> Self {
        Value::Bytes(f.to_vec(), ObjMeta::default_obj_meta(ObjType::Varchar))
    }
}

//TODO date and time

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_from_into() {
        let v = Value::from(32);
        assert_eq!(32, v.as_i32());
    }
}
