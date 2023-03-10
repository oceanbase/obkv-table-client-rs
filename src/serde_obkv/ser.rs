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

use bytes::BytesMut;
use serde::ser::{self, Impossible, Serialize};

use super::{
    error::{Error, Result},
    util,
    value::Value,
};

pub fn to_bytes_mut<T>(value: &T, size: usize) -> Result<BytesMut>
where
    T: Serialize,
{
    let mut serializer = Serializer::Output(BytesMut::with_capacity(size), false);
    value.serialize(&mut serializer)?;
    if let Serializer::Output(buf, _) = serializer {
        Ok(buf)
    } else {
        panic!("expect output")
    }
}

pub fn serialize_len<T>(value: &T) -> Result<usize>
where
    T: Serialize,
{
    let mut serializer = Serializer::Len(0, false);
    value.serialize(&mut serializer)?;
    if let Serializer::Len(len, _) = serializer {
        Ok(len)
    } else {
        panic!("expect len")
    }
}

pub enum Serializer {
    Output(BytesMut, bool),
    Len(usize, bool),
}

#[doc(hidden)]
pub struct SerializeStruct<'a> {
    ser: &'a mut Serializer,
    first: bool,
    len: usize,
}

#[doc(hidden)]
pub struct SerializeArray<'a> {
    ser: &'a mut Serializer,
}

impl<'a> ser::Serializer for &'a mut Serializer {
    type Error = Error;
    type Ok = ();
    type SerializeMap = SerializeStruct<'a>;
    type SerializeSeq = SerializeArray<'a>;
    type SerializeStruct = SerializeStruct<'a>;
    type SerializeStructVariant = Impossible<(), Error>;
    type SerializeTuple = SerializeArray<'a>;
    type SerializeTupleStruct = SerializeArray<'a>;
    type SerializeTupleVariant = Impossible<(), Error>;

    fn serialize_bool(self, v: bool) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    // Serialize a char as a single-character string. Other formats may
    // represent this differently.
    fn serialize_char(self, v: char) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v.to_string()).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v.to_string()).len();
                Ok(())
            }
        }
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, raw_key) => {
                if *raw_key {
                    util::encode_vstring(v, output)
                } else {
                    Value::from(v).encode(output)
                }
            }
            Serializer::Len(ref mut len, raw_key) => {
                if *raw_key {
                    *len += util::encoded_length_vstring(v);
                } else {
                    *len += Value::from(v).len();
                }
                Ok(())
            }
        }
    }

    // Serialize a byte array as an array of bytes. Could also use a base64
    // string here. Binary formats will typically represent byte arrays more
    // compactly.
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::from(v).encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::from(v).len();
                Ok(())
            }
        }
    }

    fn serialize_none(self) -> Result<()> {
        match self {
            Serializer::Output(ref mut output, _) => Value::default().encode(output),
            Serializer::Len(ref mut len, _) => {
                *len += Value::default().len();
                Ok(())
            }
        }
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_none()
    }

    // When serializing a unit variant (or any other kind of variant), formats
    // can choose whether to keep track of it by index or by name. Binary
    // formats typically use the index of the variant and human-readable formats
    // typically use the name.
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain.
    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::Custom("expected RawValue".into()))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let len = len.unwrap_or(0);

        match self {
            Serializer::Output(output, _) => {
                util::encode_vi64(len as i64, output)?;
            }
            Serializer::Len(ref mut ret_len, _) => {
                *ret_len += util::encoded_length_vi64(len as i64);
            }
        }

        Ok(SerializeArray { ser: self })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(Error::Custom("expected RawValue".into()))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(SerializeStruct {
            ser: self,
            len: len.unwrap(),
            first: true,
        })
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        Ok(SerializeStruct {
            ser: self,
            len,
            first: true,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(Error::Custom("expected RawValue".into()))
    }
}

impl<'a> ser::SerializeStruct for SerializeStruct<'a> {
    type Error = Error;
    type Ok = ();

    /// encode table entity properties
    fn serialize_field<V: ?Sized>(&mut self, key: &'static str, value: &V) -> Result<()>
    where
        V: ser::Serialize,
    {
        if self.first {
            match self.ser {
                Serializer::Output(output, _) => {
                    util::encode_vi64(self.len as i64, output)?;
                }
                Serializer::Len(ref mut len, _) => {
                    *len += util::encoded_length_vi64(self.len as i64);
                }
            }

            self.first = false;
        }

        key.serialize(&mut *self.ser)?;
        value.serialize(&mut *self.ser)?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeMap for SerializeStruct<'a> {
    type Error = Error;
    type Ok = ();

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<()>
    where
        T: Serialize,
    {
        match self.ser {
            Serializer::Output(_, ref mut raw_key) => *raw_key = true,
            Serializer::Len(_, ref mut raw_key) => *raw_key = true,
        }
        let ret = key.serialize(&mut *self.ser);

        match self.ser {
            Serializer::Output(_, ref mut raw_key) => *raw_key = false,
            Serializer::Len(_, ref mut raw_key) => *raw_key = false,
        }
        ret
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    /// encode table entity properties
    fn serialize_entry<K: ?Sized, V: ?Sized>(&mut self, key: &K, value: &V) -> Result<()>
    where
        K: Serialize,
        V: Serialize,
    {
        if self.first {
            match self.ser {
                Serializer::Output(output, _) => {
                    util::encode_vi64(self.len as i64, output)?;
                }
                Serializer::Len(ref mut len, _) => {
                    *len += util::encoded_length_vi64(self.len as i64);
                }
            }

            self.first = false;
        }

        self.serialize_key(key)?;
        self.serialize_value(value)?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeSeq for SerializeArray<'a> {
    type Error = Error;
    type Ok = ();

    fn serialize_element<T: ?Sized>(&mut self, elem: &T) -> Result<()>
    where
        T: ser::Serialize,
    {
        elem.serialize(&mut *self.ser)?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for SerializeArray<'a> {
    type Error = Error;
    type Ok = ();

    fn serialize_element<T: ?Sized>(&mut self, elem: &T) -> Result<()>
    where
        T: ser::Serialize,
    {
        ser::SerializeSeq::serialize_element(self, elem)
    }

    fn end(self) -> Result<()> {
        ser::SerializeSeq::end(self)
    }
}

impl<'a> ser::SerializeTupleStruct for SerializeArray<'a> {
    type Error = Error;
    type Ok = ();

    fn serialize_field<V: ?Sized>(&mut self, value: &V) -> Result<()>
    where
        V: ser::Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<()> {
        ser::SerializeSeq::end(self)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn serialize_vec() {
        let test = vec!["foo"];

        let size = serialize_len(&test).unwrap();
        let ret = to_bytes_mut(&test, size);
        assert!(ret.is_ok());
        let ret = ret.unwrap();
        assert_eq!(10, ret.len());
    }

    #[test]
    fn serialize_map() {
        let mut test = HashMap::new();
        test.insert("c2", "bar");

        let size = serialize_len(&test).unwrap();
        let ret = to_bytes_mut(&test, size);
        assert!(ret.is_ok());
        let ret = ret.unwrap();
        println!("{:?}", &ret[..]);
        assert_eq!(14, ret.len());
    }

    #[test]
    fn test_struct() {
        #[derive(Serialize)]
        struct Test {
            int: u32,
            seq: Vec<&'static str>,
        }

        let test = Test {
            int: 1,
            seq: vec!["a", "b"],
        };

        let size = serialize_len(&test).unwrap();
        let ret = to_bytes_mut(&test, size);
        assert!(ret.is_ok());
        let ret = ret.unwrap();
        assert_eq!(39, ret.len());
    }
}
