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
use serde::de::{self, Deserialize, DeserializeSeed, MapAccess, SeqAccess, Visitor};

use super::{
    error::{Error, Result},
    util,
    value::{ObjType, Value},
};

pub struct Deserializer<'de> {
    input: &'de mut BytesMut,
}

pub fn from_bytes_mut<'a, T>(s: &'a mut BytesMut) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_bytes_mut(s);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(Error::Custom("Trailing data".into()))
    }
}

impl<'de> Deserializer<'de> {
    pub fn from_bytes_mut(input: &'de mut BytesMut) -> Self {
        Deserializer { input }
    }

    fn peek_type(&mut self) -> Result<ObjType> {
        match self.input.first() {
            Some(v) => Ok(ObjType::from_u8(*v)?),
            _ => Err(Error::Custom("EOF".into())),
        }
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct enum
    }

    // Look at the input data to decide what Serde data model type to
    // deserialize as. Not all data formats are able to support this operation.
    // Formats that support `deserialize_any` are known as self-describing.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let obj_type = self.peek_type()?;
        match Value::decode(&mut *self.input, obj_type)? {
            Value::Null(_) => visitor.visit_none(),
            Value::Bool(b, _) => visitor.visit_bool(b),
            Value::Int8(i, _) => visitor.visit_i8(i),
            Value::UInt8(i, _) => visitor.visit_u8(i),
            Value::Int32(i, _) => visitor.visit_i32(i),
            Value::Int64(i, _) => visitor.visit_i64(i),
            Value::UInt32(i, _) => visitor.visit_u32(i),
            Value::UInt64(i, _) => visitor.visit_u64(i),
            Value::Float(f, _) => visitor.visit_f32(f),
            Value::Double(f, _) => visitor.visit_f64(f),
            Value::Bytes(ref vc, _) => visitor.visit_bytes(&vc[..]),
            Value::String(ref s, _) => visitor.visit_str(s),
            _ => self.deserialize_map(visitor),
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let len = util::decode_vi64(&mut *self.input)?;
        if len == 0 {
            return visitor.visit_none();
        }
        let value = visitor.visit_seq(ValueDeserializer::new(len as usize, self))?;
        Ok(value)
    }

    // Tuples look just like sequences in JSON. Some formats may be able to
    // represent tuples more efficiently.
    //
    // As indicated by the length parameter, the `Deserialize` implementation
    // for a tuple in the Serde data model is required to know the length of the
    // tuple before even looking at the input data.
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    // Tuple structs look just like sequences in JSON.
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    // Much like `deserialize_seq` but calls the visitors `visit_map` method
    // with a `MapAccess` implementation, rather than the visitor's `visit_seq`
    // method with a `SeqAccess` implementation.
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let len = util::decode_vi64(&mut *self.input)?;
        if len == 0 {
            return visitor.visit_none();
        }
        let value = visitor.visit_map(ValueDeserializer::new(len as usize, self))?;
        Ok(value)
    }

    // Structs look just like maps in JSON.
    //
    // Notice the `fields` parameter - a "struct" in the Serde data model means
    // that the `Deserialize` implementation is required to know what the fields
    // are before even looking at the input data. Any key-value pairing in which
    // the fields cannot be known ahead of time is probably a map.
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    // An identifier in Serde is the type that identifies a field of a struct or
    // the variant of an enum. In JSON, struct fields and enum variants are
    // represented as strings. In other formats they may be represented as
    // numeric indices.
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    // Like `deserialize_any` but indicates to the `Deserializer` that it makes
    // no difference which `Visitor` method is called because the data is
    // ignored.
    //
    // Some deserializers are able to implement this more efficiently than
    // `deserialize_any`, for example by rapidly skipping over matched
    // delimiters without paying close attention to the data in between.
    //
    // Some formats are not able to implement this at all. Formats that can
    // implement `deserialize_any` and `deserialize_ignored_any` are known as
    // self-describing.
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }
}

struct ValueDeserializer<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    len: usize,
    first: bool,
}

impl<'a, 'de> ValueDeserializer<'a, 'de> {
    fn new(len: usize, de: &'a mut Deserializer<'de>) -> Self {
        ValueDeserializer {
            de,
            len,
            first: true,
        }
    }
}

impl<'de, 'a> SeqAccess<'de> for ValueDeserializer<'a, 'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        let len = self.len;
        // Check if there are no more elements.
        if len == 0 {
            return Ok(None);
        }

        self.first = false;
        self.len = len - 1;
        // Deserialize an array element.
        seed.deserialize(&mut *self.de).map(Some)
    }
}

impl<'de, 'a> MapAccess<'de> for ValueDeserializer<'a, 'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        let len = self.len;
        // Check if there are no more elements.
        if len == 0 {
            return Ok(None);
        }

        self.first = false;
        self.len = len - 1;
        // Deserialize a map key.
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        // Deserialize a map value.
        seed.deserialize(&mut *self.de)
    }
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;

    use super::{
        super::ser::{serialize_len, to_bytes_mut},
        *,
    };

    #[test]
    fn test_struct() {
        #[derive(Serialize, Deserialize, PartialEq)]
        struct Test {
            int: u32,
            #[serde(with = "serde_bytes")]
            seq: Vec<u8>,
        }

        let test = Test {
            int: 1,
            seq: b"hello".to_vec(),
        };

        let size = serialize_len(&test).unwrap();
        println!("size={size}");
        let ret = to_bytes_mut(&test, size);
        assert!(ret.is_ok());
        let mut buf: BytesMut = ret.unwrap();
        assert_eq!(35, buf.len());
        let ret: Result<Test> = from_bytes_mut(&mut buf);
        //        assert!(ret.is_ok());
        let ret: Test = ret.unwrap();
        assert_eq!(1, ret.int);
        assert_eq!(b"hello".to_vec(), ret.seq);
    }
}
