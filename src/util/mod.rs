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

use std::{
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Duration,
};

use bytes::BytesMut;
use chrono::Utc;

use crate::serde_obkv::value::{ObjType, Value};

pub mod permit;
pub mod security;

#[inline]
pub fn current_time_millis() -> i64 {
    Utc::now().timestamp_millis()
}

#[inline]
pub fn assert_not_empty(s: &str, msg: &'static str) {
    assert!(!s.is_empty(), "{}", msg);
}

#[inline]
pub fn duration_to_millis(duration: &Duration) -> i64 {
    duration.as_secs() as i64 * 1000
}

#[inline]
pub fn millis_to_secs(t: i64) -> i64 {
    t / 1000
}

// A handy shortcut to replace `RwLock` write/read().unwrap() pattern to
// shortcut wl and rl
// From: https://github.com/tikv/tikv/blob/master/src/util/mod.rs
pub trait HandyRwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<T>;

    fn rl(&self) -> RwLockReadGuard<T>;
}

impl<T> HandyRwLock<T> for RwLock<T> {
    #[inline]
    fn wl(&self) -> RwLockWriteGuard<T> {
        self.write().unwrap()
    }

    #[inline]
    fn rl(&self) -> RwLockReadGuard<T> {
        self.read().unwrap()
    }
}

pub fn string_from_bytes(bs: &[u8]) -> String {
    if bs.is_empty() {
        return "".to_owned();
    }
    match String::from_utf8(bs[0..bs.len() - 1].to_vec()) {
        Ok(s) => s,
        Err(_e) => "FromUtf8 Error".to_owned(),
    }
}

#[inline]
pub fn decode_value(src: &mut BytesMut) -> std::result::Result<Value, std::io::Error> {
    let obj_type = ObjType::from_u8(*src.first().unwrap())?;
    Ok(Value::decode(src, obj_type)?)
}
