/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

use std::{
    borrow::Cow,
    error,
    fmt::{self, Display},
    str,
};

use serde::{de, ser};

pub type Result<T> = std::result::Result<T, Error>;

//Referer https://github.com/nox/serde_urlencoded/blob/master/src/ser/mod.rs

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Custom(Cow<'static, str>),
    Utf8(str::Utf8Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Custom(ref msg) => msg.fmt(f),
            Error::Utf8(ref err) => write!(f, "invalid UTF-8: {}", err),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Custom(_) => None,
            Error::Utf8(err) => Some(err),
        }
    }

    /// The lower-level cause of this error, in the case of a `Utf8` error.
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::Custom(_) => None,
            Error::Utf8(ref err) => Some(err),
        }
    }
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Custom(format!("{}", msg).into())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Custom(format!("{}", msg).into())
    }
}

use std::io;

impl From<Error> for io::Error {
    fn from(e: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, format!("cause: {}", e))
    }
}
