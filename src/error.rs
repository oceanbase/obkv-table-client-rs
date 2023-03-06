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
    io,
    num::ParseIntError,
    str::Utf8Error,
    string::{FromUtf8Error, ParseError},
};

use futures::Canceled as FutureCanceled;

use crate::{rpc::protocol::codes::ResultCodes, serde_obkv};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        IO(e: io::Error) {
            from()
            description("IO error")
            display("IO error, err:{}", e)
            cause(e)
        }
        ParseInt(e: ParseIntError) {
            from()
            description("ParseIntError error")
            display("Fail to parse integer, err:{}", e)
            cause(e)
        }
        FromUtf8(e: FromUtf8Error) {
            from()
            description("FromUtf8 error")
            display("FromUtf8 error, err:{}", e)
            cause(e)
        }
        Common(code: CommonErrCode, descr: String) {
            description(descr)
            display("Common error, code:{:?}, err:{}", code, descr)
        }
        FieldType {
            description("Field type error")
            display("Field type error")
        }
        Canceled(e: FutureCanceled) {
            from()
            description("Future canceled error")
            display("Future canceled error, err:{}", e)
            cause(e)
        }
        Utf8Error(e: Utf8Error) {
            from()
            description("UTF-8 encoding error")
            display("UTF-8 encoding error, err:{}", e)
            cause(e)
        }
        ParseError(e: ParseError) {
            from()
            description("Parsing string error")
            display("Parsing string error, err:{}", e)
            cause(e)
        }
        SerdeObkv(e: serde_obkv::Error) {
            from()
            description("Serialize/Deserialize OBKV protocol error")
            display("Serialize/Deserialize error, err:{}", e)
            cause(e)
        }
        Json(e: serde_json::Error) {
            from()
            description("Serialize/Deserialize json error")
            display("Fail to encode/decode json error, err:{}", e)
            cause(e)
        }
        Http(e: reqwest::Error) {
            from()
            description("Http request error")
            display("Http request error, err:{}", e)
            cause(e)
        }
        MySql(e: mysql::Error) {
            from()
            description("Mysql error")
            display("Query database error, err:{}", e)
            cause(e)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CommonErrCode {
    InvalidParam,
    NotFound,
    Rpc,
    ConnPool,
    MPSC,
    BrokenPipe,
    InvalidServerAddr,
    OcpError,
    NotInitialized,
    AlreadyClosed,
    PartitionError,
    ObException(ResultCodes),
    Lock,
    PermitDenied,
    ConvertFailed,
}

impl Error {
    /// Returns true when the error is an ob exception.
    pub fn is_ob_exception(&self) -> bool {
        matches!(self, Error::Common(CommonErrCode::ObException(_), _))
    }

    // Returns true when the error is common error
    pub fn is_common_err(&self) -> bool {
        matches!(self, Error::Common(_, _))
    }

    // Return the common error code if it's a common error, otherwise return None.
    pub fn common_err_code(&self) -> Option<CommonErrCode> {
        if let Error::Common(code, _) = self {
            Some(*code)
        } else {
            None
        }
    }

    /// Returns the result code of ob exception, return none if it's not an ob
    /// exception.
    pub fn ob_result_code(&self) -> Option<ResultCodes> {
        if let Error::Common(CommonErrCode::ObException(code), _desc) = self {
            Some(*code)
        } else {
            None
        }
    }

    pub fn need_retry(&self) -> bool {
        if let Error::Common(CommonErrCode::ObException(code), _) = self {
            code.need_retry()
        } else {
            false
        }
    }

    pub fn need_refresh_table(&self) -> bool {
        if let Error::Common(CommonErrCode::ObException(code), _) = self {
            code.need_refresh_table()
        } else {
            false
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn need_refresh() {
        let err = Error::Common(
            CommonErrCode::ObException(ResultCodes::OB_NOT_MASTER),
            "test_err".to_owned(),
        );
        assert!(err.need_refresh_table());
    }
}
