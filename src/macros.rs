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

#[macro_export]
macro_rules! box_future_try {
    ($expr:expr) => {{
        match $expr {
            Ok(r) => r,
            Err(e) => return Box::new(::futures::future::err(e)),
        }
    }};
}

#[macro_export]
macro_rules! box_stream_try {
    ($expr:expr) => {{
        match $expr {
            Ok(r) => r,
            Err(e) => {
                use futures::Future;
                return Box::new(::futures::future::err(e).into_stream());
            }
        }
    }};
}

#[macro_export]
macro_rules! fatal {
    ($msg:expr, $($arg:tt)+) => ({
        error!($msg, $($arg)+);
        panic!($msg, $($arg)+);
    })
}
