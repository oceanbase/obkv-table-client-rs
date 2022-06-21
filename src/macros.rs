// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

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
