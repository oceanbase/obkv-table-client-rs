// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use prometheus::*;

lazy_static! {
    pub static ref OBKV_CLIENT_RETRY_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "obkv_client_retry_total",
        "Total number of do retrying",
        &["type"]
    )
    .unwrap();
}
