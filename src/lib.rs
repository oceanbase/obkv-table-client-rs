// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

extern crate bytes;
extern crate chrono;
extern crate crossbeam;
extern crate futures;
extern crate mysql;
#[macro_use]
extern crate serde;
extern crate serde_bytes;
extern crate tokio_codec;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate quick_error;
extern crate rand;
extern crate reqwest;
extern crate serde_json;
#[macro_use]
extern crate log;
extern crate crypto;
extern crate fasthash;
extern crate futures_cpupool;
#[macro_use]
extern crate lazy_static;
extern crate prometheus;
extern crate r2d2;
extern crate scheduled_thread_pool;
extern crate spin;
extern crate uuid;
extern crate zstd;

#[macro_use]
mod macros;
pub mod client;
mod constant;
pub mod error;
mod location;
mod rpc;
pub mod serde_obkv;
mod util;
pub use self::{
    client::{
        query::{QueryResultSet, TableQuery},
        table::ObTable,
        table_client::{Builder, ObTableClient, RunningMode},
        ClientConfig, Table, TableOpResult,
    },
    rpc::protocol::{codes::ResultCodes, payloads, query},
    serde_obkv::value::{ObjType, Value},
};
