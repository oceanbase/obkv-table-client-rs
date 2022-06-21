// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, time::Duration};

use crate::{
    error::Result,
    rpc::protocol::{payloads::ObTableBatchOperation, DEFAULT_FLAG},
    serde_obkv::value::Value,
};

mod metrics;
mod ocp;
pub mod query;
pub mod table;
pub mod table_client;
use self::table::ObTable;

#[derive(Clone, Debug)]
pub enum TableOpResult {
    AffectedRows(i64),
    RetrieveRows(HashMap<String, Value>),
}

pub trait Table {
    /// Insert a record
    fn insert(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64>;

    /// Update a record
    fn update(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64>;

    /// Insert or update a record, if the record exists, update it.
    /// Otherwise insert a new one.
    fn insert_or_update(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64>;

    /// Replace a record.
    fn replace(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64>;

    /// Delete records by row keys.
    fn delete(&self, table_name: &str, row_keys: Vec<Value>) -> Result<i64>;

    /// Retrieve a record by row keys.
    fn get(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
    ) -> Result<HashMap<String, Value>>;

    /// Create a batch operation
    fn batch_operation(&self, ops_num_hint: usize) -> ObTableBatchOperation;
    // Execute a batch operation
    fn execute_batch(
        &self,
        table_name: &str,
        batch_op: ObTableBatchOperation,
    ) -> Result<Vec<TableOpResult>>;
}

/// ObTable client config
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientConfig {
    pub metadata_mysql_conn_pool_min_size: usize,
    pub metadata_mysql_conn_pool_max_size: usize,
    pub metadata_refresh_interval: Duration,
    pub ocp_model_cache_file: String,

    pub rslist_acquire_timeout: Duration,
    pub rslist_acquire_try_times: usize,
    pub rslist_acquire_retry_interval: Duration,

    pub table_entry_acquire_connect_timeout: Duration,
    pub table_entry_acquire_read_timeout: Duration,

    pub table_entry_refresh_interval_base: Duration,
    pub table_entry_refresh_interval_ceiling: Duration,
    pub table_entry_refresh_try_times: usize,
    pub table_entry_refresh_try_interval: Duration,
    pub table_entry_refresh_continuous_failure_ceiling: usize,

    pub table_batch_op_thread_num: usize,

    pub server_address_priority_timeout: Duration,
    pub runtime_continuous_failure_ceiling: usize,

    pub rpc_connect_timeout: Duration,
    pub rpc_read_timeout: Duration,
    pub rpc_operation_timeout: Duration,
    pub rpc_login_timeout: Duration,
    pub rpc_retry_limit: usize,
    pub rpc_retry_interval: Duration,

    pub refresh_workers_num: usize,

    pub max_conns_per_server: usize,
    pub min_idle_conns_per_server: usize,
    pub conn_init_thread_num: usize,
    pub query_concurrency_limit: Option<usize>,

    pub log_level_flag: u16,
}

impl Default for ClientConfig {
    fn default() -> ClientConfig {
        ClientConfig {
            metadata_mysql_conn_pool_min_size: 1,
            metadata_mysql_conn_pool_max_size: 3,
            metadata_refresh_interval: Duration::from_secs(60),
            ocp_model_cache_file: "/tmp/ocp_model_cache.json".to_owned(),

            rslist_acquire_timeout: Duration::from_secs(10),
            rslist_acquire_try_times: 3,
            rslist_acquire_retry_interval: Duration::from_millis(100),

            table_entry_acquire_connect_timeout: Duration::from_secs(5),
            table_entry_acquire_read_timeout: Duration::from_secs(3),
            table_entry_refresh_interval_base: Duration::from_millis(100),
            table_entry_refresh_interval_ceiling: Duration::from_millis(1600),
            table_entry_refresh_try_times: 3,
            table_entry_refresh_try_interval: Duration::from_millis(20),
            table_entry_refresh_continuous_failure_ceiling: 10,

            table_batch_op_thread_num: 16,

            server_address_priority_timeout: Duration::from_secs(1800),
            runtime_continuous_failure_ceiling: 100,

            rpc_connect_timeout: Duration::from_secs(5),
            rpc_read_timeout: Duration::from_secs(3),
            rpc_login_timeout: Duration::from_secs(3),
            rpc_operation_timeout: Duration::from_secs(10),
            rpc_retry_limit: 3,
            rpc_retry_interval: Duration::from_secs(0),

            refresh_workers_num: 5,

            max_conns_per_server: 10,
            min_idle_conns_per_server: 5,
            conn_init_thread_num: 2,
            query_concurrency_limit: None,

            log_level_flag: DEFAULT_FLAG,
        }
    }
}

impl ClientConfig {
    pub fn new() -> Self {
        Self::default()
    }
}
