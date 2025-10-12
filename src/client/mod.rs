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

use std::{collections::HashMap, time::Duration};

use crate::{rpc::protocol::DEFAULT_FLAG, serde_obkv::value::Value};

pub mod filter;
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

/// ObTable client config
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientConfig {
    pub sys_user_name: String,
    pub sys_password: String,

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
    pub query_concurrency_limit: Option<usize>,

    pub tcp_recv_thread_num: usize,
    pub tcp_send_thread_num: usize,
    pub bg_thread_num: usize,

    pub max_inflight_reqs_per_conn: usize,

    pub log_level_flag: u16,

    // Deprecated fields for backward compatibility
    /// Deprecated: Connection initialization thread number (no longer used, kept for compatibility)
    #[deprecated(note = "This field is no longer used. Use tcp_recv_thread_num, tcp_send_thread_num, and bg_thread_num instead.")]
    pub conn_init_thread_num: usize,

    /// Deprecated: Table batch operation thread number (no longer used, kept for compatibility)
    #[deprecated(note = "This field is no longer used. Batch operations now use bg_thread_num.")]
    pub table_batch_op_thread_num: usize,
}

impl Default for ClientConfig {
    fn default() -> ClientConfig {
        ClientConfig {
            sys_user_name: "".to_owned(),
            sys_password: "".to_owned(),
            metadata_mysql_conn_pool_min_size: 1,
            metadata_mysql_conn_pool_max_size: 3,
            metadata_refresh_interval: Duration::from_secs(3),
            ocp_model_cache_file: "/tmp/ocp_model_cache.json".to_owned(),

            rslist_acquire_timeout: Duration::from_secs(10),
            rslist_acquire_try_times: 3,
            rslist_acquire_retry_interval: Duration::from_millis(100),

            table_entry_acquire_connect_timeout: Duration::from_secs(3),
            table_entry_acquire_read_timeout: Duration::from_secs(3),
            table_entry_refresh_interval_base: Duration::from_secs(60),
            table_entry_refresh_interval_ceiling: Duration::from_secs(120),
            table_entry_refresh_try_times: 3,
            table_entry_refresh_try_interval: Duration::from_millis(20),
            table_entry_refresh_continuous_failure_ceiling: 10,

            server_address_priority_timeout: Duration::from_secs(1800),
            runtime_continuous_failure_ceiling: 10,

            rpc_connect_timeout: Duration::from_secs(3),
            rpc_read_timeout: Duration::from_secs(3),
            rpc_login_timeout: Duration::from_secs(3),
            rpc_operation_timeout: Duration::from_secs(3),
            rpc_retry_limit: 3,
            rpc_retry_interval: Duration::from_millis(500),

            refresh_workers_num: 5,

            max_conns_per_server: 10,
            min_idle_conns_per_server: 5,
            query_concurrency_limit: None,

            tcp_recv_thread_num: 4,
            tcp_send_thread_num: 2,
            bg_thread_num: 2,

            max_inflight_reqs_per_conn: 100,

            log_level_flag: DEFAULT_FLAG,

            // Deprecated fields - set to reasonable defaults
            conn_init_thread_num: 2,
            table_batch_op_thread_num: 16,
        }
    }
}

impl ClientConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a ClientConfig from legacy configuration (without new fields)
    /// This is for backward compatibility when upgrading from older versions
    #[allow(clippy::too_many_arguments)]
    pub fn from_legacy(
        sys_user_name: String,
        sys_password: String,
        metadata_mysql_conn_pool_min_size: usize,
        metadata_mysql_conn_pool_max_size: usize,
        metadata_refresh_interval: Duration,
        ocp_model_cache_file: String,
        rslist_acquire_timeout: Duration,
        rslist_acquire_try_times: usize,
        rslist_acquire_retry_interval: Duration,
        table_entry_acquire_connect_timeout: Duration,
        table_entry_acquire_read_timeout: Duration,
        table_entry_refresh_interval_base: Duration,
        table_entry_refresh_interval_ceiling: Duration,
        table_entry_refresh_try_times: usize,
        table_entry_refresh_try_interval: Duration,
        table_entry_refresh_continuous_failure_ceiling: usize,
        server_address_priority_timeout: Duration,
        runtime_continuous_failure_ceiling: usize,
        rpc_connect_timeout: Duration,
        rpc_read_timeout: Duration,
        rpc_operation_timeout: Duration,
        rpc_login_timeout: Duration,
        rpc_retry_limit: usize,
        rpc_retry_interval: Duration,
        refresh_workers_num: usize,
        max_conns_per_server: usize,
        min_idle_conns_per_server: usize,
        query_concurrency_limit: Option<usize>,
        log_level_flag: u16,
        // Deprecated fields
        conn_init_thread_num: usize,
        table_batch_op_thread_num: usize,
    ) -> Self {
        let defaults = Self::default();
        Self {
            sys_user_name,
            sys_password,
            metadata_mysql_conn_pool_min_size,
            metadata_mysql_conn_pool_max_size,
            metadata_refresh_interval,
            ocp_model_cache_file,
            rslist_acquire_timeout,
            rslist_acquire_try_times,
            rslist_acquire_retry_interval,
            table_entry_acquire_connect_timeout,
            table_entry_acquire_read_timeout,
            table_entry_refresh_interval_base,
            table_entry_refresh_interval_ceiling,
            table_entry_refresh_try_times,
            table_entry_refresh_try_interval,
            table_entry_refresh_continuous_failure_ceiling,
            server_address_priority_timeout,
            runtime_continuous_failure_ceiling,
            rpc_connect_timeout,
            rpc_read_timeout,
            rpc_operation_timeout,
            rpc_login_timeout,
            rpc_retry_limit,
            rpc_retry_interval,
            refresh_workers_num,
            max_conns_per_server,
            min_idle_conns_per_server,
            query_concurrency_limit,
            log_level_flag,
            conn_init_thread_num,
            table_batch_op_thread_num,
            // Use defaults for new fields
            tcp_recv_thread_num: defaults.tcp_recv_thread_num,
            tcp_send_thread_num: defaults.tcp_send_thread_num,
            bg_thread_num: defaults.bg_thread_num,
            max_inflight_reqs_per_conn: defaults.max_inflight_reqs_per_conn,
        }
    }
}

/// Synchronous client wrapper for backward compatibility
/// This ensures all operations are synchronous by wrapping ObTableClient
pub struct SyncObTableClient {
    inner: crate::client::table_client::ObTableClient,
}

impl SyncObTableClient {
    /// Create a synchronous client from an async ObTableClient
    pub fn new(client: crate::client::table_client::ObTableClient) -> Self {
        Self { inner: client }
    }

    /// Get the inner async client if needed
    pub fn into_inner(self) -> crate::client::table_client::ObTableClient {
        self.inner
    }

    /// Get a reference to the inner async client
    pub fn inner(&self) -> &crate::client::table_client::ObTableClient {
        &self.inner
    }

    // ========== P0 Methods (High Priority) ==========

    /// Add row key element for a table
    /// Must be called before using the table
    pub fn add_row_key_element(&self, table_name: &str, row_key_names: Vec<String>) {
        self.inner.add_row_key_element(table_name, row_key_names)
    }

    /// Execute SQL statement (DDL operations)
    /// Used for creating/dropping tables, etc.
    pub fn execute_sql(&self, sql: &str) -> crate::error::Result<()> {
        self.inner.execute_sql(sql)
    }

    /// Create a synchronous query object
    /// Returns a query builder with synchronous execute() method
    pub fn query(&self, table_name: &str) -> crate::client::table_client::SyncObTableClientQueryImpl {
        crate::client::table_client::SyncObTableClientQueryImpl::new_from_client(&self.inner, table_name)
    }

    // ========== P1 Methods (Medium Priority) ==========

    /// Check if the client is initialized
    /// Used for health checks and connection status
    pub fn is_initialized(&self) -> bool {
        self.inner.is_initialized()
    }

    /// Check if a table exists
    /// Used for table management and pre-creation checks
    pub fn check_table_exists(&self, table_name: &str) -> crate::error::Result<bool> {
        self.inner.check_table_exists(table_name)
    }

    /// Invalidate local cached table metadata
    /// Used for cache synchronization after table structure changes
    pub fn invalidate_table(&self, table_name: &str) {
        self.inner.invalidate_table(table_name)
    }

    // ========== P2 Methods (Low Priority) ==========

    /// Truncate all data in a table
    /// Primarily used for testing and data cleanup
    pub fn truncate_table(&self, table_name: &str) -> crate::error::Result<()> {
        self.inner.truncate_table(table_name)
    }
}

impl Clone for SyncObTableClient {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl AsRef<crate::client::table_client::ObTableClient> for SyncObTableClient {
    fn as_ref(&self) -> &crate::client::table_client::ObTableClient {
        &self.inner
    }
}

impl std::ops::Deref for SyncObTableClient {
    type Target = crate::client::table_client::ObTableClient;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Table trait for backward compatibility
/// This trait provides synchronous methods for table operations
pub trait Table {
    /// Insert a record
    fn insert(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> crate::error::Result<i64>;

    /// Update a record
    fn update(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> crate::error::Result<i64>;

    /// Insert or update a record, if the record exists, update it.
    /// Otherwise insert a new one.
    fn insert_or_update(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> crate::error::Result<i64>;

    /// Replace a record.
    fn replace(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> crate::error::Result<i64>;

    /// Append
    fn append(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> crate::error::Result<i64>;

    /// Increment
    fn increment(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> crate::error::Result<i64>;

    /// Delete records by row keys.
    fn delete(&self, table_name: &str, row_keys: Vec<Value>) -> crate::error::Result<i64>;

    /// Retrieve a record by row keys.
    fn get(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
    ) -> crate::error::Result<HashMap<String, Value>>;

    /// Create a batch operation
    fn batch_operation(&self, ops_num_hint: usize) -> crate::rpc::protocol::payloads::ObTableBatchOperation;

    /// Execute a batch operation
    fn execute_batch(
        &self,
        table_name: &str,
        batch_op: crate::rpc::protocol::payloads::ObTableBatchOperation,
    ) -> crate::error::Result<Vec<TableOpResult>>;
}
