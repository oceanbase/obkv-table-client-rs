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
        }
    }
}

impl ClientConfig {
    pub fn new() -> Self {
        Self::default()
    }
}
