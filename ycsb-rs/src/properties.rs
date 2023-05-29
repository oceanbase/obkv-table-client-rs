use serde::Deserialize;

fn zero_u64() -> u64 {
    0
}

fn thread_count_default() -> u64 {
    200
}

fn field_length_distribution_default() -> String {
    "constant".to_string()
}

fn request_distribution_default() -> String {
    "uniform".to_string()
}

fn field_length_default() -> u64 {
    100
}

fn read_proportion_default() -> f64 {
    0.95
}

fn update_proportion_default() -> f64 {
    0.95
}

fn insert_proportion_default() -> f64 {
    0.0
}

fn scan_proportion_default() -> f64 {
    0.0
}

fn read_modify_write_proportion_default() -> f64 {
    0.0
}

fn full_user_name_default() -> String {
    "FULL_USER_NAME".to_string()
}

fn param_url_default() -> String {
    "PARAM_URL".to_string()
}

fn test_password_default() -> String {
    "TEST_PASSWORD".to_string()
}

fn test_sys_user_name_default() -> String {
    "TEST_SYS_USER_NAME".to_string()
}

fn test_sys_password_default() -> String {
    "TEST_SYS_PASSWORD".to_string()
}

fn obkv_client_reuse_default() -> usize {
    10
}

fn rpc_connect_timeout_default() -> u64 {
    5000
}

fn rpc_read_timeout_default() -> u64 {
    3000
}

fn rpc_login_timeout_default() -> u64 {
    3000
}

fn rpc_operation_timeout_default() -> u64 {
    3000
}

fn rpc_retry_limit_default() -> usize {
    3
}

fn rpc_retry_interval_default() -> u64 {
    0
}

fn refresh_workers_num_default() -> usize {
    3
}

fn max_conns_per_server_default() -> usize {
    1
}

fn min_idle_conns_per_server_default() -> usize {
    1
}

fn conn_init_thread_num_default() -> usize {
    2
}

fn conn_reader_thread_num_default() -> usize {
    6
}

fn conn_writer_thread_num_default() -> usize {
    4
}

#[derive(Deserialize, Debug)]
pub struct Properties {
    #[serde(default = "zero_u64", rename = "insertstart")]
    pub insert_start: u64,
    #[serde(default = "zero_u64", rename = "insertcount")]
    pub insert_count: u64,
    #[serde(rename = "operationcount")]
    pub operation_count: u64,
    #[serde(default = "zero_u64", rename = "recordcount")]
    pub record_count: u64,
    #[serde(default = "thread_count_default", rename = "threacount")]
    pub thread_count: u64,
    #[serde(rename = "maxexecutiontime")]
    pub max_execution_time: Option<u64>,
    #[serde(rename = "warmuptime")]
    pub warmup_time: Option<u64>,
    // field length
    #[serde(
        default = "field_length_distribution_default",
        rename = "fieldlengthdistribution"
    )]
    pub field_length_distribution: String,
    #[serde(
        default = "request_distribution_default",
        rename = "requestdistribution"
    )]
    pub request_distribution: String,
    #[serde(default = "field_length_default", rename = "fieldlength")]
    pub field_length: u64,

    // read, update, insert, scan, read-modify-write
    #[serde(default = "read_proportion_default", rename = "readproportion")]
    pub read_proportion: f64,
    #[serde(default = "update_proportion_default", rename = "updateproportion")]
    pub update_proportion: f64,
    #[serde(default = "insert_proportion_default", rename = "insertproportion")]
    pub insert_proportion: f64,
    #[serde(default = "scan_proportion_default", rename = "scanproportion")]
    pub scan_proportion: f64,
    #[serde(
        default = "read_modify_write_proportion_default",
        rename = "readmodifywriteproportion"
    )]
    pub read_modify_write_proportion: f64,

    #[serde(default = "full_user_name_default", rename = "full_user_name")]
    pub full_user_name: String,
    #[serde(default = "param_url_default", rename = "param_url")]
    pub param_url: String,
    #[serde(default = "test_password_default", rename = "test_password")]
    pub test_password: String,
    #[serde(default = "test_sys_user_name_default", rename = "test_sys_user_name")]
    pub test_sys_user_name: String,
    #[serde(default = "test_sys_password_default", rename = "test_sys_password")]
    pub test_sys_password: String,

    // OBKV Client Config
    #[serde(default = "obkv_client_reuse_default", rename = "obkv_client_reuse")]
    pub obkv_client_reuse: usize,
    #[serde(
        default = "rpc_connect_timeout_default",
        rename = "rpc_connect_timeout"
    )]
    pub rpc_connect_timeout: u64,
    #[serde(default = "rpc_read_timeout_default", rename = "rpc_read_timeout")]
    pub rpc_read_timeout: u64,
    #[serde(default = "rpc_login_timeout_default", rename = "rpc_login_timeout")]
    pub rpc_login_timeout: u64,
    #[serde(
        default = "rpc_operation_timeout_default",
        rename = "rpc_operation_timeout"
    )]
    pub rpc_operation_timeout: u64,
    #[serde(default = "rpc_retry_limit_default", rename = "rpc_retry_limit")]
    pub rpc_retry_limit: usize,
    #[serde(default = "rpc_retry_interval_default", rename = "rpc_retry_interval")]
    pub rpc_retry_interval: u64,
    #[serde(
        default = "refresh_workers_num_default",
        rename = "refresh_workers_num"
    )]
    pub refresh_workers_num: usize,
    #[serde(
        default = "max_conns_per_server_default",
        rename = "max_conns_per_server"
    )]
    pub max_conns_per_server: usize,
    #[serde(
        default = "min_idle_conns_per_server_default",
        rename = "min_idle_conns_per_server"
    )]
    pub min_idle_conns_per_server: usize,
    #[serde(
        default = "conn_init_thread_num_default",
        rename = "conn_init_thread_num"
    )]
    pub conn_init_thread_num: usize,
    #[serde(
        default = "conn_reader_thread_num_default",
        rename = "conn_reader_thread_num"
    )]
    pub conn_reader_thread_num: usize,
    #[serde(
        default = "conn_writer_thread_num_default",
        rename = "conn_writer_thread_num"
    )]
    pub conn_writer_thread_num: usize,
}
