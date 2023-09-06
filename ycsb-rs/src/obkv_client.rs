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

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
#[allow(unused)]
use obkv::error::CommonErrCode;
use obkv::{Builder, ClientConfig, ObTableClient, RunningMode, TableOpResult, Value};

use crate::properties::Properties;

const PRIMARY_KEY: &str = "ycsb_key";

pub struct OBKVClientInitStruct {
    pub full_user_name: String,
    pub param_url: String,
    pub password: String,
    pub sys_user_name: String,
    pub sys_password: String,

    pub rpc_connect_timeout: Duration,
    pub rpc_read_timeout: Duration,
    pub rpc_operation_timeout: Duration,
    pub rpc_login_timeout: Duration,
    pub rpc_retry_limit: usize,
    pub rpc_retry_interval: Duration,

    pub refresh_workers_num: usize,

    pub max_conns_per_server: usize,
    pub min_idle_conns_per_server: usize,

    pub bg_thread_num: usize,
    pub tcp_recv_thread_num: usize,
    pub tcp_send_thread_num: usize,
}

impl OBKVClientInitStruct {
    pub fn new(props: &Properties) -> Self {
        OBKVClientInitStruct {
            full_user_name: props.full_user_name.clone(),
            param_url: props.param_url.clone(),
            password: props.test_password.clone(),
            sys_user_name: props.test_sys_user_name.clone(),
            sys_password: props.test_sys_password.clone(),
            rpc_connect_timeout: Duration::from_millis(props.rpc_connect_timeout),
            rpc_read_timeout: Duration::from_millis(props.rpc_read_timeout),
            rpc_operation_timeout: Duration::from_millis(props.rpc_operation_timeout),
            rpc_login_timeout: Duration::from_millis(props.rpc_login_timeout),
            rpc_retry_limit: props.rpc_retry_limit,
            rpc_retry_interval: Duration::from_millis(props.rpc_retry_interval),
            refresh_workers_num: props.refresh_workers_num,
            max_conns_per_server: props.max_conns_per_server,
            min_idle_conns_per_server: props.min_idle_conns_per_server,
            bg_thread_num: props.bg_thread_num,
            tcp_recv_thread_num: props.tcp_recv_thread_num,
            tcp_send_thread_num: props.tcp_send_thread_num,
        }
    }
}

pub struct OBKVClient {
    client: ObTableClient,
}

impl OBKVClient {
    pub fn build_client(config: Arc<OBKVClientInitStruct>, mode: RunningMode) -> Result<Self> {
        let client_config = ClientConfig {
            rpc_connect_timeout: config.rpc_connect_timeout,
            rpc_read_timeout: config.rpc_read_timeout,
            rpc_operation_timeout: config.rpc_operation_timeout,
            rpc_login_timeout: config.rpc_login_timeout,
            rpc_retry_limit: config.rpc_retry_limit,
            rpc_retry_interval: config.rpc_retry_interval,
            refresh_workers_num: config.refresh_workers_num,
            max_conns_per_server: config.max_conns_per_server,
            min_idle_conns_per_server: config.min_idle_conns_per_server,
            bg_thread_num: config.bg_thread_num,
            tcp_recv_thread_num: config.tcp_recv_thread_num,
            tcp_send_thread_num: config.tcp_send_thread_num,
            ..Default::default()
        };
        let builder = Builder::new()
            .config(client_config)
            .full_user_name(config.full_user_name.as_str())
            .param_url(config.param_url.as_str())
            .running_mode(mode)
            .password(config.password.as_str())
            .sys_user_name(config.sys_user_name.as_str())
            .sys_password(config.sys_password.as_str());

        let client = builder.build();

        assert!(client.is_ok());

        let client = client.unwrap();
        client.init().expect("Fail to create obkv client.");

        client.add_row_key_element("usertable", vec![PRIMARY_KEY.to_string()]);

        Ok(OBKVClient { client })
    }

    pub fn build_normal_client(config: Arc<OBKVClientInitStruct>) -> Result<Self> {
        Self::build_client(config, RunningMode::Normal)
    }

    pub fn build_hbase_client(config: Arc<OBKVClientInitStruct>) -> Result<Self> {
        Self::build_client(config, RunningMode::HBase)
    }

    pub fn init(&self) -> Result<()> {
        Ok(())
    }

    pub async fn insert(
        &self,
        table: &str,
        key: &str,
        values: &HashMap<&str, String>,
    ) -> Result<()> {
        let mut columns: Vec<String> = Vec::new();
        let mut properties: Vec<Value> = Vec::new();
        for (key, value) in values {
            columns.push(key.parse()?);
            properties.push(Value::from(value.to_owned()));
        }
        let result = self
            .client
            .insert(
                table,
                vec![Value::from(key.to_owned())],
                columns,
                properties,
            )
            .await
            .expect("fail to insert_or update");
        assert_eq!(1, result);

        Ok(())
    }

    #[allow(unused)]
    pub async fn read(
        &self,
        table: &str,
        key: &str,
        columns: &Vec<String>,
        result: &HashMap<String, String>,
    ) -> Result<()> {
        let result = self
            .client
            .get(table, vec![Value::from(key)], columns.to_owned())
            .await;
        assert!(result.is_ok());
        assert_eq!(10, result?.len());

        Ok(())
    }

    pub async fn update(
        &self,
        table: &str,
        key: &str,
        values: &HashMap<&str, String>,
    ) -> Result<()> {
        let mut columns: Vec<String> = Vec::new();
        let mut properties: Vec<Value> = Vec::new();
        for (key, value) in values {
            columns.push(key.parse()?);
            properties.push(Value::from(value.to_owned()));
        }
        let result = self
            .client
            .update(
                table,
                vec![Value::from(key.to_owned())],
                columns,
                properties,
            )
            .await
            .expect("fail to insert or update");
        assert_eq!(10, result);

        Ok(())
    }

    #[allow(unused)]
    pub async fn scan(
        &self,
        table: &str,
        startkey: &str,
        endkey: &str,
        columns: &Vec<String>,
        result: &HashMap<String, String>,
    ) -> Result<()> {
        let query = self
            .client
            .query(table)
            .select(columns.to_owned())
            .primary_index()
            .add_scan_range(
                vec![Value::from(startkey)],
                true,
                vec![Value::from(endkey)],
                true,
            );
        let result = query.execute().await;
        assert!(result.is_ok());
        Ok(())
    }

    #[allow(unused)]
    pub async fn batch_read(
        &self,
        table: &str,
        keys: &Vec<String>,
        columns: &Vec<String>,
        result: &HashMap<String, String>,
    ) -> Result<()> {
        let mut batch_op = self.client.batch_operation(keys.len());
        for key in keys {
            batch_op.get(vec![Value::from(key.to_owned())], columns.to_owned());
        }
        let results = self.client.execute_batch(table, batch_op).await;

        // Verify the results
        assert!(results.is_ok());
        let results = results.unwrap();
        assert_eq!(results.len(), keys.len());
        for result in results {
            match result {
                TableOpResult::RetrieveRows(rows) => {
                    assert_eq!(10, rows.len());
                }
                _ => {
                    unreachable!()
                }
            }
        }

        Ok(())
    }

    #[allow(unused)]
    pub async fn batch_insertup(
        &self,
        table: &str,
        keys: &Vec<String>,
        fields: &Vec<String>,
        values: &Vec<String>,
    ) -> Result<()> {
        let mut batch_op = self.client.batch_operation(keys.len());
        for key in keys {
            let mut properties: Vec<Value> = Vec::new();
            for value in values {
                properties.push(Value::from(value.to_owned()));
            }
            batch_op.insert_or_update(
                vec![Value::from(key.to_owned())],
                fields.to_owned(),
                properties,
            );
        }
        let results = self.client.execute_batch(table, batch_op).await;

        // Verify the results
        if results.is_err() {
            println!("Error: {:?}", results.as_ref().err());
        }
        assert!(results.is_ok());
        let results = results.unwrap();
        assert_eq!(results.len(), keys.len());
        for result in results {
            match result {
                TableOpResult::AffectedRows(affected_rows) => {
                    assert_eq!(1, affected_rows);
                }
                _ => {
                    unreachable!()
                }
            }
        }

        Ok(())
    }
}
