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

use std::{collections::HashMap, fmt::Formatter, sync::Arc, time::Duration};

use super::{
    query::{QueryResultSet, QueryStreamResult, StreamQuerier, TableQuery},
    ClientConfig, Table, TableOpResult,
};
use crate::{
    error::{CommonErrCode, Error::Common as CommonErr, Result},
    rpc::{
        protocol::{
            codes::ResultCodes,
            payloads::*,
            query::{
                ObHTableFilter, ObNewRange, ObScanOrder, ObTableQuery, ObTableQueryRequest,
                ObTableQueryResult, ObTableStreamRequest,
            },
            ObPayload,
        },
        proxy::Proxy,
    },
    serde_obkv::value::Value,
};

#[derive(Clone)]
pub struct ObTable {
    config: ClientConfig,
    ip: String,
    port: i32,

    tenant_name: String,
    user_name: String,
    database: String,
    rpc_proxy: Proxy,
}

impl std::fmt::Debug for ObTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObTable")
            .field("ip", &self.ip)
            .field("port", &self.port)
            .field("tenant", &self.tenant_name)
            .field("username", &self.user_name)
            .field("database", &self.database);
        Ok(())
    }
}

impl ObTable {
    pub fn execute_payload<T: ObPayload, R: ObPayload>(
        &self,
        payload: &mut T,
        result: &mut R,
    ) -> Result<()> {
        self.rpc_proxy.execute(payload, result)?;
        Ok(())
    }

    pub fn query(&self, table_name: &str) -> impl TableQuery {
        ObTableQueryImpl::new(table_name, Arc::new(self.clone()))
    }

    pub fn operation_timeout(&self) -> Duration {
        self.config.rpc_operation_timeout
    }

    fn execute(
        &self,
        table_name: &str,
        operation_type: ObTableOperationType,
        row_keys: Vec<Value>,
        columns: Option<Vec<String>>,
        properties: Option<Vec<Value>>,
    ) -> Result<ObTableOperationResult> {
        let mut payload = ObTableOperationRequest::new(
            table_name,
            operation_type,
            row_keys,
            columns,
            properties,
            self.config.rpc_operation_timeout,
            self.config.log_level_flag,
        );
        let mut result = ObTableOperationResult::new();
        self.execute_payload(&mut payload, &mut result)?;
        Ok(result)
    }
}

pub struct Builder {
    config: ClientConfig,
    ip: String,
    port: i32,

    tenant_name: String,
    user_name: String,
    password: String,
    database: String,
    rpc_proxy: Option<Proxy>,
}

impl Builder {
    pub fn new(ip: &str, port: i32) -> Self {
        Builder {
            config: ClientConfig::new(),
            ip: ip.to_owned(),
            port,
            tenant_name: "".to_owned(),
            user_name: "".to_owned(),
            password: "".to_owned(),
            database: "".to_owned(),
            rpc_proxy: None,
        }
    }

    pub fn config(mut self, config: &ClientConfig) -> Self {
        self.config = config.clone();
        self
    }

    pub fn tenant_name(mut self, s: &str) -> Self {
        self.tenant_name = s.to_owned();
        self
    }

    pub fn user_name(mut self, s: &str) -> Self {
        self.user_name = s.to_owned();
        self
    }

    pub fn password(mut self, s: &str) -> Self {
        self.password = s.to_owned();
        self
    }

    pub fn database(mut self, s: &str) -> Self {
        self.database = s.to_owned();
        self
    }

    pub fn rpc_proxy(mut self, rpc_proxy: Proxy) -> Self {
        self.rpc_proxy = Some(rpc_proxy);
        self
    }

    pub fn build(self) -> ObTable {
        assert!(self.rpc_proxy.is_some(), "missing necessary rpc proxy");
        ObTable {
            config: self.config,
            ip: self.ip,
            port: self.port,
            tenant_name: self.tenant_name,
            user_name: self.user_name,
            database: self.database,
            rpc_proxy: self.rpc_proxy.unwrap(),
        }
    }
}

// TODO: Table has no retry for any operation
impl Table for ObTable {
    fn insert(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64> {
        Ok(self
            .execute(
                table_name,
                ObTableOperationType::Insert,
                row_keys,
                Some(columns),
                Some(properties),
            )?
            .affected_rows())
    }

    fn update(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64> {
        Ok(self
            .execute(
                table_name,
                ObTableOperationType::Update,
                row_keys,
                Some(columns),
                Some(properties),
            )?
            .affected_rows())
    }

    fn insert_or_update(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64> {
        Ok(self
            .execute(
                table_name,
                ObTableOperationType::InsertOrUpdate,
                row_keys,
                Some(columns),
                Some(properties),
            )?
            .affected_rows())
    }

    fn replace(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64> {
        Ok(self
            .execute(
                table_name,
                ObTableOperationType::Replace,
                row_keys,
                Some(columns),
                Some(properties),
            )?
            .affected_rows())
    }

    fn append(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64> {
        Ok(self
            .execute(
                table_name,
                ObTableOperationType::Append,
                row_keys,
                Some(columns),
                Some(properties),
            )?
            .affected_rows())
    }

    fn increment(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64> {
        Ok(self
            .execute(
                table_name,
                ObTableOperationType::Increment,
                row_keys,
                Some(columns),
                Some(properties),
            )?
            .affected_rows())
    }

    fn delete(&self, table_name: &str, row_keys: Vec<Value>) -> Result<i64> {
        Ok(self
            .execute(table_name, ObTableOperationType::Del, row_keys, None, None)?
            .affected_rows())
    }

    fn get(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
    ) -> Result<HashMap<String, Value>> {
        Ok(self
            .execute(
                table_name,
                ObTableOperationType::Get,
                row_keys,
                Some(columns),
                None,
            )?
            .take_entity()
            .take_properties())
    }

    fn batch_operation(&self, ops_num_hint: usize) -> ObTableBatchOperation {
        ObTableBatchOperation::with_ops_num(ops_num_hint)
    }

    fn execute_batch(
        &self,
        _table_name: &str,
        batch_op: ObTableBatchOperation,
    ) -> Result<Vec<TableOpResult>> {
        let mut payload = ObTableBatchOperationRequest::new(
            batch_op,
            self.config.rpc_operation_timeout,
            self.config.log_level_flag,
        );
        let mut result = ObTableBatchOperationResult::new();

        self.rpc_proxy.execute(&mut payload, &mut result)?;

        result.into()
    }
}

impl From<ObTableBatchOperationResult> for Result<Vec<TableOpResult>> {
    fn from(batch_result: ObTableBatchOperationResult) -> Result<Vec<TableOpResult>> {
        let op_results = batch_result.take_op_results();
        let mut results = Vec::with_capacity(op_results.len());
        for op_res in op_results {
            let error_no = op_res.header().errorno();
            let result_code = ResultCodes::from_i32(error_no);

            if result_code == ResultCodes::OB_SUCCESS {
                let table_op_result = if op_res.operation_type() == ObTableOperationType::Get {
                    TableOpResult::RetrieveRows(op_res.take_entity().take_properties())
                } else {
                    TableOpResult::AffectedRows(op_res.affected_rows())
                };
                results.push(table_op_result);
            } else {
                return Err(CommonErr(
                    CommonErrCode::ObException(result_code),
                    format!("OBKV server return exception in batch response: {op_res:?}."),
                ));
            }
        }
        Ok(results)
    }
}

struct ObTableStreamQuerier;

impl ObTableStreamQuerier {
    pub fn new() -> ObTableStreamQuerier {
        ObTableStreamQuerier {}
    }
}

impl StreamQuerier for ObTableStreamQuerier {
    fn execute_query(
        &self,
        stream_result: &mut QueryStreamResult,
        (part_id, ob_table): (i64, Arc<ObTable>),
        payload: &mut ObTableQueryRequest,
    ) -> Result<i64> {
        let mut result = ObTableQueryResult::new();
        ob_table.rpc_proxy.execute(payload, &mut result)?;
        let row_count = result.row_count();
        stream_result.cache_stream_next((part_id, ob_table), result);
        Ok(row_count)
    }

    fn execute_stream(
        &self,
        stream_result: &mut QueryStreamResult,
        (part_id, ob_table): (i64, Arc<ObTable>),
        payload: &mut ObTableStreamRequest,
    ) -> Result<i64> {
        let mut result = ObTableQueryResult::new();
        let is_stream_next = payload.is_stream_next();
        ob_table.rpc_proxy.execute(payload, &mut result)?;
        let row_count = result.row_count();
        if is_stream_next {
            stream_result.cache_stream_next((part_id, ob_table), result);
        }
        Ok(row_count)
    }
}

pub struct ObTableQueryImpl {
    operation_timeout: Option<Duration>,
    entity_type: ObTableEntityType,
    table_name: String,
    table: Arc<ObTable>,
    table_query: ObTableQuery,
}

impl ObTableQueryImpl {
    pub fn new(table_name: &str, table: Arc<ObTable>) -> Self {
        Self {
            operation_timeout: None,
            entity_type: ObTableEntityType::Dynamic,
            table_name: table_name.to_owned(),
            table,
            table_query: ObTableQuery::new(),
        }
    }

    fn reset(&mut self) {
        //FIXME table query should set partition_id
        self.table_query = ObTableQuery::new();
    }
}

impl TableQuery for ObTableQueryImpl {
    fn execute(&self) -> Result<QueryResultSet> {
        let mut partition_table: HashMap<i64, (i64, Arc<ObTable>)> = HashMap::new();
        partition_table.insert(0, (0, self.table.clone()));

        self.table_query.verify()?;

        let mut stream_result = QueryStreamResult::new(
            Arc::new(ObTableStreamQuerier::new()),
            self.table_query.clone(),
        );

        stream_result.set_entity_type(self.entity_type());
        stream_result.set_table_name(&self.table_name);
        stream_result.set_expectant(partition_table);
        stream_result.set_operation_timeout(self.operation_timeout);
        stream_result.set_flag(self.table.config.log_level_flag);
        stream_result.init()?;

        Ok(QueryResultSet::from_stream_result(stream_result))
    }

    fn get_table_name(&self) -> String {
        self.table_name.to_owned()
    }

    fn set_entity_type(&mut self, entity_type: ObTableEntityType) {
        self.entity_type = entity_type;
    }

    fn entity_type(&self) -> ObTableEntityType {
        self.entity_type
    }

    fn select(mut self, columns: Vec<String>) -> Self
    where
        Self: Sized,
    {
        self.table_query.select_columns(columns);
        self
    }

    fn limit(mut self, offset: Option<i32>, limit: i32) -> Self
    where
        Self: Sized,
    {
        if let Some(v) = offset {
            self.table_query.set_offset(v);
        }
        self.table_query.set_limit(limit);
        self
    }

    fn add_scan_range(
        mut self,
        start: Vec<Value>,
        start_equals: bool,
        end: Vec<Value>,
        end_equals: bool,
    ) -> Self
    where
        Self: Sized,
    {
        let mut range = ObNewRange::from_keys(start, end);
        if start_equals {
            range.set_inclusive_start();
        } else {
            range.unset_inclusive_start();
        }

        if end_equals {
            range.set_inclusive_end();
        } else {
            range.unset_inclusive_end();
        }

        self.table_query.add_key_range(range);
        self
    }

    fn add_scan_range_starts_with(mut self, start: Vec<Value>, start_equals: bool) -> Self
    where
        Self: Sized,
    {
        let mut end = Vec::with_capacity(start.len());

        for _ in 0..start.len() {
            end.push(Value::get_max());
        }

        let mut range = ObNewRange::from_keys(start, end);

        if start_equals {
            range.set_inclusive_start();
        } else {
            range.unset_inclusive_start();
        }

        self.table_query.add_key_range(range);
        self
    }

    fn add_scan_range_ends_with(mut self, end: Vec<Value>, end_equals: bool) -> Self
    where
        Self: Sized,
    {
        let mut start = Vec::with_capacity(end.len());

        for _ in 0..end.len() {
            start.push(Value::get_min());
        }

        let mut range = ObNewRange::from_keys(start, end);

        if end_equals {
            range.set_inclusive_end();
        } else {
            range.unset_inclusive_end();
        }

        self.table_query.add_key_range(range);
        self
    }

    fn scan_order(mut self, forward: bool) -> Self
    where
        Self: Sized,
    {
        self.table_query
            .set_scan_order(ObScanOrder::from_bool(forward));
        self
    }

    fn index_name(mut self, index_name: &str) -> Self
    where
        Self: Sized,
    {
        self.table_query.set_index_name(index_name.to_owned());
        self
    }

    fn filter_string(mut self, filter_string: &str) -> Self
    where
        Self: Sized,
    {
        self.table_query.set_filter_string(filter_string.to_owned());
        self
    }

    fn htable_filter(mut self, filter: ObHTableFilter) -> Self
    where
        Self: Sized,
    {
        self.table_query.set_htable_filter(filter);
        self
    }

    fn batch_size(mut self, batch_size: i32) -> Self
    where
        Self: Sized,
    {
        self.table_query.set_batch_size(batch_size);
        self
    }

    fn operation_timeout(mut self, timeout: Duration) -> Self
    where
        Self: Sized,
    {
        self.operation_timeout = Some(timeout);
        self
    }

    fn clear(&mut self) {
        self.reset();
    }
}
