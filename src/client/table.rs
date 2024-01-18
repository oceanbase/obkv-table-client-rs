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

use std::{fmt::Formatter, time::Duration};

use super::{ClientConfig, TableOpResult};
use crate::{
    error::{CommonErrCode, Error::Common as CommonErr, Result},
    location::OB_INVALID_ID,
    payloads::ObTableOperationType::InsertOrUpdate,
    rpc::{
        protocol::{codes::ResultCodes, lsop::*, payloads::*, ObPayload},
        proxy::Proxy,
    },
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
    /// execute partition payload
    pub async fn execute_payload<T: ObPayload, R: ObPayload>(
        &self,
        payload: &mut T,
        result: &mut R,
    ) -> Result<()> {
        self.rpc_proxy.execute(payload, result).await?;
        Ok(())
    }

    pub fn operation_timeout(&self) -> Duration {
        self.config.rpc_operation_timeout
    }

    /// Execute batch operation
    pub async fn execute_batch(
        &self,
        _table_name: &str,
        mut batch_op: ObTableBatchOperation,
    ) -> Result<Vec<TableOpResult>> {
        // check Log Stream Operation
        if !batch_op.get_filters().is_empty() {
            // check filters is all exist
            if batch_op.ops_len() != batch_op.get_filters().len() {
                return Err(CommonErr(
                    CommonErrCode::InvalidParam,
                    "All operation should have filters or not (checkAndDo() can not do with other operation in batch now)".to_owned(),
                ));
            }

            // check operation type
            for op in batch_op.get_ops() {
                if op.get_type() != InsertOrUpdate {
                    return Err(CommonErr(
                        CommonErrCode::InvalidParam,
                        "All operations should be InsertOrUpdate if we use filter now".to_owned(),
                    ));
                }
            }

            // generate ObTableTabletOp from batch operation
            let mut tablet_op = batch_op.generate_tablet_ops();
            tablet_op.set_table_id(batch_op.table_id());
            tablet_op.set_partition_id(batch_op.partition_id());

            // construct ObTableLSOperation
            let mut ls_option = ObTableLSOpFlag::default();
            ls_option.set_flag_is_same_type(true);
            let ls_op = ObTableLSOperation::internal_new(OB_INVALID_ID, ls_option, vec![tablet_op]);

            let mut payload = ObTableLSOpRequest::new(
                ls_op,
                self.config.rpc_operation_timeout,
                self.config.log_level_flag,
            );

            let mut result = ObTableLSOpResult::new();

            self.rpc_proxy.execute(&mut payload, &mut result).await?;

            // we just return the ans in the order of input
            result.into()
        } else {
            let mut payload = ObTableBatchOperationRequest::new(
                batch_op,
                self.config.rpc_operation_timeout,
                self.config.log_level_flag,
            );
            let mut result = ObTableBatchOperationResult::new();

            self.rpc_proxy.execute(&mut payload, &mut result).await?;

            result.into()
        }
    }

    /// return addr
    pub fn addr(&self) -> String {
        format!("{}:{}", self.ip, self.port)
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

fn process_op_results(op_results: Vec<ObTableOperationResult>) -> Result<Vec<TableOpResult>> {
    let mut results = Vec::with_capacity(op_results.len());
    for op_res in op_results {
        let error_no = op_res.header().errorno();
        let result_code = ResultCodes::from_i32(error_no);

        // op error
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

impl From<ObTableBatchOperationResult> for Result<Vec<TableOpResult>> {
    fn from(batch_result: ObTableBatchOperationResult) -> Result<Vec<TableOpResult>> {
        let op_results = batch_result.take_op_results();
        process_op_results(op_results)
    }
}

impl From<ObTableLSOpResult> for Result<Vec<TableOpResult>> {
    fn from(ls_result: ObTableLSOpResult) -> Result<Vec<TableOpResult>> {
        let op_results = ls_result.take_op_results();
        process_op_results(op_results)
    }
}
