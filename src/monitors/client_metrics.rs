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

use std::time::Duration;

use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{counter, family::Family, histogram},
    registry::Registry,
};

use crate::payloads::ObTableOperationType;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum ObClientOpRecordType {
    Get = 0,
    Insert = 1,
    Del = 2,
    Update = 3,
    InsertOrUpdate = 4,
    Replace = 5,
    Increment = 6,
    Append = 7,
    Batch = 8,
    Query = 9,
    StreamQuery = 10,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum ObClientOpRetryType {
    Execute = 0,
    ExecuteBatch = 1,
}

impl From<ObTableOperationType> for ObClientOpRecordType {
    fn from(op: ObTableOperationType) -> Self {
        match op {
            ObTableOperationType::Get => ObClientOpRecordType::Get,
            ObTableOperationType::Insert => ObClientOpRecordType::Insert,
            ObTableOperationType::Del => ObClientOpRecordType::Del,
            ObTableOperationType::Update => ObClientOpRecordType::Update,
            ObTableOperationType::InsertOrUpdate => ObClientOpRecordType::InsertOrUpdate,
            ObTableOperationType::Replace => ObClientOpRecordType::Replace,
            ObTableOperationType::Increment => ObClientOpRecordType::Increment,
            ObTableOperationType::Append => ObClientOpRecordType::Append,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OperationLabels {
    pub operation_type: ObClientOpRecordType,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OperationRetryLabels {
    pub operation_retry_type: ObClientOpRetryType,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ClientStringLabels {
    pub string_type: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ClientStreamQueryLabels {
    pub string_type: String,
    pub string_tag: String,
}

pub struct ClientMetrics {
    client_operation_rt: Family<OperationLabels, histogram::Histogram>,
    client_sys_op_rt: Family<ClientStringLabels, histogram::Histogram>,
    client_retry: Family<OperationRetryLabels, counter::Counter>,
    client_misc: Family<ClientStringLabels, histogram::Histogram>,
    client_stream_query_counter: Family<ClientStreamQueryLabels, counter::Counter>,
}

impl Default for ClientMetrics {
    fn default() -> Self {
        ClientMetrics {
            client_operation_rt:
                Family::<OperationLabels, histogram::Histogram>::new_with_constructor(|| {
                    histogram::Histogram::new(histogram::exponential_buckets(0.0005, 2.0, 8))
                }),
            client_sys_op_rt:
                Family::<ClientStringLabels, histogram::Histogram>::new_with_constructor(|| {
                    histogram::Histogram::new(histogram::exponential_buckets(0.001, 2.0, 10))
                }),
            client_retry: Default::default(),
            client_misc: Family::<ClientStringLabels, histogram::Histogram>::new_with_constructor(
                || histogram::Histogram::new(histogram::exponential_buckets(5.0, 2.0, 8)),
            ),
            client_stream_query_counter: Default::default(),
        }
    }
}

impl ClientMetrics {
    pub fn register(&self, registry: &mut Registry) {
        let sub_registry = registry.sub_registry_with_prefix("client");
        sub_registry.register(
            "operation response time ",
            "Client operation latency (response time) in seconds.",
            self.client_operation_rt.clone(),
        );
        sub_registry.register(
            "system operation response time ",
            "Client system operation latency (response time) in seconds.",
            self.client_sys_op_rt.clone(),
        );
        sub_registry.register(
            "operation retry times ",
            "Client operation retry times.",
            self.client_retry.clone(),
        );
        sub_registry.register(
            "system miscellaneous counter histogram ",
            "Client system miscellaneous counter histogram.",
            self.client_misc.clone(),
        );
        sub_registry.register(
            "counter for common use ",
            "Client counter for common use.",
            self.client_stream_query_counter.clone(),
        );
    }

    pub fn observe_operation_opt_rt(
        &self,
        operation_type: ObTableOperationType,
        duration: Duration,
    ) {
        self.client_operation_rt
            .get_or_create(&OperationLabels {
                operation_type: operation_type.into(),
            })
            .observe(duration.as_secs_f64());
    }

    pub fn observe_operation_ort_rt(
        &self,
        operation_type: ObClientOpRecordType,
        duration: Duration,
    ) {
        self.client_operation_rt
            .get_or_create(&OperationLabels { operation_type })
            .observe(duration.as_secs_f64());
    }

    pub fn get_client_operation_rt(&self) -> &Family<OperationLabels, histogram::Histogram> {
        &self.client_operation_rt
    }

    pub fn observe_sys_operation_rt(&self, sys_operation: &str, duration: Duration) {
        self.client_sys_op_rt
            .get_or_create(&ClientStringLabels {
                string_type: sys_operation.to_string(),
            })
            .observe(duration.as_secs_f64());
    }

    pub fn get_client_sys_op_rt(&self) -> &Family<ClientStringLabels, histogram::Histogram> {
        &self.client_sys_op_rt
    }

    pub fn inc_retry_times(&self, retry_type: ObClientOpRetryType) {
        self.client_retry
            .get_or_create(&OperationRetryLabels {
                operation_retry_type: retry_type,
            })
            .inc();
    }

    pub fn inc_by_retry_times(&self, retry_type: ObClientOpRetryType, times: u64) {
        self.client_retry
            .get_or_create(&OperationRetryLabels {
                operation_retry_type: retry_type,
            })
            .inc_by(times);
    }

    pub fn get_client_retry(&self) -> &Family<OperationRetryLabels, counter::Counter> {
        &self.client_retry
    }

    pub fn observe_misc(&self, misc_type: &str, times: f64) {
        self.client_misc
            .get_or_create(&ClientStringLabels {
                string_type: misc_type.to_string(),
            })
            .observe(times);
    }

    pub fn get_client_misc(&self) -> &Family<ClientStringLabels, histogram::Histogram> {
        &self.client_misc
    }

    pub fn inc_stream_query_counter(&self, string_type: &str, string_tag: &str) {
        self.client_stream_query_counter
            .get_or_create(&ClientStreamQueryLabels {
                string_type: string_type.to_string(),
                string_tag: string_tag.to_string(),
            })
            .inc();
    }

    pub fn inc_by_stream_query_counter(&self, string_type: &str, string_tag: &str, times: u64) {
        self.client_stream_query_counter
            .get_or_create(&ClientStreamQueryLabels {
                string_type: string_type.to_string(),
                string_tag: string_tag.to_string(),
            })
            .inc_by(times);
    }

    pub fn get_common_counter(&self) -> &Family<ClientStreamQueryLabels, counter::Counter> {
        &self.client_stream_query_counter
    }
}
