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
use prometheus_client::encoding::{EncodeLabelSet};
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use prometheus_client::metrics::histogram;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RpcMiscLabels {
    pub misc_type: String,
}

pub struct RpcMetrics {
    rpc_operation_duration: Family<RpcMiscLabels, histogram::Histogram>,
    rpc_misc: Family<RpcMiscLabels, histogram::Histogram>,
}

impl Default for RpcMetrics {
    fn default() -> Self {
        RpcMetrics {
            rpc_operation_duration: Family::<RpcMiscLabels, histogram::Histogram>::new_with_constructor(|| {
                histogram::Histogram::new(histogram::exponential_buckets(0.0005, 2.0, 8))
            }),
            rpc_misc: Family::<RpcMiscLabels, histogram::Histogram>::new_with_constructor(|| {
                histogram::Histogram::new(histogram::linear_buckets(5.0, 5.0, 10))
            }),
        }
    }
}

impl RpcMetrics {
    pub fn register(&self, registry: &mut Registry) {
        let sub_registry = registry.sub_registry_with_prefix("rpc");
        sub_registry.register(
            "system operation duration ",
            "RPC system operation latency (duration time) in seconds.",
            self.rpc_operation_duration.clone(),
        );
        sub_registry.register(
            "system miscellaneous counter histogram ",
            "RPC system miscellaneous counter histogram.",
            self.rpc_misc.clone(),
        );
    }

    pub fn observe_rpc_duration(&self, misc_type: &str, duration: Duration) {
        self.rpc_operation_duration
            .get_or_create(&RpcMiscLabels { misc_type : misc_type.to_string() })
            .observe(duration.as_secs_f64());
    }

    pub fn get_rpc_operation_duration(&self) -> &Family<RpcMiscLabels, histogram::Histogram> {
        &self.rpc_operation_duration
    }

    pub fn observe_rpc_misc(&self, misc_type: &str, times: f64) {
        self.rpc_misc
            .get_or_create(&RpcMiscLabels { misc_type : misc_type.to_string() })
            .observe(times);
    }

    pub fn get_rpc_misc(&self) -> &Family<RpcMiscLabels, histogram::Histogram> {
        &self.rpc_misc
    }
}
