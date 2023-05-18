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

use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{gauge, family::Family},
    registry::Registry,
};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum ObClientRuntimeGaugeType {
    Default = 0,
    ConnWriter = 1,
    ConnReader = 2,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RuntimeThreadLabels {
    pub rt_type: ObClientRuntimeGaugeType,
}

pub struct RuntimeGaugeMetrics {
    runtime_thread_alive_gauges: Family<RuntimeThreadLabels, gauge::Gauge>,
    runtime_thread_idle_gauges: Family<RuntimeThreadLabels, gauge::Gauge>,
}

impl Default for RuntimeGaugeMetrics {
    fn default() -> Self {
        RuntimeGaugeMetrics {
            runtime_thread_alive_gauges: Default::default(),
            runtime_thread_idle_gauges: Default::default(),
        }
    }
}

impl RuntimeGaugeMetrics {
    pub fn register(&self, registry: &mut Registry) {
        let sub_registry = registry.sub_registry_with_prefix("runtime");
        sub_registry.register(
            "Runtime Alive Threads Gauges ",
            "Client alive threads num.",
            self.runtime_thread_alive_gauges.clone(),
        );
        sub_registry.register(
            "Runtime Idle Threads Gauges ",
            "Client idle threads num.",
            self.runtime_thread_idle_gauges.clone(),
        );
    }

    pub fn on_thread_start(
        &self,
        rt_type: ObClientRuntimeGaugeType,
    ) {
        self.runtime_thread_alive_gauges
            .get_or_create(&RuntimeThreadLabels { rt_type })
            .inc();
    }

    pub fn on_thread_stop(
        &self,
        rt_type: ObClientRuntimeGaugeType,
    ) {
        self.runtime_thread_alive_gauges
            .get_or_create(&RuntimeThreadLabels { rt_type })
            .dec();
    }

    pub fn on_thread_park(
        &self,
        rt_type: ObClientRuntimeGaugeType,
    ) {
        self.runtime_thread_alive_gauges
            .get_or_create(&RuntimeThreadLabels { rt_type })
            .inc();
    }

    pub fn on_thread_unpark(
        &self,
        rt_type: ObClientRuntimeGaugeType,
    ) {
        self.runtime_thread_alive_gauges
            .get_or_create(&RuntimeThreadLabels { rt_type })
            .dec();
    }

    pub fn get_runtime_thread_alive_gauges(&self) -> &Family<RuntimeThreadLabels, gauge::Gauge> {
        &self.runtime_thread_alive_gauges
    }

    pub fn get_runtime_thread_idle_gauges(&self) -> &Family<RuntimeThreadLabels, gauge::Gauge> {
        &self.runtime_thread_idle_gauges
    }
}
