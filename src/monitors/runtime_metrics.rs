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
    encoding::EncodeLabelSet,
    metrics::{family::Family, gauge},
    registry::Registry,
};

use crate::monitors::prometheus::OBKV_CLIENT_REGISTRY;

lazy_static! {
    pub static ref OBKV_RUNTIME_GAUGE_METRICS: RuntimeGaugeMetrics = {
        let runtime_metrics = RuntimeGaugeMetrics::default();
        runtime_metrics.register(&mut OBKV_CLIENT_REGISTRY.lock().unwrap().registry);
        runtime_metrics
    };
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RuntimeNameLabels {
    pub string_type: String,
}

#[derive(Default)]
pub struct RuntimeGaugeMetrics {
    runtime_thread_alive_gauges: Family<RuntimeNameLabels, gauge::Gauge>,
    runtime_thread_idle_gauges: Family<RuntimeNameLabels, gauge::Gauge>,
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

    pub fn on_thread_start(&self, rt_name: &str) {
        self.runtime_thread_alive_gauges
            .get_or_create(&RuntimeNameLabels {
                string_type: rt_name.to_string(),
            })
            .inc();
    }

    pub fn on_thread_stop(&self, rt_name: &str) {
        self.runtime_thread_alive_gauges
            .get_or_create(&RuntimeNameLabels {
                string_type: rt_name.to_string(),
            })
            .dec();
    }

    pub fn on_thread_park(&self, rt_name: &str) {
        self.runtime_thread_idle_gauges
            .get_or_create(&RuntimeNameLabels {
                string_type: rt_name.to_string(),
            })
            .inc();
    }

    pub fn on_thread_unpark(&self, rt_name: &str) {
        self.runtime_thread_idle_gauges
            .get_or_create(&RuntimeNameLabels {
                string_type: rt_name.to_string(),
            })
            .dec();
    }

    pub fn get_runtime_thread_alive_gauges(&self) -> &Family<RuntimeNameLabels, gauge::Gauge> {
        &self.runtime_thread_alive_gauges
    }

    pub fn get_runtime_thread_idle_gauges(&self) -> &Family<RuntimeNameLabels, gauge::Gauge> {
        &self.runtime_thread_idle_gauges
    }
}

/// Runtime metrics.
#[derive(Debug)]
pub struct RuntimeMetrics {
    pub runtime_name: String,
}

impl RuntimeMetrics {
    pub fn new(runtime_name: &str) -> Self {
        Self {
            runtime_name: runtime_name.to_owned(),
        }
    }

    pub fn set_runtime_name(&mut self, runtime_name: String) {
        self.runtime_name = runtime_name;
    }

    pub fn on_thread_start(&self) {
        OBKV_RUNTIME_GAUGE_METRICS.on_thread_start(&self.runtime_name);
    }

    pub fn on_thread_stop(&self) {
        OBKV_RUNTIME_GAUGE_METRICS.on_thread_stop(&self.runtime_name);
    }

    pub fn on_thread_park(&self) {
        OBKV_RUNTIME_GAUGE_METRICS.on_thread_park(&self.runtime_name);
    }

    pub fn on_thread_unpark(&self) {
        OBKV_RUNTIME_GAUGE_METRICS.on_thread_unpark(&self.runtime_name);
    }

    pub fn get_runtime_thread_alive_gauges(&self) -> i64 {
        OBKV_RUNTIME_GAUGE_METRICS
            .get_runtime_thread_alive_gauges()
            .get_or_create(&RuntimeNameLabels {
                string_type: self.runtime_name.clone(),
            })
            .get()
    }

    pub fn get_runtime_thread_idle_gauges(&self) -> i64 {
        OBKV_RUNTIME_GAUGE_METRICS
            .get_runtime_thread_idle_gauges()
            .get_or_create(&RuntimeNameLabels {
                string_type: self.runtime_name.clone(),
            })
            .get()
    }
}
