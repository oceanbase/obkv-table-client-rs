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
pub struct ProxyMiscLabels {
    pub misc_type: String,
}

pub struct ProxyMetrics {
    proxy_misc: Family<ProxyMiscLabels, histogram::Histogram>,
    conn_pool: Family<ProxyMiscLabels, histogram::Histogram>,
}

impl Default for ProxyMetrics {
    fn default() -> Self {
        ProxyMetrics {
            proxy_misc: Family::<ProxyMiscLabels, histogram::Histogram>::new_with_constructor(|| {
                histogram::Histogram::new(histogram::linear_buckets(5.0, 10.0, 5))
            }),
            conn_pool: Family::<ProxyMiscLabels, histogram::Histogram>::new_with_constructor(|| {
                histogram::Histogram::new(histogram::exponential_buckets(0.0001, 2.0, 5))
            }),
        }
    }
}

impl ProxyMetrics {
    pub fn register(&self, registry: &mut Registry) {
        let sub_registry = registry.sub_registry_with_prefix("proxy");
        sub_registry.register(
            "system miscellaneous counter histogram ",
            "Proxy system miscellaneous counter histogram.",
            self.proxy_misc.clone(),
        );
        sub_registry.register(
            "system connection pool histogram ",
            "Proxy system connection pool histogram.",
            self.conn_pool.clone(),
        );
    }

    pub fn observe_proxy_misc(&self, misc_type: &str, times: f64) {
        self.proxy_misc
            .get_or_create(&ProxyMiscLabels { misc_type : misc_type.to_string() })
            .observe(times);
    }

    pub fn get_proxy_misc(&self) -> &Family<ProxyMiscLabels, histogram::Histogram> {
        &self.proxy_misc
    }

    pub fn observe_conn_pool_duration(&self, misc_type: &str, duration: Duration) {
        self.conn_pool
            .get_or_create(&ProxyMiscLabels { misc_type : misc_type.to_string() })
            .observe(duration.as_secs_f64());
    }

    pub fn get_conn_pool(&self) -> &Family<ProxyMiscLabels, histogram::Histogram> {
        &self.conn_pool
    }
}
