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

use std::sync::Arc;

use super::{conn_pool::ConnPool, protocol::ObPayload};
use crate::{
    error::Result,
    monitors::{prometheus::OBKV_CLIENT_REGISTRY, proxy_metrics::ProxyMetrics},
};

lazy_static! {
    pub static ref OBKV_PROXY_METRICS: ProxyMetrics = {
        let proxy_metrics = ProxyMetrics::default();
        proxy_metrics.register(&mut OBKV_CLIENT_REGISTRY.lock().unwrap().registry);
        proxy_metrics
    };
}

#[derive(Clone)]
pub struct Proxy(Arc<ConnPool>);

impl Proxy {
    pub fn new(conn_pool: Arc<ConnPool>) -> Self {
        Proxy(conn_pool)
    }

    pub fn execute<T: ObPayload, R: ObPayload>(
        &self,
        payload: &mut T,
        response: &mut R,
    ) -> Result<()> {
        // the connection is ensured to be active now by checking conn.is_active
        // but it may be actually broken already.
        let conn = self.0.get()?;

        OBKV_PROXY_METRICS.observe_proxy_misc("conn_load", conn.load() as f64);

        let res = conn.execute(payload, response);
        if res.is_ok() || conn.is_active() {
            return res;
        }

        let mut retry_cnt = 0;
        // retry until all the idle connections are consumed and then a brand new
        // connection is built or an intact connection is taken because all the
        // connections may be broken together
        let retry_limit = self.0.idle_conn_num() + 1;

        OBKV_PROXY_METRICS.observe_proxy_misc("retry_idle_conns", (retry_limit - 1) as f64);

        let mut err = res.err().unwrap();

        loop {
            retry_cnt += 1;
            if retry_cnt > retry_limit {
                OBKV_PROXY_METRICS.observe_proxy_misc("retry_times", retry_cnt as f64);
                error!(
                    "Proxy::execute reach the retry limit:{}, err:{}",
                    retry_limit, err
                );
                return Err(err);
            }
            debug!(
                "Proxy::execute retry {} because connection broken, err:{}",
                retry_cnt, err
            );

            let conn = self.0.get()?;
            let res = conn.execute(payload, response);
            if res.is_ok() || conn.is_active() {
                OBKV_PROXY_METRICS.observe_proxy_misc("retry_times", retry_cnt as f64);
                return res;
            }
            err = res.err().unwrap();
        }
    }
}
