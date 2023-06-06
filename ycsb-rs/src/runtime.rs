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

use obkv::runtime;

use crate::properties::Properties;

/// OBKV Table Runtime
#[derive(Clone, Debug)]
pub struct ObYCSBRuntimes {
    /// Default runtime tasks
    pub default_runtime: runtime::RuntimeRef,
    /// Runtime for block_on
    pub block_runtime: runtime::RuntimeRef,
}

fn build_runtime(name: &str, threads_num: usize) -> runtime::Runtime {
    runtime::Builder::default()
        .worker_threads(threads_num)
        .thread_name(name)
        .enable_all()
        .build()
        .expect("Failed to create runtime")
}

pub fn build_ycsb_runtimes(props: Arc<Properties>) -> ObYCSBRuntimes {
    ObYCSBRuntimes {
        default_runtime: Arc::new(build_runtime("ycsb-default", props.ycsb_thread_num)),
        block_runtime: Arc::new(build_runtime("ycsb-block", 1)),
    }
}
