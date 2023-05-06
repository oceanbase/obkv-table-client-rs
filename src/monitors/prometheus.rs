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

use std::sync::Mutex;

use prometheus_client::{encoding::text::encode, registry::Registry};

lazy_static! {
    pub static ref OBKV_CLIENT_REGISTRY: Mutex<ObClientRegistry> =
        Mutex::new(ObClientRegistry::default());
}

#[derive(Default)]
pub struct ObClientRegistry {
    pub registry: Registry,
}

impl ObClientRegistry {
    pub fn print_registry(&self) {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).unwrap();
        println!("{buffer}");
    }
}
