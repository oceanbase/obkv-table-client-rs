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

// TODO(xikai): it seems a bug of rust procedural macro that the linter cannot
// see the expanded statements. check the referenced issue: https://github.com/rust-lang/rust/issues/73556
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

#[allow(unused)]
use obkv::error::CommonErrCode;
use obkv::{Builder, ObTableClient, RunningMode, Table};

// TODO: use test conf to control which environments to test.
const TEST_FULL_USER_NAME: &str = "test";
const TEST_URL: &str = "127.0.0.1";
const TEST_PASSWORD: &str = "test";
const TEST_SYS_USER_NAME: &str = "";
const TEST_SYS_PASSWORD: &str = "";

pub fn build_client(mode: RunningMode) -> ObTableClient {
    let builder = Builder::new()
        .full_user_name(TEST_FULL_USER_NAME)
        .param_url(TEST_URL)
        .running_mode(mode)
        .password(TEST_PASSWORD)
        .sys_user_name(TEST_SYS_USER_NAME)
        .sys_password(TEST_SYS_PASSWORD);

    let client = builder.build();

    assert!(client.is_ok());

    let client = client.unwrap();
    client.init().expect("Fail to create obkv client.");
    client
}

pub fn build_hbase_client() -> ObTableClient {
    build_client(RunningMode::HBase)
}

pub fn build_normal_client() -> ObTableClient {
    build_client(RunningMode::Normal)
}
