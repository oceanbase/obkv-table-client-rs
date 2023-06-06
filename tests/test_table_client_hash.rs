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

pub mod test_table_client_base;
#[allow(unused_imports)]
#[allow(unused)]
mod utils;

use obkv::Value;
use serial_test_derive::serial;
use tokio::task;

// ```sql
// CREATE TABLE `TEST_VARCHAR_TABLE_HASH_CONCURRENT` (
//     `c1` bigint(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by hash(c1) partitions 16;
// ```
#[tokio::test]
#[serial]
async fn test_concurrent() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_VARCHAR_TABLE_HASH_CONCURRENT";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_bigint_concurrent(TABLE_NAME).await;
    test.clean_bigint_table(TABLE_NAME).await;
}

// ```sql
// CREATE TABLE `TEST_TABLE_BATCH_HASH` (
// `c1` bigint NOT NULL,
// `c1sk` varchar(20) NOT NULL,
// `c2` varchar(20) DEFAULT NULL,
// PRIMARY KEY (`c1`, `c1sk`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by hash(`c1`) partitions 16;
// ```
#[tokio::test]
async fn test_obtable_client_hash() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_TABLE_BATCH_HASH";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string(), "c1sb".to_string()]);

    let test_key0: i64 = 100;
    let test_key1: i64 = 200;
    // insert some data
    let mut batch_op = client.batch_operation(4);
    batch_op.delete(vec![Value::from(test_key0), Value::from("subKey_0")]);
    batch_op.delete(vec![Value::from(test_key1), Value::from("subKey_1")]);
    batch_op.insert(
        vec![Value::from(test_key0), Value::from("subKey_0")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_0")],
    );
    batch_op.insert(
        vec![Value::from(test_key1), Value::from("subKey_1")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_1")],
    );
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());
}
