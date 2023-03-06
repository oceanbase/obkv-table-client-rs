/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

#[allow(unused_imports)]
#[allow(unused)]
mod utils;

use obkv::{Table, Value};
use test_log::test;

// ```sql
// CREATE TABLE TEST_HBASE_HASH(
//   K bigint,
//   Q varbinary(256),
//   T bigint,
//   V varbinary(1024),
//   PRIMARY KEY (K, Q, T)
// ) partition by hash(K) partitions 16;
// ```
#[test]
fn test_obtable_partition_hash_crud() {
    let client = utils::common::build_hbase_client();
    const TEST_TABLE: &str = "TEST_HBASE_HASH";

    let rowk_keys = vec![
        Value::from(9i64),
        Value::from("partition"),
        Value::from(1550225864000i64),
    ];
    let result = client.delete(TEST_TABLE, rowk_keys.clone());
    assert!(result.is_ok());

    let result = client.insert(
        TEST_TABLE,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("aa")],
    );
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client.get(
        TEST_TABLE,
        rowk_keys.clone(),
        vec!["V".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("V").unwrap();
    assert!(value.is_bytes());
    assert_eq!("aa".to_owned().into_bytes(), value.as_bytes());

    let result = client.update(
        TEST_TABLE,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("bb")],
    );

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client.get(
        TEST_TABLE,
        rowk_keys.clone(),
        vec!["V".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("V").unwrap();
    assert!(value.is_bytes());
    assert_eq!("bb".to_owned().into_bytes(), value.as_bytes());
}

// ```sql
//  CREATE TABLE `TEST_HBASE_PARTITION` (
//    `K` varbinary(1024) NOT NULL,
//    `Q` varbinary(256) NOT NULL,
//    `T` bigint(20) NOT NULL,
//    `V` varbinary(1024) DEFAULT NULL,
//    PRIMARY KEY (`K`, `Q`, `T`)
//  ) DEFAULT CHARSET = utf8mb4 COLLATE UTF8MB4_BIN COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3  BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(k) partitions 15;
// ```
#[test]
fn test_obtable_partition_key_varbinary_crud() {
    let client = utils::common::build_hbase_client();
    const TEST_TABLE: &str = "TEST_HBASE_PARTITION";

    // same as java sdk, when k = partitionKey, after get_partition(&table_entry,
    // row_key) part_id = 2
    let rowk_keys = vec![
        Value::from("partitionKey"),
        Value::from("partition"),
        Value::from(1550225864000i64),
    ];
    let result = client.delete(TEST_TABLE, rowk_keys.clone());
    result.unwrap();
    // assert!(result.is_ok());

    let result = client.insert(
        TEST_TABLE,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("aa")],
    );
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client.get(
        TEST_TABLE,
        rowk_keys.clone(),
        vec!["V".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("V").unwrap();
    assert!(value.is_bytes());
    assert_eq!("aa".to_owned().into_bytes(), value.as_bytes());

    let result = client.update(
        TEST_TABLE,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("bb")],
    );

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client.get(
        TEST_TABLE,
        rowk_keys.clone(),
        vec!["V".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("V").unwrap();
    assert!(value.is_bytes());
    assert_eq!("bb".to_owned().into_bytes(), value.as_bytes());
}

// The crate sql of the test table:
// ```sql
// CREATE TABLE `TEST_HBASE_PARTITION` (
//   `K` varchar(1024) NOT NULL,
//   `Q` varbinary(256) NOT NULL,
//   `T` bigint(20) NOT NULL,
//   `V` varbinary(1024) DEFAULT NULL,
//   PRIMARY KEY (`K`, `Q`, `T`)
// ) DEFAULT CHARSET = utf8mb4 COLLATE UTF8MB4_BIN COMPRESSION = 'lz4_1.0'
// REPLICA_NUM = 3  BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE =
// 134217728 PCTFREE = 10 partition by key(k) partitions 15;
// ```
#[test]
fn test_obtable_partition_key_varchar_crud() {
    let client = utils::common::build_hbase_client();
    const TABLE_NAME: &str = "TEST_HBASE_PARTITION";

    // same as java sdk, when k = partitionKey2, part_id = 9
    let rowk_keys = vec![
        Value::from("partitionKey2"),
        Value::from("partition"),
        Value::from(1550225864000i64),
    ];
    let result = client.delete(TABLE_NAME, rowk_keys.clone());
    result.unwrap();
    // assert!(result.is_ok());

    let result = client.insert(
        TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("aa")],
    );
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client.get(
        TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("V").unwrap();
    assert!(value.is_bytes());
    assert_eq!("aa".to_owned().into_bytes(), value.as_bytes());

    let result = client.update(
        TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("bb")],
    );

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client.get(
        TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("V").unwrap();
    assert!(value.is_bytes());
    assert_eq!("bb".to_owned().into_bytes(), value.as_bytes());
}

// ```sql
// create table TEST_HBASE_RANGE(
//   K varbinary(1024) NOT NULL,
//   Q varbinary(256),
//   T bigint,
//   V varbinary(1024),
// primary key(K, Q, T)) DEFAULT CHARSET = utf8mb4 COLLATE UTF8MB4_BIN COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3  BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// PARTITION BY RANGE columns (K) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);
// ```
#[test]
fn test_obtable_partition_range_crud() {
    let client = utils::common::build_hbase_client();
    const TABLE_NAME: &str = "TEST_HBASE_RANGE";

    let rowk_keys = vec![
        Value::from("partitionKey"),
        Value::from("partition"),
        Value::from(1550225864000i64),
    ];
    let result = client.delete(TABLE_NAME, rowk_keys.clone());
    result.unwrap();
    //        assert!(result.is_ok());

    let result = client.insert(
        TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("aa")],
    );
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client.get(
        TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("V").unwrap();
    assert!(value.is_bytes());
    assert_eq!("aa".to_owned().into_bytes(), value.as_bytes());

    let result = client.update(
        TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("bb")],
    );

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client.get(
        TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("V").unwrap();
    assert!(value.is_bytes());
    assert_eq!("bb".to_owned().into_bytes(), value.as_bytes());
}