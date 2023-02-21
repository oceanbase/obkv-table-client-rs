// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

#[allow(unused_imports)]
#[allow(unused)]
mod common;

use obkv::{Table, TableQuery, Value};
use serial_test_derive::serial;
use test_log::test;

// TODO: use test conf to control which environments to test.
const TEST_TABLE_NAME: &str = "test";
const TEST_HASH_TABLE_NAME: &str = "test";
const TEST_KEY_VARBINARY_TABLE_NAME: &str = "test";
const TEST_KEY_VARCHAR_TABLE_NAME: &str = "test";
const TEST_RANGE_TABLE_NAME: &str = "test";

#[test]
fn test_execute_sql() {
    let client = common::build_normal_client();

    let create_table =
        format!("create table IF NOT EXISTS test_execute_sql(id int, PRIMARY KEY(id));");
    client
        .execute_sql(&create_table)
        .expect("fail to create table");
}

#[test]
fn test_check_table_exists() {
    let client = common::build_normal_client();

    let test_table_name = "test_check_table_exists";
    let drop_table = format!("drop table IF EXISTS {};", test_table_name);

    client
        .execute_sql(&drop_table)
        .expect("fail to create table");

    let exists = client
        .check_table_exists(test_table_name)
        .expect("fail to check table exists");
    assert!(!exists, "should not exists");

    let create_table = format!(
        "create table IF NOT EXISTS {}(id int, PRIMARY KEY(id));",
        test_table_name
    );

    client
        .execute_sql(&create_table)
        .expect("fail to create table");

    let exists = client
        .check_table_exists(test_table_name)
        .expect("fail to check table exists");
    assert!(exists, "should exists");
}

#[test]
fn test_truncate_table() {
    let client = common::build_normal_client();

    let truncate_once = || {
        client
            .truncate_table(TEST_TABLE_NAME)
            .expect("Fail to truncate first test table");

        let result = client
            .get(
                TEST_TABLE_NAME,
                vec![Value::from("foo")],
                vec!["c2".to_owned()],
            )
            .expect("Fail to get row");
        assert!(result.is_empty());

        let result = client
            .insert(
                TEST_TABLE_NAME,
                vec![Value::from("foo")],
                vec!["c2".to_owned()],
                vec![Value::from("bar")],
            )
            .expect("Fail to insert row");
        assert_eq!(result, 1);

        client
            .truncate_table(TEST_TABLE_NAME)
            .expect("Fail to truncate first test table");

        let result = client
            .get(
                TEST_TABLE_NAME,
                vec![Value::from("foo")],
                vec!["c2".to_owned()],
            )
            .expect("Fail to get row");
        assert!(result.is_empty());
    };
    for _ in 0..1 {
        truncate_once();
    }
}

#[test]
fn test_obtable_client_curd() {
    let client = common::build_normal_client();

    let result = client.insert(
        TEST_TABLE_NAME,
        vec![Value::from("foo")],
        vec!["c2".to_owned()],
        vec![Value::from("bar")],
    );
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client.get(
        TEST_TABLE_NAME,
        vec![Value::from("foo")],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("c2").unwrap();
    assert!(value.is_string());
    assert_eq!("bar", value.as_string());

    let result = client.update(
        TEST_TABLE_NAME,
        vec![Value::from("foo")],
        vec!["c2".to_owned()],
        vec![Value::from("car")],
    );

    let query = client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .add_scan_range(vec![Value::get_min()], true, vec![Value::get_max()], true);

    let result_set = query.execute();

    assert!(result_set.is_ok());

    let result_set = result_set.unwrap();

    assert!(result_set.cache_size() > 0);

    for row in result_set {
        println!("{:?}", row);
    }

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client.get(
        TEST_TABLE_NAME,
        vec![Value::from("foo")],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("c2").unwrap();
    assert!(value.is_string());
    assert_eq!("car", value.as_string());

    let result = client.delete(TEST_TABLE_NAME, vec![Value::from("foo")]);
    assert!(result.is_ok());
}

#[test]
fn test_obtable_client_batch_op() {
    let client = common::build_normal_client();

    let test_key1 = "batchop-row-key-1";
    let test_key2 = "batchop-row-key-2";

    let mut batch_op = client.batch_operation(4);
    batch_op.delete(vec![Value::from(test_key1)]);
    batch_op.delete(vec![Value::from(test_key2)]);
    batch_op.insert(
        vec![Value::from(test_key1)],
        vec!["c2".to_owned()],
        vec![Value::from("p2")],
    );
    batch_op.insert(
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
        vec![Value::from("p2")],
    );
    let result = client.execute_batch(TEST_TABLE_NAME, batch_op);
    assert!(result.is_ok());

    let result = client.get(
        TEST_TABLE_NAME,
        vec![Value::from(test_key1)],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("c2").unwrap();
    assert!(value.is_string());
    assert_eq!("p2", value.as_string());

    let mut batch_op = client.batch_operation(3);

    batch_op.update(
        vec![Value::from(test_key1)],
        vec!["c2".to_owned()],
        vec![Value::from("p3")],
    );
    batch_op.update(
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
        vec![Value::from("p3")],
    );
    batch_op.update(
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
        vec![Value::from("p4")],
    );

    let result = client.execute_batch(TEST_TABLE_NAME, batch_op);
    assert!(result.is_ok());

    let result = client.get(
        TEST_TABLE_NAME,
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("c2").unwrap();
    assert!(value.is_string());
    assert_eq!("p4", value.as_string());
}

// The create sql of test table:
// ```sql
// create table testHash(
//   K bigint,
//   Q varbinary(256),
//   T bigint,
//   V varbinary(1024),
//   primary key(K, Q, T)
// ) partition by hash(K) partitions 16;
// ```
#[test]
fn test_obtable_partition_hash_crud() {
    let client = common::build_hbase_client();

    let rowk_keys = vec![
        Value::from(9i64),
        Value::from("partition"),
        Value::from(1550225864000i64),
    ];
    let result = client.delete(TEST_HASH_TABLE_NAME, rowk_keys.clone());
    assert!(result.is_ok());

    let result = client.insert(
        TEST_HASH_TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("aa")],
    );
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client.get(
        TEST_HASH_TABLE_NAME,
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
        TEST_HASH_TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("bb")],
    );

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client.get(
        TEST_HASH_TABLE_NAME,
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

// The create sql of the test table:
// ```sql
//  CREATE TABLE `testPartition` (
//    `K` varbinary(1024) NOT NULL,
//    `Q` varbinary(256) NOT NULL,
//    `T` bigint(20) NOT NULL,
//    `V` varbinary(1024) DEFAULT NULL,
//    PRIMARY KEY (`K`, `Q`, `T`)
//  ) DEFAULT CHARSET = utf8mb4 COLLATE UTF8MB4_BIN COMPRESSION = 'lz4_1.0'
// REPLICA_NUM = 3  BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE =
// 134217728 PCTFREE = 10 partition by key(k) partitions 15;
// ```
#[test]
fn test_obtable_partition_key_varbinary_crud() {
    let client = common::build_hbase_client();

    // same as java sdk, when k = partitionKey, after get_partition(&table_entry,
    // row_key) part_id = 2
    let rowk_keys = vec![
        Value::from("partitionKey"),
        Value::from("partition"),
        Value::from(1550225864000i64),
    ];
    let result = client.delete(TEST_KEY_VARBINARY_TABLE_NAME, rowk_keys.clone());
    result.unwrap();
    //        assert!(result.is_ok());

    let result = client.insert(
        TEST_KEY_VARBINARY_TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("aa")],
    );
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client.get(
        TEST_KEY_VARBINARY_TABLE_NAME,
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
        TEST_KEY_VARBINARY_TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("bb")],
    );

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client.get(
        TEST_KEY_VARBINARY_TABLE_NAME,
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
// CREATE TABLE `testPartition_2` (
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
    let client = common::build_hbase_client();

    // same as java sdk, when k = partitionKey2, part_id = 9
    let rowk_keys = vec![
        Value::from("partitionKey2"),
        Value::from("partition"),
        Value::from(1550225864000i64),
    ];
    let result = client.delete(TEST_KEY_VARCHAR_TABLE_NAME, rowk_keys.clone());
    result.unwrap();
    //        assert!(result.is_ok());

    let result = client.insert(
        TEST_KEY_VARCHAR_TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("aa")],
    );
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client.get(
        TEST_KEY_VARCHAR_TABLE_NAME,
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
        TEST_KEY_VARCHAR_TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("bb")],
    );

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client.get(
        TEST_KEY_VARCHAR_TABLE_NAME,
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

// The crate sql of the test table is:
// ```sql
// create table testRange(
//   K varbinary(1024),
//   Q varbinary(256),
//   T bigint,
//   V varbinary(102400),
//   primary key(K, Q, T)
// ) partition by range columns (K) (PARTITION p0 VALUES LESS THAN ('a'),
//   PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);
// ```
#[test]
fn test_obtable_partition_range_crud() {
    let client = common::build_hbase_client();

    let rowk_keys = vec![
        Value::from("partitionKey"),
        Value::from("partition"),
        Value::from(1550225864000i64),
    ];
    let result = client.delete(TEST_RANGE_TABLE_NAME, rowk_keys.clone());
    result.unwrap();
    //        assert!(result.is_ok());

    let result = client.insert(
        TEST_RANGE_TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("aa")],
    );
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client.get(
        TEST_RANGE_TABLE_NAME,
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
        TEST_RANGE_TABLE_NAME,
        rowk_keys.clone(),
        vec!["V".to_owned()],
        vec![Value::from("bb")],
    );

    assert!(result.is_ok());

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client.get(
        TEST_RANGE_TABLE_NAME,
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

#[test]
fn test_varchar_all_ob() {
    let client = common::build_normal_client();
    let test = common::BaseTest::new(client);
    test.clean_varchar_table();
    test.test_varchar_insert();
    for _ in 0..10 {
        test.test_varchar_get();
    }
    test.test_varchar_update();
    test.test_varchar_insert_or_update();
    test.test_varchar_replace();
    test.clean_varchar_table();
}

#[test]
fn test_blob_all() {
    let client = common::build_normal_client();
    let test = common::BaseTest::new(client);
    test.clean_blob_table();
    test.test_blob_insert();
    for _ in 0..10 {
        test.test_blob_get();
    }
    test.test_blob_update();
    test.test_blob_insert_or_update();
    test.test_blob_replace();
    test.clean_blob_table();
}

#[test]
#[serial]
fn test_ob_exceptions() {
    let client = common::build_normal_client();
    let test = common::BaseTest::new(client);
    test.test_varchar_exceptions();
}

#[test]
#[serial]
fn test_query() {
    let client = common::build_normal_client();
    let test = common::BaseTest::new(client);
    test.test_query("test_query_table");
}

#[test]
#[serial]
fn test_stream_query() {
    let client = common::build_normal_client();

    let test = common::BaseTest::new(client);
    test.test_stream_query("test_stream_query_table");
}

#[test]
#[serial]
fn test_concurrent() {
    let client = common::build_normal_client();

    let test = common::BaseTest::new(client);
    test.test_varchar_concurrent();
    test.clean_varchar_table();
}
