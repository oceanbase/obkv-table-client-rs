// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

#[allow(unused_imports)]
#[allow(unused)]
mod common;

use obkv::{Table, TableQuery, Value};
use serial_test_derive::serial;
use test_log::test;

// TODO: use test conf to control which environments to test.
const TEST_TABLE_NAME: &str = "test_varchar_table";
const TEST_HASH_TABLE_NAME: &str = "testHash";
const TEST_KEY_VARBINARY_TABLE_NAME: &str = "test";
const TEST_KEY_VARCHAR_TABLE_NAME: &str = "testPartition_2";
const TEST_RANGE_TABLE_NAME: &str = "testRange";

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

    // test atomic batch operation
    let mut batch_op = client.batch_operation(3);
    batch_op.set_atomic_op(true);
    batch_op.update(
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
        vec![Value::from("p5")],
    );
    batch_op.update(
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
        vec![Value::from("p6")],
    );
    batch_op.insert(
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
        vec![Value::from("p0")],
    );

    let result = client.execute_batch(TEST_TABLE_NAME, batch_op);
    assert!(result.is_err());
    assert_eq!(obkv::ResultCodes::OB_ERR_PRIMARY_KEY_DUPLICATE, result.err().expect("Common").ob_result_code().unwrap());

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
// CREATE TABLE `testRangePartition` (
// `c1` bigint NOT NULL,
// `c2` varchar(20) DEFAULT NULL,
// PRIMARY KEY (`c1`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by range(`c1`)(partition p0 values less than(200),
// partition p1 values less than(500), partition p2 values less than(900));
// ```
// ```sql
// CREATE TABLE `testRangePartitionComplex` (
// `c1` bigint NOT NULL,
// `c1sk` varchar(20) NOT NULL,
// `c2` varchar(20) DEFAULT NULL,
// PRIMARY KEY (`c1`, `c1sk`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by range(`c1`)(partition p0 values less than(200),
// partition p1 values less than(500), partition p2 values less than(900));
// ```
#[test]
fn test_obtable_client_batch_atomic_op() {
    const ATOMIC_TABLE_NAME: &str = "testRangePartition";
    const ATOMIC_TABLE_NAME_COMPLEX: &str = "testRangePartitionComplex";
    let client = common::build_normal_client();
    client.add_row_key_element(ATOMIC_TABLE_NAME, vec!["c1".to_string()],);
    client.add_row_key_element(ATOMIC_TABLE_NAME_COMPLEX, vec!["c1".to_string(), "c1sb".to_string()],);

    let test_key0 : i64 = 0;
    let test_key1 : i64 = 1;

    // insert some data
    let mut batch_op = client.batch_operation(4);
    batch_op.delete(vec![Value::from(test_key0)]);
    batch_op.delete(vec![Value::from(test_key1)]);
    batch_op.insert(
        vec![Value::from(test_key0)],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_0")],
    );
    batch_op.insert(
        vec![Value::from(test_key1)],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_1")],
    );
    let result = client.execute_batch(ATOMIC_TABLE_NAME, batch_op);
    assert!(result.is_ok());

    let result = client.get(
        ATOMIC_TABLE_NAME,
        vec![Value::from(test_key0)],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("c2").unwrap();
    assert!(value.is_string());
    assert_eq!("batchValue_0", value.as_string());

    // test atomic
    let mut batch_op = client.batch_operation(3);
    batch_op.set_atomic_op(true);
    batch_op.update(
        vec![Value::from(test_key0)],
        vec!["c2".to_owned()],
        vec![Value::from("AlterValue_0")],
    );
    batch_op.update(
        vec![Value::from(test_key1)],
        vec!["c2".to_owned()],
        vec![Value::from("AlterValue_1")],
    );
    batch_op.insert(
        vec![Value::from(test_key0)],
        vec!["c2".to_owned()],
        vec![Value::from("AlterValue_3")],
    );

    let result = client.execute_batch(ATOMIC_TABLE_NAME, batch_op);
    assert!(result.is_err());
    assert_eq!(obkv::ResultCodes::OB_ERR_PRIMARY_KEY_DUPLICATE, result.err().expect("Common").ob_result_code().unwrap());

    let result = client.get(
        ATOMIC_TABLE_NAME,
        vec![Value::from(test_key0)],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("c2").unwrap();
    assert!(value.is_string());
    assert_eq!("batchValue_0", value.as_string());

    // test atomic across multi-partition
    let test_key2 : i64 = 500;
    let mut batch_op = client.batch_operation(3);
    batch_op.set_atomic_op(true);
    batch_op.update(
        vec![Value::from(test_key0)],
        vec!["c2".to_owned()],
        vec![Value::from("AlterValue_0")],
    );
    batch_op.insert(
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
        vec![Value::from("ERRORVALUE_2")],
    );
    batch_op.update(
        vec![Value::from(test_key0)],
        vec!["c2".to_owned()],
        vec![Value::from("AlterValue_1")],
    );

    let result = client.execute_batch(ATOMIC_TABLE_NAME, batch_op);
    assert!(result.is_err());
    assert_eq!(obkv::ResultCodes::OB_INVALID_PARTITION, result.err().expect("Common").ob_result_code().unwrap());

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
    let result = client.execute_batch(ATOMIC_TABLE_NAME_COMPLEX, batch_op);
    assert!(result.is_ok());
}

// ```sql
// CREATE TABLE `testHashPartitionComplex` (
// `c1` bigint NOT NULL,
// `c1sk` varchar(20) NOT NULL,
// `c2` varchar(20) DEFAULT NULL,
// PRIMARY KEY (`c1`, `c1sk`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by hash(`c1`) partitions 16;
// ```
#[test]
fn test_obtable_client_hash() {
    const TABLE_HASH_PARTITION_COMPLEX: &str = "testHashPartitionComplex";
    let client = common::build_normal_client();
    client.add_row_key_element(TABLE_HASH_PARTITION_COMPLEX, vec!["c1".to_string(), "c1sb".to_string()],);

    let test_key0 : i64 = 100;
    let test_key1 : i64 = 200;
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
    let result = client.execute_batch(TABLE_HASH_PARTITION_COMPLEX, batch_op);
    assert!(result.is_ok());
}

// ```sql
// CREATE TABLE `testKeyPartitionComplex` (
// `c1` varchar(20) NOT NULL,
// `c1sk` varchar(20) NOT NULL,
// `c2` varchar(20) DEFAULT NULL,
// PRIMARY KEY (`c1`, `c1sk`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(`c1`) partitions 16;
// ```
#[test]
fn test_obtable_client_key() {
    const TABLE_KEY_PARTITION_COMPLEX: &str = "testKeyPartitionComplex";
    let client = common::build_normal_client();
    client.add_row_key_element(TABLE_KEY_PARTITION_COMPLEX, vec!["c1".to_string(), "c1sb".to_string()],);

    // insert some data
    let mut batch_op = client.batch_operation(4);
    batch_op.delete(vec![Value::from("Key_0"), Value::from("subKey_0")]);
    batch_op.delete(vec![Value::from("Key_1"), Value::from("subKey_1")]);
    batch_op.insert(
        vec![Value::from(Value::from("Key_0")), Value::from("subKey_0")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_0")],
    );
    batch_op.insert(
        vec![Value::from("Key_1"), Value::from("subKey_1")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_1")],
    );
    let result = client.execute_batch(TABLE_KEY_PARTITION_COMPLEX, batch_op);
    assert!(result.is_ok());
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
