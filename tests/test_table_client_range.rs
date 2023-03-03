// Licensed under Apache-2.0.

#[allow(unused_imports)]
#[allow(unused)]
mod utils;
pub mod test_table_client_base;

use obkv::{Table, Value};
use serial_test_derive::serial;
use test_log::test;

// ```sql
// CREATE TABLE `TEST_VARCHAR_TABLE_RANGE` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by range columns (c1) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);
// ```
#[test]
fn test_varchar_all_ob() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_VARCHAR_TABLE_RANGE";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.clean_varchar_table(TABLE_NAME);
    test.test_varchar_insert(TABLE_NAME);
    for _ in 0..10 {
        test.test_varchar_get(TABLE_NAME);
    }
    test.test_varchar_update(TABLE_NAME);
    test.test_varchar_insert_or_update(TABLE_NAME);
    test.test_varchar_replace(TABLE_NAME);
    test.clean_varchar_table(TABLE_NAME);
}

// ```sql
// CREATE TABLE `TEST_BLOB_TABLE_RANGE` (
//     `c1` varchar(20) NOT NULL,
//     `c2` blob DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by range columns (c1) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);
// ```
#[test]
fn test_blob_all() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_BLOB_TABLE_RANGE";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.clean_blob_table(TABLE_NAME);
    test.test_blob_insert(TABLE_NAME);
    for _ in 0..10 {
        test.test_blob_get(TABLE_NAME);
    }
    test.test_blob_update(TABLE_NAME);
    test.test_blob_insert_or_update(TABLE_NAME);
    test.test_blob_replace(TABLE_NAME);
    test.clean_blob_table(TABLE_NAME);
}

#[test]
fn test_ob_exceptions() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_VARCHAR_TABLE_RANGE";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_varchar_exceptions(TABLE_NAME);
}

// ```sql
// CREATE TABLE `TEST_QUERY_TABLE_RANGE` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by range columns (c1) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);
// ```
#[test]
#[serial]
fn test_query() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_QUERY_TABLE_RANGE";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_query(TABLE_NAME);
}

// ```sql
// CREATE TABLE `TEST_STREAM_QUERY_TABLE_RANGE` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by range columns (c1) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);
// ```
#[test]
#[serial]
fn test_stream_query() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_STREAM_QUERY_TABLE_RANGE";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_stream_query(TABLE_NAME);
}

// ```sql
// CREATE TABLE `TEST_VARCHAR_TABLE_RANGE_CONCURRENT` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by range columns (c1) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);
// ```
#[test]
fn test_concurrent() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_VARCHAR_TABLE_RANGE_CONCURRENT";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_varchar_concurrent(TABLE_NAME);
    test.clean_varchar_table(TABLE_NAME);
}

// ```sql
// CREATE TABLE `TEST_TABLE_BATCH_RANGE` (
// `c1` bigint NOT NULL,
// `c2` varchar(20) DEFAULT NULL,
// PRIMARY KEY (`c1`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by range(`c1`)(partition p0 values less than(200),
// partition p1 values less than(500), partition p2 values less than(900));
//
// CREATE TABLE `TEST_TABLE_BATCH_RANGE_COMPLEX` (
// `c1` bigint NOT NULL,
// `c1sk` varchar(20) NOT NULL,
// `c2` varchar(20) DEFAULT NULL,
// PRIMARY KEY (`c1`, `c1sk`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by range(`c1`)(partition p0 values less than(200),
// partition p1 values less than(500), partition p2 values less than(900));
// ```
#[test]
fn test_obtable_client_batch_atomic_op() {
    const TABLE_NAME: &str = "TEST_TABLE_BATCH_RANGE";
    const TABLE_NAME_COMPLEX: &str = "TEST_TABLE_BATCH_RANGE_COMPLEX";
    let client = utils::common::build_normal_client();
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()],);
    client.add_row_key_element(TABLE_NAME_COMPLEX, vec!["c1".to_string(), "c1sb".to_string()],);

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
    let result = client.execute_batch(TABLE_NAME, batch_op);
    assert!(result.is_ok());

    let result = client.get(
        TABLE_NAME,
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

    let result = client.execute_batch(TABLE_NAME, batch_op);
    assert!(result.is_err());
    assert_eq!(obkv::ResultCodes::OB_ERR_PRIMARY_KEY_DUPLICATE, result.err().expect("Common").ob_result_code().unwrap());

    let result = client.get(
        TABLE_NAME,
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

    let result = client.execute_batch(TABLE_NAME, batch_op);
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
    let result = client.execute_batch(TABLE_NAME_COMPLEX, batch_op);
    assert!(result.is_ok());
}