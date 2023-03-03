// Licensed under Apache-2.0.

#[allow(unused_imports)]
#[allow(unused)]
mod utils;
pub mod test_table_client_base;

use obkv::{Table, Value};
use serial_test_derive::serial;
use test_log::test;

// ```sql
// CREATE TABLE `TEST_VARCHAR_TABLE_KEY` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(c1) partitions 16;
// ```
#[test]
fn test_varchar_all_ob() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_VARCHAR_TABLE_KEY";
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
// CREATE TABLE `TEST_BLOB_TABLE_KEY` (
//     `c1` varchar(20) NOT NULL,
//     `c2` blob DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(c1) partitions 16;
// ```
#[test]
fn test_blob_all() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_BLOB_TABLE_KEY";
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
    const TABLE_NAME: &str = "TEST_VARCHAR_TABLE_KEY";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_varchar_exceptions(TABLE_NAME);
}

// ```sql
// CREATE TABLE `TEST_QUERY_TABLE_KEY` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(c1) partitions 16;
// ```
#[test]
#[serial]
fn test_query() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_QUERY_TABLE_KEY";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_query(TABLE_NAME);
}

// ```sql
// CREATE TABLE `TEST_STREAM_QUERY_TABLE_KEY` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(c1) partitions 16;
// ```
#[test]
#[serial]
fn test_stream_query() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_STREAM_QUERY_TABLE_KEY";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_stream_query(TABLE_NAME);
}

// ```sql
// CREATE TABLE `TEST_VARCHAR_TABLE_KEY_CONCURRENT` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(c1) partitions 16;
// ```
#[test]
fn test_concurrent() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_VARCHAR_TABLE_KEY_CONCURRENT";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_varchar_concurrent(TABLE_NAME);
    test.clean_varchar_table(TABLE_NAME);
}

// ```sql
// CREATE TABLE `TEST_TABLE_BATCH_KEY` (
// `c1` varchar(20) NOT NULL,
// `c1sk` varchar(20) NOT NULL,
// `c2` varchar(20) DEFAULT NULL,
// PRIMARY KEY (`c1`, `c1sk`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(`c1`) partitions 16;
// ```
#[test]
fn test_batch() {
    let client = utils::common::build_normal_client();
    const TABLE_NAME: &str = "TEST_TABLE_BATCH_KEY";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string(), "c1sb".to_string()],);

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
    let result = client.execute_batch(TABLE_NAME, batch_op);
    assert!(result.is_ok());
}
