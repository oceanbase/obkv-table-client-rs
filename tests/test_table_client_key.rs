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

use std::sync::Arc;

use obkv::{ObTableClient, Value};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serial_test_derive::serial;
use tokio::task;

// ```sql
// CREATE TABLE `TEST_VARCHAR_TABLE_KEY` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(c1) partitions 16;
// ```
#[tokio::test]
async fn test_varchar_all_ob() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_VARCHAR_TABLE_KEY";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.clean_varchar_table(TABLE_NAME).await;
    test.test_varchar_insert(TABLE_NAME).await;
    for _ in 0..10 {
        test.test_varchar_get(TABLE_NAME).await;
    }
    test.test_varchar_update(TABLE_NAME).await;
    test.test_varchar_insert_or_update(TABLE_NAME).await;
    test.test_varchar_replace(TABLE_NAME).await;
    test.test_varchar_append(TABLE_NAME).await;
    test.test_varchar_increment(TABLE_NAME).await;
    test.clean_varchar_table(TABLE_NAME).await;
}

// ```sql
// CREATE TABLE `TEST_BLOB_TABLE_KEY` (
//     `c1` varchar(20) NOT NULL,
//     `c2` blob DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(c1) partitions 16;
// ```
#[tokio::test]
async fn test_blob_all() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_BLOB_TABLE_KEY";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.clean_blob_table(TABLE_NAME).await;
    test.test_blob_insert(TABLE_NAME).await;
    for _ in 0..10 {
        test.test_blob_get(TABLE_NAME).await;
    }
    test.test_blob_update(TABLE_NAME).await;
    test.test_blob_insert_or_update(TABLE_NAME).await;
    test.test_blob_replace(TABLE_NAME).await;
    test.clean_blob_table(TABLE_NAME).await;
}

#[tokio::test]
async fn test_ob_exceptions() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_VARCHAR_TABLE_KEY";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_varchar_exceptions(TABLE_NAME).await;
}

async fn insert_query_test_record(
    client: &ObTableClient,
    table_name: &str,
    row_key: &str,
    value: &str,
) {
    let result = client
        .insert_or_update(
            table_name,
            vec![Value::from(row_key)],
            vec!["c2".to_owned()],
            vec![Value::from(value)],
        )
        .await;
    assert!(result.is_ok());
    assert_eq!(1, result.unwrap());
}

// ```sql
// CREATE TABLE `TEST_QUERY_TABLE_KEY` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(c1) partitions 16;
// ```
#[tokio::test]
#[serial]
async fn test_query() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = Arc::new(client_handle.await.unwrap());

    const TABLE_NAME: &str = "TEST_QUERY_TABLE_KEY";
    clean_table(client.clone(), TABLE_NAME).await;
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    insert_query_test_record(&client, TABLE_NAME, "123", "123c2").await;
    insert_query_test_record(&client, TABLE_NAME, "124", "124c2").await;
    insert_query_test_record(&client, TABLE_NAME, "234", "234c2").await;
    insert_query_test_record(&client, TABLE_NAME, "456", "456c2").await;
    insert_query_test_record(&client, TABLE_NAME, "567", "567c2").await;

    println!("test_query");
    let query = client
        .query(TABLE_NAME)
        .select(vec!["c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from("123")],
            true,
            vec![Value::from("567")],
            true,
        );

    let result_set = query.execute().await;
    println!("result_set: {result_set:?}");
    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();
    assert_eq!(5, result_set.cache_size());

    // >= 123 && <= 123
    let mut query = client
        .query(TABLE_NAME)
        .select(vec!["c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from("123")],
            true,
            vec![Value::from("123")],
            true,
        );

    let result_set = query.execute().await;
    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();
    assert_eq!(1, result_set.cache_size());
    assert_eq!(
        "123c2",
        result_set
            .next()
            .await
            .unwrap()
            .unwrap()
            .remove("c2")
            .unwrap()
            .as_string()
    );

    // >= 124 && <= 456
    query.clear();
    let mut query = query
        .select(vec!["c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from("124")],
            true,
            vec![Value::from("456")],
            true,
        );

    let result_set = query.execute().await;
    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();
    assert_eq!(3, result_set.cache_size());

    // > 123 && < 567
    query.clear();
    let mut query = query
        .select(vec!["c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from("123")],
            false,
            vec![Value::from("567")],
            false,
        );

    let result_set = query.execute().await;
    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();
    assert_eq!(3, result_set.cache_size());

    // > 123 && <= 567
    query.clear();
    let mut query = query
        .select(vec!["c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from("123")],
            false,
            vec![Value::from("567")],
            true,
        );

    let result_set = query.execute().await;
    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();
    assert_eq!(4, result_set.cache_size());

    // >=123 && < 567
    query.clear();
    let mut query = query
        .select(vec!["c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from("123")],
            true,
            vec![Value::from("567")],
            false,
        );

    let result_set = query.execute().await;
    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();
    assert_eq!(4, result_set.cache_size());

    // >= 12 && <= 126
    query.clear();
    let mut query = query
        .select(vec!["c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from("12")],
            true,
            vec![Value::from("126")],
            true,
        );

    let result_set = query.execute().await;
    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();
    assert_eq!(2, result_set.cache_size());

    // (>=12 && <=126) || (>="456" && <="567")
    query.clear();
    let query = query
        .select(vec!["c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from("12")],
            true,
            vec![Value::from("126")],
            true,
        )
        .add_scan_range(
            vec![Value::from("456")],
            true,
            vec![Value::from("567")],
            true,
        );

    let result_set = query.execute().await;
    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();
    assert_eq!(4, result_set.cache_size());

    // (>=124 && <=124)
    let query = client
        .query(TABLE_NAME)
        .select(vec!["c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from("124")],
            true,
            vec![Value::from("124")],
            true,
        );

    let result_set = query.execute().await;
    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();
    assert_eq!(1, result_set.cache_size());
    assert_eq!(
        "124c2",
        result_set
            .next()
            .await
            .unwrap()
            .unwrap()
            .remove("c2")
            .unwrap()
            .as_string()
    );

    // (>=124 && <=123)
    let query = client
        .query(TABLE_NAME)
        .select(vec!["c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from("124")],
            true,
            vec![Value::from("123")],
            true,
        );

    let result_set = query.execute().await;
    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();
    assert_eq!(0, result_set.cache_size());

    // TODO: add more test cases on batchsize query
}

// ```sql
// CREATE TABLE `TEST_STREAM_QUERY_TABLE_KEY` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(c1) partitions 16;
// ```
#[tokio::test]
#[serial]
async fn test_stream_query() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_STREAM_QUERY_TABLE_KEY";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_stream_query(TABLE_NAME).await;
}

// ```sql
// CREATE TABLE `TEST_VARCHAR_TABLE_KEY_CONCURRENT` (
//     `c1` varchar(20) NOT NULL,
//     `c2` varchar(20) DEFAULT NULL,
//     PRIMARY KEY (`c1`)
// ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(c1) partitions 16;
// ```
#[tokio::test]
async fn test_concurrent() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_VARCHAR_TABLE_KEY_CONCURRENT";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);
    let test = test_table_client_base::BaseTest::new(client);

    test.test_varchar_concurrent(TABLE_NAME).await;
    test.clean_varchar_table(TABLE_NAME).await;
}

// ```sql
// CREATE TABLE `TEST_TABLE_BATCH_KEY` (
// `c1` varchar(20) NOT NULL,
// `c1sk` varchar(20) NOT NULL,
// `c2` varchar(20) DEFAULT NULL,
// PRIMARY KEY (`c1`, `c1sk`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
// partition by key(`c1`) partitions 16;
// ```
#[tokio::test]
async fn test_batch() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_TABLE_BATCH_KEY";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string(), "c1sb".to_string()]);

    // insert some data
    let mut batch_op = client.batch_operation(4);
    batch_op.delete(vec![Value::from("Key_0"), Value::from("subKey_0")]);
    batch_op.delete(vec![Value::from("Key_1"), Value::from("subKey_1")]);
    batch_op.insert(
        vec![Value::from("Key_0"), Value::from("subKey_0")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_0")],
    );
    batch_op.insert(
        vec![Value::from("Key_1"), Value::from("subKey_1")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_1")],
    );
    batch_op.set_atomic_op(false);
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());
}

async fn clean_table(client: Arc<ObTableClient>, table_name: &str) {
    let sql = format!("DELETE FROM {table_name}");
    let _ = utils::common::execute_sql(client, sql).await;
}

#[tokio::test]
async fn test_partition_varchar_general_ci() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const VARCHAR_TABLE_NAME: &str = "TEST_TABLE_PARTITION_VARCHAR_KEY";
    client.add_row_key_element(VARCHAR_TABLE_NAME, vec!["c1".to_string()]);

    // test varchar partition
    for i in 926..977 {
        let rowkey = format!("{i}");
        let result = client
            .delete(VARCHAR_TABLE_NAME, vec![Value::from(rowkey.to_owned())])
            .await;
        assert!(result.is_ok());
        let insert_sql = format!("insert into {VARCHAR_TABLE_NAME} values({rowkey}, 'value');");
        client.execute_sql(&insert_sql).expect("fail to insert");
    }
    for i in 926..977 {
        let rowkey = format!("{i}");
        let result = client
            .get(
                VARCHAR_TABLE_NAME,
                vec![Value::from(rowkey.to_owned())],
                vec!["c2".to_owned()],
            )
            .await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(1, result.len());
    }
    for _i in 0..64 {
        let rowkey: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .map(|c| std::char::from_u32(c as u32).unwrap())
            .take(512)
            .collect();
        let sql_rowkey = format!("'{rowkey}'");
        let result = client
            .delete(VARCHAR_TABLE_NAME, vec![Value::from(rowkey.to_owned())])
            .await;
        assert!(result.is_ok());
        let insert_sql = format!("insert into {VARCHAR_TABLE_NAME} values({sql_rowkey}, 'value');");
        client.execute_sql(&insert_sql).expect("fail to insert");
        let result = client
            .get(
                VARCHAR_TABLE_NAME,
                vec![Value::from(rowkey.to_owned())],
                vec!["c2".to_owned()],
            )
            .await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(1, result.len());
    }
}

#[tokio::test]
async fn test_partition_complex() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_TABLE_PARTITION_COMPLEX_KEY";
    client.add_row_key_element(
        TABLE_NAME,
        vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
    );

    for i in 0..16 {
        let rowkey_c2: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .map(|c| std::char::from_u32(c as u32).unwrap())
            .take(512)
            .collect();
        let rowkey_c3: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .map(|c| std::char::from_u32(c as u32).unwrap())
            .take(768)
            .collect();
        let sql_rowkeyc2 = format!("'{rowkey_c2}'");
        let sql_rowkeyc3 = format!("'{rowkey_c3}'");
        let result = client
            .delete(
                TABLE_NAME,
                vec![
                    Value::from(i as u64),
                    Value::from(rowkey_c2.to_owned()),
                    Value::from(rowkey_c3.to_owned()),
                ],
            )
            .await;
        assert!(result.is_ok());
        let insert_sql = format!(
            "insert into {TABLE_NAME} values({i}, {sql_rowkeyc2}, {sql_rowkeyc3}, 'value');"
        );
        client.execute_sql(&insert_sql).expect("fail to insert");
        let result = client
            .get(
                TABLE_NAME,
                vec![
                    Value::from(i as u64),
                    Value::from(rowkey_c2.to_owned()),
                    Value::from(rowkey_c3.to_owned()),
                ],
                vec!["c4".to_owned()],
            )
            .await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(1, result.len());
    }
}

#[tokio::test]
async fn test_sub_partition_complex() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = Arc::new(client_handle.await.unwrap());
    const TABLE_NAME: &str = "TEST_TABLE_SUB_PARTITION_COMPLEX_KEY";
    client.add_row_key_element(
        TABLE_NAME,
        vec![
            "c1".to_string(),
            "c2".to_string(),
            "c3".to_string(),
            "c4".to_string(),
        ],
    );

    clean_table(client.clone(), TABLE_NAME).await;

    for i in 0..16 {
        let rowkey_c2: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .map(|c| std::char::from_u32(c as u32).unwrap())
            .take(512)
            .collect();
        let rowkey_c3: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .map(|c| std::char::from_u32(c as u32).unwrap())
            .take(768)
            .collect();
        let rowkey_c4: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .map(|c| std::char::from_u32(c as u32).unwrap())
            .take(768)
            .collect();
        let sql_rowkeyc2 = format!("'{rowkey_c2}'");
        let sql_rowkeyc3 = format!("'{rowkey_c3}'");
        let sql_rowkeyc4 = format!("'{rowkey_c4}'");
        let result = client
            .delete(
                TABLE_NAME,
                vec![
                    Value::from(i as i64),
                    Value::from(rowkey_c2.to_owned()),
                    Value::from(rowkey_c3.to_owned()),
                    Value::from(rowkey_c4.to_owned()),
                ],
            )
            .await;
        assert!(result.is_ok());
        let insert_sql = format!("insert into {TABLE_NAME} values({i}, {sql_rowkeyc2}, {sql_rowkeyc3}, {sql_rowkeyc4}, 'value');");
        client.execute_sql(&insert_sql).expect("fail to insert");
        let result = client
            .get(
                TABLE_NAME,
                vec![
                    Value::from(i as i64),
                    Value::from(rowkey_c2.to_owned()),
                    Value::from(rowkey_c3.to_owned()),
                    Value::from(rowkey_c4.to_owned()),
                ],
                vec!["c5".to_owned()],
            )
            .await;
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(1, result.len());
    }
}
