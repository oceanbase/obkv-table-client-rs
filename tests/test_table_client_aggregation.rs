/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2023 OceanBase
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

#[allow(unused_imports)]
#[allow(unused)]
mod utils;

use obkv::{ResultCodes, Value};
use serial_test_derive::serial;
use tokio::task;

#[tokio::test]
#[serial]
async fn test_aggregation() {
    /*
     * CREATE TABLE test_aggregation (
     *   `c1` int NOT NULL,
     *   `c2` tinyint unsigned NOT NULL,
     *   `c3` double DEFAULT NULL,
     *   PRIMARY KEY(`c1`, `c2`)
     * ) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned(), "c2".to_owned()]);

    // prepare data
    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50u8)],
            vec!["c3".to_owned()],
            vec![Value::from(50.0)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(70u8)],
            vec!["c3".to_owned()],
            vec![Value::from(150.0)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(120u8)],
            vec!["c3".to_owned()],
            vec![Value::from(300.0)],
        )
        .await;
    assert!(result.is_ok());

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .min("c2".to_owned())
        .max("c2".to_owned())
        .count()
        .sum("c2".to_owned())
        .avg("c2".to_owned())
        .min("c3".to_owned())
        .max("c3".to_owned())
        .sum("c3".to_owned())
        .avg("c3".to_owned())
        .add_scan_range(
            vec![Value::from(200i32), Value::from(0u8)],
            true,
            vec![Value::from(200i32), Value::from(127u8)],
            true,
        );

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();

    // test tinyint
    let single_result = result_set.get("max(c2)");

    match single_result {
        Some(singel_value) => {
            assert_eq!(70, singel_value.as_u8());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("min(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(50, singel_value.as_u8());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("count(*)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(2, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("sum(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(120, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("avg(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(60.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    // test double
    let single_result = result_set.get("max(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(150.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("min(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(50.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("sum(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(200.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("avg(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(100.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    // clear data
    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50u8)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(70u8)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(120u8)],
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn test_multiple_partition() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  INDEX i1(`c1`, `c3`) local,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned(), "c2".to_owned()]);

    // prepare data
    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50i64)],
            vec!["c3".to_owned()],
            vec![Value::from(50.0)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(150i64)],
            vec!["c3".to_owned()],
            vec![Value::from(150.0)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(300i64)],
            vec!["c3".to_owned()],
            vec![Value::from(300.0)],
        )
        .await;
    assert!(result.is_ok());

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .min("c2".to_owned())
        .max("c2".to_owned())
        .count()
        .sum("c2".to_owned())
        .avg("c2".to_owned())
        .min("c3".to_owned())
        .max("c3".to_owned())
        .sum("c3".to_owned())
        .avg("c3".to_owned())
        .add_scan_range(
            vec![Value::from(200i32), Value::from(0i64)],
            true,
            vec![Value::from(200i32), Value::from(1000i64)],
            true,
        );

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();

    // test bigint
    let single_result = result_set.get("max(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(150, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("min(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(50, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("count(*)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(2, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("sum(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(200, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("avg(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(100.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    // test double
    let single_result = result_set.get("max(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(150.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("min(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(50.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("sum(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(200.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("avg(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(100.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    // clear data
    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(150i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(300i64)],
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn test_local_index() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  INDEX i1(`c1`, `c3`) local,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned(), "c2".to_owned()]);

    // prepare data
    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50i64)],
            vec!["c3".to_owned()],
            vec![Value::from(50.0)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(150i64)],
            vec!["c3".to_owned()],
            vec![Value::from(150.0)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(300i64)],
            vec!["c3".to_owned()],
            vec![Value::from(300.0)],
        )
        .await;
    assert!(result.is_ok());

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .min("c2".to_owned())
        .max("c2".to_owned())
        .count()
        .sum("c2".to_owned())
        .avg("c2".to_owned())
        .min("c3".to_owned())
        .max("c3".to_owned())
        .sum("c3".to_owned())
        .avg("c3".to_owned())
        .add_scan_range(
            vec![Value::from(200i32), Value::from(0f64)],
            true,
            vec![Value::from(200i32), Value::from(1000f64)],
            true,
        )
        .index_name("i1");

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();

    // test bigint
    let single_result = result_set.get("max(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(150, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("min(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(50, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("count(*)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(2, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("sum(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(200, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("avg(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(100.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    // test double
    let single_result = result_set.get("max(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(150.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("min(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(50.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("sum(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(200.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("avg(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(100.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    // clear data
    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(150i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(300i64)],
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn test_multiple_partition_illegal() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  INDEX i1(`c1`, `c3`) local,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // prepare data
    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50i64)],
            vec!["c3".to_owned()],
            vec![Value::from(50.0)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(150i64)],
            vec!["c3".to_owned()],
            vec![Value::from(150.0)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .insert(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(300i64)],
            vec!["c3".to_owned()],
            vec![Value::from(300.0)],
        )
        .await;
    assert!(result.is_ok());

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .min("c2".to_owned())
        .max("c2".to_owned())
        .count()
        .sum("c2".to_owned())
        .avg("c2".to_owned())
        .add_scan_range(
            vec![Value::from(200i32), Value::from(0i64)],
            true,
            vec![Value::from(201i32), Value::from(1000i64)],
            true,
        );

    // get result
    let result_set = aggregation.execute().await;

    // test
    assert!(result_set.is_err());

    match result_set {
        Ok(_) => unreachable!(),
        Err(e) => {
            assert_eq!(
                obkv::error::CommonErrCode::InvalidParam,
                e.common_err_code().unwrap()
            );
        }
    }

    // clear data
    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(150i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(300i64)],
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn test_aggregation_with_null() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  INDEX i1(`c1`, `c3`) local,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // prepare data
    let result =
        client.execute_sql("insert into test_partition_aggregation(c1, c2) values('200', '50')");
    assert!(result.is_ok());

    let result =
        client.execute_sql("insert into test_partition_aggregation(c1, c2) values('200', '150')");
    assert!(result.is_ok());

    let result =
        client.execute_sql("insert into test_partition_aggregation(c1, c2) values('300', '50')");
    assert!(result.is_ok());

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .min("c3".to_owned())
        .max("c3".to_owned())
        .count()
        .sum("c3".to_owned())
        .avg("c3".to_owned())
        .add_scan_range(
            vec![Value::from(200i32), Value::from(0i64)],
            true,
            vec![Value::from(200i32), Value::from(1000i64)],
            true,
        );

    // get result
    let result_set = aggregation.execute().await;
    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();

    // test
    match result_set.get("min(c3)") {
        Some(e) => {
            assert!(e.is_none())
        }
        _ => {
            unreachable!();
        }
    }

    match result_set.get("max(c3)") {
        Some(e) => {
            assert!(e.is_none())
        }
        _ => {
            unreachable!();
        }
    }

    match result_set.get("sum(c3)") {
        Some(e) => {
            assert!(e.is_none())
        }
        _ => {
            unreachable!();
        }
    }

    match result_set.get("avg(c3)") {
        Some(e) => {
            assert!(e.is_none())
        }
        _ => {
            unreachable!();
        }
    }

    let single_result = result_set.get("count(*)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(2, singel_value.as_i64());
        }
        _ => unreachable!(),
    }

    // clear data
    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(150i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn test_multiple_aggregation_some_null() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  INDEX i1(`c1`, `c3`) local,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // prepare data
    let result = client.execute_sql(
        "insert into test_partition_aggregation(c1, c2, c3) values('200', '50', '50')",
    );
    assert!(result.is_ok());

    let result = client.execute_sql(
        "insert into test_partition_aggregation(c1, c2, c3) values('200', '150', '150')",
    );
    assert!(result.is_ok());

    let result = client.execute_sql(
        "insert into test_partition_aggregation(c1, c2, c3) values('300', '50', null)",
    );
    assert!(result.is_ok());

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .min("c3".to_owned())
        .max("c3".to_owned())
        .count()
        .sum("c3".to_owned())
        .avg("c3".to_owned())
        .add_scan_range(
            vec![Value::from(200i32), Value::from(0i64)],
            true,
            vec![Value::from(200i32), Value::from(1000i64)],
            true,
        );

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();

    // test
    let single_result = result_set.get("max(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(150.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("min(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(50.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("sum(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(200.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    let single_result = result_set.get("avg(c3)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(100.0, singel_value.as_f64());
        }
        _ => unreachable!(),
    }

    // clear data
    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(150i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn test_aggregation_empty_table() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  INDEX i1(`c1`, `c3`) local,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // aggregate empty table
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .min("c3".to_owned())
        .max("c3".to_owned())
        .count()
        .sum("c3".to_owned())
        .avg("c3".to_owned())
        .add_scan_range(
            vec![Value::from(200i32), Value::from(0i64)],
            true,
            vec![Value::from(200i32), Value::from(1000i64)],
            true,
        );

    // get result
    let result_set = aggregation.execute().await;
    // todo:server behaviors must be compatible
    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();

    // test
    match result_set.get("min(c3)") {
        Some(e) => {
            assert!(e.is_none())
        }
        _ => {
            unreachable!();
        }
    }

    match result_set.get("max(c3)") {
        Some(e) => {
            assert!(e.is_none())
        }
        _ => {
            unreachable!();
        }
    }

    match result_set.get("sum(c3)") {
        Some(e) => {
            assert!(e.is_none())
        }
        _ => {
            unreachable!();
        }
    }

    match result_set.get("avg(c3)") {
        Some(e) => {
            assert!(e.is_none())
        }
        _ => {
            unreachable!();
        }
    }

    let single_result = result_set.get("count(*)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(0, singel_value.as_i64());
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
#[serial]
async fn test_aggregation_illegal_column() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  INDEX i1(`c1`, `c3`) local,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // prepare data
    let result = client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c4) values('200', '50', 'a')");
    assert!(result.is_ok());

    let result = client.execute_sql(
        "insert into test_partition_aggregation(c1, c2, c4) values('200', '150', 'a')",
    );
    assert!(result.is_ok());

    let result = client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c4) values('300', '50', 'a')");
    assert!(result.is_ok());

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .sum("c4".to_owned())
        .add_scan_range(
            vec![Value::from(200i32), Value::from(0i64)],
            true,
            vec![Value::from(200i32), Value::from(1000i64)],
            true,
        );

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_err());

    match result_set {
        Ok(_) => unreachable!(),
        Err(e) => {
            assert_eq!(
                obkv::error::CommonErrCode::ObException(ResultCodes::OB_ERR_UNEXPECTED),
                e.common_err_code().unwrap()
            );
        }
    }

    // clear data
    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(150i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn test_aggregation_not_exist_column() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  INDEX i1(`c1`, `c3`) local,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // prepare data
    let result = client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c4) values('200', '50', 'a')");
    assert!(result.is_ok());

    let result = client.execute_sql(
        "insert into test_partition_aggregation(c1, c2, c4) values('200', '150', 'a')",
    );
    assert!(result.is_ok());

    let result = client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c4) values('300', '50', 'a')");
    assert!(result.is_ok());

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .sum("c9".to_owned())
        .add_scan_range(
            vec![Value::from(200i32), Value::from(0i64)],
            true,
            vec![Value::from(200i32), Value::from(1000i64)],
            true,
        );

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_err());

    match result_set {
        Ok(_) => unreachable!(),
        Err(e) => {
            assert_eq!(
                obkv::error::CommonErrCode::ObException(ResultCodes::OB_ERR_UNEXPECTED),
                e.common_err_code().unwrap()
            );
        }
    }

    // clear data
    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(200i32), Value::from(150i64)],
        )
        .await;
    assert!(result.is_ok());

    let result = client
        .delete(
            TEST_TABLE_NAME,
            vec![Value::from(300i32), Value::from(50i64)],
        )
        .await;
    assert!(result.is_ok());
}
