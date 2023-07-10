#[allow(unused_imports)]
#[allow(unused)]
mod utils;

use tokio::task;
use obkv::ResultCodes::{OB_ERR_UNEXPECTED, OB_NOT_SUPPORTED};
use serial_test_derive::serial;
use obkv::query::ObTableAggregationType::MIN;
use obkv::Value;

#[tokio::test]
#[serial]
async fn test_obtable_client_aggregation() {
    /*
     * CREATE TABLE test_aggregation (
     *   `c1` int NOT NULL,
     *   `c2` tinyint NOT NULL,
     *   `c3` double DEFAULT NULL,
     *   PRIMARY KEY(`c1`, `c2`)
     * ) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned(), "c2".to_owned()]);

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i8)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(70i8)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(120i8)]).await.expect("delete failed");

    // prepare data
    client.insert(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i8)],
                  vec!["c3".to_owned()],
                  vec![Value::from(50.0)]).await.expect("insert failed");
    client.insert(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(70i8)],
                  vec!["c3".to_owned()],
                  vec![Value::from(150.0)]).await.expect("insert failed");
    client.insert(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(120i8)],
                  vec!["c3".to_owned()],
                  vec![Value::from(300.0)]).await.expect("insert failed");

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
        .add_scan_range(vec![Value::from(200i32), Value::from(0i8)], true, vec![Value::from(200i32), Value::from(127i8)], true);

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();

    // test tinyint
    assert_eq!(70, result_set.get("max(c2)".to_owned()).as_i8());
    assert_eq!(50, result_set.get("min(c2)".to_owned()).as_i8());
    assert_eq!(2, result_set.get("count(*)".to_owned()).as_i64());
    assert_eq!(120, result_set.get("sum(c2)".to_owned()).as_i64());
    assert_eq!(60.0, result_set.get("avg(c2)".to_owned()).as_f64());

    // test double
    assert_eq!(150.0, result_set.get("max(c3)".to_owned()).as_f64());
    assert_eq!(50.0, result_set.get("min(c3)".to_owned()).as_f64());
    assert_eq!(200.0, result_set.get("sum(c3)".to_owned()).as_f64());
    assert_eq!(100.0, result_set.get("avg(c3)".to_owned()).as_f64());

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i8)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(70i8)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(120i8)]).await.expect("delete failed");
}

#[tokio::test]
#[serial]
async fn test_obtable_client_multiple_partition() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned(), "c2".to_owned()]);

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(300i64)]).await.expect("delete failed");

    // prepare data
    client.insert(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)],
                  vec!["c3".to_owned()],
                  vec![Value::from(50.0)]).await.expect("insert failed");
    client.insert(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)],
                  vec!["c3".to_owned()],
                  vec![Value::from(150.0)]).await.expect("insert failed");
    client.insert(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(300i64)],
                  vec!["c3".to_owned()],
                  vec![Value::from(300.0)]).await.expect("insert failed");

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
        .add_scan_range(vec![Value::from(200i32), Value::from(0i64)], true, vec![Value::from(200i32), Value::from(1000i64)], true);

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();

    // test bigint
    assert_eq!(150, result_set.get("max(c2)".to_owned()).as_i64());
    assert_eq!(50, result_set.get("min(c2)".to_owned()).as_i64());
    assert_eq!(2, result_set.get("count(*)".to_owned()).as_i64());
    assert_eq!(200, result_set.get("sum(c2)".to_owned()).as_i64());
    assert_eq!(100.0, result_set.get("avg(c2)".to_owned()).as_f64());

    // test double
    assert_eq!(150.0, result_set.get("max(c3)".to_owned()).as_f64());
    assert_eq!(50.0, result_set.get("min(c3)".to_owned()).as_f64());
    assert_eq!(200.0, result_set.get("sum(c3)".to_owned()).as_f64());
    assert_eq!(100.0, result_set.get("avg(c3)".to_owned()).as_f64());

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(300i64)]).await.expect("delete failed");
}

#[tokio::test]
#[serial]
async fn test_obtable_client_multiple_partition_illegal() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(300i64)]).await.expect("delete failed");

    // prepare data
    client.insert(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)],
                  vec!["c3".to_owned()],
                  vec![Value::from(50.0)]).await.expect("insert failed");
    client.insert(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)],
                  vec!["c3".to_owned()],
                  vec![Value::from(150.0)]).await.expect("insert failed");
    client.insert(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(300i64)],
                  vec!["c3".to_owned()],
                  vec![Value::from(300.0)]).await.expect("insert failed");

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .min("c2".to_owned())
        .max("c2".to_owned())
        .count()
        .sum("c2".to_owned())
        .avg("c2".to_owned())
        .add_scan_range(vec![Value::from(200i32), Value::from(0i64)], true, vec![Value::from(201i32), Value::from(1000i64)], true);

    // get result
    let result_set = aggregation.execute().await;

    // test
    assert!(result_set.is_err());

    match result_set {
        Ok(_) => unreachable!(),
        Err(e) => {
            assert_eq!(obkv::error::CommonErrCode::ObException(OB_NOT_SUPPORTED), e.common_err_code().unwrap());
        }
    }

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(300i64)]).await.expect("delete failed");
}

#[tokio::test]
#[serial]
async fn test_obtable_client_aggregation_with_null() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
     *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
     */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(50i64)]).await.expect("delete failed");

    // prepare data
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2) values('200', '50')").expect("insert fail");
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2) values('200', '150')").expect("insert fail");
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2) values('300', '50')").expect("insert fail");

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .min("c3".to_owned())
        .max("c3".to_owned())
        .count()
        .sum("c3".to_owned())
        .avg("c3".to_owned())
        .add_scan_range(vec![Value::from(200i32), Value::from(0i64)], true, vec![Value::from(200i32), Value::from(1000i64)], true);

    // get result
    let result_set = aggregation.execute().await;

    // test
    let result_set = result_set.unwrap();
    if !result_set.get("min(c3)".to_owned()).is_none() {
        assert!(false);
    }
    if !result_set.get("max(c3)".to_owned()).is_none() {
        assert!(false);
    }
    if !result_set.get("sum(c3)".to_owned()).is_none() {
        assert!(false);
    }
    if !result_set.get("avg(c3)".to_owned()).is_none() {
        assert!(false);
    }

    assert_eq!(2, result_set.get("count(*)".to_owned()).as_i64());

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");

}

#[tokio::test]
#[serial]
async fn test_obtable_client_multiple_aggregation_some_null() {
    /*
         * CREATE TABLE test_partition_aggregation (
         *  `c1` int NOT NULL,
         *  `c2` bigint NOT NULL,
         *  `c3` double DEFAULT NULL,
         *  `c4` varchar(5) DEFAULT NULL,
         *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
         */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(50i64)]).await.expect("delete failed");

    // prepare data
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c3) values('200', '50', '50')").expect("insert fail");
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c3) values('200', '150', '150')").expect("insert fail");
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c3) values('300', '50', null)").expect("insert fail");

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .min("c3".to_owned())
        .max("c3".to_owned())
        .count()
        .sum("c3".to_owned())
        .avg("c3".to_owned())
        .add_scan_range(vec![Value::from(200i32), Value::from(0i64)], true, vec![Value::from(200i32), Value::from(1000i64)], true);

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();

    // test
    assert_eq!(150.0, result_set.get("max(c3)".to_owned()).as_f64());
    assert_eq!(50.0, result_set.get("min(c3)".to_owned()).as_f64());
    assert_eq!(200.0, result_set.get("sum(c3)".to_owned()).as_f64());
    assert_eq!(100.0, result_set.get("avg(c3)".to_owned()).as_f64());

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
}

#[tokio::test]
#[serial]
async fn test_obtable_client_aggregation_empty_table() {
    /*
     * CREATE TABLE test_partition_aggregation (
     *  `c1` int NOT NULL,
     *  `c2` bigint NOT NULL,
     *  `c3` double DEFAULT NULL,
     *  `c4` varchar(5) DEFAULT NULL,
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
        .add_scan_range(vec![Value::from(200i32), Value::from(0i64)], true, vec![Value::from(200i32), Value::from(1000i64)], true);

    // get result
    let result_set = aggregation.execute().await;

    // test
    let result_set = result_set.unwrap();
    if !result_set.get("min(c3)".to_owned()).is_none() {
        assert!(false);
    }
    if !result_set.get("max(c3)".to_owned()).is_none() {
        assert!(false);
    }
    if !result_set.get("sum(c3)".to_owned()).is_none() {
        assert!(false);
    }
    if !result_set.get("avg(c3)".to_owned()).is_none() {
        assert!(false);
    }

    assert_eq!(0, result_set.get("count(*)".to_owned()).as_i64());
}

#[tokio::test]
#[serial]
async fn test_obtable_client_aggregation_illegal_column() {
    /*
         * CREATE TABLE test_partition_aggregation (
         *  `c1` int NOT NULL,
         *  `c2` bigint NOT NULL,
         *  `c3` double DEFAULT NULL,
         *  `c4` varchar(5) DEFAULT NULL,
         *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
         */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(50i64)]).await.expect("delete failed");

    // prepare data
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c4) values('200', '50', 'a')").expect("insert fail");
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c4) values('200', '150', 'a')").expect("insert fail");
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c4) values('300', '50', 'a')").expect("insert fail");

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .sum("c4".to_owned())
        .add_scan_range(vec![Value::from(200i32), Value::from(0i64)], true, vec![Value::from(200i32), Value::from(1000i64)], true);

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_err());

    match result_set {
        Ok(_) => unreachable!(),
        Err(e) => {
            assert_eq!(obkv::error::CommonErrCode::ObException(OB_ERR_UNEXPECTED), e.common_err_code().unwrap());
        }
    }

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
}

#[tokio::test]
#[serial]
async fn test_obtable_client_aggregation_not_exist_column() {
    /*
         * CREATE TABLE test_partition_aggregation (
         *  `c1` int NOT NULL,
         *  `c2` bigint NOT NULL,
         *  `c3` double DEFAULT NULL,
         *  `c4` varchar(5) DEFAULT NULL,
         *  PRIMARY KEY(`c1`, `c2`)) PARTITION BY KEY(`c1`) PARTITIONS 200;
         */

    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TEST_TABLE_NAME: &str = "test_partition_aggregation";
    client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(300i32), Value::from(50i64)]).await.expect("delete failed");

    // prepare data
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c4) values('200', '50', 'a')").expect("insert fail");
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c4) values('200', '150', 'a')").expect("insert fail");
    client
        .execute_sql("insert into test_partition_aggregation(c1, c2, c4) values('300', '50', 'a')").expect("insert fail");

    // aggregate
    let aggregation = client
        .aggregate(TEST_TABLE_NAME)
        .sum("c9".to_owned())
        .add_scan_range(vec![Value::from(200i32), Value::from(0i64)], true, vec![Value::from(200i32), Value::from(1000i64)], true);

    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_err());

    match result_set {
        Ok(_) => unreachable!(),
        Err(e) => {
            assert_eq!(obkv::error::CommonErrCode::ObException(OB_ERR_UNEXPECTED), e.common_err_code().unwrap());
        }
    }

    // clear data
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(150i64)]).await.expect("delete failed");
    client.delete(TEST_TABLE_NAME,
                  vec![Value::from(200i32), Value::from(50i64)]).await.expect("delete failed");
}
