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

use obkv::{
    filter::{Filter, FilterOp, ObCompareOperator, ObTableFilterList, ObTableValueFilter},
    TableOpResult, Value,
};
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
    batch_op.set_atomic_op(false);
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn test_batch_order() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_TABLE_BATCH_HASH";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string(), "c1sb".to_string()]);

    // delete previous data
    let mut batch_op = client.batch_operation(4);
    batch_op.delete(vec![Value::from(0i64), Value::from("subKey_0")]);
    batch_op.delete(vec![Value::from(1i64), Value::from("subKey_1")]);
    batch_op.delete(vec![Value::from(2i64), Value::from("subKey_2")]);
    batch_op.delete(vec![Value::from(3i64), Value::from("subKey_3")]);
    batch_op.set_atomic_op(false);
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());

    // prepare some data 1/2/3/4
    let mut batch_op = client.batch_operation(4);
    batch_op.insert(
        vec![Value::from(0i64), Value::from("subKey_0")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_0")],
    );
    batch_op.insert(
        vec![Value::from(1i64), Value::from("subKey_1")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_1")],
    );
    batch_op.insert(
        vec![Value::from(2i64), Value::from("subKey_2")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_2")],
    );
    batch_op.insert(
        vec![Value::from(3i64), Value::from("subKey_3")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_3")],
    );
    batch_op.set_atomic_op(false);
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());

    // verify batch order
    // prepare some data 0/1/2/3
    let mut batch_op = client.batch_operation(5);
    batch_op.get(
        vec![Value::from(0i64), Value::from("subKey_0")],
        vec!["c2".to_owned()],
    );
    batch_op.insert_or_update(
        vec![Value::from(1i64), Value::from("subKey_1")],
        vec!["c2".to_owned()],
        vec![Value::from("updateValue_1")],
    );
    batch_op.get(
        vec![Value::from(3i64), Value::from("subKey_3")],
        vec!["c2".to_owned()],
    );
    batch_op.insert_or_update(
        vec![Value::from(3i64), Value::from("subKey_3")],
        vec!["c2".to_owned()],
        vec![Value::from("updateValue_3")],
    );
    batch_op.get(
        vec![Value::from(1i64), Value::from("subKey_1")],
        vec!["c2".to_owned()],
    );
    batch_op.set_atomic_op(false);
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());
    // verify batch order
    let mut result = result.unwrap();
    if let TableOpResult::RetrieveRows(mut res) = result.remove(0) {
        assert_eq!(
            "batchValue_0".to_string(),
            res.remove("c2").unwrap().as_string()
        );
    } else {
        unreachable!()
    }
    if let TableOpResult::AffectedRows(res) = result.remove(0) {
        assert_eq!(1, res);
    } else {
        unreachable!()
    }
    if let TableOpResult::RetrieveRows(mut res) = result.remove(0) {
        assert_eq!(
            "batchValue_3".to_string(),
            res.remove("c2").unwrap().as_string()
        );
    } else {
        unreachable!()
    }
    if let TableOpResult::AffectedRows(res) = result.remove(0) {
        assert_eq!(1, res);
    } else {
        unreachable!()
    }

    if let TableOpResult::RetrieveRows(mut res) = result.remove(0) {
        assert_eq!(
            "updateValue_1".to_string(),
            res.remove("c2").unwrap().as_string()
        );
    } else {
        unreachable!()
    }
}

#[tokio::test]
#[serial]
async fn test_batch_insert_or_update_with_filter() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_TABLE_BATCH_HASH";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string(), "c1sb".to_string()]);

    // delete previous data
    let mut batch_op = client.batch_operation(4);
    batch_op.delete(vec![Value::from(0i64), Value::from("subKey_0")]);
    batch_op.delete(vec![Value::from(1i64), Value::from("subKey_1")]);
    batch_op.delete(vec![Value::from(2i64), Value::from("subKey_2")]);
    batch_op.delete(vec![Value::from(3i64), Value::from("subKey_3")]);
    batch_op.set_atomic_op(false);
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());

    // insert some data, will insert 0/1/2, 3 can't insert since exec_if_exist
    let mut batch_op = client.batch_operation(4);
    let filter_0 = ObTableValueFilter::new(ObCompareOperator::Equal, "c2".to_string(), "value");
    batch_op.check_and_upsert(
        vec!["c1".to_string(), "c1sk".to_string()],
        vec![Value::from(0i64), Value::from("subKey_0")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_0")],
        filter_0,
        false,
    );
    let filter_0 = ObTableValueFilter::new(ObCompareOperator::Equal, "c2".to_string(), "value");
    batch_op.check_and_upsert(
        vec!["c1".to_string(), "c1sk".to_string()],
        vec![Value::from(1i64), Value::from("subKey_1")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_1")],
        filter_0,
        false,
    );
    let filter_0 = ObTableValueFilter::new(ObCompareOperator::Equal, "c2".to_string(), "value");
    batch_op.check_and_upsert(
        vec!["c1".to_string(), "c1sk".to_string()],
        vec![Value::from(2i64), Value::from("subKey_2")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_2")],
        filter_0,
        false,
    );
    let filter_0 = ObTableValueFilter::new(ObCompareOperator::Equal, "c2".to_string(), "value");
    batch_op.check_and_upsert(
        vec!["c1".to_string(), "c1sk".to_string()],
        vec![Value::from(3i64), Value::from("subKey_3")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_3")],
        filter_0,
        true,
    );
    batch_op.set_atomic_op(false);
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());

    // update some data, only 1 will update
    let mut batch_op = client.batch_operation(4);
    let filter_1 = Filter::List(ObTableFilterList::new(
        FilterOp::And,
        vec![Filter::Value(ObTableValueFilter::new(
            ObCompareOperator::Equal,
            "c2".to_string(),
            "batchValue_1",
        ))],
    ));
    batch_op.check_and_upsert(
        vec!["c1".to_string(), "c1sk".to_string()],
        vec![Value::from(0i64), Value::from("subKey_0")],
        vec!["c2".to_owned()],
        vec![Value::from("updateValue_0")],
        filter_1,
        true,
    );
    let filter_1 = Filter::List(ObTableFilterList::new(
        FilterOp::And,
        vec![
            Filter::Value(ObTableValueFilter::new(
                ObCompareOperator::Equal,
                "c2".to_string(),
                "batchValue_1",
            )),
            Filter::Value(ObTableValueFilter::new(
                ObCompareOperator::LessThan,
                "c3".to_string(),
                1,
            )),
        ],
    ));
    batch_op.check_and_upsert(
        vec!["c1".to_string(), "c1sk".to_string()],
        vec![Value::from(1i64), Value::from("subKey_1")],
        vec!["c2".to_owned()],
        vec![Value::from("updateValue_1")],
        filter_1,
        true,
    );
    let filter_1 = Filter::List(ObTableFilterList::new(
        FilterOp::And,
        vec![Filter::Value(ObTableValueFilter::new(
            ObCompareOperator::Equal,
            "c2".to_string(),
            "batchValue_1",
        ))],
    ));
    batch_op.check_and_upsert(
        vec!["c1".to_string(), "c1sk".to_string()],
        vec![Value::from(2i64), Value::from("subKey_2")],
        vec!["c2".to_owned()],
        vec![Value::from("updateValue_2")],
        filter_1,
        true,
    );
    let filter_1 = Filter::List(ObTableFilterList::new(
        FilterOp::And,
        vec![
            Filter::Value(ObTableValueFilter::new(
                ObCompareOperator::Equal,
                "c2".to_string(),
                "batchValue_3",
            )),
            Filter::Value(ObTableValueFilter::new(
                ObCompareOperator::LessThan,
                "c3".to_string(),
                1,
            )),
        ],
    ));
    batch_op.check_and_upsert(
        vec!["c1".to_string(), "c1sk".to_string()],
        vec![Value::from(3i64), Value::from("subKey_3")],
        vec!["c2".to_owned()],
        vec![Value::from("updateValue_3")],
        filter_1,
        true,
    );
    batch_op.set_atomic_op(false);
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());

    // check answer
    let result = client
        .get(
            TABLE_NAME,
            vec![Value::from(3i64), Value::from("subKey_3")],
            vec!["c2".to_owned()],
        )
        .await;
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(0, result.len());

    // check answer
    let result = client
        .get(
            TABLE_NAME,
            vec![Value::from(1i64), Value::from("subKey_1")],
            vec!["c2".to_owned()],
        )
        .await;
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(
        "updateValue_1".to_string(),
        result.remove("c2").unwrap().as_string()
    );

    // check answer
    let result = client
        .get(
            TABLE_NAME,
            vec![Value::from(2i64), Value::from("subKey_2")],
            vec!["c2".to_owned()],
        )
        .await;
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(
        "batchValue_2".to_string(),
        result.remove("c2").unwrap().as_string()
    );
}

#[tokio::test]
#[serial]
async fn test_uint_filter() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    const TABLE_NAME: &str = "TEST_UINT_FILTER";
    client.add_row_key_element(TABLE_NAME, vec!["c1".to_string()]);

    // delete previous data
    let mut batch_op = client.batch_operation(4);
    batch_op.delete(vec![Value::from(0u8)]);
    batch_op.delete(vec![Value::from(1u8)]);
    batch_op.delete(vec![Value::from(2u8)]);
    batch_op.delete(vec![Value::from(3u8)]);
    batch_op.set_atomic_op(false);
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());

    // insert some data, will insert 0/1/2, 3 can't insert since exec_if_exist
    let mut batch_op = client.batch_operation(4);
    let filter_0 = ObTableValueFilter::new(ObCompareOperator::Equal, "c2".to_string(), 0u16);
    batch_op.check_and_upsert(
        vec!["c1".to_string()],
        vec![Value::from(0u8)],
        vec!["c2".to_owned(), "c3".to_owned(), "c4".to_owned()],
        vec![Value::from(0u16), Value::from(0u32), Value::from(0u64)],
        filter_0,
        false,
    );
    let filter_0 = ObTableValueFilter::new(ObCompareOperator::Equal, "c2".to_string(), 0u16);
    batch_op.check_and_upsert(
        vec!["c1".to_string()],
        vec![Value::from(1u8)],
        vec!["c2".to_owned(), "c3".to_owned(), "c4".to_owned()],
        vec![
            Value::from(100u16),
            Value::from(100u32),
            Value::from(100u64),
        ],
        filter_0,
        false,
    );
    let filter_0 = ObTableValueFilter::new(ObCompareOperator::Equal, "c2".to_string(), 0u16);
    batch_op.check_and_upsert(
        vec!["c1".to_string()],
        vec![Value::from(2u8)],
        vec!["c2".to_owned(), "c3".to_owned(), "c4".to_owned()],
        vec![Value::from(0u16), Value::from(0u32), Value::from(u64::MAX)],
        filter_0,
        false,
    );
    let filter_0 = ObTableValueFilter::new(ObCompareOperator::Equal, "c2".to_string(), 0u16);
    batch_op.check_and_upsert(
        vec!["c1".to_string()],
        vec![Value::from(3u8)],
        vec!["c2".to_owned(), "c3".to_owned(), "c4".to_owned()],
        vec![Value::from(0u16), Value::from(0u32), Value::from(0u64)],
        filter_0,
        true,
    );
    batch_op.set_atomic_op(false);
    let ins_up_result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(ins_up_result.is_ok());

    // > select * from TEST_UINT_FILTER;
    // +----+------+------+----------------------+
    // | c1 | c2   | c3   | c4                   |
    // +----+------+------+----------------------+
    // |  0 |    0 |    0 |                    0 |
    // |  1 |  100 |  100 |                  100 |
    // |  2 |    0 |    0 | 18446744073709551615 |
    // +----+------+------+----------------------+

    // update some data, only 0 and 2 will update
    let mut batch_op = client.batch_operation(4);
    let filter_1 = ObTableFilterList::new(
        FilterOp::And,
        vec![Filter::Value(ObTableValueFilter::new(
            ObCompareOperator::Equal,
            "c2".to_string(),
            0u16,
        ))],
    );
    batch_op.check_and_upsert(
        vec!["c1".to_string()],
        vec![Value::from(0u8)],
        vec!["c2".to_owned()],
        vec![Value::from(1u16)],
        filter_1,
        true,
    );
    let filter_1 = ObTableFilterList::new(
        FilterOp::And,
        vec![
            Filter::Value(ObTableValueFilter::new(
                ObCompareOperator::Equal,
                "c2".to_string(),
                100u16,
            )),
            Filter::Value(ObTableValueFilter::new(
                ObCompareOperator::LessThan,
                "c3".to_string(),
                100u32,
            )),
        ],
    );
    batch_op.check_and_upsert(
        vec!["c1".to_string()],
        vec![Value::from(1u8)],
        vec!["c2".to_owned()],
        vec![Value::from(1u16)],
        filter_1,
        true,
    );
    let filter_1 = ObTableFilterList::new(
        FilterOp::And,
        vec![Filter::Value(ObTableValueFilter::new(
            ObCompareOperator::Equal,
            "c4".to_string(),
            u64::MAX,
        ))],
    );
    batch_op.check_and_upsert(
        vec!["c1".to_string()],
        vec![Value::from(2u8)],
        vec!["c2".to_owned()],
        vec![Value::from(1u16)],
        filter_1,
        true,
    );
    let filter_1 = ObTableFilterList::new(
        FilterOp::And,
        vec![
            Filter::Value(ObTableValueFilter::new(
                ObCompareOperator::Equal,
                "c2".to_string(),
                0u16,
            )),
            Filter::Value(ObTableValueFilter::new(
                ObCompareOperator::GreaterOrEqualThan,
                "c3".to_string(),
                0u64,
            )),
        ],
    );
    batch_op.check_and_upsert(
        vec!["c1".to_string()],
        vec![Value::from(3u8)],
        vec!["c2".to_owned()],
        vec![Value::from(1u16)],
        filter_1,
        true,
    );
    batch_op.set_atomic_op(false);
    let result = client.execute_batch(TABLE_NAME, batch_op).await;
    assert!(result.is_ok());

    // > select * from TEST_UINT_FILTER;
    // +----+------+------+----------------------+
    // | c1 | c2   | c3   | c4                   |
    // +----+------+------+----------------------+
    // |  0 |    1 |    0 |                    0 |
    // |  1 |  100 |  100 |                  100 |
    // |  2 |    1 |    0 | 18446744073709551615 |
    // +----+------+------+----------------------+

    // check answer
    let result = client
        .get(TABLE_NAME, vec![Value::from(3u8)], vec!["c2".to_owned()])
        .await;
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(0, result.len());

    // check answer
    let result = client
        .get(TABLE_NAME, vec![Value::from(2u8)], vec!["c2".to_owned()])
        .await;
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1u16, result.remove("c2").unwrap().as_u16(),);

    // check answer
    let result = client
        .get(TABLE_NAME, vec![Value::from(0u8)], vec!["c2".to_owned()])
        .await;
    assert!(result.is_ok());
    let mut result = result.unwrap();
    assert_eq!(1u16, result.remove("c2").unwrap().as_u16(),);
}
