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

#[allow(unused_imports)]
#[allow(unused)]
mod utils;

use obkv::{ResultCodes, Table, TableQuery, Value};
use test_log::test;

#[test]
fn test_obtable_client_curd() {
    let client = utils::common::build_normal_client();
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    let result = client.delete(TEST_TABLE_NAME, vec![Value::from("foo")]);
    assert!(result.is_ok());

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
        println!("{row:?}");
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
    let client = utils::common::build_normal_client();
    const TEST_TABLE_NAME: &str = "test_varchar_table";

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
    let code = result.err().unwrap().ob_result_code().unwrap();
    assert_eq!(code, ResultCodes::OB_ERR_PRIMARY_KEY_DUPLICATE);

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
