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

use obkv::{SyncObTableClient, Table, Value};
use std::time::Duration;

/// Test SyncObTableClient basic CRUD operations
#[test]
fn test_sync_client_crud() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Setup: clean test data
    let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from("sync_test_key")]);

    // Test insert
    let result = sync_client.insert(
        TEST_TABLE_NAME,
        vec![Value::from("sync_test_key")],
        vec!["c2".to_owned()],
        vec![Value::from("sync_test_value")],
    );
    assert!(result.is_ok());
    assert_eq!(1, result.unwrap());

    // Test get
    let result = sync_client.get(
        TEST_TABLE_NAME,
        vec![Value::from("sync_test_key")],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut row = result.unwrap();
    assert_eq!(1, row.len());
    let value = row.remove("c2").unwrap();
    assert_eq!("sync_test_value", value.as_string());

    // Test update
    let result = sync_client.update(
        TEST_TABLE_NAME,
        vec![Value::from("sync_test_key")],
        vec!["c2".to_owned()],
        vec![Value::from("sync_updated_value")],
    );
    assert!(result.is_ok());
    assert_eq!(1, result.unwrap());

    // Verify update
    let result = sync_client.get(
        TEST_TABLE_NAME,
        vec![Value::from("sync_test_key")],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut row = result.unwrap();
    let value = row.remove("c2").unwrap();
    assert_eq!("sync_updated_value", value.as_string());

    // Test delete
    let result = sync_client.delete(TEST_TABLE_NAME, vec![Value::from("sync_test_key")]);
    assert!(result.is_ok());
    assert_eq!(1, result.unwrap());

    // Verify delete
    let result = sync_client.get(
        TEST_TABLE_NAME,
        vec![Value::from("sync_test_key")],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let row = result.unwrap();
    assert_eq!(0, row.len());
}

/// Test SyncObTableClient insert_or_update operation
#[test]
fn test_sync_client_insert_or_update() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Clean test data
    let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from("insert_or_update_key")]);

    // Test insert_or_update (insert)
    let result = sync_client.insert_or_update(
        TEST_TABLE_NAME,
        vec![Value::from("insert_or_update_key")],
        vec!["c2".to_owned()],
        vec![Value::from("value1")],
    );
    assert!(result.is_ok());
    assert_eq!(1, result.unwrap());

    // Test insert_or_update (update)
    let result = sync_client.insert_or_update(
        TEST_TABLE_NAME,
        vec![Value::from("insert_or_update_key")],
        vec!["c2".to_owned()],
        vec![Value::from("value2")],
    );
    assert!(result.is_ok());
    assert_eq!(1, result.unwrap()); // insert_or_update always returns 1

    // Verify final value
    let result = sync_client.get(
        TEST_TABLE_NAME,
        vec![Value::from("insert_or_update_key")],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut row = result.unwrap();
    let value = row.remove("c2").unwrap();
    assert_eq!("value2", value.as_string());

    // Cleanup
    let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from("insert_or_update_key")]);
}

/// Test SyncObTableClient replace operation
#[test]
fn test_sync_client_replace() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Clean test data
    let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from("replace_key")]);

    // Insert initial data
    let result = sync_client.insert(
        TEST_TABLE_NAME,
        vec![Value::from("replace_key")],
        vec!["c2".to_owned()],
        vec![Value::from("initial_value")],
    );
    assert!(result.is_ok());

    // Test replace
    let result = sync_client.replace(
        TEST_TABLE_NAME,
        vec![Value::from("replace_key")],
        vec!["c2".to_owned()],
        vec![Value::from("replaced_value")],
    );
    assert!(result.is_ok());

    // Verify replace
    let result = sync_client.get(
        TEST_TABLE_NAME,
        vec![Value::from("replace_key")],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut row = result.unwrap();
    let value = row.remove("c2").unwrap();
    assert_eq!("replaced_value", value.as_string());

    // Cleanup
    let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from("replace_key")]);
}

/// Test SyncObTableClient batch operations
#[test]
fn test_sync_client_batch_operations() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    let test_key1 = "batch_key_1";
    let test_key2 = "batch_key_2";

    // Clean test data
    let mut batch_op = sync_client.batch_operation(2);
    batch_op.delete(vec![Value::from(test_key1)]);
    batch_op.delete(vec![Value::from(test_key2)]);
    let _ = sync_client.execute_batch(TEST_TABLE_NAME, batch_op);

    // Test batch insert
    let mut batch_op = sync_client.batch_operation(2);
    batch_op.insert(
        vec![Value::from(test_key1)],
        vec!["c2".to_owned()],
        vec![Value::from("batch_value1")],
    );
    batch_op.insert(
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
        vec![Value::from("batch_value2")],
    );
    let result = sync_client.execute_batch(TEST_TABLE_NAME, batch_op);
    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(2, results.len());

    // Verify batch insert
    let result = sync_client.get(
        TEST_TABLE_NAME,
        vec![Value::from(test_key1)],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut row = result.unwrap();
    let value = row.remove("c2").unwrap();
    assert_eq!("batch_value1", value.as_string());

    // Test batch update
    let mut batch_op = sync_client.batch_operation(2);
    batch_op.update(
        vec![Value::from(test_key1)],
        vec!["c2".to_owned()],
        vec![Value::from("updated_batch_value1")],
    );
    batch_op.update(
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
        vec![Value::from("updated_batch_value2")],
    );
    let result = sync_client.execute_batch(TEST_TABLE_NAME, batch_op);
    assert!(result.is_ok());

    // Verify batch update
    let result = sync_client.get(
        TEST_TABLE_NAME,
        vec![Value::from(test_key2)],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut row = result.unwrap();
    let value = row.remove("c2").unwrap();
    assert_eq!("updated_batch_value2", value.as_string());

    // Cleanup
    let mut batch_op = sync_client.batch_operation(2);
    batch_op.delete(vec![Value::from(test_key1)]);
    batch_op.delete(vec![Value::from(test_key2)]);
    let _ = sync_client.execute_batch(TEST_TABLE_NAME, batch_op);
}

/// Test SyncObTableClient query with basic scan range
#[test]
fn test_sync_client_query_basic() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Setup test data
    let test_keys = vec!["query_test_1", "query_test_2", "query_test_3"];
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
        let _ = sync_client.insert(
            TEST_TABLE_NAME,
            vec![Value::from(*key)],
            vec!["c2".to_owned()],
            vec![Value::from(format!("value_{}", key))],
        );
    }

    // Test query with scan range
    let result_set = sync_client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .add_scan_range(
            vec![Value::from("query_test_1")],
            true,
            vec![Value::from("query_test_3")],
            true,
        )
        .scan_order(true)
        .execute();

    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();

    // Count results
    let mut count = 0;
    for result in &mut result_set {
        assert!(result.is_ok());
        let row = result.unwrap();
        assert!(row.contains_key("c1"));
        assert!(row.contains_key("c2"));
        count += 1;
    }
    assert!(count >= 3);

    // Close result set
    let close_result = result_set.check_close();
    assert!(close_result.is_ok());

    // Cleanup
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
    }
}

/// Test SyncObTableClient query with limit
#[test]
fn test_sync_client_query_with_limit() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Setup test data
    let test_keys = vec![
        "limit_test_1",
        "limit_test_2",
        "limit_test_3",
        "limit_test_4",
        "limit_test_5",
    ];
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
        let _ = sync_client.insert(
            TEST_TABLE_NAME,
            vec![Value::from(*key)],
            vec!["c2".to_owned()],
            vec![Value::from(format!("value_{}", key))],
        );
    }

    // Test query with limit
    let result_set = sync_client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .add_scan_range(
            vec![Value::from("limit_test_1")],
            true,
            vec![Value::from("limit_test_5")],
            true,
        )
        .limit(None, 3)
        .execute();

    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();

    // Count results - should be limited to 3
    let mut count = 0;
    for result in &mut result_set {
        assert!(result.is_ok());
        count += 1;
    }
    assert_eq!(3, count);

    // Close result set
    let _ = result_set.check_close();

    // Cleanup
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
    }
}

/// Test SyncObTableClient query scan range
/// Note: filter_string is not commonly used and may require special server configuration,
/// so this test focuses on scan range functionality instead
#[test]
fn test_sync_client_query_scan_range_multiple_keys() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Setup test data
    let test_keys = vec!["scan_range_a", "scan_range_b", "scan_range_c"];
    for (i, key) in test_keys.iter().enumerate() {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
        let _ = sync_client.insert(
            TEST_TABLE_NAME,
            vec![Value::from(*key)],
            vec!["c2".to_owned()],
            vec![Value::from(format!("scan_value_{}", i))],
        );
    }

    // Test query with scan range that should return multiple rows
    let result_set = sync_client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .add_scan_range(
            vec![Value::from("scan_range_a")],
            true,
            vec![Value::from("scan_range_c")],
            true,
        )
        .execute();

    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();

    // Should get all 3 rows in the range
    let mut count = 0;
    let mut found_keys = Vec::new();
    for result in &mut result_set {
        assert!(result.is_ok());
        let mut row = result.unwrap();
        let c1_value = row.remove("c1").unwrap().as_string();
        found_keys.push(c1_value);
        count += 1;
    }
    assert!(count >= 3, "Expected at least 3 rows, got {}", count);
    assert!(found_keys.contains(&"scan_range_a".to_string()));
    assert!(found_keys.contains(&"scan_range_b".to_string()));
    assert!(found_keys.contains(&"scan_range_c".to_string()));

    // Close result set
    let _ = result_set.check_close();

    // Cleanup
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
    }
}

/// Test SyncObTableClient query with batch size
#[test]
fn test_sync_client_query_with_batch_size() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Setup test data
    let test_keys = vec!["batch_size_1", "batch_size_2", "batch_size_3"];
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
        let _ = sync_client.insert(
            TEST_TABLE_NAME,
            vec![Value::from(*key)],
            vec!["c2".to_owned()],
            vec![Value::from(format!("value_{}", key))],
        );
    }

    // Test query with batch size
    let result_set = sync_client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .add_scan_range(
            vec![Value::from("batch_size_1")],
            true,
            vec![Value::from("batch_size_3")],
            true,
        )
        .batch_size(2)
        .execute();

    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();

    // Verify results
    let mut count = 0;
    for result in &mut result_set {
        assert!(result.is_ok());
        count += 1;
    }
    assert!(count >= 3);

    // Close result set
    let _ = result_set.check_close();

    // Cleanup
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
    }
}

/// Test SyncObTableClient query with scan order
#[test]
fn test_sync_client_query_scan_order() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Setup test data
    let test_keys = vec!["order_test_1", "order_test_2", "order_test_3"];
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
        let _ = sync_client.insert(
            TEST_TABLE_NAME,
            vec![Value::from(*key)],
            vec!["c2".to_owned()],
            vec![Value::from(format!("value_{}", key))],
        );
    }

    // Test forward scan order
    let result_set = sync_client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .add_scan_range(
            vec![Value::from("order_test_1")],
            true,
            vec![Value::from("order_test_3")],
            true,
        )
        .scan_order(true)
        .execute();

    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();

    // Collect forward results
    let mut forward_keys = Vec::new();
    for result in &mut result_set {
        assert!(result.is_ok());
        let mut row = result.unwrap();
        let key = row.remove("c1").unwrap().as_string();
        forward_keys.push(key);
    }

    // Close result set
    let _ = result_set.check_close();

    // Test backward scan order
    let result_set = sync_client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .add_scan_range(
            vec![Value::from("order_test_1")],
            true,
            vec![Value::from("order_test_3")],
            true,
        )
        .scan_order(false)
        .execute();

    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();

    // Collect backward results
    let mut backward_keys = Vec::new();
    for result in &mut result_set {
        assert!(result.is_ok());
        let mut row = result.unwrap();
        let key = row.remove("c1").unwrap().as_string();
        backward_keys.push(key);
    }

    // Close result set
    let _ = result_set.check_close();

    // Verify backward is reverse of forward
    backward_keys.reverse();
    assert_eq!(forward_keys, backward_keys);

    // Cleanup
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
    }
}

/// Test SyncObTableClient query with operation timeout
#[test]
fn test_sync_client_query_with_timeout() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Setup test data
    let test_key = "timeout_test_key";
    let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(test_key)]);
    let _ = sync_client.insert(
        TEST_TABLE_NAME,
        vec![Value::from(test_key)],
        vec!["c2".to_owned()],
        vec![Value::from("timeout_value")],
    );

    // Test query with custom timeout
    let result_set = sync_client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .add_scan_range(
            vec![Value::from(test_key)],
            true,
            vec![Value::from(test_key)],
            true,
        )
        .operation_timeout(Duration::from_secs(10))
        .execute();

    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();

    // Verify we can get results
    let mut count = 0;
    for result in &mut result_set {
        assert!(result.is_ok());
        count += 1;
    }
    assert!(count > 0);

    // Close result set
    let _ = result_set.check_close();

    // Cleanup
    let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(test_key)]);
}

/// Test SyncObTableClient utility methods
#[test]
fn test_sync_client_utility_methods() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Test is_initialized
    assert!(sync_client.is_initialized());

    // Test check_table_exists
    let result = sync_client.check_table_exists(TEST_TABLE_NAME);
    assert!(result.is_ok());
    assert!(result.unwrap());

    // Test check non-existent table
    let result = sync_client.check_table_exists("non_existent_table_xyz");
    assert!(result.is_ok());
    assert!(!result.unwrap());

    // Test add_row_key_element
    sync_client.add_row_key_element(TEST_TABLE_NAME, vec!["c1".to_owned()]);

    // Test invalidate_table
    sync_client.invalidate_table(TEST_TABLE_NAME);
}

/// Test SyncObTableClient clone
#[test]
fn test_sync_client_clone() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    
    // Clone the client
    let cloned_client = sync_client.clone();

    // Both should be initialized
    assert!(sync_client.is_initialized());
    assert!(cloned_client.is_initialized());

    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Test that cloned client works
    let test_key = "clone_test_key";
    let _ = cloned_client.delete(TEST_TABLE_NAME, vec![Value::from(test_key)]);
    
    let result = cloned_client.insert(
        TEST_TABLE_NAME,
        vec![Value::from(test_key)],
        vec!["c2".to_owned()],
        vec![Value::from("clone_value")],
    );
    assert!(result.is_ok());

    // Verify with original client
    let result = sync_client.get(
        TEST_TABLE_NAME,
        vec![Value::from(test_key)],
        vec!["c2".to_owned()],
    );
    assert!(result.is_ok());
    let mut row = result.unwrap();
    let value = row.remove("c2").unwrap();
    assert_eq!("clone_value", value.as_string());

    // Cleanup
    let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(test_key)]);
}

/// Test SyncObTableClient query with primary index
#[test]
fn test_sync_client_query_primary_index() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Setup test data
    let test_key = "primary_index_test";
    let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(test_key)]);
    let _ = sync_client.insert(
        TEST_TABLE_NAME,
        vec![Value::from(test_key)],
        vec!["c2".to_owned()],
        vec![Value::from("primary_index_value")],
    );

    // Test query with primary index
    let result_set = sync_client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .primary_index()
        .add_scan_range(
            vec![Value::from(test_key)],
            true,
            vec![Value::from(test_key)],
            true,
        )
        .execute();

    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();

    // Verify results
    let mut count = 0;
    for result in &mut result_set {
        assert!(result.is_ok());
        let mut row = result.unwrap();
        assert_eq!("primary_index_value", row.remove("c2").unwrap().as_string());
        count += 1;
    }
    assert_eq!(1, count);

    // Close result set
    let _ = result_set.check_close();

    // Cleanup
    let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(test_key)]);
}

/// Test SyncObTableClient query with add_scan_range_starts_with
#[test]
fn test_sync_client_query_starts_with() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Setup test data
    let test_keys = vec!["starts_with_a", "starts_with_b", "starts_with_c"];
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
        let _ = sync_client.insert(
            TEST_TABLE_NAME,
            vec![Value::from(*key)],
            vec!["c2".to_owned()],
            vec![Value::from(format!("value_{}", key))],
        );
    }

    // Test query with starts_with
    let result_set = sync_client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .add_scan_range_starts_with(vec![Value::from("starts_with_a")], true)
        .execute();

    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();

    // Verify results - should get all keys from "starts_with_a" onwards
    let mut count = 0;
    for result in &mut result_set {
        assert!(result.is_ok());
        count += 1;
    }
    assert!(count >= 3);

    // Close result set
    let _ = result_set.check_close();

    // Cleanup
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
    }
}

/// Test SyncObTableClient query with add_scan_range_ends_with
#[test]
fn test_sync_client_query_ends_with() {
    let client = utils::common::build_normal_client();
    let sync_client = SyncObTableClient::new(client);
    const TEST_TABLE_NAME: &str = "test_varchar_table";

    // Setup test data
    let test_keys = vec!["ends_with_a", "ends_with_b", "ends_with_c"];
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
        let _ = sync_client.insert(
            TEST_TABLE_NAME,
            vec![Value::from(*key)],
            vec!["c2".to_owned()],
            vec![Value::from(format!("value_{}", key))],
        );
    }

    // Test query with ends_with
    let result_set = sync_client
        .query(TEST_TABLE_NAME)
        .select(vec!["c1".to_owned(), "c2".to_owned()])
        .add_scan_range_ends_with(vec![Value::from("ends_with_b")], true)
        .execute();

    assert!(result_set.is_ok());
    let mut result_set = result_set.unwrap();

    // Verify we can get results
    let mut count = 0;
    for result in &mut result_set {
        assert!(result.is_ok());
        count += 1;
    }
    assert!(count > 0);

    // Close result set
    let _ = result_set.check_close();

    // Cleanup
    for key in &test_keys {
        let _ = sync_client.delete(TEST_TABLE_NAME, vec![Value::from(*key)]);
    }
}

