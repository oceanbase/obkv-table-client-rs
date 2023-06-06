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

#[allow(unused)]
mod utils;
use std::collections::HashSet;

use obkv::Value;
#[allow(unused_imports)]
use serial_test_derive::serial;
use tokio::task;

// ```sql
// create table cse_data_20190308_1 (
// series_id bigint NOT NULL,
// field_id int NOT NULL,
// start_time INT NOT NULL,
// value MEDIUMBLOB NOT NULL,
// extra_value MEDIUMBLOB default NULL,
// PRIMARY KEY(series_id, field_id, start_time))
// partition by range(start_time) (partition p00 values less than(3600), partition p01 values less than(7200), partition p02 values less than(10800),
// partition p03 values less than(14400), partition p04 values less than(18000), partition p05 values less than(21600), partition p06 values less than(25200),
// partition p07 values less than(28800), partition p08 values less than(32400), partition p09 values less than(36000), partition p10 values less than(39600),
// partition p11 values less than(43200), partition p12 values less than(46800), partition p13 values less than(50400), partition p14 values less than(54000),
// partition p15 values less than(57600), partition p16 values less than(61200), partition p17 values less than(64800), partition p18 values less than(68400),
// partition p19 values less than(72000), partition p20 values less than(75600), partition p21 values less than(79200), partition p22 values less than(82800),
// partition p23 values less than(86400), partition p24 values less than(MAXVALUE));
// ```
#[tokio::test]
#[serial]
async fn test_cse_data_range_table() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    let cse_table = "cse_data_20190308_1";

    client
        .truncate_table(cse_table)
        .expect("Fail to truncate table");
    client.add_row_key_element(
        cse_table,
        vec![
            "series_id".to_string(),
            "field_id".to_string(),
            "start_time".to_string(),
        ],
    );
    let rowk_keys = vec![Value::from(11i64), Value::from(1i32), Value::from(3600i32)];
    let result = client.delete(cse_table, rowk_keys.clone()).await;
    assert!(result.is_ok());
    result.unwrap();

    let result = client
        .insert(
            cse_table,
            rowk_keys.clone(),
            vec!["value".to_owned()],
            vec![Value::from("aa")],
        )
        .await;
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client
        .get(cse_table, rowk_keys.clone(), vec!["value".to_owned()])
        .await;
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("value").unwrap();
    assert!(value.is_bytes());
    assert_eq!("aa".to_owned().into_bytes(), value.as_bytes());

    let result = client
        .update(
            cse_table,
            rowk_keys.clone(),
            vec!["value".to_owned()],
            vec![Value::from("bb")],
        )
        .await;

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client
        .get(cse_table, rowk_keys, vec!["value".to_owned()])
        .await;
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("value").unwrap();
    assert!(value.is_bytes());
    assert_eq!("bb".to_owned().into_bytes(), value.as_bytes());
}

#[tokio::test]
#[serial]
async fn test_data_range_part() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    let cse_table = "cse_data_20190308_1";
    client
        .truncate_table(cse_table)
        .expect("Succeed in truncating table");
    client.add_row_key_element(
        cse_table,
        vec![
            "series_id".to_string(),
            "field_id".to_string(),
            "start_time".to_string(),
        ],
    );
    let rowk_keys = vec![Value::from(11i64), Value::from(1i32), Value::from(3600i32)];

    let ret = client.get_table(cse_table, &rowk_keys, false);
    assert!(ret.is_ok());
    assert_eq!(1, ret.unwrap().0);
}

// ```sql
// create table cse_meta_data_0 (
//      id INT NOT NULL AUTO_INCREMENT,
//      name VARCHAR(1024) NOT NULL DEFAULT '',
//      data_table_name VARCHAR(100) NOT NULL,
//      data_table_start_time_ms INT NOT NULL,
//      status TINYINT NOT NULL DEFAULT 0,
//      start_time_ms BIGINT(20) DEFAULT 0,
//      end_time_ms BIGINT(20) DEFAULT 0,
//      interval_ms INT DEFAULT 0,
//      PRIMARY KEY(id), UNIQUE KEY data_table_loc(data_table_name, data_table_start_time_ms));
// ```
#[tokio::test]
#[serial]
async fn test_cse_meta_data_table() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    let cse_table = "cse_meta_data_0";

    client
        .truncate_table(cse_table)
        .expect("Fail to truncate table");
    client.add_row_key_element(cse_table, vec!["id".to_owned()]);

    client
        .insert(
            cse_table,
            vec![Value::from(0i32)],
            vec![
                "data_table_name".to_owned(),
                "data_table_start_time_ms".to_owned(),
            ],
            vec![Value::from("data_000_0"), Value::from(0i32)],
        )
        .await
        .expect("Fail to insert one test entry");

    let mut batch_op = client.batch_operation(1);
    batch_op.update(
        vec![Value::from(0i32)],
        vec![
            "name".to_owned(),
            "status".to_owned(),
            "start_time_ms".to_owned(),
            "data_table_name".to_owned(),
            "data_table_start_time_ms".to_owned(),
            "end_time_ms".to_owned(),
            "interval_ms".to_owned(),
        ],
        vec![
            Value::from(""),
            Value::from(1i8),
            Value::from(0i64),
            Value::from(""),
            Value::from(0i32),
            Value::from(1000i64),
            Value::from(100i32),
        ],
    );
    client
        .execute_batch(cse_table, batch_op)
        .await
        .expect("Fail to update row");
}

// ```sql
// create table cse_index_1 (
//  measurement VARBINARY(1024) NOT NULL,
//   tag_key VARBINARY(1024) NOT NULL,
//      tag_value VARBINARY(1024) NOT NULL,
//      series_ids MEDIUMBLOB NOT NULL,
//      PRIMARY KEY(measurement, tag_key, tag_value))
// partition by key(measurement, tag_key, tag_value) partitions 13;
// ```
#[tokio::test]
#[serial]
async fn test_cse_index_key_table() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    let cse_table = "cse_index_1";

    client
        .truncate_table(cse_table)
        .expect("Fail to truncate table");
    client.add_row_key_element(
        cse_table,
        vec![
            "measurement".to_owned(),
            "tag_key".to_owned(),
            "tag_value".to_owned(),
        ],
    );
    let mut rows = HashSet::with_capacity(100);
    for i in 0..100 {
        let (m, k, v) = (
            format!("{}-measurement", i * i),
            format!("{}-tagkey", i * i * i),
            format!("{}-tagvalue", i + i),
        );
        rows.insert(vec![m, k, v]);
    }

    let ids = "ids";
    let mut batch_ops = client.batch_operation(100);
    for row in rows.clone() {
        let row = row.into_iter().map(Value::from).collect();
        batch_ops.insert(
            row,
            vec!["series_ids".to_owned()],
            vec![Value::from(ids.to_owned())],
        );
    }
    let res = client
        .execute_batch(cse_table, batch_ops)
        .await
        .expect("Fail to execute batch operations");
    assert_eq!(100, res.len());

    let columns = vec![
        "measurement".to_owned(),
        "tag_key".to_owned(),
        "tag_value".to_owned(),
        "series_ids".to_owned(),
    ];
    let query = client.query(cse_table);
    let query = query.select(columns.clone()).add_scan_range(
        vec![Value::get_min(), Value::get_min(), Value::get_min()],
        false,
        vec![Value::get_max(), Value::get_max(), Value::get_max()],
        false,
    );
    let mut result_set = query.execute().await.expect("Fail to execute");

    for i in 0..result_set.cache_size() {
        match result_set.next().await {
            Some(Ok(mut res)) => {
                let row: Vec<String> = columns
                    .iter()
                    .map(|name| {
                        String::from_utf8(res.remove(name).unwrap().as_bytes().to_vec()).unwrap()
                    })
                    .collect();
                let (m, k, v, series_ids) = (
                    row[0].to_owned(),
                    row[1].to_owned(),
                    row[2].to_owned(),
                    row[3].to_owned(),
                );
                assert!(rows.contains(&vec![m, k, v]));
                assert_eq!(ids, series_ids);
            }
            None => {
                assert_eq!(i, 100);
                break;
            }
            Some(Err(e)) => {
                panic!("Error: {e:?}");
            }
        }
    }
    result_set
        .async_close()
        .await
        .expect("Fail to close result set");
}

#[tokio::test]
#[serial]
async fn test_index_key_part() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    let cse_table = "cse_index_1";

    client
        .truncate_table(cse_table)
        .expect("Fail to truncate table");
    client.add_row_key_element(
        cse_table,
        vec![
            "measurement".to_string(),
            "tag_key".to_string(),
            "tag_value".to_string(),
        ],
    );
    let rowk_keys = vec![Value::from("a"), Value::from("site"), Value::from("et2")];

    let result = client.get_table(cse_table, &rowk_keys, false);
    assert_eq!(8, result.unwrap().0);
}

// ```sql
// create table cse_field_1 (
//  measurement VARBINARY(1024) NOT NULL,
//  field_name VARBINARY(1024) NOT NULL,
//  field_type INT NOT NULL,
//  id INT NOT NULL,
//  PRIMARY KEY(measurement, field_name))
// partition by key(measurement, field_name) partitions 13;
// ```
#[tokio::test]
#[serial]
async fn test_cse_field_key_table() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    let cse_table = "cse_field_1";

    client
        .truncate_table(cse_table)
        .expect("Fail to truncate table");
    client.add_row_key_element(
        cse_table,
        vec!["measurement".to_string(), "field_name".to_string()],
    );
    let rowk_keys = vec![Value::from("a"), Value::from("site")];
    let result = client.delete(cse_table, rowk_keys.clone()).await;
    result.unwrap();

    let result = client
        .insert(
            cse_table,
            rowk_keys.clone(),
            vec!["field_type".to_owned(), "id".to_owned()],
            vec![Value::from(1i32), Value::from(2i32)],
        )
        .await;
    let result = result.unwrap();
    assert_eq!(1, result);

    let result = client
        .get(
            cse_table,
            rowk_keys.clone(),
            vec!["field_type".to_owned(), "id".to_owned()],
        )
        .await;
    let mut result = result.unwrap();
    assert_eq!(2, result.len());
    let value = result.remove("field_type").unwrap();
    assert!(value.is_i32());
    assert_eq!(1i32, value.as_i32());
    let value = result.remove("id").unwrap();
    assert!(value.is_i32());
    assert_eq!(2i32, value.as_i32());

    let result = client
        .update(
            cse_table,
            rowk_keys.clone(),
            vec!["field_type".to_owned(), "id".to_owned()],
            vec![Value::from(3i32), Value::from(4i32)],
        )
        .await;

    let result = result.unwrap();

    assert_eq!(1, result);

    let result = client
        .get(
            cse_table,
            rowk_keys,
            vec!["field_type".to_owned(), "id".to_owned()],
        )
        .await;
    let mut result = result.unwrap();
    assert_eq!(2, result.len());
    let value = result.remove("field_type").unwrap();
    assert!(value.is_i32());
    assert_eq!(3i32, value.as_i32());
    let value = result.remove("id").unwrap();
    assert!(value.is_i32());
    assert_eq!(4i32, value.as_i32());
}

#[tokio::test]
#[serial]
async fn test_field_key_part() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    let cse_table = "cse_field_1";

    client
        .truncate_table(cse_table)
        .expect("Fail to truncate table");
    client.add_row_key_element(
        cse_table,
        vec!["measurement".to_string(), "field_name".to_string()],
    );
    let rowk_keys = vec![Value::from("11bb"), Value::from("11cc")];

    let ret = client.get_table(cse_table, &rowk_keys, false);
    assert!(ret.is_ok());
    assert_eq!(6, ret.unwrap().0);
}

// The create sql of the test table is:
// ```sql
// create table cse_series_key_to_id_1 (
//  series_key VARBINARY(8096) NOT NULL,
//  series_id BIGINT NOT NULL,
// PRIMARY KEY(series_key), KEY index_id(series_id));
// ```
#[tokio::test]
#[serial]
async fn test_series_key_table() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();
    let cse_table = "cse_series_key_to_id_1";

    client
        .truncate_table(cse_table)
        .expect("Fail to truncate table");
    client.add_row_key_element(cse_table, vec!["series_key".to_string()]);
    let rowk_keys = vec![Value::from("a")];
    let result = client.delete(cse_table, rowk_keys.clone()).await;
    assert!(result.is_ok());
    result.unwrap();

    let result = client
        .insert(
            cse_table,
            rowk_keys.clone(),
            vec!["series_id".to_owned()],
            vec![Value::from(1i64)],
        )
        .await;
    let result = result.unwrap();
    assert_eq!(1i64, result);

    let result = client
        .get(cse_table, rowk_keys.clone(), vec!["series_id".to_owned()])
        .await;
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    println!("result get:{result:?}");
    let value = result.remove("series_id").unwrap();
    assert!(value.is_i64());
    assert_eq!(1i64, value.as_i64());

    let result = client
        .update(
            cse_table,
            rowk_keys.clone(),
            vec!["series_id".to_owned()],
            vec![Value::from(3i64)],
        )
        .await;

    let result = result.unwrap();

    assert_eq!(1i64, result);

    let result = client
        .get(cse_table, rowk_keys, vec!["series_id".to_owned()])
        .await;
    let mut result = result.unwrap();
    assert_eq!(1, result.len());
    let value = result.remove("series_id").unwrap();
    assert!(value.is_i64());
    assert_eq!(3i64, value.as_i64());
}
