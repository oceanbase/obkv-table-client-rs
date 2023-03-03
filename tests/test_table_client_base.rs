// Licensed under Apache-2.0.

#[allow(unused_imports)]
#[allow(unused)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use obkv::error::CommonErrCode;
use obkv::{ObTableClient, ResultCodes, Table, TableQuery, Value};
use time::PreciseTime;

pub struct BaseTest {
    client: Arc<ObTableClient>,
}

impl BaseTest {
    pub fn new(client: ObTableClient) -> BaseTest {
        BaseTest {
            client: Arc::new(client),
        }
    }

    pub fn test_varchar_concurrent(&self, table_name: &'static str) {
        let mut handles = vec![];
        let start = PreciseTime::now();
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..10 {
            let client = self.client.clone();
            let counter = counter.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("foo{}", i);
                    let value = format!("bar{}", i);
                    let result = client
                        .insert_or_update(
                            table_name,
                            vec![Value::from(key.to_owned())],
                            vec!["c2".to_owned()],
                            vec![Value::from(value.to_owned())],
                        )
                        .expect("fail to insert_or update");
                    assert_eq!(1, result);

                    let mut result = client
                        .get(
                            table_name,
                            vec![Value::from(key)],
                            vec!["c2".to_owned()],
                        )
                        .expect("fail to get");
                    assert_eq!(1, result.len());
                    let v = result.remove("c2").unwrap();
                    assert!(v.is_string());
                    assert_eq!(value, v.as_string());

                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("should succeed to join");
        }
        let end = PreciseTime::now();
        assert_eq!(1000, counter.load(Ordering::SeqCst));
        println!(
            "{} seconds for insert_or_update {} rows.",
            start.to(end),
            1000
        );
    }

    pub fn test_bigint_concurrent(&self, table_name: &'static str) {
        let mut handles = vec![];
        let start = PreciseTime::now();
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..10 {
            let client = self.client.clone();
            let counter = counter.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key :i64 = i;
                    let value = format!("value{}", i);
                    let result = client
                        .insert_or_update(
                            table_name,
                            vec![Value::from(key)],
                            vec!["c2".to_owned()],
                            vec![Value::from(value.to_owned())],
                        )
                        .expect("fail to insert_or update");
                    assert_eq!(1, result);

                    let mut result = client
                        .get(
                            table_name,
                            vec![Value::from(key)],
                            vec!["c2".to_owned()],
                        )
                        .expect("fail to get");
                    assert_eq!(1, result.len());
                    let v = result.remove("c2").unwrap();
                    assert!(v.is_string());
                    assert_eq!(value, v.as_string());

                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("should succeed to join");
        }
        let end = PreciseTime::now();
        assert_eq!(1000, counter.load(Ordering::SeqCst));
        println!(
            "{} seconds for insert_or_update {} rows.",
            start.to(end),
            1000
        );
    }

    pub fn test_varchar_insert(&self, table_name: &str) {
        let client = &self.client;

        let result = client.insert(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from("bar")],
        );
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(1, result);

        let result = client.insert(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from("baz")],
        );

        let e = result.unwrap_err();
        assert!(e.is_ob_exception());
        assert_eq!(
            ResultCodes::OB_ERR_PRIMARY_KEY_DUPLICATE,
            e.ob_result_code().unwrap()
        );

        let result = client.insert(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from("bar")],
        );
        let e = result.unwrap_err();
        assert!(e.is_ob_exception());
        assert_eq!(
            ResultCodes::OB_ERR_PRIMARY_KEY_DUPLICATE,
            e.ob_result_code().unwrap()
        );
    }

    fn assert_varchar_get_result(&self, table_name: &str, row_key: &str, expected: &str) {
        let result = self.client.get(
            table_name,
            vec![Value::from(row_key)],
            vec!["c2".to_owned()],
        );
        assert!(result.is_ok());
        let mut result = result.unwrap();
        assert_eq!(1, result.len());
        let value = result.remove("c2").unwrap();
        assert!(value.is_string());
        assert_eq!(expected, value.as_string());
    }

    pub fn test_varchar_get(&self, table_name: &str) {
        let result = self.client.get(
            table_name,
            vec![Value::from("bar")],
            vec!["c2".to_owned()],
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());

        self.assert_varchar_get_result(table_name,"foo", "bar");
    }

    pub fn test_varchar_update(&self, table_name: &str) {
        let result = self.client.update(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from("baz")],
        );
        assert!(result.is_ok());
        assert_eq!(1, result.unwrap());

        self.assert_varchar_get_result(table_name, "foo", "baz");
    }

    pub fn test_varchar_insert_or_update(&self, table_name: &str) {
        let result = self.client.insert_or_update(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from("quux")],
        );
        assert!(result.is_ok());
        assert_eq!(1, result.unwrap());
        self.assert_varchar_get_result(table_name, "foo", "quux");

        let result = self.client.insert_or_update(
            table_name,
            vec![Value::from("bar")],
            vec!["c2".to_owned()],
            vec![Value::from("baz")],
        );
        assert!(result.is_ok());
        assert_eq!(1, result.unwrap());
        self.assert_varchar_get_result(table_name, "bar", "baz");
    }

    pub fn test_varchar_replace(&self, table_name: &str) {
        let result = self.client.replace(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from("bar")],
        );
        assert!(result.is_ok());
        assert_eq!(2, result.unwrap());
        self.assert_varchar_get_result(table_name, "foo", "bar");

        let result = self.client.replace(
            table_name,
            vec![Value::from("bar")],
            vec!["c2".to_owned()],
            vec![Value::from("baz")],
        );
        assert!(result.is_ok());
        assert_eq!(2, result.unwrap());
        self.assert_varchar_get_result(table_name, "bar", "baz");

        let result = self.client.replace(
            table_name,
            vec![Value::from("unknown")],
            vec!["c2".to_owned()],
            vec![Value::from("baz")],
        );
        assert!(result.is_ok());
        assert_eq!(1, result.unwrap());

        self.assert_varchar_get_result(table_name,"unknown", "baz");
    }

    pub fn clean_varchar_table(&self, table_name: &str) {
        let result = self
            .client
            .delete(table_name, vec![Value::from("unknown")]);
        assert!(result.is_ok());
        let result = self
            .client
            .delete(table_name, vec![Value::from("foo")]);
        assert!(result.is_ok());
        let result = self
            .client
            .delete(table_name, vec![Value::from("bar")]);
        assert!(result.is_ok());
        let result = self
            .client
            .delete(table_name, vec![Value::from("baz")]);
        assert!(result.is_ok());

        for i in 0..100 {
            let key = format!("foo{}", i);
            let result = self
                .client
                .delete(table_name, vec![Value::from(key)]);
            assert!(result.is_ok());
        }
    }

    pub fn clean_bigint_table(&self, table_name: &str) {
        for i in 0..100 {
            let key :i64 = i;
            let result = self
                .client
                .delete(table_name, vec![Value::from(key)]);
            assert!(result.is_ok());
        }
    }

    pub fn test_blob_insert(&self, table_name: &str) {
        let client = &self.client;

        let bs = "hello".as_bytes();

        let result = client.insert(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from(bs)],
        );
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(1, result);

        let result = client.insert(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from(bs)],
        );

        let e = result.unwrap_err();
        assert!(e.is_ob_exception());
        assert_eq!(
            ResultCodes::OB_ERR_PRIMARY_KEY_DUPLICATE,
            e.ob_result_code().unwrap()
        );

        //test insert string
        let result = client.insert(
            table_name,
            vec![Value::from("qux")],
            vec!["c2".to_owned()],
            vec![Value::from("qux")],
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(1, result);
    }

    fn assert_blob_get_result(&self, table_name: &str, row_key: &str, expected: &str) {
        let result = self.client.get(
            table_name,
            vec![Value::from(row_key)],
            vec!["c2".to_owned()],
        );
        assert!(result.is_ok());
        let mut result = result.unwrap();
        assert_eq!(1, result.len());
        let value = result.remove("c2").unwrap();
        assert!(value.is_bytes());
        assert_eq!(expected, String::from_utf8(value.as_bytes()).unwrap());
    }

    pub fn test_blob_get(&self, table_name: &str) {
        let result = self.client.get(
            table_name,
            vec![Value::from("bar")],
            vec!["c2".to_owned()],
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());

        self.assert_blob_get_result(table_name, "foo", "hello");
        self.assert_blob_get_result(table_name, "qux", "qux");
    }

    pub fn test_blob_update(&self, table_name: &str) {
        let result = self.client.update(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from("baz".as_bytes())],
        );
        assert!(result.is_ok());
        assert_eq!(1, result.unwrap());
        self.assert_blob_get_result(table_name, "foo", "baz");

        let result = self.client.update(
            table_name,
            vec![Value::from("qux")],
            vec!["c2".to_owned()],
            vec![Value::from("baz".as_bytes())],
        );
        assert!(result.is_ok());
        assert_eq!(1, result.unwrap());
        self.assert_blob_get_result(table_name, "qux", "baz");
    }

    pub fn test_blob_insert_or_update(&self, table_name: &str) {
        let result = self.client.insert_or_update(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from("quux".as_bytes())],
        );
        assert!(result.is_ok());
        assert_eq!(1, result.unwrap());
        self.assert_blob_get_result(table_name, "foo", "quux");

        let result = self.client.insert_or_update(
            table_name,
            vec![Value::from("bar")],
            vec!["c2".to_owned()],
            vec![Value::from("baz")],
        );
        assert!(result.is_ok());
        assert_eq!(1, result.unwrap());
        self.assert_blob_get_result(table_name, "bar", "baz");
    }

    pub fn test_blob_replace(&self, table_name: &str) {
        let result = self.client.replace(
            table_name,
            vec![Value::from("foo")],
            vec!["c2".to_owned()],
            vec![Value::from("bar")],
        );
        assert!(result.is_ok());
        assert_eq!(2, result.unwrap());
        self.assert_blob_get_result(table_name, "foo", "bar");

        let result = self.client.replace(
            table_name,
            vec![Value::from("bar")],
            vec!["c2".to_owned()],
            vec![Value::from("baz")],
        );
        assert!(result.is_ok());
        assert_eq!(2, result.unwrap());
        self.assert_blob_get_result(table_name, "bar", "baz");

        let result = self.client.replace(
            table_name,
            vec![Value::from("baz")],
            vec!["c2".to_owned()],
            vec![Value::from("baz")],
        );
        assert!(result.is_ok());
        assert_eq!(1, result.unwrap());

        self.assert_blob_get_result(table_name, "baz", "baz");
    }

    pub fn clean_blob_table(&self, table_name: &str) {
        self.client
            .delete(table_name, vec![Value::from("qux")])
            .expect("fail to delete row");
        self.client
            .delete(table_name, vec![Value::from("bar")])
            .expect("fail to delete row");
        self.client
            .delete(table_name, vec![Value::from("baz")])
            .expect("fail to delete row");
        self.client
            .delete(table_name, vec![Value::from("foo")])
            .expect("fail to delete row");
    }

    pub fn test_varchar_exceptions(&self, table_name: &str) {
        // delete exception_key
        let result = self.client.delete(
            table_name,
            vec![Value::from("exception_key")],
        );
        // assert result is ok
        assert!(result.is_ok());

        //table not exists
        let result = self.client.insert(
            "not_exist_table",
            vec![Value::from("exception_key")],
            vec!["c2".to_owned()],
            vec![Value::from("baz")],
        );

        let e = result.unwrap_err();
        assert!(e.is_ob_exception());
        assert_eq!(
            ResultCodes::OB_ERR_UNKNOWN_TABLE,
            e.ob_result_code().unwrap()
        );

        // column not found
        let result = self.client.insert(
            table_name,
            vec![Value::from("exception_key")],
            vec!["c3".to_owned()],
            vec![Value::from("baz")],
        );

        let e = result.unwrap_err();
        assert!(e.is_ob_exception());
        assert_eq!(
            ResultCodes::OB_ERR_COLUMN_NOT_FOUND,
            e.ob_result_code().unwrap()
        );

        // TODO
        // column/rowkey type error
        // let result = self.client.insert(
        //     table_name,
        //     vec![Value::from(1)],
        //     vec!["c2".to_owned()],
        //     vec![Value::from("baz")],
        // );
        // let e = result.unwrap_err();
        // assert!(e.is_ob_exception());
        // assert_eq!(ResultCodes::OB_OBJ_TYPE_ERROR, e.ob_result_code().unwrap());

        let result = self.client.insert(
            table_name,
            vec![Value::from("exception_key")],
            vec!["c2".to_owned()],
            vec![Value::from(1)],
        );
        let e = result.unwrap_err();
        assert!(e.is_ob_exception());
        assert_eq!(ResultCodes::OB_OBJ_TYPE_ERROR, e.ob_result_code().unwrap());

        // null value
        let result = self.client.insert(
            table_name,
            vec![Value::from("exception_key")],
            vec!["c2".to_owned()],
            vec![Value::default()],
        );
        // assert result is ok
        assert!(result.is_ok());
    }

    pub fn insert_query_test_record(&self, table_name: &str, row_key: &str, value: &str) {
        let result = self.client.insert_or_update(
            table_name,
            vec![Value::from(row_key)],
            vec!["c2".to_owned()],
            vec![Value::from(value)],
        );
        assert!(result.is_ok());
        assert_eq!(1, result.unwrap());
    }

    pub fn test_stream_query(&self, table_name: &str) {
        // print table name
        println!("test_stream_query for table name: {} is unsupported now", table_name);
        // for i in 0..10 {
        //     let key = format!("{}", i);
        //     self.insert_query_test_record(table_name, &key, &key);
        // }
        //
        // let query = self
        //     .client
        //     .query(table_name)
        //     .batch_size(2)
        //     .select(vec!["c2".to_owned()])
        //     .primary_index()
        //     .add_scan_range(vec![Value::from("0")], true, vec![Value::from("9")], true);
        //
        // let result_set = query.execute();
        //
        // assert!(result_set.is_ok());
        //
        // let result_set = result_set.unwrap();
        //
        // assert_eq!(0, result_set.cache_size());
        //
        // let mut i = 0;
        // for row in result_set {
        //     assert!(row.is_ok());
        //     let mut row = row.unwrap();
        //     let key = format!("{}", i);
        //     assert_eq!(key, row.remove("c2").unwrap().as_string());
        //     i = i + 1;
        // }
        //
        // assert_eq!(10, i);
    }

    pub fn test_query(&self, table_name: &str) {
        self.insert_query_test_record(table_name, "123", "123c2");
        self.insert_query_test_record(table_name, "124", "124c2");
        self.insert_query_test_record(table_name, "234", "234c2");
        self.insert_query_test_record(table_name, "456", "456c2");
        self.insert_query_test_record(table_name, "567", "567c2");

        let query = self
            .client
            .query(table_name)
            .select(vec!["c2".to_owned()])
            .primary_index()
            .add_scan_range(
                vec![Value::from("123")],
                true,
                vec![Value::from("567")],
                true,
            );

        let result_set = query.execute();

        assert!(result_set.is_ok());

        let result_set = result_set.unwrap();

        assert_eq!(5, result_set.cache_size());

        let mut i = 0;
        for row in result_set {
            assert!(row.is_ok());
            let mut row = row.unwrap();
            match i {
                0 => assert_eq!("123c2", row.remove("c2").unwrap().as_string()),
                1 => assert_eq!("124c2", row.remove("c2").unwrap().as_string()),
                2 => assert_eq!("234c2", row.remove("c2").unwrap().as_string()),
                3 => assert_eq!("456c2", row.remove("c2").unwrap().as_string()),
                4 => assert_eq!("567c2", row.remove("c2").unwrap().as_string()),
                _ => unreachable!(),
            }
            i = i + 1;
        }

        //reverse order
        let query = self
            .client
            .query(table_name)
            .select(vec!["c2".to_owned()])
            .primary_index()
            .scan_order(false)
            .add_scan_range(
                vec![Value::from("123")],
                true,
                vec![Value::from("567")],
                true,
            );

        let result_set = query.execute();

        assert!(result_set.is_ok());

        let result_set = result_set.unwrap();

        assert_eq!(5, result_set.cache_size());

        let mut i = 0;
        for row in result_set {
            assert!(row.is_ok());
            let mut row = row.unwrap();
            match i {
                0 => assert_eq!("567c2", row.remove("c2").unwrap().as_string()),
                1 => assert_eq!("456c2", row.remove("c2").unwrap().as_string()),
                2 => assert_eq!("234c2", row.remove("c2").unwrap().as_string()),
                3 => assert_eq!("124c2", row.remove("c2").unwrap().as_string()),
                4 => assert_eq!("123c2", row.remove("c2").unwrap().as_string()),
                _ => unreachable!(),
            }
            i = i + 1;
        }

        // >= 123 && <= 123
        let mut query = self
            .client
            .query(table_name)
            .select(vec!["c2".to_owned()])
            .primary_index()
            .add_scan_range(
                vec![Value::from("123")],
                true,
                vec![Value::from("123")],
                true,
            );

        let result_set = query.execute();

        assert!(result_set.is_ok());

        let mut result_set = result_set.unwrap();

        assert_eq!(1, result_set.cache_size());

        assert_eq!(
            "123c2",
            result_set
                .next()
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

        let result_set = query.execute();

        assert!(result_set.is_ok());

        let result_set = result_set.unwrap();

        assert_eq!(3, result_set.cache_size());

        let mut i = 0;
        for row in result_set {
            assert!(row.is_ok());
            let mut row = row.unwrap();
            match i {
                0 => assert_eq!("124c2", row.remove("c2").unwrap().as_string()),
                1 => assert_eq!("234c2", row.remove("c2").unwrap().as_string()),
                2 => assert_eq!("456c2", row.remove("c2").unwrap().as_string()),
                _ => unreachable!(),
            }
            i = i + 1;
        }

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

        let result_set = query.execute();

        assert!(result_set.is_ok());

        let result_set = result_set.unwrap();

        assert_eq!(3, result_set.cache_size());

        let mut i = 0;
        for row in result_set {
            assert!(row.is_ok());
            let mut row = row.unwrap();
            match i {
                0 => assert_eq!("124c2", row.remove("c2").unwrap().as_string()),
                1 => assert_eq!("234c2", row.remove("c2").unwrap().as_string()),
                2 => assert_eq!("456c2", row.remove("c2").unwrap().as_string()),
                _ => unreachable!(),
            }
            i = i + 1;
        }

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

        let result_set = query.execute();

        assert!(result_set.is_ok());

        let result_set = result_set.unwrap();

        assert_eq!(4, result_set.cache_size());

        let mut i = 0;
        for row in result_set {
            assert!(row.is_ok());
            let mut row = row.unwrap();
            match i {
                0 => assert_eq!("124c2", row.remove("c2").unwrap().as_string()),
                1 => assert_eq!("234c2", row.remove("c2").unwrap().as_string()),
                2 => assert_eq!("456c2", row.remove("c2").unwrap().as_string()),
                3 => assert_eq!("567c2", row.remove("c2").unwrap().as_string()),
                _ => unreachable!(),
            }
            i = i + 1;
        }

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

        let result_set = query.execute();

        assert!(result_set.is_ok());

        let result_set = result_set.unwrap();

        assert_eq!(4, result_set.cache_size());

        let mut i = 0;
        for row in result_set {
            assert!(row.is_ok());
            let mut row = row.unwrap();
            match i {
                0 => assert_eq!("123c2", row.remove("c2").unwrap().as_string()),
                1 => assert_eq!("124c2", row.remove("c2").unwrap().as_string()),
                2 => assert_eq!("234c2", row.remove("c2").unwrap().as_string()),
                3 => assert_eq!("456c2", row.remove("c2").unwrap().as_string()),
                _ => unreachable!(),
            }
            i = i + 1;
        }

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

        let result_set = query.execute();

        assert!(result_set.is_ok());

        let result_set = result_set.unwrap();

        assert_eq!(2, result_set.cache_size());

        let mut i = 0;
        for row in result_set {
            assert!(row.is_ok());
            let mut row = row.unwrap();
            match i {
                0 => assert_eq!("123c2", row.remove("c2").unwrap().as_string()),
                1 => assert_eq!("124c2", row.remove("c2").unwrap().as_string()),
                _ => unreachable!(),
            }
            i = i + 1;
        }

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

        let result_set = query.execute();
        assert!(result_set.is_ok());
        let result_set = result_set.unwrap();
        assert_eq!(4, result_set.cache_size());
        let mut i = 0;
        for row in result_set {
            assert!(row.is_ok());
            let mut row = row.unwrap();
            match i {
                0 => assert_eq!("123c2", row.remove("c2").unwrap().as_string()),
                1 => assert_eq!("124c2", row.remove("c2").unwrap().as_string()),
                2 => assert_eq!("456c2", row.remove("c2").unwrap().as_string()),
                3 => assert_eq!("567c2", row.remove("c2").unwrap().as_string()),
                _ => unreachable!(),
            }
            i = i + 1;
        }

        // (>=124 && <=124)
        let query = self
            .client
            .query(table_name)
            .select(vec!["c2".to_owned()])
            .primary_index()
            .add_scan_range(
                vec![Value::from("124")],
                true,
                vec![Value::from("124")],
                true,
            );

        let result_set = query.execute();

        assert!(result_set.is_ok());

        let mut result_set = result_set.unwrap();

        assert_eq!(1, result_set.cache_size());

        assert_eq!(
            "124c2",
            result_set
                .next()
                .unwrap()
                .unwrap()
                .remove("c2")
                .unwrap()
                .as_string()
        );

        //>=124 && <=123)
        let query = self
            .client
            .query(table_name)
            .select(vec!["c2".to_owned()])
            .primary_index()
            .add_scan_range(
                vec![Value::from("124")],
                true,
                vec![Value::from("123")],
                true,
            );

        let result_set = query.execute();
        assert!(result_set.is_ok());
        let result_set = result_set.unwrap();
        assert_eq!(0, result_set.cache_size());

        // TODO batch not supported in query now
        let query = self
            .client
            .query(table_name)
            .select(vec!["c2".to_owned()])
            .primary_index()
            // .batch_size(1)
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

        let query_result_set = query.execute();
        assert!(query_result_set.is_ok());
        let query_result_set = query_result_set.unwrap();
        assert_eq!(4, query_result_set.cache_size());
        let mut j = 0;
        for row in query_result_set {
            assert!(row.is_ok());
            let mut row = row.unwrap();
            match j {
                0 => assert_eq!("123c2", row.remove("c2").unwrap().as_string()),
                1 => assert_eq!("124c2", row.remove("c2").unwrap().as_string()),
                2 => assert_eq!("456c2", row.remove("c2").unwrap().as_string()),
                3 => assert_eq!("567c2", row.remove("c2").unwrap().as_string()),
                _ => unreachable!(),
            }
            j = j + 1;
        }

        //Close result set before usage
        let query = self
            .client
            .query(table_name)
            .select(vec!["c2".to_owned()])
            .primary_index()
            .batch_size(1)
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

        let result_set = query.execute();
        assert!(result_set.is_ok());
        let mut result_set = result_set.unwrap();
        assert_eq!(0, result_set.cache_size());
        for i in 0..1 {
            let row = result_set.next().unwrap();
            assert!(row.is_ok());
            let mut row = row.unwrap();
            match i {
                0 => assert_eq!("123c2", row.remove("c2").unwrap().as_string()),
                _ => unreachable!(),
            }
        }
        let ret = result_set.close();
        assert!(ret.is_ok());

        match result_set.next() {
            Some(Err(e)) => {
                assert!(e.is_common_err());
                assert_eq!(CommonErrCode::AlreadyClosed, e.common_err_code().unwrap());
            }
            _other => unreachable!(),
        }

        // TODO
        // Session timeout expired
        // let query = self
        //     .client
        //     .query(table_name)
        //     .select(vec!["c2".to_owned()])
        //     .primary_index()
        //     .operation_timeout(Duration::from_secs(1))
        //     .batch_size(1)
        //     .add_scan_range(
        //         vec![Value::from("12")],
        //         true,
        //         vec![Value::from("126")],
        //         true,
        //     )
        //     .add_scan_range(
        //         vec![Value::from("456")],
        //         true,
        //         vec![Value::from("567")],
        //         true,
        //     );
        //
        // let result_set = query.execute();
        // assert!(result_set.is_ok());
        // let mut result_set = result_set.unwrap();
        // assert_eq!(0, result_set.cache_size());
        //
        // let row = result_set.next();
        // assert!(row.is_some());
        //
        // thread::sleep(Duration::from_secs(2));
        //
        // let e = result_set.next().unwrap().unwrap_err();
        // assert!(e.is_ob_exception());
        // // the exception is OB_TIMEOUT on ob2.x and is OB_TRANS_ROLLBACKED in ob1.x.
        // let code = e.ob_result_code().unwrap();
        // assert!(code == ResultCodes::OB_TRANS_ROLLBACKED || code == ResultCodes::OB_TRANS_TIMEOUT);

        // TODO
        //In session timeout
        let query = self
            .client
            .query(table_name)
            .select(vec!["c2".to_owned()])
            .primary_index()
            .operation_timeout(Duration::from_secs(3))
            .batch_size(1)
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

        let result_set = query.execute();
        assert!(result_set.is_ok());
        let mut result_set = result_set.unwrap();
        assert_eq!(0, result_set.cache_size());

        let row = result_set.next();
        assert!(row.is_some());

        thread::sleep(Duration::from_secs(2));
        let row = result_set.next();
        assert!(row.is_some());
        let row = row.unwrap();
        println!("TODO: could not find data, row error code: {:?}", row.unwrap_err().ob_result_code());
        // assert!(row.is_ok());
    }
}
