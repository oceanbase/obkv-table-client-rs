[# Demo for obkv-table-client-rs
Edited by OBKV developers on July 10, 2023.

## Introduction
obkv-table-client-rs is Rust Library that can access table data from OceanBase storage layer.
Now we provide an interface to access data from OceanBase, which we will introduce in this document.

***Notice that we will also provide another interface to access data from OceanBase in the future(Like [Mutation](https://github.com/oceanbase/obkv-table-client-java/tree/master/example/simple-mutation)).***

## demo
### Aggregate
Aggregate allows the user to get a range of data for ```max()```、```min()```、```count()```、```sum()```、```avg()```.
A **Aggregate** could get from **ObTableClient** by calling ```aggregate()``` method, then you could customize your aggregate by calling methods in **ObTableAggregation**.
```rust ObTableAggregation
impl ObTableAggregation {
    pub fn max(mut self, column_name: String) -> Self {}
    // ... 
    pub fn avg(mut self, column_name: String) -> Self {}
}
```
A simple aggregate example is shown below:
```rust aggregate exampleObTableAggregation
async fn aggregate() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();


    let aggregation = client
        .aggregate("your_table_name")
        .min("c2".to_owned())
        .add_scan_range(vec![Value::from(200i32)], true, vec![Value::from(200i32)], true);

    let result_set = aggregation.execute().await;

    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();

    assert_eq!(70, result_set.get("min(c2)".to_owned()).as_i8());
}
```
More demos can be found in [test](https://github.com/oceanbase/obkv-table-client-rs/blob/main/tests/test_table_client_aggregation.rs).