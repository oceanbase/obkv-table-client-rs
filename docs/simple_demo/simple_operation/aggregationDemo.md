# Demo for obkv-table-client-rs
Edited by OBKV developers on July 11, 2023.

## Introduction
obkv-table-client-rs is Rust Library that can access table data from OceanBase storage layer.
Now we provide an interface to access data from OceanBase, which we will introduce in this document.

***Notice that we will also provide another interface to access data from OceanBase in the future(Like [Mutation](https://github.com/oceanbase/obkv-table-client-java/tree/master/example/simple-mutation)).***

## Notice
**We only support aggregate on a single partition now. Aggregation across partitions may lead to consistency problems. You can limit your aggregation in one partition by restricting your scan range.**

## demo
### Aggregate
Aggregate allows the user to get a range of data for ```max()```、```min()```、```count()```、```sum()```、```avg()```.

An **Aggregate** could get from **ObTableClient** by calling ```aggregate()``` method, then you could customize your aggregate by calling methods in **ObTableAggregation**.
```rust ObTableAggregation
impl ObTableAggregation {
    pub fn max(mut self, column_name: String) -> Self {}
    // ... 
    pub fn avg(mut self, column_name: String) -> Self {}
}
```
The **column_name** is the name of the column you want to aggregate.

For example, you can use `max(c1)` to get the aggregation result of the maxinum of the column `c1`

The aggregation result is a **hash map**, you can get the single aggregation result by calling the method `get()` of the hash map with **operation_name**

For example, you can use `get(max(c1))` to get the aggregation result, the string `"max(c1)"` is the **operation_name**

A simple aggregate example is shown below:
```rust aggregate exampleObTableAggregation
async fn aggregate() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();


    let aggregation = client
        .aggregate("your_table_name")
        .min("c2".to_owned())
        .add_scan_range(vec![Value::from(200i32)], true, vec![Value::from(200i32)], true);
    
    // get result
    let result_set = aggregation.execute().await;

    assert!(result_set.is_ok());
    let result_set = result_set.unwrap();
    
    let single_result = result_set.get("min(c2)");
    match single_result {
        Some(singel_value) => {
            assert_eq!(150, singel_value.as_i64());
        }
        _ => assert!(false)
    }
}
```
More demos can be found in [test](https://github.com/oceanbase/obkv-table-client-rs/blob/main/tests/test_table_client_aggregation.rs).
