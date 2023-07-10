# Demo for obkv-table-client-rs
Edited by OBKV developers on June 6, 2023.

## Introduction
obkv-table-client-rs is Rust Library that can access table data from OceanBase storage layer.
Now we provide an interface to access data from OceanBase, which we will introduce in this document.

***Notice that we will also provide another interface to access data from OceanBase in the future(Like [Mutation](https://github.com/oceanbase/obkv-table-client-java/tree/master/example/simple-mutation)).***

## demo

### Simple Operation
obkv-table-client-rs support several simple operations, such as get, insert, update, insert_or_update, replace, append, increment, delete.

```rust Table and ObTableClient
impl ObTableClient {
    // implement operation
    #[inline]
    pub async fn insert(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64> {}

    #[inline]
    pub async fn update(
        &self,
        table_name: &str,
        row_keys: Vec<Value>,
        columns: Vec<String>,
        properties: Vec<Value>,
    ) -> Result<i64> {}
    // ...
}
```

A simple operation example is shown below:
```rust simple operation example
async fn simple_operation() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();

    let result = client.insert(
        "your_table_name",
        vec![Value::from("foo")],
        vec!["c2".to_owned()],
        vec![Value::from("baz")],
    ).await;
    
    assert!(result.is_ok());
}
```
More demos can be found in [test](https://github.com/oceanbase/obkv-table-client-rs/blob/main/tests/test_table_client_base.rs).

### Batch Operation
All operations supported by **BatchOperation** could be found in **ObTableBatchOperation**.
```rust ObTableBatchOperation
impl ObTableBatchOperation {
    pub fn get(&mut self, row_keys: Vec<Value>, columns: Vec<String>) {}
    pub fn insert(&mut self, row_keys: Vec<Value>, columns: Vec<String>, properties: Vec<Value>) {}
    pub fn delete(&mut self, row_keys: Vec<Value>) {}
    pub fn update(&mut self, row_keys: Vec<Value>, columns: Vec<String>, properties: Vec<Value>) {}
    pub fn insert_or_update(&mut self, row_keys: Vec<Value>, columns: Vec<String>, properties: Vec<Value>) {}
    pub fn replace(&mut self, row_keys: Vec<Value>, columns: Vec<String>, properties: Vec<Value>) {}
    pub fn increment(&mut self, row_keys: Vec<Value>, columns: Vec<String>, properties: Vec<Value>) {}
    pub fn append(&mut self, row_keys: Vec<Value>, columns: Vec<String>, properties: Vec<Value>) {}
    // ...
}
```
A simple batch operation example is shown below:
```rust batch operation example
async fn batch_operation() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();

    //  set number of operations in batch_op
    let mut batch_op = client.batch_operation(2);

    // add operation into batch_op
    batch_op.delete(vec![Value::from("Key_0"), Value::from("subKey_0")]);
    batch_op.insert(
        vec![Value::from(Value::from("Key_0")), Value::from("subKey_0")],
        vec!["c2".to_owned()],
        vec![Value::from("batchValue_0")],
    );

    // execute
    let result = client.execute_batch("your_table_name", batch_op).await;
    assert!(result.is_ok());
}
```
More [demos](https://github.com/oceanbase/obkv-table-client-rs/blob/main/tests/test_table_client_key.rs) can be found in related test cases.

### Query
Query is different from get, it allows the user to get a range of data.
A **Query** could get from **ObTableClient** by calling ```query()``` method, then you could customize your query by calling methods in **ObTableClientQueryImpl** and **TableQuery**.
```rust ObTableClientQueryImpll
impl ObTableClientQueryImpl {
    pub async fn execute(&self) -> Result<QueryResultSet> {}
    pub fn select(self, columns: Vec<String>) -> Self {}
    // ...
    pub fn clear(&mut self) {}
}
```
A simple query example is shown below:
```rust query example
async fn query() {
    let client_handle = task::spawn_blocking(utils::common::build_normal_client);
    let client = client_handle.await.unwrap();

    let query = client
        .query("your_table_name")
        .select(vec!["c1".to_owned()])
        .scan_order(false)
        .add_scan_range(vec![Value::from("123")], true, vec![Value::from("567")], true);
    
    let result = query.execute().await;
    assert!(result.is_ok());
}
```

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
More demos can be found in [test](https://github.com/oceanbase/obkv-table-client-rs/blob/main/tests/test_table_client_base.rs).
