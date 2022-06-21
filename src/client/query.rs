// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, VecDeque},
    fmt, mem,
    sync::Arc,
    time::Duration,
};

use prometheus::*;

/// Query API for ob table
use super::ObTable;
use crate::{
    error::{CommonErrCode, Error::Common as CommonErr, Result},
    rpc::protocol::{
        payloads::ObTableEntityType,
        query::{
            ObHTableFilter, ObTableQuery, ObTableQueryRequest, ObTableQueryResult,
            ObTableStreamRequest,
        },
        DEFAULT_FLAG,
    },
    serde_obkv::value::Value,
};

lazy_static! {
    pub static ref OBKV_CLIENT_STREAM_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "obkv_client_stream_metric_distribution",
        "Bucketed histogram of stream metric distribution",
        &["type"],
        exponential_buckets(0.0005, 2.0, 18).unwrap()
    )
    .unwrap();
    pub static ref OBKV_CLIENT_STREAM_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "obkv_client_stream_command_total",
        "Total number of commands group by type.",
        &["type", "tag"]
    )
    .unwrap();
}

// const CLOSE_STREAM_MIN_TIMEOUT_MS: Duration = Duration::from_millis(500);
// Zero timeout means no-wait request.
const ZERO_TIMEOUT_MS: Duration = Duration::from_millis(0);

pub trait StreamQuerier {
    fn execute_query(
        &self,
        result: &mut QueryStreamResult,
        part_id_and_table: (i64, Arc<ObTable>),
        payload: &mut ObTableQueryRequest,
    ) -> Result<i64>;

    fn execute_stream(
        &self,
        result: &mut QueryStreamResult,
        part_id_and_table: (i64, Arc<ObTable>),
        payload: &mut ObTableStreamRequest,
    ) -> Result<i64>;
}

type PartitionQueryResultDeque = VecDeque<((i64, Arc<ObTable>), ObTableQueryResult)>;

pub struct QueryStreamResult {
    querier: Arc<dyn StreamQuerier + Send + Sync>,
    initialized: bool,
    eof: bool,
    closed: bool,
    row_index: i32,
    table_query: ObTableQuery,
    operation_timeout: Option<Duration>,
    table_name: String,
    entity_type: ObTableEntityType,
    expectant: HashMap<i64, (i64, Arc<ObTable>)>,
    cache_properties: Vec<String>,
    cache_rows: VecDeque<Vec<Value>>,
    partition_last_result: PartitionQueryResultDeque,
    flag: u16,
}

impl fmt::Debug for QueryStreamResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "QueryStreamResult {{ table_name: {}, entity_type: {:?}, cache_properties: {:?}, cache_rows: {:?}, row_index: {}, closed: {}, eof: {}}}",
               self.table_name, self.entity_type,
               self.cache_properties, self.cache_rows, self.row_index,
               self.closed, self.eof)
    }
}

impl QueryStreamResult {
    pub fn new(querier: Arc<dyn StreamQuerier + Send + Sync>, table_query: ObTableQuery) -> Self {
        Self {
            querier,
            initialized: false,
            closed: false,
            eof: false,
            row_index: 0,
            table_query,
            operation_timeout: None,
            table_name: "".to_owned(),
            entity_type: ObTableEntityType::Dynamic,
            expectant: HashMap::new(),
            cache_properties: vec![],
            cache_rows: VecDeque::new(),
            partition_last_result: VecDeque::new(),
            flag: DEFAULT_FLAG,
        }
    }

    fn refer_to_new_partition(&mut self, (part_id, ob_table): (i64, Arc<ObTable>)) -> Result<i64> {
        let mut req = ObTableQueryRequest::new(
            &self.table_name,
            part_id,
            self.entity_type.to_owned(),
            self.table_query.to_owned(),
            self.operation_timeout
                .unwrap_or_else(|| ob_table.operation_timeout()),
            self.flag,
        );

        let result = self
            .querier
            .clone()
            .execute_query(self, (part_id, ob_table), &mut req);

        if result.is_err() {
            self.close_eagerly("err");
        }

        result
    }

    fn refer_to_last_stream_result(
        &mut self,
        (part_id, ob_table): (i64, Arc<ObTable>),
        last_result: &ObTableQueryResult,
    ) -> Result<i64> {
        let mut req = ObTableStreamRequest::new(
            last_result.session_id(),
            self.operation_timeout
                .unwrap_or_else(|| ob_table.operation_timeout()),
            self.flag,
        );
        req.set_stream_next();
        let result = self
            .querier
            .clone()
            .execute_stream(self, (part_id, ob_table), &mut req);

        if result.is_err() {
            self.close_eagerly("err");
        }

        result
    }

    pub fn set_table_query(&mut self, table_query: ObTableQuery) {
        self.table_query = table_query;
    }

    pub fn set_entity_type(&mut self, entity_type: ObTableEntityType) {
        self.entity_type = entity_type;
    }

    pub fn set_table_name(&mut self, table_name: &str) {
        self.table_name = table_name.to_owned();
    }

    pub fn set_expectant(&mut self, expectant: HashMap<i64, (i64, Arc<ObTable>)>) {
        self.expectant = expectant;
    }

    pub fn set_operation_timeout(&mut self, timeout: Option<Duration>) {
        self.operation_timeout = timeout;
    }

    pub fn set_flag(&mut self, flag: u16) {
        self.flag = flag;
    }

    pub fn cache_stream_next(
        &mut self,
        part_id_and_table: (i64, Arc<ObTable>),
        mut query_result: ObTableQueryResult,
    ) {
        self.cache_properties = query_result.take_properties_names();

        self.cache_rows.extend(query_result.take_properties_rows());

        if query_result.is_stream() && query_result.is_stream_next() {
            self.partition_last_result
                .push_back((part_id_and_table, query_result));
        }
    }

    pub fn init(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        if self.table_query.batch_size() == -1 {
            let tuples = std::mem::replace(&mut self.expectant, HashMap::new());

            for (_, tuple) in tuples {
                self.refer_to_new_partition(tuple)?;
            }
        }

        self.initialized = true;

        Ok(())
    }

    fn cache_size(&self) -> usize {
        self.cache_rows.len()
    }

    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        let last_result_num = self.partition_last_result.len();
        let _timer = OBKV_CLIENT_STREAM_HISTOGRAM_VEC
            .with_label_values(&["close_stream"])
            .start_timer();

        let mut loop_cnt = 0;
        loop {
            let last_part_result = self.partition_last_result.pop_front();
            match last_part_result {
                None => break,
                Some((tuple, last_result)) => {
                    if last_result.is_stream() && last_result.is_stream_next() {
                        if let Err(e) = self.close_last_stream_result(tuple, last_result) {
                            debug!(
                                "QueryStreamResult::close fail to close \
                                 last stream result, err: {}",
                                e
                            );
                        }
                    }
                }
            }
            loop_cnt += 1;
        }

        if loop_cnt != last_result_num {
            error!(
                "QueryStreamResult::close found invalid times closing stream, last_result_num:{}, \
                 actual loop times:{}",
                last_result_num, loop_cnt
            );
        }
        Ok(())
    }

    // NOTE: it may hang on closing stream according to the observation in prod env
    fn gen_close_stream_timeout(&self, _table_timeout: Duration) -> Duration {
        // if let Some(timeout) = self.operation_timeout {
        //    return timeout;
        //}
        // otherwise we use ob_table.operation_timeout / 10
        //if table_timeout > CLOSE_STREAM_MIN_TIMEOUT_MS {
        //    return table_timeout;
        //}

        //let timeout = table_timeout / 10;
        //if timeout < CLOSE_STREAM_MIN_TIMEOUT_MS {
        //  CLOSE_STREAM_MIN_TIMEOUT_MS
        //} else {
        //   timeout
        //}
        ZERO_TIMEOUT_MS
    }

    fn close_last_stream_result(
        &mut self,
        (part_id, ob_table): (i64, Arc<ObTable>),
        last_result: ObTableQueryResult,
    ) -> Result<i64> {
        let mut req = ObTableStreamRequest::new(
            last_result.session_id(),
            self.gen_close_stream_timeout(ob_table.operation_timeout()),
            self.flag,
        );

        req.set_stream_last();
        self.querier
            .clone()
            .execute_stream(self, (part_id, ob_table), &mut req)
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>> {
        self.row_index += 1;
        Ok(self.cache_rows.pop_front())
    }

    #[inline]
    fn close_eagerly(&mut self, tag: &str) {
        if let Err(e) = self.close() {
            error!(
                "QueryStreamResult::close_eagerly fail to close stream result, err: {}",
                e
            );
        }
        OBKV_CLIENT_STREAM_COUNTER_VEC
            .with_label_values(&["close_eagerly", tag])
            .inc();
    }

    pub fn row_index(&self) -> i32 {
        self.row_index
    }

    pub fn cache_properties(&self) -> Vec<String> {
        self.cache_properties.clone()
    }

    pub fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if !self.initialized {
            return Err(CommonErr(
                CommonErrCode::NotInitialized,
                "Query is not initialized".to_owned(),
            ));
        }

        if self.eof {
            return Ok(None);
        }

        if self.closed {
            return Err(CommonErr(
                CommonErrCode::AlreadyClosed,
                "Query was closed".to_owned(),
            ));
        }

        //1. Found from cache.
        if !self.cache_rows.is_empty() {
            return self.next_row();
        }

        //2. Get from the last stream request result
        loop {
            let last_part_result = self.partition_last_result.pop_front();
            match last_part_result {
                None => break,
                Some((tuple, last_result)) => {
                    if last_result.is_stream() && last_result.is_stream_next() {
                        let row_count = self.refer_to_last_stream_result(tuple, &last_result)?;
                        if row_count == 0 {
                            continue;
                        }
                        return self.next_row();
                    }
                }
            }
        }

        //3. Query from new parttion
        let mut refered_partitions = vec![];
        let mut has_next = false;

        for (k, tuple) in self.expectant.clone() {
            refered_partitions.push(k);
            let row_count = self.refer_to_new_partition(tuple)?;

            if row_count != 0 {
                has_next = true;
                break;
            }
        }

        for k in refered_partitions {
            self.expectant.remove(&k);
        }

        if has_next {
            self.next_row()
        } else {
            //4. Reach the end.
            self.eof = true;
            self.close_eagerly("eof");
            Ok(None)
        }
    }
}

impl Drop for QueryStreamResult {
    fn drop(&mut self) {
        match self.close() {
            Ok(()) => (),
            Err(e) => error!("QueryStreamResult::close fail: #{:?}", e),
        }
    }
}

#[derive(Debug)]
pub enum QueryResultSet {
    Some(QueryStreamResult),
    None,
}

impl Default for QueryResultSet {
    fn default() -> Self {
        QueryResultSet::None
    }
}

impl QueryResultSet {
    pub fn new() -> Self {
        QueryResultSet::default()
    }

    pub fn is_none(&self) -> bool {
        matches!(self, QueryResultSet::None)
    }

    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    pub fn from_stream_result(stream_result: QueryStreamResult) -> Self {
        QueryResultSet::Some(stream_result)
    }

    pub fn cache_size(&self) -> usize {
        match self {
            QueryResultSet::None => 0,
            QueryResultSet::Some(stream_result) => stream_result.cache_size(),
        }
    }

    pub fn close(&mut self) -> Result<()> {
        match self {
            QueryResultSet::None => Ok(()),
            QueryResultSet::Some(stream_result) => stream_result.close(),
        }
    }
}

impl Iterator for QueryResultSet {
    type Item = Result<HashMap<String, Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            QueryResultSet::None => None,
            QueryResultSet::Some(ref mut stream_result) => {
                match stream_result.next() {
                    //Find a row.
                    Ok(Some(mut row)) => {
                        let mut names = stream_result.cache_properties();
                        assert_eq!(names.len(), row.len());
                        let mut row_map = HashMap::new();

                        for i in 0..names.len() {
                            let name = mem::take(&mut names[i]);
                            let value = mem::take(&mut row[i]);

                            row_map.insert(name, value);
                        }
                        Some(Ok(row_map))
                    }
                    //Reach end
                    Ok(None) => None,
                    //Error happens
                    Err(e) => Some(Err(e)),
                }
            }
        }
    }
}

impl Drop for QueryResultSet {
    fn drop(&mut self) {
        match self.close() {
            Ok(()) => (),
            Err(e) => error!("QueryResultSet:drop failed: {:?}", e),
        }
    }
}

/// Table Query Trait

const PRIMARY_INDEX_NAME: &str = "PRIMARY";

pub trait TableQuery {
    fn execute(&self) -> Result<QueryResultSet>;
    fn get_table_name(&self) -> String;
    fn set_entity_type(&mut self, entity_type: ObTableEntityType);
    fn entity_type(&self) -> ObTableEntityType;
    fn select(self, columns: Vec<String>) -> Self;
    fn limit(self, offset: Option<i32>, limit: i32) -> Self;
    fn add_scan_range(
        self,
        start: Vec<Value>,
        start_equals: bool,
        end: Vec<Value>,
        end_equals: bool,
    ) -> Self;
    fn add_scan_range_starts_with(self, start: Vec<Value>, start_equals: bool) -> Self;
    fn add_scan_range_ends_with(self, end: Vec<Value>, end_equals: bool) -> Self;
    fn scan_order(self, forward: bool) -> Self;
    fn index_name(self, index_name: &str) -> Self;
    fn primary_index(self) -> Self
    where
        Self: Sized,
    {
        self.index_name(PRIMARY_INDEX_NAME)
    }
    fn filter_string(self, filter_string: &str) -> Self;
    fn htable_filter(self, filter: ObHTableFilter) -> Self;
    fn batch_size(self, batch_size: i32) -> Self;
    fn operation_timeout(self, timeout: Duration) -> Self;
    fn clear(&mut self);
}
