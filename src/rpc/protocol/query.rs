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

use std::{io, mem, time::Duration};

use bytes::{BufMut, BytesMut};

use super::{
    payloads::{ObRowKey, ObTableConsistencyLevel, ObTableEntityType},
    BasePayLoad, ObPayload, ObRpcPacketHeader, ObTablePacketCode, ProtoDecoder, ProtoEncoder,
    Result, STREAM_FLAG, STREAM_LAST_FLAG,
};
use crate::{
    error::{self as error, CommonErrCode, Error::Common as CommonErr},
    location::OB_INVALID_ID,
    serde_obkv::{util, value::Value},
    util::{decode_value, duration_to_millis},
};

#[derive(Debug, Clone)]
pub struct ObTableQueryResult {
    base: BasePayLoad,
    header: ObRpcPacketHeader,
    properties_names: Vec<String>,
    row_count: i64,
    properties_rows: Vec<Vec<Value>>,
}

impl Default for ObTableQueryResult {
    fn default() -> Self {
        Self::new()
    }
}

impl ObTableQueryResult {
    pub fn new() -> Self {
        Self {
            base: BasePayLoad::default(),
            header: ObRpcPacketHeader::new(),
            properties_names: vec![],
            row_count: 0,
            properties_rows: vec![],
        }
    }

    pub fn is_stream(&self) -> bool {
        self.header.is_stream()
    }

    pub fn is_stream_next(&self) -> bool {
        self.header.is_stream_next()
    }

    pub fn is_stream_last(&self) -> bool {
        self.header.is_stream_last()
    }

    pub fn session_id(&self) -> u64 {
        self.header.session_id
    }

    pub fn row_count(&self) -> i64 {
        self.row_count
    }

    pub fn take_properties_names(&mut self) -> Vec<String> {
        mem::take(&mut self.properties_names)
    }

    pub fn take_properties_rows(&mut self) -> Vec<Vec<Value>> {
        mem::take(&mut self.properties_rows)
    }
}

impl ObPayload for ObTableQueryResult {
    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    fn content_len(&self) -> Result<usize> {
        unimplemented!()
    }

    fn pcode(&self) -> ObTablePacketCode {
        ObTablePacketCode::ExecuteQuery
    }

    fn set_header(&mut self, header: ObRpcPacketHeader) {
        self.header = header;
    }
}

impl ProtoEncoder for ObTableQueryResult {
    fn encode(&self, _buf: &mut BytesMut) -> Result<()> {
        unimplemented!();
    }
}

impl ProtoDecoder for ObTableQueryResult {
    fn decode(&mut self, src: &mut BytesMut) -> Result<()> {
        self.decode_base(src)?;
        let mut len = util::decode_vi64(src)?;
        let mut props = vec![];
        while len > 0 {
            props.push(util::decode_vstring(src)?);
            len -= 1;
        }
        self.properties_names = props;

        let properties_num = self.properties_names.len();

        let mut len = util::decode_vi64(src)?;
        self.row_count = len;
        //Drop data buffer length
        let _buf_len = util::decode_vi64(src)?;
        let mut props_rows = vec![];
        while len > 0 {
            let mut rows = vec![];
            for _i in 0..properties_num {
                rows.push(decode_value(src)?);
            }
            props_rows.push(rows);
            len -= 1;
        }
        self.properties_rows = props_rows;
        Ok(())
    }
}

#[derive(Default, PartialEq, Debug, Clone)]
pub struct ObBorderFlag {
    value: i8,
}

pub const INCLUSIVE_START: i8 = 0x1;
pub const INCLUSIVE_END: i8 = 0x2;
pub const MIN_VALUE: i8 = 0x3;
pub const MAX_VALUE: i8 = 0x8;

impl ObBorderFlag {
    pub fn new() -> Self {
        Self { value: 0 }
    }

    pub fn from_i8(value: i8) -> Self {
        Self { value }
    }

    pub fn value(&self) -> i8 {
        self.value
    }

    pub fn set_inclusive_start(&mut self) {
        self.value |= INCLUSIVE_START;
    }

    pub fn unset_inclusive_start(&mut self) {
        self.value &= !INCLUSIVE_START;
    }

    pub fn is_inclusive_start(&self) -> bool {
        self.value & INCLUSIVE_START == INCLUSIVE_START
    }

    pub fn set_inclusive_end(&mut self) {
        self.value |= INCLUSIVE_END;
    }

    pub fn unset_inclusive_end(&mut self) {
        self.value &= !INCLUSIVE_END;
    }

    pub fn is_inclusive_end(&self) -> bool {
        self.value & INCLUSIVE_END == INCLUSIVE_END
    }

    pub fn set_max_value(&mut self) {
        self.value |= MAX_VALUE;
    }

    pub fn unset_max_value(&mut self) {
        self.value &= !MAX_VALUE;
    }

    pub fn is_max_value(&self) -> bool {
        self.value & MAX_VALUE == MAX_VALUE
    }

    pub fn set_min_value(&mut self) {
        self.value |= MIN_VALUE;
    }

    pub fn unset_min_value(&mut self) {
        self.value &= !MIN_VALUE;
    }

    pub fn is_min_value(&self) -> bool {
        self.value & MIN_VALUE == MIN_VALUE
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct ObNewRange {
    table_id: i64,
    border_flag: ObBorderFlag,
    start_key: ObRowKey,
    end_key: ObRowKey,
}

impl ObNewRange {
    pub fn new() -> Self {
        Self::from_keys(vec![], vec![])
    }

    pub fn from_keys(start_key: Vec<Value>, end_key: Vec<Value>) -> Self {
        let mut border_flag = ObBorderFlag::new();
        border_flag.set_inclusive_start();
        border_flag.set_inclusive_end();

        Self {
            table_id: OB_INVALID_ID,
            border_flag,
            start_key: ObRowKey::new(start_key),
            end_key: ObRowKey::new(end_key),
        }
    }

    pub fn get_border_flag(&self) -> &ObBorderFlag {
        &self.border_flag
    }

    pub fn get_start_key(&self) -> &ObRowKey {
        &self.start_key
    }

    pub fn get_end_key(&self) -> &ObRowKey {
        &self.end_key
    }

    pub fn content_len(&self) -> Result<usize> {
        Ok(util::encoded_length_vi64(self.table_id)
            + 1
            + self.start_key.content_len()?
            + self.end_key.content_len()?)
    }

    pub fn set_inclusive_start(&mut self) {
        self.border_flag.set_inclusive_start()
    }

    pub fn unset_inclusive_start(&mut self) {
        self.border_flag.unset_inclusive_start()
    }

    pub fn is_inclusive_start(&self) -> bool {
        self.border_flag.is_inclusive_start()
    }

    pub fn set_inclusive_end(&mut self) {
        self.border_flag.set_inclusive_end()
    }

    pub fn unset_inclusive_end(&mut self) {
        self.border_flag.unset_inclusive_end()
    }

    pub fn is_inclusive_end(&self) -> bool {
        self.border_flag.is_inclusive_end()
    }

    pub fn set_max_value(&mut self) {
        self.border_flag.set_max_value()
    }

    pub fn unset_max_value(&mut self) {
        self.border_flag.unset_max_value()
    }

    pub fn is_max_value(&self) -> bool {
        self.border_flag.is_max_value()
    }

    pub fn set_min_value(&mut self) {
        self.border_flag.set_min_value()
    }

    pub fn unset_min_value(&mut self) {
        self.border_flag.unset_min_value()
    }

    pub fn is_min_value(&self) -> bool {
        self.border_flag.is_min_value()
    }
}

impl ProtoEncoder for ObNewRange {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        buf.reserve(self.content_len()?);

        util::encode_vi64(self.table_id, buf)?;
        buf.put_i8(self.border_flag.value());
        self.start_key.encode(buf)?;
        self.end_key.encode(buf)
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum ObScanOrder {
    ImplementedOrder = 0,
    #[default]
    Forward = 1,
    Reverse = 2,
    KeepOrder = 3,
}

impl ObScanOrder {
    pub fn from_i32(value: i32) -> Result<ObScanOrder> {
        match value {
            0 => Ok(ObScanOrder::ImplementedOrder),
            1 => Ok(ObScanOrder::Forward),
            2 => Ok(ObScanOrder::Reverse),
            3 => Ok(ObScanOrder::KeepOrder),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid ob scan order: {value}."),
            )),
        }
    }

    pub fn from_bool(forward: bool) -> ObScanOrder {
        if forward {
            ObScanOrder::Forward
        } else {
            ObScanOrder::Reverse
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObHTableFilter {
    base: BasePayLoad,
    is_valid: bool,
    select_column_qualifier: Vec<String>,
    min_stamp: i64,
    max_stamp: i64,
    max_versions: i32,
    limit_per_row_per_cf: i32,
    offset_per_row_per_cf: i32,
    filter_string: String,
}

impl ObHTableFilter {}

impl ObPayload for ObHTableFilter {
    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    //payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        let mut len: usize = 0;

        len += 1;

        len += util::encoded_length_vi32(self.select_column_qualifier.len() as i32);
        for q in &self.select_column_qualifier {
            len += util::encoded_length_vstring(q);
        }

        len += util::encoded_length_vi64(self.min_stamp);
        len += util::encoded_length_vi64(self.max_stamp);
        len += util::encoded_length_vi32(self.max_versions);
        len += util::encoded_length_vi32(self.limit_per_row_per_cf);
        len += util::encoded_length_vi32(self.offset_per_row_per_cf);
        len += util::encoded_length_vstring(&self.filter_string);

        Ok(len)
    }
}

impl ProtoEncoder for ObHTableFilter {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;

        buf.put_i8(self.is_valid as i8);
        //select_column_qualifier
        util::encode_vi32(self.select_column_qualifier.len() as i32, buf)?;
        for q in &self.select_column_qualifier {
            util::encode_vstring(q, buf)?;
        }

        util::encode_vi64(self.min_stamp, buf)?;
        util::encode_vi64(self.max_stamp, buf)?;
        util::encode_vi32(self.max_versions, buf)?;
        util::encode_vi32(self.limit_per_row_per_cf, buf)?;
        util::encode_vi32(self.offset_per_row_per_cf, buf)?;
        util::encode_vstring(&self.filter_string, buf)?;

        Ok(())
    }
}

impl ProtoDecoder for ObHTableFilter {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!();
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct ObTableQuery {
    base: BasePayLoad,
    key_ranges: Vec<ObNewRange>,
    select_columns: Vec<String>,
    filter_string: String,
    limit: i32,
    offset: i32,
    scan_order: ObScanOrder,
    index_name: String,
    batch_size: i32,
    max_result_size: i64,
    htable_filter: Option<ObHTableFilter>,
    is_hbase_query: bool,
    scan_range_columns: Vec<String>,
    aggregations: Vec<ObTableAggregationOp>,
}

const HTABLE_FILTER_DUMMY_BYTES: &[u8] = &[0x01, 0x00];

impl ObPayload for ObTableQuery {
    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    //payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        let mut len: usize = 0;

        len += util::encoded_length_vi64(self.key_ranges.len() as i64);
        for r in &self.key_ranges {
            len += r.content_len()?;
        }

        len += util::encoded_length_vi64(self.select_columns.len() as i64);
        for s in &self.select_columns {
            len += util::encoded_length_vstring(s);
        }

        len += util::encoded_length_vstring(&self.filter_string);
        len += util::encoded_length_vi32(self.limit);
        len += util::encoded_length_vi32(self.offset);
        len += 1; //scan order
        len += util::encoded_length_vstring(&self.index_name);
        len += util::encoded_length_vi32(self.batch_size);
        len += util::encoded_length_vi64(self.max_result_size);

        if self.is_hbase_query {
            match &self.htable_filter {
                Some(filter) => {
                    len += filter.content_len()?;
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Missing htable_filter.",
                    ));
                }
            }
        } else {
            len += HTABLE_FILTER_DUMMY_BYTES.len();
        }

        len += util::encoded_length_vi64(self.scan_range_columns.len() as i64);
        for s in &self.scan_range_columns {
            len += util::encoded_length_vstring(s);
        }

        len += util::encoded_length_vi64(self.aggregations.len() as i64);
        for agg in &self.aggregations {
            len += agg.clone().encode_size()?;
        }

        Ok(len)
    }
}

impl ProtoEncoder for ObTableQuery {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;

        util::encode_vi64(self.key_ranges.len() as i64, buf)?;
        for r in &self.key_ranges {
            r.encode(buf)?;
        }
        util::encode_vi64(self.select_columns.len() as i64, buf)?;
        for s in &self.select_columns {
            util::encode_vstring(s, buf)?;
        }
        util::encode_vstring(&self.filter_string, buf)?;
        util::encode_vi32(self.limit, buf)?;
        util::encode_vi32(self.offset, buf)?;
        buf.put_i8(self.scan_order.clone() as i8);
        util::encode_vstring(&self.index_name, buf)?;
        util::encode_vi32(self.batch_size, buf)?;
        util::encode_vi64(self.max_result_size, buf)?;
        if self.is_hbase_query {
            match &self.htable_filter {
                Some(filter) => {
                    filter.encode(buf)?;
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Missing htable_filter.",
                    ));
                }
            }
        } else {
            buf.put_slice(HTABLE_FILTER_DUMMY_BYTES);
        }

        util::encode_vi64(self.scan_range_columns.len() as i64, buf)?;
        for s in &self.scan_range_columns {
            util::encode_vstring(s, buf)?;
        }

        util::encode_vi64(self.aggregations.len() as i64, buf)?;
        for agg in &self.aggregations {
            agg.encode(buf)?;
        }

        Ok(())
    }
}

impl ProtoDecoder for ObTableQuery {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!();
    }
}

impl ObTableQuery {
    pub fn new() -> Self {
        Self {
            base: BasePayLoad::new(),
            key_ranges: vec![],
            select_columns: vec![],
            filter_string: "".to_owned(),
            limit: -1,
            offset: 0,
            scan_order: ObScanOrder::Forward,
            index_name: "".to_owned(),
            batch_size: -1,
            max_result_size: -1,
            htable_filter: None,
            is_hbase_query: false,
            scan_range_columns: vec![],
            aggregations: vec![],
        }
    }

    /// check aggregation
    pub fn is_aggregation(&self) -> bool {
        !self.aggregations.is_empty()
    }

    /// add single aggregate operation
    pub fn add_aggregation(mut self, aggtype: ObTableAggregationType, aggcolumn: String) -> Self {
        self.aggregations
            .push(ObTableAggregationOp::new(aggtype, aggcolumn));
        self
    }

    pub fn batch_size(&self) -> i32 {
        self.batch_size
    }

    pub fn set_batch_size(&mut self, batch_size: i32) {
        self.batch_size = batch_size;
    }

    pub fn select_columns(&mut self, columns: Vec<String>) {
        self.select_columns = columns;
    }

    pub fn set_limit(&mut self, limit: i32) {
        self.limit = limit;
    }

    pub fn set_offset(&mut self, offset: i32) {
        self.offset = offset;
    }

    pub fn add_key_range(&mut self, key_range: ObNewRange) {
        self.key_ranges.push(key_range);
    }

    pub fn set_scan_order(&mut self, scan_order: ObScanOrder) {
        self.scan_order = scan_order;
    }

    pub fn set_index_name(&mut self, index_name: String) {
        self.index_name = index_name;
    }

    pub fn set_filter_string(&mut self, s: String) {
        self.filter_string = s;
    }

    pub fn set_htable_filter(&mut self, filter: ObHTableFilter) {
        self.htable_filter = Some(filter);
    }

    pub fn get_key_ranges(&self) -> &[ObNewRange] {
        &self.key_ranges
    }

    /// Verify whether the query is valid.
    pub fn verify(&self) -> error::Result<()> {
        if self.select_columns.is_empty() {
            return Err(CommonErr(
                CommonErrCode::InvalidParam,
                "TableQuery select columns is empty.".to_owned(),
            ));
        }

        Ok(())
    }
}

//TODO refactor with ObTableOperationRequest/ObTableBatchOperationRequest
pub struct ObTableQueryRequest {
    base: BasePayLoad,
    credential: Vec<u8>,
    table_name: String,
    table_id: i64,
    partition_id: i64,
    entity_type: ObTableEntityType,
    table_query: ObTableQuery,
    consistency_level: ObTableConsistencyLevel,
    _return_row_key: bool,
    _return_affected_entity: bool,
    _return_affected_rows: bool,
}

impl ObTableQueryRequest {
    pub fn new(
        table_name: &str,
        partition_id: i64,
        entity_type: ObTableEntityType,
        table_query: ObTableQuery,
        timeout: Duration,
        flag: u16,
    ) -> Self {
        let mut base = BasePayLoad::new();
        base.timeout = duration_to_millis(&timeout);
        base.flag = flag;
        Self {
            base,
            credential: vec![],
            table_name: table_name.to_owned(),
            table_id: OB_INVALID_ID,
            partition_id,
            entity_type,
            table_query,
            consistency_level: ObTableConsistencyLevel::Strong,
            //TODO these fields is not used right now.
            _return_row_key: false,
            _return_affected_entity: false,
            _return_affected_rows: true,
        }
    }
}

impl ObPayload for ObTableQueryRequest {
    fn set_credential(&mut self, credential: &[u8]) {
        self.credential = credential.to_owned();
    }

    fn pcode(&self) -> ObTablePacketCode {
        ObTablePacketCode::ExecuteQuery
    }

    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    //payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        Ok(util::encoded_length_bytes_string(&self.credential)
            + util::encoded_length_vstring(&self.table_name)
            + util::encoded_length_vi64(self.table_id)
            + util::encoded_length_vi64(self.partition_id)
            + self.table_query.len()?
            + 2)
    }
}

impl ProtoEncoder for ObTableQueryRequest {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;

        util::encode_bytes_string(&self.credential, buf)?;
        util::encode_vstring(&self.table_name, buf)?;
        util::encode_vi64(self.table_id, buf)?;
        util::encode_vi64(self.partition_id, buf)?;
        buf.put_i8(self.entity_type as i8);
        buf.put_i8(self.consistency_level as i8);

        self.table_query.encode(buf)?;

        Ok(())
    }
}

impl ProtoDecoder for ObTableQueryRequest {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!()
    }
}

pub struct ObTableStreamRequest {
    base: BasePayLoad,
    session_id: u64,
    flag: u16,
}

impl ObPayload for ObTableStreamRequest {
    fn pcode(&self) -> ObTablePacketCode {
        ObTablePacketCode::ExecuteQuery
    }

    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    //payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        Ok(0)
    }

    fn session_id(&self) -> u64 {
        self.session_id
    }

    fn flag(&self) -> u16 {
        self.flag
    }
}

impl ObTableStreamRequest {
    pub fn new(session_id: u64, timeout: Duration, flag: u16) -> Self {
        let mut base = BasePayLoad::new();
        base.timeout = duration_to_millis(&timeout);
        base.flag = flag;
        Self {
            base,
            session_id,
            flag,
        }
    }

    pub fn set_stream_next(&mut self) {
        self.flag &= !STREAM_LAST_FLAG;
        self.flag |= STREAM_FLAG;
    }

    pub fn set_stream_last(&mut self) {
        self.flag |= STREAM_LAST_FLAG;
        self.flag |= STREAM_FLAG;
    }

    pub fn is_stream_next(&self) -> bool {
        self.flag & STREAM_FLAG != 0 && self.flag & STREAM_LAST_FLAG == 0
    }
}

impl ProtoEncoder for ObTableStreamRequest {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;
        Ok(())
    }
}

impl ProtoDecoder for ObTableStreamRequest {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub enum ObTableAggregationType {
    #[default]
    INVAILD = 0,
    MAX = 1,
    MIN = 2,
    COUNT = 3,
    SUM = 4,
    AVG = 5,
}

/// this struct is a single aggregation
#[derive(Default, Debug, Clone, PartialEq)]
pub struct ObTableAggregationOp {
    base: BasePayLoad,
    agg_type: ObTableAggregationType,
    agg_column: String,
}

impl ObTableAggregationOp {
    pub(crate) fn new(aggtype: ObTableAggregationType, aggcolumn: String) -> Self {
        Self {
            base: BasePayLoad::default(),
            agg_type: aggtype,
            agg_column: aggcolumn,
        }
    }

    /// Total size of aggregation op for encode
    pub fn encode_size(self) -> Result<usize> {
        Ok(util::encoded_length_vi64(1) // version
            + util::encoded_length_vi64(self.clone().content_len()? as i64)
            + util::encoded_length_vi8(self.agg_type as i8)
            + util::encoded_length_vstring(&self.agg_column))
    }
}

impl ProtoEncoder for ObTableAggregationOp {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;
        buf.put_i8(self.agg_type.clone() as i8);
        util::encode_vstring(&self.agg_column, buf)?;
        Ok(())
    }
}

impl ProtoDecoder for ObTableAggregationOp {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!()
    }
}

impl ObPayload for ObTableAggregationOp {
    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    //payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        let mut len: usize = 0;
        len += util::encoded_length_vi8(self.agg_type.clone() as i8);
        len += util::encoded_length_vstring(&self.agg_column);
        Ok(len)
    }
}
