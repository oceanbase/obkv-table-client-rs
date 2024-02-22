/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2024 OceanBase
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

#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use std::{cmp, time::Duration};

use bytes::{Buf, BufMut, BytesMut};
use linked_hash_map::LinkedHashMap;

use crate::payloads::{ObRowKey, ObTableOperationType, ObTableResult};
use crate::query::ObNewRange;
use crate::rpc::protocol::TraceId;
use crate::serde_obkv::util::decode_u8;
use crate::util::decode_value;
use crate::{
    location::OB_INVALID_ID,
    payloads::{ObTableConsistencyLevel, ObTableEntityType},
    rpc::protocol::{
        BasePayLoad, ObPayload, ObTablePacketCode, ProtoDecoder, ProtoEncoder, Result,
    },
    serde_obkv::util,
    util::duration_to_millis,
    Value,
};

/// [`ColumnNamePair`] is used for sorting
/// Used by [`ObTableSingleOpQuery`] and [`ObTableSingleOpEntity`]
struct ColumnNamePair {
    idx: i64,
    origin_idx: i64,
}

impl ColumnNamePair {
    fn new(idx: i64, origin_idx: i64) -> Self {
        Self { idx, origin_idx }
    }
}

impl PartialEq for ColumnNamePair {
    fn eq(&self, other: &Self) -> bool {
        self.idx == other.idx
    }
}

impl Eq for ColumnNamePair {}

impl PartialOrd for ColumnNamePair {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ColumnNamePair {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.idx.cmp(&other.idx)
    }
}

/// Option flag for [`ObTableSingleOp`]
#[derive(Debug, Clone, PartialEq)]
pub struct ObTableSingleOpFlag {
    flags: i64,
}

impl Default for ObTableSingleOpFlag {
    fn default() -> Self {
        ObTableSingleOpFlag::new()
    }
}

impl ObTableSingleOpFlag {
    const FLAG_IS_CHECK_NOT_EXISTS: i64 = 1 << 0;

    pub fn new() -> Self {
        let mut flag = ObTableSingleOpFlag { flags: 0 };
        flag.set_flag_is_check_not_exist(false);
        flag
    }

    pub fn value(&self) -> i64 {
        self.flags
    }

    pub fn set_value(&mut self, flags: i64) {
        self.flags = flags;
    }

    pub fn set_flag_is_check_not_exist(&mut self, is_check_not_exist: bool) {
        if is_check_not_exist {
            self.flags |= Self::FLAG_IS_CHECK_NOT_EXISTS;
        } else {
            self.flags &= !Self::FLAG_IS_CHECK_NOT_EXISTS;
        }
    }

    pub fn is_check_not_exist(&self) -> bool {
        (self.flags & Self::FLAG_IS_CHECK_NOT_EXISTS) != 0
    }
}

/// Option flag for [`ObTableTabletOp`]
#[derive(Debug, Clone, PartialEq)]
pub struct ObTableTabletOpFlag {
    flags: i64,
}

impl Default for ObTableTabletOpFlag {
    fn default() -> Self {
        ObTableTabletOpFlag::new()
    }
}

impl ObTableTabletOpFlag {
    const FLAG_IS_SAME_PROPERTIES_NAMES: i64 = 1 << 1;
    const FLAG_IS_SAME_TYPE: i64 = 1 << 0;

    pub fn new() -> Self {
        let mut flag = ObTableTabletOpFlag { flags: 0 };
        flag.set_flag_is_same_type(false);
        flag.set_flag_is_same_properties_names(false);
        flag
    }

    pub fn value(&self) -> i64 {
        self.flags
    }

    pub fn set_value(&mut self, flags: i64) {
        self.flags = flags;
    }

    pub fn set_flag_is_same_type(&mut self, is_same_type: bool) {
        if is_same_type {
            self.flags |= Self::FLAG_IS_SAME_TYPE;
        } else {
            self.flags &= !Self::FLAG_IS_SAME_TYPE;
        }
    }

    pub fn set_flag_is_same_properties_names(&mut self, is_same_properties_names: bool) {
        if is_same_properties_names {
            self.flags |= Self::FLAG_IS_SAME_PROPERTIES_NAMES;
        } else {
            self.flags &= !Self::FLAG_IS_SAME_PROPERTIES_NAMES;
        }
    }

    pub fn is_same_type(&self) -> bool {
        (self.flags & Self::FLAG_IS_SAME_TYPE) != 0
    }

    fn is_same_properties_names(&self) -> bool {
        (self.flags & Self::FLAG_IS_SAME_PROPERTIES_NAMES) != 0
    }
}

/// Option flag for [`ObTableLSOperation`]
#[derive(Debug, Clone, PartialEq)]
pub struct ObTableLSOpFlag {
    flags: i64,
}

impl Default for ObTableLSOpFlag {
    fn default() -> Self {
        ObTableLSOpFlag::new()
    }
}

impl ObTableLSOpFlag {
    const FLAG_IS_SAME_PROPERTIES_NAMES: i64 = 1 << 1;
    const FLAG_IS_SAME_TYPE: i64 = 1 << 0;

    pub fn new() -> Self {
        let mut flag = ObTableLSOpFlag { flags: 0 };
        flag.set_flag_is_same_type(false);
        flag.set_flag_is_same_properties_names(false);
        flag
    }

    pub fn value(&self) -> i64 {
        self.flags
    }

    pub fn set_value(&mut self, flags: i64) {
        self.flags = flags;
    }

    pub fn set_flag_is_same_type(&mut self, is_same_type: bool) {
        if is_same_type {
            self.flags |= Self::FLAG_IS_SAME_TYPE;
        } else {
            self.flags &= !Self::FLAG_IS_SAME_TYPE;
        }
    }

    pub fn set_flag_is_same_properties_names(&mut self, is_same_properties_names: bool) {
        if is_same_properties_names {
            self.flags |= Self::FLAG_IS_SAME_PROPERTIES_NAMES;
        } else {
            self.flags &= !Self::FLAG_IS_SAME_PROPERTIES_NAMES;
        }
    }

    pub fn is_same_type(&self) -> bool {
        (self.flags & Self::FLAG_IS_SAME_TYPE) != 0
    }

    pub fn is_same_properties_names(&self) -> bool {
        (self.flags & Self::FLAG_IS_SAME_PROPERTIES_NAMES) != 0
    }
}

/// Single operation query in OBKV
/// [`ObTableSingleOpQuery`] & [`ObTableSingleOpEntity`] -> [`ObTableSingleOp`] -> [`ObTableTabletOp`] -> [`ObTableLSOperation`]
#[derive(Debug, Clone)]
pub struct ObTableSingleOpQuery {
    // encoded params
    base: BasePayLoad,
    index_name: String,
    scan_range_len: usize,
    scan_range_cols_bm: Vec<u8>,
    key_ranges: Vec<ObNewRange>,
    filter_string: String,

    // intermediate params
    scan_range_columns: Vec<String>,
    agg_column_names: Option<Arc<Vec<String>>>,
}

impl Default for ObTableSingleOpQuery {
    fn default() -> Self {
        ObTableSingleOpQuery::new(Vec::new(), Vec::new())
    }
}

impl ObTableSingleOpQuery {
    pub fn new(scan_range_columns: Vec<String>, key_ranges: Vec<ObNewRange>) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            index_name: String::new(),
            scan_range_cols_bm: Vec::new(),
            key_ranges,
            filter_string: String::new(),
            scan_range_len: 0usize,
            scan_range_columns,
            agg_column_names: None,
        }
    }

    pub fn set_agg_column_names(&mut self, agg_column_names: Arc<Vec<String>>) {
        self.agg_column_names = Some(agg_column_names)
    }

    pub fn set_filter_string(&mut self, filter_string: String) {
        self.filter_string = filter_string
    }

    pub fn scan_range_columns(&self) -> &Vec<String> {
        &self.scan_range_columns
    }

    pub fn adjust_scan_range_columns(&mut self, column_name_idx_map: &HashMap<String, i64>) {
        self.scan_range_len = column_name_idx_map.len();
        let size = (self.scan_range_len as f64 / 8.0).ceil() as usize;
        let mut byte_array = vec![0u8; size];
        let mut column_name_idx = Vec::with_capacity(self.key_ranges.len());

        // construct columns names bitmap
        for name in &self.scan_range_columns {
            if let Some(&index) = column_name_idx_map.get(name) {
                column_name_idx.push(index);
                let byte_index = (index / 8) as usize;
                let bit_index = (index % 8) as usize;
                byte_array[byte_index] |= 1 << bit_index;
            }
        }

        // sort column names
        let mut pairs: Vec<ColumnNamePair> = column_name_idx
            .iter()
            .enumerate()
            .map(|(origin_idx, &idx)| ColumnNamePair::new(idx, origin_idx as i64))
            .collect();

        pairs.sort();

        // rearrange key ranges
        for range in &mut self.key_ranges {
            let start_key = range.get_start_key();
            let end_key = range.get_end_key();
            let adjust_start_key: Vec<Value> = pairs
                .iter()
                .map(|pair| start_key.keys()[pair.origin_idx as usize].clone())
                .collect();
            let adjust_end_key: Vec<Value> = pairs
                .iter()
                .map(|pair| end_key.keys()[pair.origin_idx as usize].clone())
                .collect();

            range.set_start_key(ObRowKey::new(adjust_start_key));
            range.set_end_key(ObRowKey::new(adjust_end_key));
        }

        self.scan_range_cols_bm = byte_array;
    }
}

impl ObPayload for ObTableSingleOpQuery {
    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    // payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        let mut len: usize = 0;
        len += util::encoded_length_vstring(&self.index_name);
        len += util::encoded_length_vi64(self.scan_range_len as i64);
        len += self.scan_range_cols_bm.len();
        len += util::encoded_length_vi64(self.key_ranges.len() as i64);
        for r in &self.key_ranges {
            len += r.content_len()?;
        }
        len += util::encoded_length_vstring(&self.filter_string);
        Ok(len)
    }
}

impl ProtoEncoder for ObTableSingleOpQuery {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;
        // 1. encode index name
        util::encode_vstring(&self.index_name, buf)?;
        // 2. encode column name bitmap
        util::encode_vi64(self.scan_range_len as i64, buf)?;
        buf.put_slice(&self.scan_range_cols_bm);
        // 3. encode key ranges
        util::encode_vi64(self.key_ranges.len() as i64, buf)?;
        for r in &self.key_ranges {
            r.encode(buf)?;
        }
        // 4. encode filter string
        util::encode_vstring(&self.filter_string, buf)?;
        Ok(())
    }
}

impl ProtoDecoder for ObTableSingleOpQuery {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!();
    }
}

/// Single operation entity in OBKV
/// [`ObTableSingleOpQuery`] & [`ObTableSingleOpEntity`] -> [`ObTableSingleOp`] -> [`ObTableTabletOp`] -> [`ObTableLSOperation`]
#[derive(Debug, Clone)]
pub struct ObTableSingleOpEntity {
    // encoded params
    base: BasePayLoad,
    row_key_bit_len: usize,
    row_key_bitmap: Vec<u8>,
    row_key: Vec<Value>,
    properties_bit_len: usize,
    properties_bitmap: Vec<u8>,
    properties: Vec<Value>,

    // intermediate params
    row_key_names: Vec<String>,
    properties_names: Vec<String>,
    agg_row_key_names: Option<Arc<Vec<String>>>,
    agg_properties_names: Option<Arc<Vec<String>>>,

    // options
    ignore_encode_properties_col_names: bool,
}

impl Default for ObTableSingleOpEntity {
    fn default() -> Self {
        ObTableSingleOpEntity::new(Vec::new(), Vec::new(), Vec::new(), Vec::new())
    }
}

impl ObTableSingleOpEntity {
    pub fn new(
        row_key_names: Vec<String>,
        row_key: Vec<Value>,
        properties_names: Vec<String>,
        properties: Vec<Value>,
    ) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            row_key_bit_len: 0usize,
            row_key_bitmap: Vec::new(),
            row_key,
            properties_bit_len: 0usize,
            properties_bitmap: Vec::new(),
            properties,
            row_key_names,
            properties_names,
            agg_row_key_names: None,
            agg_properties_names: None,
            ignore_encode_properties_col_names: false,
        }
    }

    pub fn set_agg_properties_names(&mut self, agg_properties_names: Arc<Vec<String>>) {
        self.agg_properties_names = Some(agg_properties_names)
    }

    pub fn take_properties(self) -> HashMap<String, Value> {
        let name_value_pairs = self.properties_names.into_iter().zip(self.properties);
        name_value_pairs.collect()
    }

    /// parse_bit_map is used by [`ProtoDecoder`]
    /// Accept source byte, length and aggregation column names
    /// Output bit map and corresponding column names
    pub fn parse_bit_map(
        len: usize,
        agg_names: Arc<Vec<String>>,
        src: &mut BytesMut,
        bit_map: &mut Vec<u8>,
        column_names: &mut Vec<String>,
    ) -> Result<()> {
        bit_map.clear();
        column_names.clear();
        let bm_len = (len + 7) / 8;
        for idx in 0..bm_len {
            bit_map.push(decode_u8(src)?);
            for bit_idx in 0usize..8 {
                if bit_map[idx] & (1u8 << bit_idx) != 0 && idx * 8 + bit_idx < agg_names.len() {
                    column_names.push(agg_names[idx * 8 + bit_idx].clone());
                }
            }
        }
        Ok(())
    }

    pub fn row_key_names(&self) -> &Vec<String> {
        &self.row_key_names
    }

    pub fn properties_names(&self) -> &Vec<String> {
        &self.properties_names
    }

    /// [`is_same_properties_names_len`] check whether length is equal
    pub fn is_same_properties_names_len(&self, column_name_idx_map_len: usize) -> bool {
        self.properties_names.len() == column_name_idx_map_len
    }

    pub fn set_ignore_encode_properties_column_names(
        &mut self,
        ignore_encode_properties_col_names: bool,
    ) {
        self.ignore_encode_properties_col_names = ignore_encode_properties_col_names;
    }

    pub fn adjust_row_key_column_name(&mut self, column_name_idx_map: &HashMap<String, i64>) {
        self.row_key_bit_len = column_name_idx_map.len();
        let size = (self.row_key_bit_len as f64 / 8.0).ceil() as usize;
        let mut byte_array = vec![0u8; size];
        let mut column_name_idx = Vec::with_capacity(self.row_key.len());

        // construct row key columns names bitmap
        for name in &self.row_key_names {
            if let Some(&index) = column_name_idx_map.get(name) {
                column_name_idx.push(index);
                let byte_index = (index / 8) as usize;
                let bit_index = (index % 8) as usize;
                byte_array[byte_index] |= 1 << bit_index;
            }
        }

        // sort row key column names
        let mut pairs: Vec<ColumnNamePair> = column_name_idx
            .iter()
            .enumerate()
            .map(|(origin_idx, &idx)| ColumnNamePair::new(idx, origin_idx as i64))
            .collect();

        pairs.sort();

        // rearrange row key names
        let mut res_row_key = Vec::with_capacity(column_name_idx.len());
        for pair in pairs {
            res_row_key.push(self.row_key[pair.origin_idx as usize].to_owned());
        }
        self.row_key = res_row_key;

        self.row_key_bitmap = byte_array;
    }

    pub fn adjust_properties_column_name(&mut self, column_name_idx_map: &HashMap<String, i64>) {
        if !self.ignore_encode_properties_col_names {
            self.properties_bit_len = 0usize;
        } else {
            return;
        }
        let size = (self.properties_bit_len as f64 / 8.0).ceil() as usize;
        let mut byte_array = vec![0u8; size];
        let mut column_name_idx = Vec::with_capacity(self.properties.len());

        // construct properties columns names bitmap
        for name in &self.properties_names {
            if let Some(&index) = column_name_idx_map.get(name) {
                column_name_idx.push(index);
                let byte_index = (index / 8) as usize;
                let bit_index = (index % 8) as usize;
                byte_array[byte_index] |= 1 << bit_index;
            }
        }

        // sort properties names
        let mut pairs: Vec<ColumnNamePair> = column_name_idx
            .iter()
            .enumerate()
            .map(|(origin_idx, &idx)| ColumnNamePair::new(idx, origin_idx as i64))
            .collect();

        pairs.sort();

        // rearrange properties names
        let mut res_properties_names = Vec::with_capacity(column_name_idx.len());
        for pair in pairs {
            res_properties_names.push(self.properties_names[pair.origin_idx as usize].to_owned());
        }
        self.properties_names = res_properties_names;

        self.properties_bitmap = byte_array;
    }
}

impl ObPayload for ObTableSingleOpEntity {
    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    // payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        let mut len: usize = 0;
        len += util::encoded_length_vi64(self.row_key_bit_len as i64);
        len += self.row_key_bitmap.len();
        len += util::encoded_length_vi64(self.row_key.len() as i64);
        for rk in &self.row_key {
            len += rk.len();
        }
        if self.ignore_encode_properties_col_names {
            len += util::encoded_length_vi64(0i64);
        } else {
            len += util::encoded_length_vi64(self.properties_bit_len as i64);
            len += self.properties_bitmap.len();
        }
        len += util::encoded_length_vi64(self.properties.len() as i64);
        for ppt in &self.properties {
            len += ppt.len();
        }
        Ok(len)
    }
}

impl ProtoEncoder for ObTableSingleOpEntity {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;
        // 1. encode rowkey bitmap
        util::encode_vi64(self.row_key_bit_len as i64, buf)?;
        buf.put_slice(&self.row_key_bitmap);
        // 2. encode rowkey
        util::encode_vi64(self.row_key.len() as i64, buf)?;
        for key in &self.row_key {
            key.encode(buf)?;
        }
        // 3. encode properties bitmap
        if self.ignore_encode_properties_col_names {
            util::encode_vi64(0i64, buf)?;
        } else {
            util::encode_vi64(self.properties_bit_len as i64, buf)?;
            buf.put_slice(&self.properties_bitmap);
        }
        // 4. encode properties
        util::encode_vi64(self.properties.len() as i64, buf)?;
        for key in &self.properties {
            key.encode(buf)?;
        }
        Ok(())
    }
}

impl ProtoDecoder for ObTableSingleOpEntity {
    fn decode(&mut self, src: &mut BytesMut) -> Result<()> {
        self.decode_base(src)?;

        // 1. row key bitmap
        self.row_key_bit_len = util::decode_vi64(src)? as usize;
        ObTableSingleOpEntity::parse_bit_map(
            self.row_key_bit_len,
            self.agg_row_key_names
                .as_ref()
                .unwrap_or(&Arc::new(Vec::new()))
                .clone(),
            src,
            &mut self.row_key_bitmap,
            &mut self.row_key_names,
        )?;

        // 2. row key obobj
        self.row_key.clear();
        let row_key_len = util::decode_vi64(src)?;
        for _ in 0..row_key_len {
            self.row_key.push(decode_value(src)?);
        }

        // 3. properties bitmaps
        self.properties_bit_len = util::decode_vi64(src)? as usize;
        ObTableSingleOpEntity::parse_bit_map(
            self.properties_bit_len,
            self.agg_properties_names
                .as_ref()
                .unwrap_or(&Arc::new(Vec::new()))
                .clone(),
            src,
            &mut self.properties_bitmap,
            &mut self.properties_names,
        )?;

        // 4. properties obobj
        self.properties_names.clear();
        let properties_len = util::decode_vi64(src)?;
        for _ in 0..properties_len {
            self.properties.push(decode_value(src)?);
        }

        Ok(())
    }
}

/// Single operation in OBKV
/// [`ObTableSingleOpQuery`] & [`ObTableSingleOpEntity`] -> [`ObTableSingleOp`] -> [`ObTableTabletOp`] -> [`ObTableLSOperation`]
#[derive(Debug, Clone)]
pub struct ObTableSingleOp {
    base: BasePayLoad,
    op_type: ObTableOperationType,
    op_flag: ObTableSingleOpFlag,
    query: Option<ObTableSingleOpQuery>,
    entities: Vec<ObTableSingleOpEntity>,
}

impl Default for ObTableSingleOp {
    fn default() -> Self {
        ObTableSingleOp::new(ObTableOperationType::Invalid)
    }
}

impl ObTableSingleOp {
    pub fn new(op_type: ObTableOperationType) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            op_type,
            op_flag: ObTableSingleOpFlag::new(),
            query: None,
            entities: Vec::new(),
        }
    }

    pub fn query(&self) -> &Option<ObTableSingleOpQuery> {
        &self.query
    }

    pub fn query_mut(&mut self) -> &mut Option<ObTableSingleOpQuery> {
        &mut self.query
    }

    pub fn set_query(&mut self, query: ObTableSingleOpQuery) {
        self.query = Some(query)
    }

    pub fn set_is_check_not_exists(&mut self, is_check_not_exists: bool) {
        self.op_flag
            .set_flag_is_check_not_exist(is_check_not_exists)
    }

    pub fn is_check_not_exist(&self) -> bool {
        self.op_flag.is_check_not_exist()
    }

    pub fn single_op_type(&self) -> ObTableOperationType {
        self.op_type
    }

    pub fn set_single_op_type(&mut self, op_type: ObTableOperationType) {
        self.op_type = op_type
    }

    pub fn entities(&self) -> &Vec<ObTableSingleOpEntity> {
        &self.entities
    }

    pub fn entities_mut(&mut self) -> &mut Vec<ObTableSingleOpEntity> {
        &mut self.entities
    }

    pub fn add_entity(&mut self, entity: ObTableSingleOpEntity) {
        self.entities.push(entity)
    }
}

impl ObPayload for ObTableSingleOp {
    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    // payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        let mut len: usize = 0;
        len += 1; // op type
        len += util::encoded_length_vi64(self.op_flag.value());
        if self.op_type.need_encode_query() {
            len += self
                .query
                .as_ref()
                .map_or(0, |query| query.len().unwrap_or(0));
        }
        len += util::encoded_length_vi64(self.entities.len() as i64);
        for entity in &self.entities {
            len += entity.len()?;
        }
        Ok(len)
    }
}

impl ProtoEncoder for ObTableSingleOp {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;
        // 1. op type
        buf.put_i8(self.op_type as i8);
        // 2. op flag
        util::encode_vi64(self.op_flag.value(), buf)?;
        // 3. single op query
        if self.op_type.need_encode_query() {
            if let Some(query) = &self.query {
                query.encode(buf)?;
            }
        }
        // 4. single op entity
        util::encode_vi64(self.entities.len() as i64, buf)?;
        for entity in &self.entities {
            entity.encode(buf)?;
        }
        Ok(())
    }
}

impl ProtoDecoder for ObTableSingleOp {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!();
    }
}

/// ObTableSingleOpResult is the result of [`ObTableSingleOp`] from server
#[derive(Debug)]
pub struct ObTableSingleOpResult {
    base: BasePayLoad,
    header: ObTableResult,
    operation_type: ObTableOperationType,
    entity: ObTableSingleOpEntity,
    affected_rows: i64,

    // intermediate params
    agg_properties_names: Option<Arc<Vec<String>>>,

    // debug info
    trace_id: TraceId,
    peer_addr: Option<SocketAddr>,
}

impl Default for ObTableSingleOpResult {
    fn default() -> ObTableSingleOpResult {
        ObTableSingleOpResult::new()
    }
}

impl ObTableSingleOpResult {
    pub fn new() -> ObTableSingleOpResult {
        ObTableSingleOpResult {
            base: BasePayLoad::dummy(),
            operation_type: ObTableOperationType::Invalid,
            header: ObTableResult::new(),
            entity: ObTableSingleOpEntity::default(),
            affected_rows: 0,
            agg_properties_names: None,
            trace_id: TraceId(0, 0),
            peer_addr: None,
        }
    }

    pub fn header(&self) -> &ObTableResult {
        &self.header
    }

    pub fn operation_type(&self) -> ObTableOperationType {
        self.operation_type
    }

    pub fn affected_rows(&self) -> i64 {
        self.affected_rows
    }

    pub fn take_entity(self) -> ObTableSingleOpEntity {
        self.entity
    }

    pub fn set_agg_properties_names(&mut self, agg_properties_names: Arc<Vec<String>>) {
        self.agg_properties_names = Some(agg_properties_names)
    }

    pub fn trace_id(&self) -> TraceId {
        self.trace_id
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }
}

impl ObPayload for ObTableSingleOpResult {
    fn pcode(&self) -> ObTablePacketCode {
        ObTablePacketCode::Execute
    }

    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    fn set_trace_id(&mut self, trace_id: TraceId) {
        self.trace_id = trace_id;
    }

    fn set_peer_addr(&mut self, addr: SocketAddr) {
        self.peer_addr = Some(addr);
    }
}

impl ProtoEncoder for ObTableSingleOpResult {
    fn encode(&self, _buf: &mut BytesMut) -> Result<()> {
        unimplemented!();
    }
}

impl ProtoDecoder for ObTableSingleOpResult {
    fn decode(&mut self, src: &mut BytesMut) -> Result<()> {
        self.decode_base(src)?;

        // 1. obTableResult
        self.header.decode(src)?;

        // 2. operation type
        self.operation_type = ObTableOperationType::from_i8(util::split_buf_to(src, 1)?.get_i8())?;

        // 3. entity
        if let Some(agg_properties_names) = &self.agg_properties_names {
            self.entity
                .set_agg_properties_names(agg_properties_names.clone());
        }
        self.entity.decode(src)?;

        // 4. affected rows
        self.affected_rows = util::decode_vi64(src)?;

        Ok(())
    }
}

/// Operations in one tablet in OBKV
/// [`ObTableSingleOp`] -> [`ObTableTabletOp`] -> [`ObTableLSOperation`]
#[derive(Debug, Clone)]
pub struct ObTableTabletOp {
    base: BasePayLoad,
    partition_id: i64,
    option_flag: ObTableTabletOpFlag,
    single_ops: Vec<ObTableSingleOp>,
}

impl Default for ObTableTabletOp {
    fn default() -> Self {
        ObTableTabletOp::dummy()
    }
}

impl ObTableTabletOp {
    pub fn internal_new(
        partition_id: i64,
        option_flag: ObTableTabletOpFlag,
        single_ops: Vec<ObTableSingleOp>,
    ) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            partition_id,
            option_flag,
            single_ops,
        }
    }

    pub fn new(ops_num: usize) -> Self {
        ObTableTabletOp::internal_new(
            0,
            ObTableTabletOpFlag::default(),
            Vec::with_capacity(ops_num),
        )
    }

    pub fn dummy() -> Self {
        ObTableTabletOp::new(0)
    }

    pub fn single_ops(&self) -> &Vec<ObTableSingleOp> {
        &self.single_ops
    }

    pub fn single_ops_mut(&mut self) -> &mut Vec<ObTableSingleOp> {
        &mut self.single_ops
    }

    pub fn set_single_ops(&mut self, single_ops: Vec<ObTableSingleOp>) {
        self.single_ops = single_ops
    }

    pub fn partition_id(&self) -> i64 {
        self.partition_id
    }

    pub fn set_partition_id(&mut self, partition_id: i64) {
        self.partition_id = partition_id
    }

    pub fn is_same_type(&self) -> bool {
        self.option_flag.is_same_type()
    }

    pub fn set_is_same_type(&mut self, is_same_type: bool) {
        self.option_flag
            .set_flag_is_same_properties_names(is_same_type)
    }
}

impl ObPayload for ObTableTabletOp {
    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    // payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        let mut len: usize = 0;
        len += 8; // partition/tablet_id
        len += util::encoded_length_vi64(self.option_flag.value());
        len += util::encoded_length_vi64(self.single_ops.len() as i64);
        for op in &self.single_ops {
            len += op.len()?;
        }
        Ok(len)
    }
}

impl ProtoEncoder for ObTableTabletOp {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;
        buf.put_i64(self.partition_id);
        util::encode_vi64(self.option_flag.value(), buf)?;
        util::encode_vi64(self.single_ops.len() as i64, buf)?;
        for op in &self.single_ops {
            op.encode(buf)?;
        }
        Ok(())
    }
}

impl ProtoDecoder for ObTableTabletOp {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!();
    }
}

/// ObTableTabletOpResult is the result of [`ObTableTabletOp`] from server
/// ObTableTabletOpResult may be modify in the future
#[derive(Debug, Default)]
pub struct ObTableTabletOpResult {
    base: BasePayLoad,
    op_results: Vec<ObTableSingleOpResult>,

    // intermediate params
    agg_properties_names: Option<Arc<Vec<String>>>,
}

impl ObTableTabletOpResult {
    pub fn new() -> Self {
        Self {
            base: BasePayLoad::dummy(),
            op_results: Vec::new(),
            agg_properties_names: None,
        }
    }

    pub fn get_op_results(&self) -> &[ObTableSingleOpResult] {
        &self.op_results
    }

    pub fn take_op_results(self) -> Vec<ObTableSingleOpResult> {
        self.op_results
    }

    pub fn set_agg_properties_names(&mut self, agg_properties_names: Arc<Vec<String>>) {
        self.agg_properties_names = Some(agg_properties_names)
    }
}

impl ObPayload for ObTableTabletOpResult {
    fn pcode(&self) -> ObTablePacketCode {
        ObTablePacketCode::BatchExecute
    }

    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }
}

impl ProtoEncoder for ObTableTabletOpResult {
    fn encode(&self, _buf: &mut BytesMut) -> Result<()> {
        unimplemented!();
    }
}

impl ProtoDecoder for ObTableTabletOpResult {
    fn decode(&mut self, src: &mut BytesMut) -> Result<()> {
        self.decode_base(src)?;

        let op_res_num = util::decode_vi64(src)?;
        if op_res_num < 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid operation results num:{op_res_num}"),
            ));
        }
        assert_eq!(0, self.op_results.len());
        self.op_results.reserve(op_res_num as usize);

        for _ in 0..op_res_num {
            let mut op_res = ObTableSingleOpResult::new();
            if let Some(agg_properties_names) = &self.agg_properties_names {
                op_res.set_agg_properties_names(agg_properties_names.clone());
            }
            op_res.decode(src)?;
            self.op_results.push(op_res);
        }

        Ok(())
    }
}

/// Operation in one Log Stream in OBKV
/// [`ObTableSingleOp`] -> [`ObTableTabletOp`] -> [`ObTableLSOperation`]
#[derive(Debug, Clone)]
pub struct ObTableLSOperation {
    // encoded params
    base: BasePayLoad,
    ls_id: i64,
    table_name: String,
    table_id: i64,
    row_key_names: Vec<String>,
    properties_names: Vec<String>,
    option_flag: ObTableLSOpFlag,
    tablet_ops: Vec<ObTableTabletOp>,

    // intermediate params
    row_key_names_set: LinkedHashMap<String, ()>,
    properties_names_set: LinkedHashMap<String, ()>,
    row_key_names_idx_map: HashMap<String, i64>,
    properties_names_idx_map: HashMap<String, i64>,
}

impl Default for ObTableLSOperation {
    fn default() -> Self {
        ObTableLSOperation::dummy()
    }
}

impl ObTableLSOperation {
    pub fn internal_new(
        ls_id: i64,
        table_name: String,
        table_id: i64,
        row_key_names: Vec<String>,
        properties_names: Vec<String>,
        option_flag: ObTableLSOpFlag,
        tablet_ops: Vec<ObTableTabletOp>,
    ) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            ls_id,
            table_name,
            table_id,
            row_key_names,
            properties_names,
            option_flag,
            tablet_ops,
            row_key_names_set: LinkedHashMap::new(),
            properties_names_set: LinkedHashMap::new(),
            row_key_names_idx_map: HashMap::new(),
            properties_names_idx_map: HashMap::new(),
        }
    }

    pub fn new(ops_num: usize) -> Self {
        ObTableLSOperation::internal_new(
            OB_INVALID_ID,
            String::new(),
            OB_INVALID_ID,
            Vec::default(),
            Vec::default(),
            ObTableLSOpFlag::default(),
            Vec::with_capacity(ops_num),
        )
    }

    pub fn dummy() -> Self {
        ObTableLSOperation::new(0)
    }

    pub fn add_op(&mut self, op: ObTableTabletOp) {
        if op.single_ops().is_empty() {
            return;
        }

        // adjust is_same_type
        if self.is_same_type()
            && (!op.is_same_type()
                || (!op.single_ops().is_empty()
                    && op.single_ops()[0].single_op_type()
                        != self.tablet_ops[0].single_ops()[0].single_op_type()))
        {
            self.set_is_same_type(false)
        }

        // set column names
        for single_op in op.single_ops() {
            // add row key column names in single operation query
            if let Some(query) = single_op.query() {
                for row_key_names in query.scan_range_columns() {
                    self.row_key_names_set.insert(row_key_names.to_owned(), ());
                }
            }

            for entity in &single_op.entities {
                // add row key names from entity
                for row_key_names in entity.row_key_names() {
                    self.row_key_names_set.insert(row_key_names.to_owned(), ());
                }
                // add properties names from entity
                for properties_names in entity.properties_names() {
                    self.properties_names_set
                        .insert(properties_names.to_owned(), ());
                }
            }
        }

        self.tablet_ops.push(op);
    }

    pub fn set_ls_id(&mut self, ls_id: i64) {
        self.ls_id = ls_id
    }

    pub fn set_is_same_type(&mut self, is_same_type: bool) {
        self.option_flag.set_flag_is_same_type(is_same_type)
    }

    pub fn is_same_type(&self) -> bool {
        self.option_flag.is_same_type()
    }

    /// [`self.generate_column_names_idx_map`] collect the column name and generate idx map for bitmap
    fn generate_column_names_idx_map(&mut self) {
        // prepare row key names idx map
        for (index, (row_key_name, _)) in self.row_key_names_set.iter().enumerate() {
            self.row_key_names.push(row_key_name.clone());
            self.row_key_names_idx_map
                .insert(row_key_name.clone(), index as i64);
        }

        // prepare properties names idx map
        for (index, (properties_names, _)) in self.properties_names_set.iter().enumerate() {
            self.properties_names.push(properties_names.clone());
            self.properties_names_idx_map
                .insert(properties_names.clone(), index as i64);
        }
    }

    /// [`self.before_option`] is used to collect necessary info from single operation before [`self.prepare_option`]
    fn before_option(&mut self) {
        let properties_column_names_idx_map_len = self.properties_names_idx_map.len();
        let is_same_properties_names = self.tablet_ops.iter().all(|tablet_op| {
            tablet_op.single_ops().iter().all(|single_op| {
                single_op.entities().iter().all(|entity| {
                    entity.is_same_properties_names_len(properties_column_names_idx_map_len)
                })
            })
        });
        if is_same_properties_names {
            self.option_flag.set_flag_is_same_properties_names(true);
        }
    }

    /// [`self.prepare_option`] set the option into every entity inside
    fn prepare_option(&mut self) {
        // set is_same_properties into entities
        if self.option_flag.is_same_properties_names() {
            self.tablet_ops.iter_mut().for_each(|tablet_op| {
                tablet_op.single_ops_mut().iter_mut().for_each(|single_op| {
                    single_op.entities_mut().iter_mut().for_each(|entity| {
                        entity.set_ignore_encode_properties_column_names(true);
                        // could also set other option in one loop
                    })
                })
            })
        }
    }

    fn prepare_column_names_bit_map(&mut self) {
        // adjust query & entity
        self.tablet_ops.iter_mut().for_each(|tablet_op| {
            tablet_op.single_ops_mut().iter_mut().for_each(|single_op| {
                if let Some(query) = single_op.query_mut() {
                    query.adjust_scan_range_columns(&self.row_key_names_idx_map);
                }
                single_op.entities_mut().iter_mut().for_each(|entity| {
                    entity.adjust_row_key_column_name(&self.row_key_names_idx_map);
                    entity.adjust_properties_column_name(&self.properties_names_idx_map);
                })
            })
        })
    }

    /// Adjust ObTableLSOperation by [`self.prepare`]
    /// We set column name bitmap & option flag during [`self.prepare`]
    pub fn prepare(&mut self) {
        self.generate_column_names_idx_map();
        self.before_option();
        self.prepare_option();
        self.prepare_column_names_bit_map();
    }
}

impl ObPayload for ObTableLSOperation {
    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    // payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        let mut len: usize = 0;
        len += 8; // ls_id
        len += util::encoded_length_vstring(&self.table_name);
        len += util::encoded_length_vi64(self.table_id);
        len += util::encoded_length_vi64(self.row_key_names.len() as i64);
        for rk_name in &self.row_key_names {
            len += util::encoded_length_vstring(rk_name);
        }
        len += util::encoded_length_vi64(self.properties_names.len() as i64);
        for ppt_name in &self.properties_names {
            len += util::encoded_length_vstring(ppt_name);
        }
        len += util::encoded_length_vi64(self.option_flag.value());
        len += util::encoded_length_vi64(self.tablet_ops.len() as i64);
        for op in self.tablet_ops.iter() {
            len += op.len()?;
        }
        Ok(len)
    }
}

impl ProtoEncoder for ObTableLSOperation {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;
        // 1. Log Stream ID
        buf.put_i64(self.ls_id);

        // 2. table name
        util::encode_vstring(&self.table_name, buf)?;

        // 3. table id
        util::encode_vi64(self.table_id, buf)?;

        // 4. row key names
        util::encode_vi64(self.row_key_names.len() as i64, buf)?;
        for idx in 0..self.row_key_names.len() {
            util::encode_vstring(&self.row_key_names[idx], buf)?;
        }

        // 5. properties names
        util::encode_vi64(self.properties_names.len() as i64, buf)?;
        for idx in 0..self.properties_names.len() {
            util::encode_vstring(&self.properties_names[idx], buf)?;
        }

        // 6. option flag
        util::encode_vi64(self.option_flag.value(), buf)?;

        // 7. operation
        util::encode_vi64(self.tablet_ops.len() as i64, buf)?;
        for op in self.tablet_ops.iter() {
            op.encode(buf)?;
        }

        Ok(())
    }
}

impl ProtoDecoder for ObTableLSOperation {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!();
    }
}

/// ObTableLSOpRequest is the request of [`ObTableLSOperation`]
pub struct ObTableLSOpRequest {
    base: BasePayLoad,
    credential: Vec<u8>,
    entity_type: ObTableEntityType,
    consistency_level: ObTableConsistencyLevel,
    ls_op: ObTableLSOperation,
}

impl ObTableLSOpRequest {
    pub fn new(ls_op: ObTableLSOperation, timeout: Duration, flag: u16) -> Self {
        let mut base = BasePayLoad::new();
        base.timeout = duration_to_millis(&timeout);
        base.flag = flag;
        Self {
            base,
            credential: vec![],
            entity_type: ObTableEntityType::Dynamic,
            consistency_level: ObTableConsistencyLevel::Strong,
            ls_op,
        }
    }
}

impl ObPayload for ObTableLSOpRequest {
    fn pcode(&self) -> ObTablePacketCode {
        ObTablePacketCode::LSExecute
    }

    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    // payload size, without header bytes
    fn content_len(&self) -> Result<usize> {
        Ok(util::encoded_length_bytes_string(&self.credential)
            + util::encoded_length_i8(self.entity_type as i8)
            + util::encoded_length_i8(self.consistency_level as i8)
            + self.ls_op.len()?)
    }

    fn set_credential(&mut self, credential: &[u8]) {
        self.credential = credential.to_owned();
    }
}

impl ProtoEncoder for ObTableLSOpRequest {
    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        self.encode_header(buf)?;
        util::encode_bytes_string(&self.credential, buf)?;
        buf.put_i8(self.entity_type as i8);
        buf.put_i8(self.consistency_level as i8);
        self.ls_op.encode(buf)?;
        Ok(())
    }
}

impl ProtoDecoder for ObTableLSOpRequest {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!()
    }
}

/// ObTableLSOpResult is the result of [`ObTableLSOpRequest`] from server
#[derive(Debug, Default)]
pub struct ObTableLSOpResult {
    base: BasePayLoad,
    op_results: Vec<ObTableTabletOpResult>,
    properties_column_names: Arc<Vec<String>>,
}

impl ObTableLSOpResult {
    pub fn new() -> Self {
        Self {
            base: BasePayLoad::dummy(),
            op_results: Vec::new(),
            properties_column_names: Arc::new(Vec::new()),
        }
    }

    pub fn get_op_results(&self) -> Vec<&ObTableSingleOpResult> {
        let mut count = 0;
        for tablet_res in &self.op_results {
            count += tablet_res.get_op_results().len();
        }
        let mut res = Vec::with_capacity(count);
        for tablet_res in &self.op_results {
            res.extend(tablet_res.get_op_results());
        }
        res
    }

    pub fn take_op_results(self) -> Vec<ObTableSingleOpResult> {
        let mut count = 0;
        for tablet_res in &self.op_results {
            count += tablet_res.get_op_results().len();
        }
        let mut res = Vec::with_capacity(count);
        for tablet_res in self.op_results {
            res.extend(tablet_res.take_op_results());
        }
        res
    }
}

impl ObPayload for ObTableLSOpResult {
    fn pcode(&self) -> ObTablePacketCode {
        ObTablePacketCode::BatchExecute
    }

    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }
}

impl ProtoEncoder for ObTableLSOpResult {
    fn encode(&self, _buf: &mut BytesMut) -> Result<()> {
        unimplemented!();
    }
}

impl ProtoDecoder for ObTableLSOpResult {
    fn decode(&mut self, src: &mut BytesMut) -> Result<()> {
        self.decode_base(src)?;
        // 1. column names
        let column_names_len = util::decode_vi64(src)?;
        if column_names_len < 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid operation results num:{column_names_len}"),
            ));
        }
        assert_eq!(0, self.properties_column_names.len());
        self.op_results.reserve(column_names_len as usize);

        let mut agg_properties_neams = Vec::new();
        for _ in 0..column_names_len {
            let column_name = util::decode_vstring(src)?;
            agg_properties_neams.push(column_name);
        }
        self.properties_column_names = Arc::new(agg_properties_neams);

        // 2. tablet result
        let op_res_num = util::decode_vi64(src)?;
        if op_res_num < 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid operation results num:{op_res_num}"),
            ));
        }
        assert_eq!(0, self.op_results.len());
        self.op_results.reserve(op_res_num as usize);

        for _ in 0..op_res_num {
            let mut op_res = ObTableTabletOpResult::new();
            op_res.set_agg_properties_names(self.properties_column_names.clone());
            op_res.decode(src)?;
            self.op_results.push(op_res);
        }

        Ok(())
    }
}
