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

use std::{io, time::Duration};
use std::collections::HashMap;
use std::rc::Rc;

use bytes::{BufMut, BytesMut};
use linked_hash_map::LinkedHashMap;

use crate::{location::OB_INVALID_ID, payloads::{
    ObTableConsistencyLevel, ObTableEntityType, ObTableOperationResult,
}, rpc::protocol::{
    BasePayLoad, ObPayload, ObTablePacketCode,
    ProtoDecoder, ProtoEncoder, Result,
}, serde_obkv::util, util::duration_to_millis, Value};
use crate::payloads::ObTableOperationType;
use crate::query::ObNewRange;
use crate::serde_obkv::util::decode_i8;
use crate::util::decode_value;

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
/// [`ObTableSingleOpQuery`]|[`ObTableSingleOpEntity`] -> [`ObTableSingleOp`] -> [`ObTableTabletOp`] -> [`ObTableLSOperation`]
#[derive(Debug, Clone)]
pub struct ObTableSingleOpQuery {
    base: BasePayLoad,
    index_name: String,
    scan_range_cols_bm: Vec<i8>,
    key_ranges: Vec<ObNewRange>,
    filter_string: String,
    scan_range_len: i64,
    scan_range_columns: Vec<String>,
    agg_column_names: Vec<String>,
}

impl Default for ObTableSingleOpQuery {
    fn default() -> Self {
        ObTableSingleOpQuery::new(Vec::new(), Vec::new())
    }
}

impl ObTableSingleOpQuery {
    pub fn new(scan_range_columns: Vec<String>, key_ranges: Vec<ObNewRange>,) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            index_name: String::new(),
            scan_range_cols_bm: Vec::new(),
            key_ranges,
            filter_string: String::new(),
            scan_range_len: 0i64,
            scan_range_columns,
            agg_column_names: Vec::new(),
        }
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
        len += util::encoded_length_vi64(self.scan_range_len);
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
        util::encode_vi64(self.scan_range_len, buf)?;
        for bm in &self.scan_range_cols_bm {
            buf.put_i8(*bm);
        }
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
/// [`ObTableSingleOpQuery`]|[`ObTableSingleOpEntity`] -> [`ObTableSingleOp`] -> [`ObTableTabletOp`] -> [`ObTableLSOperation`]
#[derive(Debug, Clone)]
pub struct ObTableSingleOpEntity {
    // encoded params
    base: BasePayLoad,
    row_key_bit_len: i64,
    row_key_bitmap: Vec<i8>,
    row_key: Vec<Value>,
    properties_bit_len: i64,
    properties_bitmap: Vec<i8>,
    properties: Vec<Value>,

    // intermediate params
    row_key_names: Vec<String>,
    properties_names: Vec<String>,
    agg_row_key_names: Option<Rc<Vec<String>>>,
    agg_properties_names: Option<Rc<Vec<String>>>,

    // options
    ignore_encode_properties_col_names: bool,
}

impl Default for ObTableSingleOpEntity {
    fn default() -> Self {
        ObTableSingleOpEntity::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }
}

impl ObTableSingleOpEntity {
    pub fn new(row_key_names: Vec<String>, row_key: Vec<Value>, properties_names: Vec<String>, properties: Vec<Value>,) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            row_key_bit_len: 0i64,
            row_key_bitmap: Vec::new(),
            row_key,
            properties_bit_len: 0i64,
            properties_bitmap: Vec::new(),
            properties,
            row_key_names,
            properties_names,
            agg_row_key_names: None,
            agg_properties_names: None,
            ignore_encode_properties_col_names: false,
        }
    }

    /// parse_bit_map is used by [`ProtoEncoder`]
    /// Accept source byte, length and aggregation column names
    /// Output bit map and corresponding column names
    fn parse_bit_map(len: i64, agg_names: Rc<Vec<String>>, src: &mut BytesMut, bit_map: &mut Vec<i8>, column_names: &mut Vec<String>) {
        bit_map.clear();
        column_names.clear();
        let bm_len = (len + 7) / 8;
        for idx in 0..bm_len {
            bit_map.push(decode_i8(src)?);
            for bit_idx in 0usize..8 {
                if bit_map[idx] & (1i8 << bit_idx) != 0 {
                    if idx * 8 + bit_idx < agg_names.len() as i64 {
                        column_names.push(agg_names[idx * 8 + bit_idx]);
                    }
                }
            }
        }
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
        len += util::encoded_length_vi64(self.row_key_bit_len);
        len += self.row_key_bitmap.len();
        len += util::encoded_length_vi64(self.row_key.len() as i64);
        for rk in &self.row_key {
            len += rk.len();
        }
        if self.ignore_encode_properties_col_names {
            len += util::encoded_length_vi64(0i64);
        } else {
            len += util::encoded_length_vi64(self.properties_bit_len);
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
        util::encode_vi64(self.row_key_bit_len, buf)?;
        for bm in self.row_key_bitmap {
            buf.put_i8(bm);
        }
        // 2. encode rowkey
        util::encode_vi64(self.row_key.len() as i64, buf)?;
        for key in &self.row_key {
            key.encode(buf)?;
        }
        // 3. encode properties bitmap
        if self.ignore_encode_properties_col_names {
            util::encode_vi64(0i64, buf)?;
        } else {
            util::encode_vi64(self.properties_bit_len, buf)?;
            for bm in self.properties_bitmap {
                buf.put_i8(bm);
            }
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
        self.row_key_bit_len = util::decode_vi64(src)?;
        self.parse_bit_map(self.row_key_bit_len, self.agg_row_key_names.unwrap_or_else(|| Rc::new(Vec::new())).clone(), src, &mut self.row_key_bitmap, &mut self.row_key_names);

        // 2. row key obobj
        self.row_key.clear();
        let row_key_len = util::decode_vi64(src)?;
        for _ in 0..row_key_len {
            self.row_key.push(decode_value(src)?);
        }

        // 3. properties bitmaps
        self.properties_bit_len = util::decode_vi64(src)?;
        self.parse_bit_map(self.properties_bit_len, self.agg_properties_names.unwrap_or_else(|| Rc::new(Vec::new())).clone(), src, &mut self.properties_bitmap, &mut self.properties_names);

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
/// [`ObTableSingleOpQuery`]|[`ObTableSingleOpEntity`] -> [`ObTableSingleOp`] -> [`ObTableTabletOp`] -> [`ObTableLSOperation`]
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
        ObTableSingleOp::new(
            ObTableOperationType::Invalid,
        )
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

    pub fn set_query(&mut self, query: ObTableSingleOpQuery) {
        self.query = Some(query)
    }

    pub fn set_is_check_not_exists(&mut self, is_check_not_exists: bool) {
        self.op_flag.set_flag_is_check_not_exist(is_check_not_exists)
    }

    pub fn is_check_not_exists(&self) -> bool {
        self.op_flag.is_check_not_exists()
    }

    pub fn single_op_type(&self) -> ObTableOperationType {
        self.op_type
    }

    pub fn set_single_op_type(&mut self, op_type: ObTableOperationType) {
        self.op_type = op_type
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
            len += self.query.map_or(0, |query| query.len()?);
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
            self.query?.encode(buf)?;
        }

        // 4. single op entity
        util::encode_vi64(self.entities.len() as i64, buf)?;
        for entity in &self.entities {
            entity.encode(buf)
        }

        Ok(())
    }
}

impl ProtoDecoder for ObTableSingleOp {
    fn decode(&mut self, _src: &mut BytesMut) -> Result<()> {
        unimplemented!();
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
        for op in self.single_ops.iter() {
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
        for op in self.single_ops.iter() {
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
    op_results: Vec<ObTableOperationResult>,
}

impl ObTableTabletOpResult {
    pub fn new() -> Self {
        Self {
            base: BasePayLoad::dummy(),
            op_results: Vec::new(),
        }
    }

    pub fn get_op_results(&self) -> &[ObTableOperationResult] {
        &self.op_results
    }

    pub fn take_op_results(self) -> Vec<ObTableOperationResult> {
        self.op_results
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid operation results num:{op_res_num}"),
            ));
        }
        assert_eq!(0, self.op_results.len());
        self.op_results.reserve(op_res_num as usize);

        for _ in 0..op_res_num {
            let mut op_res = ObTableOperationResult::new();
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
}

impl ObTableLSOpResult {
    pub fn new() -> Self {
        Self {
            base: BasePayLoad::dummy(),
            op_results: Vec::new(),
        }
    }

    pub fn get_op_results(&self) -> Vec<&ObTableOperationResult> {
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

    pub fn take_op_results(self) -> Vec<ObTableOperationResult> {
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

        let op_res_num = util::decode_vi64(src)?;
        if op_res_num < 0 {
            return Err(io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid operation results num:{op_res_num}"),
            ));
        }
        assert_eq!(0, self.op_results.len());
        self.op_results.reserve(op_res_num as usize);

        for _ in 0..op_res_num {
            let mut op_res = ObTableTabletOpResult::new();
            op_res.decode(src)?;
            self.op_results.push(op_res);
        }

        Ok(())
    }
}
