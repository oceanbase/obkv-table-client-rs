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

use bytes::{BufMut, BytesMut};

use crate::{
    location::OB_INVALID_ID,
    payloads::{
        ObTableBatchOperation, ObTableConsistencyLevel, ObTableEntityType, ObTableOperationResult,
    },
    query::ObTableQuery,
    rpc::protocol::{
        query_and_mutate::ObTableQueryAndMutate, BasePayLoad, ObPayload, ObTablePacketCode,
        ProtoDecoder, ProtoEncoder,
    },
    serde_obkv::util,
    util::duration_to_millis,
};

/// Type of Operation for [`ObTableSingleOp`]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ObTableSingleOpType {
    SingleGet,
    SingleInsert,
    SingleDel,
    SingleUpdate,
    SingleInsertOrUpdate,
    SingleReplace,
    SingleIncrement,
    SingleAppend,
    SingleMax, // reserved
    SyncQuery,
    AsyncQuery,
    QueryAndMutate,
    SingleOpTypeMax,
}

impl ObTableSingleOpType {
    fn value(&self) -> i64 {
        match self {
            ObTableSingleOpType::SingleGet => 0,
            ObTableSingleOpType::SingleInsert => 1,
            ObTableSingleOpType::SingleDel => 2,
            ObTableSingleOpType::SingleUpdate => 3,
            ObTableSingleOpType::SingleInsertOrUpdate => 4,
            ObTableSingleOpType::SingleReplace => 5,
            ObTableSingleOpType::SingleIncrement => 6,
            ObTableSingleOpType::SingleAppend => 7,
            ObTableSingleOpType::SingleMax => 63,
            ObTableSingleOpType::SyncQuery => 64,
            ObTableSingleOpType::AsyncQuery => 65,
            ObTableSingleOpType::QueryAndMutate => 66,
            ObTableSingleOpType::SingleOpTypeMax => 67,
        }
    }
}

impl From<i64> for ObTableSingleOpType {
    fn from(value: i64) -> Self {
        match value {
            0 => ObTableSingleOpType::SingleGet,
            1 => ObTableSingleOpType::SingleInsert,
            2 => ObTableSingleOpType::SingleDel,
            3 => ObTableSingleOpType::SingleUpdate,
            4 => ObTableSingleOpType::SingleInsertOrUpdate,
            5 => ObTableSingleOpType::SingleReplace,
            6 => ObTableSingleOpType::SingleIncrement,
            7 => ObTableSingleOpType::SingleAppend,
            63 => ObTableSingleOpType::SingleMax,
            64 => ObTableSingleOpType::SyncQuery,
            65 => ObTableSingleOpType::AsyncQuery,
            66 => ObTableSingleOpType::QueryAndMutate,
            67 => ObTableSingleOpType::SingleOpTypeMax,
            _ => panic!("Invalid value for ObTableSingleOpType"),
        }
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

/// Single operation in OBKV
/// [`ObTableSingleOp`] -> [`ObTableTabletOp`] -> [`ObTableLSOperation`]
#[derive(Debug, Clone)]
pub struct ObTableSingleOp {
    base: BasePayLoad,
    op_type: ObTableSingleOpType,
    op: ObTableQueryAndMutate,
}

impl Default for ObTableSingleOp {
    fn default() -> Self {
        ObTableSingleOp::new(
            ObTableSingleOpType::SingleMax,
            ObTableQueryAndMutate::dummy(),
        )
    }
}

impl ObTableSingleOp {
    pub fn new(op_type: ObTableSingleOpType, op: ObTableQueryAndMutate) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            op_type,
            op,
        }
    }

    pub fn mutations(&self) -> &Option<ObTableBatchOperation> {
        self.op.mutations()
    }

    pub fn set_mutations(&mut self, mutations: ObTableBatchOperation) {
        self.op.set_mutations(mutations)
    }

    pub fn query(&self) -> &Option<ObTableQuery> {
        self.op.query()
    }

    pub fn set_query(&mut self, query: ObTableQuery) {
        self.op.set_query(query)
    }

    pub fn set_is_check_and_execute(&mut self, is_check_and_execute: bool) {
        self.op.set_is_check_and_execute(is_check_and_execute)
    }

    pub fn is_check_and_execute(&self) -> bool {
        self.op.is_check_and_execute()
    }

    pub fn set_is_check_not_exists(&mut self, is_check_not_exists: bool) {
        self.op.set_is_check_not_exists(is_check_not_exists)
    }

    pub fn is_check_not_exists(&self) -> bool {
        self.op.is_check_not_exists()
    }

    pub fn single_op_type(&self) -> ObTableSingleOpType {
        self.op_type
    }

    pub fn set_single_op_type(&mut self, op_type: ObTableSingleOpType) {
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
    fn content_len(&self) -> crate::rpc::protocol::Result<usize> {
        let mut len: usize = 0;
        len += util::encoded_length_vi64(self.op_type.value());
        len += self.op.len()?;
        Ok(len)
    }
}

impl ProtoEncoder for ObTableSingleOp {
    fn encode(&self, buf: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
        self.encode_header(buf)?;
        util::encode_vi64(self.op_type.value(), buf)?;
        self.op.encode(buf)?;
        Ok(())
    }
}

impl ProtoDecoder for ObTableSingleOp {
    fn decode(&mut self, _src: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
        unimplemented!();
    }
}

/// Operations in one tablet in OBKV
/// [`ObTableSingleOp`] -> [`ObTableTabletOp`] -> [`ObTableLSOperation`]
#[derive(Debug, Clone)]
pub struct ObTableTabletOp {
    base: BasePayLoad,
    table_id: i64,
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
        table_id: i64,
        partition_id: i64,
        option_flag: ObTableTabletOpFlag,
        single_ops: Vec<ObTableSingleOp>,
    ) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            table_id,
            partition_id,
            option_flag,
            single_ops,
        }
    }

    pub fn new(ops_num: usize) -> Self {
        ObTableTabletOp::internal_new(
            OB_INVALID_ID,
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

    pub fn table_id(&self) -> i64 {
        self.table_id
    }

    pub fn set_table_id(&mut self, table_id: i64) {
        self.table_id = table_id
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
    fn content_len(&self) -> crate::rpc::protocol::Result<usize> {
        let mut len: usize = 0;
        len += util::encoded_length_vi64(self.table_id);
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
    fn encode(&self, buf: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
        self.encode_header(buf)?;
        util::encode_vi64(self.table_id, buf)?;
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
    fn decode(&mut self, _src: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
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
    fn encode(&self, _buf: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
        unimplemented!();
    }
}

impl ProtoDecoder for ObTableTabletOpResult {
    fn decode(&mut self, src: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
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
    base: BasePayLoad,
    ls_id: i64,
    option_flag: ObTableLSOpFlag,
    tablet_ops: Vec<ObTableTabletOp>,
}

impl Default for ObTableLSOperation {
    fn default() -> Self {
        ObTableLSOperation::dummy()
    }
}

impl ObTableLSOperation {
    pub fn internal_new(
        ls_id: i64,
        option_flag: ObTableLSOpFlag,
        tablet_ops: Vec<ObTableTabletOp>,
    ) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            ls_id,
            option_flag,
            tablet_ops,
        }
    }

    pub fn new(ops_num: usize) -> Self {
        ObTableLSOperation::internal_new(
            OB_INVALID_ID,
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
    fn content_len(&self) -> crate::rpc::protocol::Result<usize> {
        let mut len: usize = 0;
        len += 8; // ls_id
        len += util::encoded_length_vi64(self.option_flag.value());
        len += util::encoded_length_vi64(self.tablet_ops.len() as i64);
        for op in self.tablet_ops.iter() {
            len += op.len()?;
        }
        Ok(len)
    }
}

impl ProtoEncoder for ObTableLSOperation {
    fn encode(&self, buf: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
        self.encode_header(buf)?;
        buf.put_i64(self.ls_id);
        util::encode_vi64(self.option_flag.value(), buf)?;
        util::encode_vi64(self.tablet_ops.len() as i64, buf)?;
        for op in self.tablet_ops.iter() {
            op.encode(buf)?;
        }
        Ok(())
    }
}

impl ProtoDecoder for ObTableLSOperation {
    fn decode(&mut self, _src: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
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
    fn content_len(&self) -> crate::rpc::protocol::Result<usize> {
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
    fn encode(&self, buf: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
        self.encode_header(buf)?;
        util::encode_bytes_string(&self.credential, buf)?;
        buf.put_i8(self.entity_type as i8);
        buf.put_i8(self.consistency_level as i8);
        self.ls_op.encode(buf)?;
        Ok(())
    }
}

impl ProtoDecoder for ObTableLSOpRequest {
    fn decode(&mut self, _src: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
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
    fn encode(&self, _buf: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
        unimplemented!();
    }
}

impl ProtoDecoder for ObTableLSOpResult {
    fn decode(&mut self, src: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
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
