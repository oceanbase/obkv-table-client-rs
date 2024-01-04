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

use bytes::BytesMut;

use crate::{
    location::OB_INVALID_ID,
    payloads::ObTableBatchOperation,
    query::ObTableQuery,
    rpc::protocol::{
        query_and_mutate::ObTableQueryAndMutate, BasePayLoad, ObPayload, ProtoDecoder, ProtoEncoder,
    },
    serde_obkv::util,
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
    Query,
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
            ObTableSingleOpType::Query => 64,
            ObTableSingleOpType::QueryAndMutate => 65,
            ObTableSingleOpType::SingleOpTypeMax => 66,
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
            64 => ObTableSingleOpType::Query,
            65 => ObTableSingleOpType::QueryAndMutate,
            66 => ObTableSingleOpType::SingleOpTypeMax,
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

    fn new() -> Self {
        let mut flag = ObTableTabletOpFlag { flags: 0 };
        flag.set_flag_is_same_type(true);
        flag.set_flag_is_same_properties_names(false);
        flag
    }

    fn value(&self) -> i64 {
        self.flags
    }

    fn set_value(&mut self, flags: i64) {
        self.flags = flags;
    }

    fn set_flag_is_same_type(&mut self, is_same_type: bool) {
        if is_same_type {
            self.flags |= Self::FLAG_IS_SAME_TYPE;
        } else {
            self.flags &= !Self::FLAG_IS_SAME_TYPE;
        }
    }

    fn set_flag_is_same_properties_names(&mut self, is_same_properties_names: bool) {
        if is_same_properties_names {
            self.flags |= Self::FLAG_IS_SAME_PROPERTIES_NAMES;
        } else {
            self.flags &= !Self::FLAG_IS_SAME_PROPERTIES_NAMES;
        }
    }

    fn is_same_type(&self) -> bool {
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

    fn new() -> Self {
        let mut flag = ObTableLSOpFlag { flags: 0 };
        flag.set_flag_is_same_type(true);
        flag.set_flag_is_same_properties_names(false);
        flag
    }

    fn value(&self) -> i64 {
        self.flags
    }

    fn set_value(&mut self, flags: i64) {
        self.flags = flags;
    }

    fn set_flag_is_same_type(&mut self, is_same_type: bool) {
        if is_same_type {
            self.flags |= Self::FLAG_IS_SAME_TYPE;
        } else {
            self.flags &= !Self::FLAG_IS_SAME_TYPE;
        }
    }

    fn set_flag_is_same_properties_names(&mut self, is_same_properties_names: bool) {
        if is_same_properties_names {
            self.flags |= Self::FLAG_IS_SAME_PROPERTIES_NAMES;
        } else {
            self.flags &= !Self::FLAG_IS_SAME_PROPERTIES_NAMES;
        }
    }

    fn is_same_type(&self) -> bool {
        (self.flags & Self::FLAG_IS_SAME_TYPE) != 0
    }

    fn is_same_properties_names(&self) -> bool {
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
            OB_INVALID_ID,
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
        len += util::encoded_length_vi64(self.partition_id);
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
        util::encode_vi64(self.partition_id, buf)?;
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

        len += util::encoded_length_vi64(self.ls_id);
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

        util::encode_vi64(self.ls_id, buf)?;
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
