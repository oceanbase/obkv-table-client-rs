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

#![allow(dead_code)]
use bytes::{BufMut, BytesMut};

use crate::{
    payloads::ObTableBatchOperation,
    query::ObTableQuery,
    rpc::protocol::{BasePayLoad, ObPayload, ProtoDecoder, ProtoEncoder},
    serde_obkv::util,
};

#[derive(Default, Debug, Clone, PartialEq)]
pub struct ObTableQueryAndMutateFlag {
    flags: i64,
}

impl ObTableQueryAndMutateFlag {
    const FLAG_IS_CHECK_AND_EXECUTE: i64 = 1 << 0;
    const FLAG_IS_CHECK_NOT_EXISTS: i64 = 1 << 1;

    pub fn new() -> Self {
        ObTableQueryAndMutateFlag { flags: 0 }
    }

    pub fn value(&self) -> i64 {
        self.flags
    }

    pub fn set_is_check_and_execute(&mut self, is_check_and_execute: bool) {
        if is_check_and_execute {
            self.flags |= Self::FLAG_IS_CHECK_AND_EXECUTE;
        } else {
            self.flags &= !Self::FLAG_IS_CHECK_AND_EXECUTE;
        }
    }

    pub fn set_check_not_exists(&mut self, check_not_exists: bool) {
        if check_not_exists {
            self.flags |= Self::FLAG_IS_CHECK_NOT_EXISTS;
        } else {
            self.flags &= !Self::FLAG_IS_CHECK_NOT_EXISTS;
        }
    }

    pub fn is_check_not_exists(&self) -> bool {
        (self.flags & Self::FLAG_IS_CHECK_NOT_EXISTS) != 0
    }

    pub fn is_check_and_execute(&self) -> bool {
        (self.flags & Self::FLAG_IS_CHECK_AND_EXECUTE) != 0
    }
}

#[derive(Debug, Clone)]
pub struct ObTableQueryAndMutate {
    base: BasePayLoad,
    query: Option<ObTableQuery>,
    mutations: Option<ObTableBatchOperation>,
    return_affected_entity: bool,
    option_flag: ObTableQueryAndMutateFlag,
}

impl Default for ObTableQueryAndMutate {
    fn default() -> ObTableQueryAndMutate {
        ObTableQueryAndMutate::dummy()
    }
}

impl ObTableQueryAndMutate {
    fn internal_new(query: Option<ObTableQuery>, mutations: Option<ObTableBatchOperation>) -> Self {
        Self {
            base: BasePayLoad::dummy(),
            query,
            mutations,
            return_affected_entity: false,
            option_flag: ObTableQueryAndMutateFlag::new(),
        }
    }

    pub fn new(query: ObTableQuery, mutations: ObTableBatchOperation) -> Self {
        ObTableQueryAndMutate::internal_new(Some(query), Some(mutations))
    }

    pub fn dummy() -> Self {
        ObTableQueryAndMutate::internal_new(None, None)
    }

    pub fn mutations(&self) -> &Option<ObTableBatchOperation> {
        &self.mutations
    }

    pub fn set_mutations(&mut self, mutations: ObTableBatchOperation) {
        self.mutations = Some(mutations)
    }

    pub fn query(&self) -> &Option<ObTableQuery> {
        &self.query
    }

    pub fn set_query(&mut self, query: ObTableQuery) {
        self.query = Some(query)
    }

    pub fn set_return_affected_entity(&mut self, return_affected_entity: bool) {
        self.return_affected_entity = return_affected_entity;
    }

    pub fn set_is_check_and_execute(&mut self, is_check_and_execute: bool) {
        self.option_flag
            .set_is_check_and_execute(is_check_and_execute)
    }

    pub fn is_check_and_execute(&self) -> bool {
        self.option_flag.is_check_and_execute()
    }

    pub fn set_check_not_exists(&mut self, check_not_exists: bool) {
        self.option_flag.set_check_not_exists(check_not_exists)
    }

    pub fn is_check_not_exists(&self) -> bool {
        self.option_flag.is_check_not_exists()
    }
}

impl ObPayload for ObTableQueryAndMutate {
    fn base(&self) -> &BasePayLoad {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePayLoad {
        &mut self.base
    }

    // payload size, without header bytes
    fn content_len(&self) -> crate::rpc::protocol::Result<usize> {
        let mut len: usize = 0;

        len += self
            .query
            .as_ref()
            .expect("query should not be empty")
            .len()?;
        len += self
            .mutations
            .as_ref()
            .expect("mutations should not be empty")
            .len()?;

        len += util::encoded_length_vi64(self.option_flag.value());

        // 1 for bool
        Ok(len + 1)
    }
}

impl ProtoEncoder for ObTableQueryAndMutate {
    fn encode(&self, buf: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
        self.encode_header(buf)?;

        // query and mutations need to be not empty now
        // this is guarantee by developers
        self.query
            .as_ref()
            .expect("query should not be empty")
            .encode(buf)?;
        self.mutations
            .as_ref()
            .expect("mutations should not be empty")
            .encode(buf)?;

        buf.put_i8(self.return_affected_entity as i8);
        util::encode_vi64(self.option_flag.value(), buf)?;

        Ok(())
    }
}

impl ProtoDecoder for ObTableQueryAndMutate {
    fn decode(&mut self, _src: &mut BytesMut) -> crate::rpc::protocol::Result<()> {
        unimplemented!();
    }
}
