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

#[derive(Clone, Debug, PartialEq)]
pub enum PartFuncType {
    Unknown = -1,
    Hash = 0,
    Key = 1,
    KeyImplicit = 2,
    Range = 3,
    RangeColumns = 4,
    List = 5,
    KeyV2 = 6,
    ListColumns = 7,
    HashV2 = 8,
    KeyV3 = 9,
    KeyImplicitV2 = 10,
}

impl PartFuncType {
    pub fn from_i32(index: i32) -> PartFuncType {
        match index {
            0 => PartFuncType::Hash,
            1 => PartFuncType::Key,
            2 => PartFuncType::KeyImplicit,
            3 => PartFuncType::Range,
            4 => PartFuncType::RangeColumns,
            5 => PartFuncType::List,
            6 => PartFuncType::KeyV2,
            7 => PartFuncType::ListColumns,
            8 => PartFuncType::HashV2,
            9 => PartFuncType::KeyV3,
            10 => PartFuncType::KeyImplicitV2,
            _ => PartFuncType::Unknown,
        }
    }

    pub fn is_list_part(&self) -> bool {
        matches!(self, PartFuncType::List | PartFuncType::ListColumns)
    }

    pub fn is_key_part(&self) -> bool {
        matches!(
            self,
            PartFuncType::Key
                | PartFuncType::KeyImplicit
                | PartFuncType::KeyV2
                | PartFuncType::KeyV3
                | PartFuncType::KeyImplicitV2
        )
    }

    pub fn is_range_part(&self) -> bool {
        matches!(self, PartFuncType::Range | PartFuncType::RangeColumns)
    }

    pub fn is_hash_part(&self) -> bool {
        matches!(self, PartFuncType::Hash | PartFuncType::HashV2)
    }
}
