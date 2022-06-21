// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

#[derive(Clone, Debug, PartialEq)]
pub enum PartFuncType {
    UNKNOWN = -1,
    HASH = 0,
    KEY = 1,
    KeyImplicit = 2,
    RANGE = 3,
    RangeColumns = 4,
    LIST = 5,
    KeyV2 = 6,
    ListColumns = 7,
}

impl PartFuncType {
    pub fn from_i32(index: i32) -> PartFuncType {
        match index {
            0 => PartFuncType::HASH,
            1 => PartFuncType::KEY,
            2 => PartFuncType::KeyImplicit,
            3 => PartFuncType::RANGE,
            4 => PartFuncType::RangeColumns,
            5 => PartFuncType::LIST,
            6 => PartFuncType::KeyV2,
            7 => PartFuncType::ListColumns,
            _ => PartFuncType::UNKNOWN,
        }
    }

    pub fn is_list_part(&self) -> bool {
        matches!(self, PartFuncType::LIST | PartFuncType::ListColumns)
    }

    pub fn is_key_part(&self) -> bool {
        matches!(self, PartFuncType::KeyImplicit | PartFuncType::KeyV2)
    }

    pub fn is_range_part(&self) -> bool {
        matches!(self, PartFuncType::RANGE | PartFuncType::RangeColumns)
    }

    pub fn is_hash_part(&self) -> bool {
        matches!(self, PartFuncType::HASH)
    }
}
