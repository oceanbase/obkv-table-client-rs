// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

pub const PART_ID_BITNUM: i32 = 28;
pub const PART_ID_SHIFT: i32 = 32;
pub const MASK: i64 = (1i64 << PART_ID_BITNUM) | (1i64 << (PART_ID_BITNUM + PART_ID_SHIFT));
