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

pub const OB_PART_IDS_BITNUM: i32 = 28;
pub const OB_PART_ID_SHIFT: i32 = 32;
pub const OB_TWOPART_BEGIN_MASK: i64 =
    (1i64 << OB_PART_IDS_BITNUM) | (1i64 << (OB_PART_IDS_BITNUM + OB_PART_ID_SHIFT));

#[inline]
pub fn generate_phy_part_id(part_idx: i64, sub_part_idx: i64) -> i64 {
    if part_idx < 0 || sub_part_idx < 0 {
        -1
    } else {
        ((part_idx << OB_PART_ID_SHIFT) | sub_part_idx) | OB_TWOPART_BEGIN_MASK
    }
}

// get subpart_id with PARTITION_LEVEL_TWO_MASK
#[inline]
pub fn extract_subpart_id(id: i64) -> i64 {
    id & (!(u64::MAX << OB_PART_ID_SHIFT)) as i64
}

// get part_id with PARTITION_LEVEL_TWO_MASK
#[inline]
pub fn extract_part_id(id: i64) -> i64 {
    id >> OB_PART_ID_SHIFT
}

// get part idx from one level partid
#[inline]
pub fn extract_idx_from_partid(id: i64) -> i64 {
    id & (!(u64::MAX << OB_PART_IDS_BITNUM)) as i64
}

// get part space from one level partid
#[allow(dead_code)]
#[inline]
pub fn extract_space_from_partid(id: i64) -> i64 {
    id >> OB_PART_IDS_BITNUM
}

// get part_idx
#[inline]
pub fn extract_part_idx(id: i64) -> i64 {
    extract_idx_from_partid(extract_part_id(id))
}

// get sub_part_idx
#[inline]
pub fn extract_subpart_idx(id: i64) -> i64 {
    extract_idx_from_partid(extract_subpart_id(id))
}

#[cfg(test)]
mod test {
    use crate::location::ob_part_constants::{
        extract_part_idx, extract_space_from_partid, extract_subpart_idx, generate_phy_part_id,
    };

    #[test]
    pub fn generate_phy_part_id_test() {
        let mut n = generate_phy_part_id(23, 0);
        assert_eq!(n, 1152921603659530240);
        n = generate_phy_part_id(23, -1);
        assert_eq!(n, -1);
    }

    #[test]
    pub fn extract_space_from_partid_test() {
        let n = extract_space_from_partid(1152921603659530240);
        assert_eq!(n, 4294967665);
    }

    #[test]
    pub fn extract_subpart_idx_test() {
        let n = extract_subpart_idx(1152921603659530240);
        assert_eq!(n, 0);
    }

    #[test]
    pub fn extract_part_idx_test() {
        let n = extract_part_idx(1152921603659530240);
        assert_eq!(n, 23);
    }
}
