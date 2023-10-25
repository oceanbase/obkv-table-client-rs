/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2023 OceanBase
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

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

lazy_static! {
    pub static ref OB_VERSION: AtomicU64 = AtomicU64::new(0);
}

#[allow(dead_code)]
const OB_VSN_MAJOR_SHIFT: u64 = 32;
#[allow(dead_code)]
const OB_VSN_MINOR_SHIFT: u64 = 16;
#[allow(dead_code)]
const OB_VSN_MAJOR_PATCH_SHIFT: u64 = 8;
#[allow(dead_code)]
const OB_VSN_MINOR_PATCH_SHIFT: u64 = 0;
#[allow(dead_code)]
const OB_VSN_MAJOR_MASK: u64 = 0xffffffff;
#[allow(dead_code)]
const OB_VSN_MINOR_MASK: u64 = 0xffff;
#[allow(dead_code)]
const OB_VSN_MAJOR_PATCH_MASK: u64 = 0xff;
#[allow(dead_code)]
const OB_VSN_MINOR_PATCH_MASK: u64 = 0xff;

#[allow(dead_code)]
pub fn calc_version(major: i32, minor: i16, major_patch: i8, minor_patch: i8) -> u64 {
    ((major as u64) << OB_VSN_MAJOR_SHIFT)
        + ((minor as u64) << OB_VSN_MINOR_SHIFT)
        + ((major_patch as u64) << OB_VSN_MAJOR_PATCH_SHIFT)
        + ((minor_patch as u64) << OB_VSN_MINOR_PATCH_SHIFT)
}

#[allow(dead_code)]
pub fn ob_vsn_major() -> i32 {
    get_ob_vsn_major(OB_VERSION.load(Relaxed))
}

#[allow(dead_code)]
pub fn get_ob_vsn_major(version: u64) -> i32 {
    ((version >> OB_VSN_MAJOR_SHIFT) & OB_VSN_MAJOR_MASK) as i32
}

#[allow(dead_code)]
pub fn ob_vsn_minor() -> i16 {
    get_ob_vsn_minor(OB_VERSION.load(Relaxed))
}

#[allow(dead_code)]
pub fn get_ob_vsn_minor(version: u64) -> i16 {
    ((version >> OB_VSN_MINOR_SHIFT) & OB_VSN_MINOR_MASK) as i16
}

#[allow(dead_code)]
pub fn ob_vsn_major_patch() -> i8 {
    get_ob_vsn_major_patch(OB_VERSION.load(Relaxed))
}

#[allow(dead_code)]
pub fn get_ob_vsn_major_patch(version: u64) -> i8 {
    ((version >> OB_VSN_MAJOR_PATCH_SHIFT) & OB_VSN_MAJOR_PATCH_MASK) as i8
}

#[allow(dead_code)]
pub fn ob_vsn_minor_patch() -> i8 {
    get_ob_vsn_minor_patch(OB_VERSION.load(Relaxed))
}

#[allow(dead_code)]
pub fn get_ob_vsn_minor_patch(version: u64) -> i8 {
    ((version >> OB_VSN_MINOR_PATCH_SHIFT) & OB_VSN_MINOR_PATCH_MASK) as i8
}

#[allow(dead_code)]
pub fn ob_vsn_string() -> String {
    format!(
        "{}.{}.{}.{}",
        ob_vsn_major(),
        ob_vsn_minor(),
        ob_vsn_major_patch(),
        ob_vsn_minor_patch()
    )
}

#[allow(dead_code)]
pub fn get_ob_vsn_string(version: u64) -> String {
    format!(
        "{}.{}.{}.{}",
        get_ob_vsn_major(version),
        get_ob_vsn_minor(version),
        get_ob_vsn_major_patch(version),
        get_ob_vsn_minor_patch(version)
    )
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ob_version() {
        assert_eq!(ob_vsn_major(), 0);
        assert_eq!(ob_vsn_minor(), 0);
        assert_eq!(ob_vsn_major_patch(), 0);
        assert_eq!(ob_vsn_minor_patch(), 0);

        let my_version = calc_version(4, 2, 1, 4);
        assert_eq!(get_ob_vsn_major(my_version), 4);
        assert_eq!(get_ob_vsn_minor(my_version), 2);
        assert_eq!(get_ob_vsn_major_patch(my_version), 1);
        assert_eq!(get_ob_vsn_minor_patch(my_version), 4);

        assert_eq!(ob_vsn_string(), "0.0.0.0");
        assert_eq!(get_ob_vsn_string(my_version), "4.2.1.4");
    }
}
