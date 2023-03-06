/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

/// Password security module
use std::cell::RefCell;
use std::{iter::repeat, num::Wrapping};

use crypto::{digest::Digest, sha1::Sha1};

use super::current_time_millis;

const BYTES: &[char] = &[
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o',
    'p', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm', 'Q', 'W',
    'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Z', 'X',
    'C', 'V', 'B', 'N', 'M',
];
const MULTIPLIER: i64 = 0x0005_DEEC_E66D;
const ADDEND: i64 = 0xB;
const MASK: i64 = (1 << 48) - 1;
const INTEGER_MASK: i64 = (1 << 33) - 1;
const SEED_UNIQUIFIER: i64 = 8_682_522_807_148_012;

thread_local!(
    static SEED: RefCell<i64> = RefCell::new(((current_time_millis() + SEED_UNIQUIFIER) ^ MULTIPLIER) & MASK)
);

pub fn get_password_scramble(capacity: usize) -> String {
    let mut ret = String::with_capacity(capacity);

    for _ in 0..capacity {
        ret.push(rand_char());
    }

    ret
}

fn rand_char() -> char {
    let ran = ((random() & INTEGER_MASK) >> 16) as usize;
    BYTES[ran % BYTES.len()].to_owned()
}

fn random() -> i64 {
    SEED.with(|seed| {
        let old_seed = Wrapping(*seed.borrow());
        let mut next_seed;
        loop {
            next_seed = (old_seed * Wrapping(MULTIPLIER) + Wrapping(ADDEND)) & Wrapping(MASK);
            if old_seed != next_seed {
                break;
            }
        }

        let ret = next_seed.0;
        *seed.borrow_mut() = ret;
        ret
    })
}

pub fn scramble_password(password: &str, seed: &str) -> Vec<u8> {
    if password.is_empty() {
        return vec![];
    }

    let mut hasher = Sha1::new();
    hasher.input_str(password);
    let mut pass1: Vec<u8> = repeat(0).take((hasher.output_bits() + 7) / 8).collect();
    hasher.result(&mut pass1);
    hasher.reset();
    hasher.input(&pass1);
    let mut pass2: Vec<u8> = repeat(0).take((hasher.output_bits() + 7) / 8).collect();
    hasher.result(&mut pass2);
    hasher.reset();
    hasher.input_str(seed);
    hasher.input(&pass2);
    let mut pass3: Vec<u8> = repeat(0).take((hasher.output_bits() + 7) / 8).collect();
    hasher.result(&mut pass3);

    for i in 0..pass3.len() {
        pass3[i] ^= pass1[i];
    }
    pass3
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_password_scramble() {
        for i in 1..20 {
            let s = get_password_scramble(i);
            assert_eq!(i, s.len());
        }
    }

    #[test]
    fn test_scramble_password() {
        let password = "hello";
        let seed = "qXA4YhW5PwaWsARvj3KC";

        let s_pass = scramble_password(password, seed);

        assert_eq!(20, s_pass.len());
    }
}
