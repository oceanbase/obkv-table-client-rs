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

use std::cmp::Ordering;

use crate::serde_obkv::value::Value;

#[derive(Debug, Clone)]
pub enum Comparable {
    MaxValue,
    MinValue,
    Value(Value),
}

impl PartialOrd for Comparable {
    fn partial_cmp(&self, other: &Comparable) -> Option<Ordering> {
        match &self {
            Comparable::MaxValue => Some(Ordering::Greater),
            Comparable::MinValue => Some(Ordering::Less),
            Comparable::Value(v) => match other {
                Comparable::MaxValue => Some(Ordering::Less),
                Comparable::MinValue => Some(Ordering::Greater),
                Comparable::Value(o) => v.partial_cmp(o),
            },
        }
    }
}

impl PartialEq for Comparable {
    fn eq(&self, other: &Comparable) -> bool {
        match &self {
            Comparable::MaxValue => false,
            Comparable::MinValue => false,
            Comparable::Value(v) => match other {
                Comparable::MaxValue => false,
                Comparable::MinValue => false,
                Comparable::Value(o) => v == o,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObPartitionKey {
    partition_elements: Vec<Comparable>,
}

impl ObPartitionKey {
    pub fn new(partition_elements: Vec<Comparable>) -> Self {
        Self { partition_elements }
    }
}

impl PartialOrd for ObPartitionKey {
    fn partial_cmp(&self, that: &ObPartitionKey) -> Option<Ordering> {
        if self.partition_elements.len() != that.partition_elements.len() {
            return None;
        }
        for i in 0..self.partition_elements.len() {
            if self.partition_elements[i] == that.partition_elements[i] {
                continue;
            }
            if self.partition_elements[i] == Comparable::MaxValue
                || that.partition_elements[i] == Comparable::MinValue
            {
                return Some(Ordering::Greater);
            }

            if that.partition_elements[i] == Comparable::MaxValue
                || self.partition_elements[i] == Comparable::MinValue
            {
                return Some(Ordering::Less);
            }
            // TODO: ObCollationType
            // if self.order_part_columns[i].get_ob_collation_type() ==
            // CollationType::UTF8MB4GeneralCi {    let tmp_ret =
            // self.partition_elements[i].partial_cmp(&that.partition_elements[i]);
            // } else {
            let tmp_ret = self.partition_elements[i].partial_cmp(&that.partition_elements[i]);
            //            }
            if tmp_ret != Some(Ordering::Equal) {
                return tmp_ret;
            }
        }
        Some(Ordering::Equal)
    }
}

impl PartialEq for ObPartitionKey {
    fn eq(&self, that: &ObPartitionKey) -> bool {
        if self.partition_elements.len() != that.partition_elements.len() {
            return false;
        }
        for i in 0..self.partition_elements.len() {
            if self.partition_elements[i] == that.partition_elements[i] {
                continue;
            }
            if self.partition_elements[i] == Comparable::MaxValue
                || that.partition_elements[i] == Comparable::MinValue
            {
                return false;
            }

            if that.partition_elements[i] == Comparable::MaxValue
                || self.partition_elements[i] == Comparable::MinValue
            {
                return false;
            }
            // TODO: ObCollationType
            let tmp_ret = self.partition_elements[i].partial_cmp(&that.partition_elements[i]);
            if tmp_ret != Some(Ordering::Equal) {
                return false;
            }
        }
        true
    }
}
