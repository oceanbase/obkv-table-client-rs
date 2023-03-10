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

use std::collections::HashMap;

use super::{ob_part_constants, part_func_type::PartFuncType};
use crate::{
    error::{CommonErrCode, Error::Common as CommonErr, Result},
    location::part_func_type::PartFuncType::{KeyImplicitV2, KeyV3},
    rpc::{
        protocol::partition::{
            ob_column::ObColumn,
            ob_partition_key::{Comparable, ObPartitionKey},
        },
        util::hash::ob_hash_sort_utf8mb4::ObHashSortUtf8mb4,
    },
    serde_obkv::value::{CollationType, ObjType, Value},
};

#[derive(Clone, Debug)]
pub enum ObPartDesc {
    Range(ObRangePartDesc),
    Hash(ObHashPartDesc),
    Key(ObKeyPartDesc),
    // TODO: impl list part
}

impl ObPartDesc {
    pub fn set_row_key_element(&mut self, row_key_element: HashMap<String, i32>) {
        match self {
            ObPartDesc::Range(ref mut v) => v.ob_part_desc_obj.set_row_key_element(row_key_element),

            ObPartDesc::Hash(ref mut v) => v.ob_part_desc_obj.set_row_key_element(row_key_element),

            ObPartDesc::Key(ref mut v) => v.ob_part_desc_obj.set_row_key_element(row_key_element),
        }
    }

    pub fn get_part_func_type(&self) -> PartFuncType {
        match self {
            ObPartDesc::Range(v) => v.ob_part_desc_obj.part_func_type.clone(),
            ObPartDesc::Hash(v) => v.ob_part_desc_obj.part_func_type.clone(),
            ObPartDesc::Key(v) => v.ob_part_desc_obj.part_func_type.clone(),
        }
    }

    pub fn get_ordered_part_column_names(&self) -> &[String] {
        match self {
            ObPartDesc::Range(v) => &v.ob_part_desc_obj.ordered_part_column_names,
            ObPartDesc::Hash(v) => &v.ob_part_desc_obj.ordered_part_column_names,
            ObPartDesc::Key(v) => &v.ob_part_desc_obj.ordered_part_column_names,
        }
    }

    pub fn get_part_name_id_map(&self) -> &HashMap<String, i64> {
        match &self {
            ObPartDesc::Range(v) => &v.ob_part_desc_obj.part_name_id_map,
            ObPartDesc::Hash(v) => &v.ob_part_desc_obj.part_name_id_map,
            ObPartDesc::Key(v) => &v.ob_part_desc_obj.part_name_id_map,
        }
    }

    pub fn get_part_id(&self, row_key: &[Value]) -> Result<i64> {
        match &self {
            ObPartDesc::Range(v) => v.get_part_id(row_key),
            ObPartDesc::Hash(v) => v.get_part_id(row_key),
            ObPartDesc::Key(v) => v.get_part_id(row_key),
        }
    }

    pub fn get_part_ids(
        &self,
        start: &[Value],
        start_inclusive: bool,
        end: &[Value],
        end_inclusive: bool,
    ) -> Result<Vec<i64>> {
        match &self {
            ObPartDesc::Range(v) => v.get_part_ids(start, start_inclusive, end, end_inclusive),
            ObPartDesc::Hash(v) => v.get_part_ids(start, start_inclusive, end, end_inclusive),
            ObPartDesc::Key(v) => v.get_part_ids(start, start_inclusive, end, end_inclusive),
        }
    }

    pub fn set_part_columns(&mut self, part_columns: Vec<Box<dyn ObColumn>>) {
        match self {
            ObPartDesc::Range(ref mut v) => v.ob_part_desc_obj.part_columns = part_columns,
            ObPartDesc::Hash(ref mut v) => v.ob_part_desc_obj.part_columns = part_columns,
            ObPartDesc::Key(ref mut v) => v.ob_part_desc_obj.part_columns = part_columns,
        }
    }

    pub fn set_ordered_compare_columns(&mut self, ordered_part_column: Vec<Box<dyn ObColumn>>) {
        match self {
            ObPartDesc::Range(ref mut v) => v.ordered_compare_column = ordered_part_column,

            ObPartDesc::Hash(_) => (),
            ObPartDesc::Key(_) => (),
        }
    }

    pub fn set_part_name_id_map(&mut self, part_name_id_map: HashMap<String, i64>) {
        match self {
            ObPartDesc::Range(ref mut v) => v.ob_part_desc_obj.part_name_id_map = part_name_id_map,

            ObPartDesc::Hash(ref mut v) => v.ob_part_desc_obj.part_name_id_map = part_name_id_map,

            ObPartDesc::Key(ref mut v) => v.ob_part_desc_obj.part_name_id_map = part_name_id_map,
        }
    }

    pub fn is_list_part(&self) -> bool {
        match self {
            ObPartDesc::Range(v) => v.ob_part_desc_obj.part_func_type.is_list_part(),
            ObPartDesc::Hash(v) => v.ob_part_desc_obj.part_func_type.is_list_part(),
            ObPartDesc::Key(v) => v.ob_part_desc_obj.part_func_type.is_list_part(),
        }
    }

    pub fn is_key_part(&self) -> bool {
        match self {
            ObPartDesc::Range(v) => v.ob_part_desc_obj.part_func_type.is_key_part(),
            ObPartDesc::Hash(v) => v.ob_part_desc_obj.part_func_type.is_key_part(),
            ObPartDesc::Key(v) => v.ob_part_desc_obj.part_func_type.is_key_part(),
        }
    }

    pub fn is_range_part(&self) -> bool {
        match self {
            ObPartDesc::Range(v) => v.ob_part_desc_obj.part_func_type.is_range_part(),
            ObPartDesc::Hash(v) => v.ob_part_desc_obj.part_func_type.is_range_part(),
            ObPartDesc::Key(v) => v.ob_part_desc_obj.part_func_type.is_range_part(),
        }
    }

    pub fn prepare(&mut self) -> Result<()> {
        match self {
            ObPartDesc::Range(v) => v.ob_part_desc_obj.prepare(),
            ObPartDesc::Hash(v) => v.ob_part_desc_obj.prepare(),
            ObPartDesc::Key(v) => v.ob_part_desc_obj.prepare(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObPartDescObj {
    part_func_type: PartFuncType,
    part_expr: String,
    ordered_part_column_names: Vec<String>,
    ordered_part_ref_column_row_key_relations: Vec<(Box<dyn ObColumn>, Vec<i32>)>,
    part_columns: Vec<Box<dyn ObColumn>>,
    part_name_id_map: HashMap<String, i64>,
    row_key_element: HashMap<String, i32>,
}

impl ObPartDescObj {
    pub fn new() -> Self {
        Self {
            part_func_type: PartFuncType::Unknown,
            part_expr: String::new(),
            ordered_part_column_names: Vec::new(),
            ordered_part_ref_column_row_key_relations: Vec::new(),
            part_columns: Vec::new(),
            part_name_id_map: HashMap::new(),
            row_key_element: HashMap::new(),
        }
    }

    pub fn set_row_key_element(&mut self, row_key_element: HashMap<String, i32>) {
        self.row_key_element = row_key_element;
    }

    pub fn prepare(&mut self) -> Result<()> {
        if self.ordered_part_column_names.is_empty() {
            error!(
                "ObPartDescObj::prepare prepare ObPartDesc failed. orderedPartColumnNames is empty"
            );
            return Err(CommonErr(
                CommonErrCode::PartitionError,
                "ObPartDescObj::prepare prepare ObPartDesc failed. orderedPartColumnNames is empty"
                    .to_owned(),
            ));
        }

        if self.row_key_element.is_empty() {
            error!("ObPartDescObj::prepare prepare ObPartDesc failed. rowKeyElement is empty");
            return Err(CommonErr(
                CommonErrCode::PartitionError,
                "ObPartDescObj::prepare prepare ObPartDesc failed. rowKeyElement is empty"
                    .to_owned(),
            ));
        }

        if self.part_columns.is_empty() {
            error!("ObPartDescObj::prepare prepare ObPartDesc failed. partColumns is empty");
            return Err(CommonErr(
                CommonErrCode::PartitionError,
                "ObPartDescObj::prepare prepare ObPartDesc failed. partColumns is empty".to_owned(),
            ));
        }
        let mut order_part_ref_column_row_key_relations =
            Vec::with_capacity(self.ordered_part_column_names.len());
        for part_order_column_name in &self.ordered_part_column_names {
            for column in &self.part_columns {
                if column
                    .get_column_name()
                    .eq_ignore_ascii_case(part_order_column_name)
                {
                    let mut part_ref_column_row_key_indexes = Vec::new();
                    for ref_column in column.get_ref_column_names() {
                        let mut row_key_element_refer = false;
                        for (key, val) in &self.row_key_element {
                            if key.eq_ignore_ascii_case(&ref_column) {
                                part_ref_column_row_key_indexes.push(*val);
                                row_key_element_refer = true;
                            }
                        }
                        if !row_key_element_refer {
                            error!(
                                    "ObPartDescObj::prepare partition order column {:?} refer to non-row-key column {:?}",
                                    part_order_column_name, ref_column
                                );
                            return Err(CommonErr(
                                    CommonErrCode::PartitionError,
                                    format!(
                                        "ObPartDescObj::prepare partition order column {part_order_column_name:?} refer to non-row-key column {ref_column:?}",
                                    ),
                                ));
                        }
                    }
                    order_part_ref_column_row_key_relations
                        .push((column.clone(), part_ref_column_row_key_indexes));
                }
            }
        }
        self.ordered_part_ref_column_row_key_relations = order_part_ref_column_row_key_relations;
        Ok(())
    }

    // TODO: support expression
    pub fn eval_row_key_values(&self, row_key: &[Value]) -> Result<Vec<Value>> {
        let mut eval_values: Vec<Value> = Vec::new();
        for i in 0..self.ordered_part_ref_column_row_key_relations.len() {
            if row_key.len() != self.row_key_element.len() {
                error!(
                    "ObPartDescObj::eval_row_key_values row key is consist of {:?}, but found {:?}",
                    self.row_key_element, row_key
                );
                return Err(CommonErr(
                    CommonErrCode::PartitionError,
                    format!(
                        "ObPartDescObj::eval_row_key_values row key is consist of {:?}, but found {:?}",
                        self.row_key_element, row_key
                    ),
                ));
            }
            let ordered_part_ref_column_row_key_relation =
                &self.ordered_part_ref_column_row_key_relations[i];
            let mut eval_params: Vec<Value> =
                Vec::with_capacity(ordered_part_ref_column_row_key_relation.1.len());
            for j in 0..ordered_part_ref_column_row_key_relation.1.len() {
                eval_params
                    .push(row_key[ordered_part_ref_column_row_key_relation.1[j] as usize].clone());
            }
            eval_values.push(
                ordered_part_ref_column_row_key_relation
                    .0
                    .eval_value(&eval_params)?,
            );
        }
        Ok(eval_values)
    }

    pub fn init_comparable_element_by_types(
        &self,
        row_key: &[Value],
        ob_columns: &[Box<dyn ObColumn>],
    ) -> Vec<Comparable> {
        let mut comparable_element = Vec::with_capacity(row_key.len());
        for i in 0..row_key.len() {
            let _column = &ob_columns[i];
            // TODO: Collation type, _column.get_ob_collation_type()
            if row_key[i].is_max() {
                comparable_element.push(Comparable::MaxValue);
            } else if row_key[i].is_min() {
                comparable_element.push(Comparable::MinValue);
            } else {
                comparable_element.push(Comparable::Value(row_key[i].clone()))
            }
        }
        comparable_element
    }
}

#[derive(Clone, Debug)]
pub struct ObRangePartDesc {
    ob_part_desc_obj: ObPartDescObj,
    ordered_compare_column: Vec<Box<dyn ObColumn>>,
    ordered_compare_column_types: Vec<ObjType>,
    bounds: Vec<(ObPartitionKey, i64)>,
}

impl Default for ObRangePartDesc {
    fn default() -> ObRangePartDesc {
        ObRangePartDesc::new()
    }
}

impl ObRangePartDesc {
    pub fn new() -> Self {
        Self {
            ob_part_desc_obj: ObPartDescObj::new(),
            ordered_compare_column: Vec::new(),
            ordered_compare_column_types: Vec::new(),
            bounds: Vec::new(),
        }
    }

    pub fn get_part_ids(
        &self,
        start: &[Value],
        _start_inclusive: bool,
        end: &[Value],
        _end_inclusive: bool,
    ) -> Result<Vec<i64>> {
        let start_part_id = self.get_part_id(start)?;
        let stop_part_id = self.get_part_id(end)?;

        let mut part_ids = Vec::with_capacity(2);
        for i in start_part_id..=stop_part_id {
            part_ids.push(i);
        }
        Ok(part_ids)
    }

    pub fn get_part_id(&self, row_key: &[Value]) -> Result<i64> {
        if row_key.len() != self.ob_part_desc_obj.row_key_element.len() {
            error!(
                "ObRangePartDesc::get_part_id row key is consist of :{:?}, but found: {:?}",
                self.ob_part_desc_obj.row_key_element, row_key
            );
            return Err(CommonErr(
                CommonErrCode::PartitionError,
                format!(
                    "ObRangePartDesc::get_part_id row key is consist of :{:?}, but found: {:?}",
                    self.ob_part_desc_obj.row_key_element, row_key
                ),
            ));
        }
        let row_key = self.ob_part_desc_obj.eval_row_key_values(row_key)?;
        let comparable_element = self
            .ob_part_desc_obj
            .init_comparable_element_by_types(&row_key, &self.ordered_compare_column);
        let search_key = ObPartitionKey::new(comparable_element);
        // diff with java sdk
        let idx = self.upper_bound(search_key);
        Ok(self.bounds[idx as usize].1)
    }

    pub fn get_ordered_compare_column(&self) -> &Vec<Box<dyn ObColumn>> {
        &self.ordered_compare_column
    }

    pub fn set_bounds(&mut self, bounds: Vec<(ObPartitionKey, i64)>) {
        self.bounds = bounds;
    }

    pub fn set_part_func_type(&mut self, part_func_type: PartFuncType) {
        self.ob_part_desc_obj.part_func_type = part_func_type;
    }

    pub fn set_part_expr(&mut self, part_expr: String) {
        self.ob_part_desc_obj.part_expr = part_expr;
    }

    pub fn set_ordered_part_column_names(&mut self, ordered_part_column_names: Vec<String>) {
        self.ob_part_desc_obj.ordered_part_column_names = ordered_part_column_names;
    }

    pub fn set_ordered_compare_column_types(&mut self, ordered_compare_column_types: Vec<ObjType>) {
        self.ordered_compare_column_types = ordered_compare_column_types;
    }

    pub fn upper_bound(&self, key: ObPartitionKey) -> i32 {
        let mut first: i32 = 0;
        let mut len: i32 = self.bounds.len() as i32 - 1;
        while len > 0 {
            let half = len >> 1;
            let middle = first + half;
            if self.bounds[middle as usize].0 > key {
                // search left
                len = half;
            } else {
                // search right
                first = middle + 1;
                len = len - half - 1;
            }
        }
        first
    }
}

#[derive(Clone, Debug)]
pub struct ObHashPartDesc {
    ob_part_desc_obj: ObPartDescObj,
    complete_works: Vec<i64>,
    part_space: i32,
    part_num: i32,
}

impl Default for ObHashPartDesc {
    fn default() -> ObHashPartDesc {
        ObHashPartDesc::new()
    }
}

impl ObHashPartDesc {
    pub fn new() -> Self {
        Self {
            ob_part_desc_obj: ObPartDescObj::new(),
            complete_works: Vec::new(),
            part_space: 0,
            part_num: 0,
        }
    }

    pub fn set_complete_works(&mut self, complete_works: Vec<i64>) {
        self.complete_works = complete_works;
    }

    pub fn set_part_space(&mut self, part_space: i32) {
        self.part_space = part_space;
    }

    pub fn set_part_num(&mut self, part_num: i32) {
        self.part_num = part_num;
    }

    pub fn set_part_func_type(&mut self, part_func_type: PartFuncType) {
        self.ob_part_desc_obj.part_func_type = part_func_type;
    }

    pub fn set_part_expr(&mut self, part_expr: String) {
        self.ob_part_desc_obj.part_expr = part_expr;
    }

    pub fn set_ordered_part_column_names(&mut self, ordered_part_column_names: Vec<String>) {
        self.ob_part_desc_obj.ordered_part_column_names = ordered_part_column_names;
    }

    pub fn set_part_name_id_map(&mut self, part_name_id_map: HashMap<String, i64>) {
        self.ob_part_desc_obj.part_name_id_map = part_name_id_map;
    }

    pub fn get_part_num(&self) -> i32 {
        self.part_num
    }

    pub fn get_part_ids(
        &self,
        start: &[Value],
        start_inclusive: bool,
        end: &[Value],
        end_inclusive: bool,
    ) -> Result<Vec<i64>> {
        let start_value = &start[0];
        let end_value = &end[0];
        // TODO: impl parse_to_i64 in serde_obkv
        let start_long_value: i64 = match start_value {
            Value::String(v, _meta) => v.parse::<i64>()?,
            Value::Int64(v, _meta) => *v,
            Value::Int32(v, _meta) => *v as i64,
            Value::Int8(v, _meta) => *v as i64,
            // TODO: support value bytes
            Value::Bytes(_v, _meta) => unimplemented!(),
            _ => 0,
        };
        let end_long_value: i64 = match end_value {
            Value::String(v, _meta) => v.parse::<i64>()?,
            Value::Int64(v, _meta) => *v,
            Value::Int32(v, _meta) => *v as i64,
            Value::Int8(v, _meta) => *v as i64,
            // TODO: support value bytes
            Value::Bytes(_v, _meta) => unimplemented!(),
            _ => 0,
        };

        let start_hash_value = if start_inclusive {
            start_long_value
        } else {
            start_long_value + 1
        };
        let end_hash_value = if end_inclusive {
            end_long_value
        } else {
            end_long_value - 1
        };

        if end_hash_value - start_hash_value + 1 >= self.part_num as i64 {
            Ok(self.complete_works.clone())
        } else {
            let mut part_ids = Vec::with_capacity((end_hash_value - start_hash_value + 1) as usize);
            for i in start_hash_value..=end_hash_value {
                part_ids.push(self.inner_hash(i));
            }
            Ok(part_ids)
        }
    }

    pub fn get_part_id(&self, row_key: &[Value]) -> Result<i64> {
        if row_key.is_empty() {
            error!(
                "ObHashPartDesc::get_part_id invalid row keys :{:?}",
                row_key
            );
            return Err(CommonErr(
                CommonErrCode::PartitionError,
                "ObHashPartDesc::get_part_id get_part_id: row_key is empty".to_owned(),
            ));
        }
        // TODO: evalRowKeyValues

        // Note: There was a loop in java version
        let value: i64 = match &row_key[0] {
            Value::String(v, _meta) => v.parse::<i64>()?,
            Value::Int64(v, _meta) => *v,
            Value::Int32(v, _meta) => *v as i64,
            Value::Int8(v, _meta) => *v as i64,
            // TODO: support value bytes
            Value::Bytes(_v, _meta) => unimplemented!(),
            _ => 0,
        };
        let current_part_id = self.inner_hash(value);
        // TODO: diff with java sdk
        Ok(current_part_id)
    }

    fn inner_hash(&self, value: i64) -> i64 {
        let hash_value = value.abs();
        ((self.part_space as i64) << ob_part_constants::PART_ID_BITNUM)
            | (hash_value % self.part_num as i64)
    }
}

#[derive(Clone, Debug)]
pub struct ObKeyPartDesc {
    ob_part_desc_obj: ObPartDescObj,
    part_space: i32,
    part_num: i32,
}

impl Default for ObKeyPartDesc {
    fn default() -> Self {
        Self::new()
    }
}

impl ObKeyPartDesc {
    pub fn new() -> Self {
        Self {
            ob_part_desc_obj: ObPartDescObj::new(),
            part_space: 0,
            part_num: 0,
        }
    }

    pub fn set_part_space(&mut self, part_space: i32) {
        self.part_space = part_space;
    }

    pub fn set_part_num(&mut self, part_num: i32) {
        self.part_num = part_num;
    }

    pub fn set_part_func_type(&mut self, part_func_type: PartFuncType) {
        self.ob_part_desc_obj.part_func_type = part_func_type;
    }

    pub fn set_part_expr(&mut self, part_expr: String) {
        self.ob_part_desc_obj.part_expr = part_expr;
    }

    pub fn set_ordered_part_column_names(&mut self, ordered_part_column_names: Vec<String>) {
        self.ob_part_desc_obj.ordered_part_column_names = ordered_part_column_names;
    }

    pub fn set_part_name_id_map(&mut self, part_name_id_map: HashMap<String, i64>) {
        self.ob_part_desc_obj.part_name_id_map = part_name_id_map;
    }

    pub fn get_part_num(&self) -> i32 {
        self.part_num
    }

    pub fn get_part_ids(
        &self,
        start: &[Value],
        _start_inclusive: bool,
        end: &[Value],
        _end_inclusive: bool,
    ) -> Result<Vec<i64>> {
        // if start=[min,min,min] end=[max,max,max], scan all partitions
        let mut is_min_max = true;
        for v in start {
            if !is_min_max {
                break;
            }
            is_min_max &= v.is_min();
        }

        for v in end {
            if !is_min_max {
                break;
            }
            is_min_max &= v.is_max();
        }

        // Note: Java / ODP may not query all the partitions, and will return an error
        // instead
        if is_min_max || !self.is_equal_keys(start, end) {
            let mut part_ids: Vec<i64> = Vec::with_capacity(self.part_num as usize);
            for i in 0..self.part_num as i64 {
                part_ids.push(i);
            }
            return Ok(part_ids);
        }

        let mut row_keys: Vec<Value> = Vec::with_capacity(2);
        row_keys.append(&mut start.to_vec());
        row_keys.append(&mut end.to_vec());
        let part_ids = vec![self.get_part_id(&row_keys)?];
        Ok(part_ids)
    }

    pub fn get_part_id(&self, row_key: &[Value]) -> Result<i64> {
        if row_key.is_empty() {
            error!("ObKeyPartDesc::get_part_id invalid row keys :{:?}", row_key);
            return Err(CommonErr(
                CommonErrCode::PartitionError,
                "ObKeyPartDesc::get_part_id get_part_id: row_key is empty".to_owned(),
            ));
        }
        // TODO: evalRowKeyValues
        let part_ref_column_size = self
            .ob_part_desc_obj
            .ordered_part_ref_column_row_key_relations
            .len();

        let mut hash_value = 0u64;
        // FIXME: shall we do a check to ensure part_ref_column_size <= row_key.len()?
        for (idx, row_key) in row_key.iter().enumerate().take(part_ref_column_size) {
            hash_value = ObKeyPartDesc::to_hashcode(
                row_key,
                &self
                    .ob_part_desc_obj
                    .ordered_part_ref_column_row_key_relations[idx]
                    .0,
                hash_value,
                &self.ob_part_desc_obj.part_func_type,
            )?;
        }
        let mut hash_value = hash_value as i64;
        hash_value = hash_value.abs();
        Ok(
            ((self.part_space as i64) << ob_part_constants::PART_ID_BITNUM)
                | (hash_value % (self.part_num as i64)),
        )
    }

    pub fn is_equal_keys(&self, start: &[Value], end: &[Value]) -> bool {
        for (start_value, end_value) in start.iter().zip(end.iter()) {
            if start_value != end_value {
                return false;
            }
        }
        true
    }

    #[allow(clippy::wrong_self_convention)]
    #[allow(clippy::borrowed_box)]
    pub fn to_hashcode(
        value: &Value,
        ref_column: &Box<dyn ObColumn>,
        hash_code: u64,
        part_func_type: &PartFuncType,
    ) -> Result<u64> {
        match value {
            // varchar & varbinary
            Value::String(v, meta) => ObKeyPartDesc::varchar_hash(
                Value::String(v.clone(), meta.clone()),
                ref_column.get_ob_collation_type(),
                hash_code,
                part_func_type.to_owned(),
            ),

            Value::Int64(v, _meta) => ObKeyPartDesc::long_hash(*v, hash_code),
            Value::Int32(v, _meta) => ObKeyPartDesc::long_hash(*v as i64, hash_code),
            Value::Int8(v, _meta) => ObKeyPartDesc::long_hash(*v as i64, hash_code),
            Value::Bytes(v, meta) => ObKeyPartDesc::varchar_hash(
                Value::Bytes(v.clone(), meta.clone()),
                ref_column.get_ob_collation_type(),
                hash_code,
                part_func_type.to_owned(),
            ),
            // TODO: support value Time
            Value::Time(_v, _meta) => unimplemented!(),
            // TODO: support value Date
            Value::Date(_v, _meta) => unimplemented!(),
            _ => Ok(0),
        }
    }

    // TODO: check if murmur2 hash value is correct(java sdk)
    pub fn long_hash(value: i64, hash_code: u64) -> Result<u64> {
        Ok(murmur2::murmur64a(&value.to_ne_bytes(), hash_code))
    }

    // TODO: support value Time
    //pub fn timestamp_hash() -> Result<i64> {
    //    unimplemented!();
    //}

    // TODO: support value Date
    //pub fn date_hash() -> Result<i64> {
    //    unimplemented!();
    //}

    pub fn varchar_hash(
        value: Value,
        collation_type: &CollationType,
        hash_code: u64,
        part_func_type: PartFuncType,
    ) -> Result<u64> {
        let seed: u64 = 0xc6a4_a793_5bd1_e995;
        let bytes = match value {
            Value::String(v, _meta) => v.into_bytes(),

            Value::Bytes(v, _meta) => v,
            _ => {
                error!(
                    "ObKeyPartDesc::varchar_hash varchar not supported, ObCollationType:{:?} object:{:?}",
                    collation_type, value
                );
                return Err(CommonErr(
                    CommonErrCode::PartitionError,
                    format!(
                        "ObKeyPartDesc::varchar_hash varchar not supported, ObCollationType:{collation_type:?} object:{value:?}",
                    ),
                ));
            }
        };
        match collation_type {
            CollationType::UTF8MB4GeneralCi => {
                if part_func_type == KeyV3 || part_func_type == KeyImplicitV2 {
                    Ok(ObHashSortUtf8mb4::ob_hash_sort_utf8_mb4(
                        &bytes,
                        bytes.len() as i32,
                        hash_code,
                        seed,
                        true,
                    ))
                } else {
                    Ok(ObHashSortUtf8mb4::ob_hash_sort_utf8_mb4(
                        &bytes,
                        bytes.len() as i32,
                        hash_code,
                        seed,
                        false,
                    ))
                }
            }
            CollationType::UTF8MB4Bin => {
                if part_func_type == KeyV3 || part_func_type == KeyImplicitV2 {
                    Ok(murmur2::murmur64a(&bytes, hash_code))
                } else {
                    Ok(ObHashSortUtf8mb4::ob_hash_sort_mb_bin(
                        &bytes,
                        bytes.len() as i32,
                        hash_code,
                        seed,
                    ))
                }
            }
            CollationType::Binary => Ok(ObHashSortUtf8mb4::ob_hash_sort_bin(
                &bytes,
                bytes.len() as i32,
                hash_code,
                seed,
            )),
            _ => {
                error!(
                    "ObKeyPartDesc::varchar_hash not supported collation type, type:{:?}",
                    collation_type
                );
                Err(CommonErr(
                    CommonErrCode::PartitionError,
                    format!(
                        "ObKeyPartDesc::varchar_hash not supported collation type, type:{collation_type:?}",
                    ),
                ))
            }
        }
    }
}
