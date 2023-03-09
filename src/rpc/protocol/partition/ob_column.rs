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

use std::{clone::Clone, fmt::Debug};

use crate::{
    error::{CommonErrCode, Error::Common as CommonErr, Result},
    serde_obkv::value::{CollationLevel, CollationType, ObjMeta, ObjType, Value},
};

pub trait ObColumn: ObColumnClone + Debug + Send + Sync {
    fn get_column_name(&self) -> String;
    fn get_index(&self) -> i32;
    fn get_ob_obj_type(&self) -> ObjType;
    fn get_ref_column_names(&self) -> Vec<String>;
    fn get_ob_collation_type(&self) -> &CollationType;
    fn eval_value(&self, refs: &[Value]) -> Result<Value>;
}

pub trait ObColumnClone {
    fn clone_box(&self) -> Box<dyn ObColumn>;
}

impl<T> ObColumnClone for T
where
    T: 'static + ObColumn + Clone,
{
    fn clone_box(&self) -> Box<dyn ObColumn> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ObColumn> {
    fn clone(&self) -> Box<dyn ObColumn> {
        self.clone_box()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObGeneratedColumn {
    column_name: String,
    index: i32,
    ob_obj_type: ObjType,
    ob_collation_type: CollationType,
    ref_column_names: Vec<String>,
    // TODO:columnExpress
    column_express: String,
}

impl ObGeneratedColumn {
    pub fn new(
        column_name: String,
        index: i32,
        ob_obj_type: ObjType,
        ob_collation_type: CollationType,
    ) -> Self {
        Self {
            column_name,
            index,
            ob_obj_type,
            ob_collation_type,
            ref_column_names: Vec::new(),
            column_express: String::new(),
        }
    }
}

impl ObColumn for ObGeneratedColumn {
    fn get_column_name(&self) -> String {
        self.column_name.clone()
    }

    fn get_index(&self) -> i32 {
        self.index
    }

    fn get_ob_obj_type(&self) -> ObjType {
        self.ob_obj_type.clone()
    }

    fn get_ref_column_names(&self) -> Vec<String> {
        self.ref_column_names.clone()
    }

    fn get_ob_collation_type(&self) -> &CollationType {
        &self.ob_collation_type
    }

    // TODO: impl express, ObGeneratedColumn in java sdk
    fn eval_value(&self, refs: &[Value]) -> Result<Value> {
        Ok(refs[0].clone())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObSimpleColumn {
    column_name: String,
    index: i32,
    ob_obj_type: ObjType,
    ob_collation_type: CollationType,
    ref_column_names: Vec<String>,
}

impl ObSimpleColumn {
    pub fn new(
        column_name: String,
        index: i32,
        ob_obj_type: ObjType,
        ob_collation_type: CollationType,
    ) -> Self {
        let ref_column_names = vec![column_name.clone()];
        Self {
            column_name,
            index,
            ob_obj_type,
            ob_collation_type,
            ref_column_names,
        }
    }
}

impl ObColumn for ObSimpleColumn {
    fn get_column_name(&self) -> String {
        self.column_name.clone()
    }

    fn get_index(&self) -> i32 {
        self.index
    }

    fn get_ob_obj_type(&self) -> ObjType {
        self.ob_obj_type.clone()
    }

    fn get_ref_column_names(&self) -> Vec<String> {
        self.ref_column_names.clone()
    }

    fn get_ob_collation_type(&self) -> &CollationType {
        &self.ob_collation_type
    }

    fn eval_value(&self, refs: &[Value]) -> Result<Value> {
        if refs.len() != 1 {
            error!("ObSimpleColumn::eval_value ObSimpleColumn is refer to itself so that the length of the refs must be 1. refs:{refs:?}");
            return Err(CommonErr(
                CommonErrCode::InvalidParam,
                format!("ObSimpleColumn::eval_value ObSimpleColumn is refer to itself so that the length of the refs must be 1. refs:{refs:?}"),
            ));
        }
        match self.ob_obj_type {
            ObjType::Varchar => match &refs[0] {
                Value::String(_v, _meta) => Ok(refs[0].clone()),
                _ => unimplemented!(),
            },
            ObjType::UInt64 | ObjType::Int64 | ObjType::UInt32 | ObjType::Int32 => {
                if refs[0].is_min() || refs[0].is_max() {
                    return Ok(refs[0].clone());
                }
                match refs[0].to_owned() {
                    Value::String(v, _meta) => Ok(Value::Int64(
                        v.parse::<i64>()?,
                        ObjMeta::new(
                            ObjType::Int64,
                            CollationLevel::Numeric,
                            CollationType::Binary,
                            10,
                        ),
                    )),
                    Value::Int32(v, _meta) => Ok(Value::Int64(
                        v as i64,
                        ObjMeta::new(
                            ObjType::Int64,
                            CollationLevel::Numeric,
                            CollationType::Binary,
                            10,
                        ),
                    )),
                    Value::UInt32(v, _meta) => Ok(Value::Int64(
                        v as i64,
                        ObjMeta::new(
                            ObjType::Int64,
                            CollationLevel::Numeric,
                            CollationType::Binary,
                            10,
                        ),
                    )),
                    Value::Int64(v, _meta) => Ok(Value::Int64(
                        v,
                        ObjMeta::new(
                            ObjType::Int64,
                            CollationLevel::Numeric,
                            CollationType::Binary,
                            10,
                        ),
                    )),
                    Value::UInt64(v, _meta) => Ok(Value::Int64(
                        v as i64,
                        ObjMeta::new(
                            ObjType::Int64,
                            CollationLevel::Numeric,
                            CollationType::Binary,
                            10,
                        ),
                    )),
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }
}
