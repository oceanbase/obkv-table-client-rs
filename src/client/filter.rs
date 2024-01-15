/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2024 OceanBase
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

use std::{any::Any, fmt::Write};

const TABLE_COMPARE_FILTER_PREFIX: &str = "TableCompareFilter";

pub trait Filter: Any {
    fn as_any(&self) -> &dyn Any;
    fn string(&self) -> String;
    fn concat_string(&self, filter_string: &mut String);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObCompareOperator {
    LessThan = 0,
    GreaterThan = 1,
    LessOrEqualThan = 2,
    GreaterOrEqualThan = 3,
    NotEqual = 4,
    Equal = 5,
    IsNull = 6,
    IsNotNull = 7,
}

impl ObCompareOperator {
    pub fn string(&self) -> &'static str {
        match self {
            ObCompareOperator::LessThan => "<",
            ObCompareOperator::GreaterThan => ">",
            ObCompareOperator::LessOrEqualThan => "<=",
            ObCompareOperator::GreaterOrEqualThan => ">=",
            ObCompareOperator::NotEqual => "!=",
            ObCompareOperator::Equal => "=",
            ObCompareOperator::IsNull => "IS",
            ObCompareOperator::IsNotNull => "IS_NOT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterOp {
    And = 0,
    Or = 1,
}

pub struct ObTableFilterList {
    pub op: FilterOp,
    pub filters: Vec<Box<dyn Filter>>,
}

#[macro_export]
macro_rules! filter_list {
    ($op:expr, $($filter:expr),+ $(,)?) => {
        ObTableFilterList {
            op: $op,
            filters: vec![$(Box::new($filter) as Box<dyn Filter>),+],
        }
    };
}

impl ObTableFilterList {
    pub fn new<I>(op: FilterOp, filters: I) -> Self
    where
        I: IntoIterator<Item = Box<dyn Filter>>,
    {
        ObTableFilterList {
            op,
            filters: filters.into_iter().collect(),
        }
    }

    pub fn add_filter(&mut self, filter: Box<dyn Filter>) {
        self.filters.push(filter)
    }

    pub fn add_filters<I>(&mut self, filters: I)
    where
        I: IntoIterator<Item = Box<dyn Filter>>,
    {
        self.filters.extend(filters.into_iter())
    }
}

impl Filter for ObTableFilterList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn string(&self) -> String {
        let string_op = match self.op {
            FilterOp::And => " && ",
            FilterOp::Or => " || ",
        };

        // Use an iterator with map and collect to efficiently concatenate strings
        self.filters
            .iter()
            .map(|filter| {
                let filter_string = filter.string();
                if filter.as_any().is::<ObTableValueFilter>() {
                    filter_string
                } else {
                    format!("({})", filter_string)
                }
            })
            .collect::<Vec<_>>()
            .join(string_op)
    }

    fn concat_string(&self, filter_string: &mut String) {
        let string_op = match self.op {
            FilterOp::And => " && ",
            FilterOp::Or => " || ",
        };

        for (i, filter) in self.filters.iter().enumerate() {
            if i != 0 {
                filter_string.push_str(string_op);
            }
            if filter.as_any().is::<ObTableValueFilter>() {
                filter.concat_string(filter_string);
            } else {
                filter_string.push('(');
                filter.concat_string(filter_string);
                filter_string.push(')');
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObTableValueFilter {
    pub op: ObCompareOperator,
    pub column_name: String,
    pub value: String,
}

impl ObTableValueFilter {
    pub fn new<U: ToString, V: ToString>(op: ObCompareOperator, column_name: U, value: V) -> Self {
        ObTableValueFilter {
            op,
            column_name: column_name.to_string(),
            value: value.to_string(),
        }
    }
}

impl Filter for ObTableValueFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn string(&self) -> String {
        if self.column_name.is_empty() {
            return String::new();
        }
        format!(
            "{}({},'{}:{}')",
            TABLE_COMPARE_FILTER_PREFIX,
            self.op.string(),
            self.column_name,
            self.value
        )
    }

    fn concat_string(&self, filter_string: &mut String) {
        if !self.column_name.is_empty() {
            if let Err(e) = write!(
                filter_string,
                "{}({},'{}:{}')",
                TABLE_COMPARE_FILTER_PREFIX,
                self.op.string(),
                self.column_name,
                self.value
            ) {
                warn!("Failed to write to filter_string: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_value_filter_micro() {
        let op = ObCompareOperator::Equal;
        let column_name = "column";
        let string_column_name = "string_column";

        // create ObTableValueFilter by micro rules
        let filter_i16 = ObTableValueFilter::new(op.clone(), column_name, 51i16);
        let filter_i32 = ObTableValueFilter::new(op.clone(), string_column_name, 51i32);
        let filter_i64 = ObTableValueFilter::new(op.clone(), column_name, 51i64);
        let filter_u16 = ObTableValueFilter::new(op.clone(), string_column_name, 51u16);
        let filter_u32 = ObTableValueFilter::new(op.clone(), column_name, 51u32);
        let filter_u64 = ObTableValueFilter::new(op.clone(), string_column_name, 51u64);
        let filter_f32 = ObTableValueFilter::new(op.clone(), column_name, 51.0f32);
        let filter_f64 = ObTableValueFilter::new(op.clone(), string_column_name, 51.0f64);
        let filter_string = ObTableValueFilter::new(op.clone(), column_name, "51".to_string());
        let filter_str = ObTableValueFilter::new(op.clone(), string_column_name, "51");

        assert_eq!("TableCompareFilter(=,'column:51')", filter_i16.string());
        assert_eq!(
            "TableCompareFilter(=,'string_column:51')",
            filter_i32.string()
        );
        assert_eq!("TableCompareFilter(=,'column:51')", filter_i64.string());
        assert_eq!(
            "TableCompareFilter(=,'string_column:51')",
            filter_u16.string()
        );
        assert_eq!("TableCompareFilter(=,'column:51')", filter_u32.string());
        assert_eq!(
            "TableCompareFilter(=,'string_column:51')",
            filter_u64.string()
        );
        assert_eq!("TableCompareFilter(=,'column:51')", filter_f32.string());
        assert_eq!(
            "TableCompareFilter(=,'string_column:51')",
            filter_f64.string()
        );
        assert_eq!("TableCompareFilter(=,'column:51')", filter_string.string());
        assert_eq!(
            "TableCompareFilter(=,'string_column:51')",
            filter_str.string()
        );
    }

    #[test]
    fn test_filter_list() {
        let column_name = "column";
        let filter_list_0 = ObTableFilterList::new(
            FilterOp::And,
            vec![
                Box::new(ObTableValueFilter::new(
                    ObCompareOperator::Equal,
                    column_name,
                    "0",
                )) as Box<dyn Filter>,
                Box::new(ObTableValueFilter::new(
                    ObCompareOperator::GreaterThan,
                    column_name,
                    "1",
                )) as Box<dyn Filter>,
            ],
        );
        let mut filter_list_component = ObTableFilterList::new(
            FilterOp::And,
            vec![Box::new(ObTableValueFilter::new(
                ObCompareOperator::Equal,
                column_name,
                2,
            )) as Box<dyn Filter>],
        );
        filter_list_component.add_filter(Box::new(ObTableValueFilter::new(
            ObCompareOperator::GreaterThan,
            column_name,
            "3",
        )) as Box<dyn Filter>);
        let mut filter_list_1 = filter_list!(FilterOp::Or, filter_list_component,);
        filter_list_1.add_filters(vec![Box::new(ObTableValueFilter::new(
            ObCompareOperator::GreaterThan,
            column_name,
            "4",
        )) as Box<dyn Filter>]);

        println!("{:?}", filter_list_0.string());
        println!("{:?}", filter_list_1.string());
        assert_eq!(
            "TableCompareFilter(=,'column:0') && TableCompareFilter(>,'column:1')",
            filter_list_0.string()
        );
        assert_eq!("(TableCompareFilter(=,'column:2') && TableCompareFilter(>,'column:3')) || TableCompareFilter(>,'column:4')", filter_list_1.string());
    }
}
