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

use std::fmt::Write;

const TABLE_COMPARE_FILTER_PREFIX: &str = "TableCompareFilter";

pub trait FilterEncoder {
    /// Encode the filter as string.
    fn encode(&self) -> String;

    /// Encode the filter to the buffer.
    fn encode_to(&self, buffer: &mut String);
}

pub enum Filter {
    Value(ObTableValueFilter),
    List(ObTableFilterList),
}

impl FilterEncoder for Filter {
    /// Encode the filter as string.
    fn encode(&self) -> String {
        match self {
            Filter::List(filter) => filter.encode(),
            Filter::Value(filter) => filter.encode(),
        }
    }

    /// Encode the filter to the buffer.
    fn encode_to(&self, buffer: &mut String) {
        match self {
            Filter::List(filter) => filter.encode_to(buffer),
            Filter::Value(filter) => filter.encode_to(buffer),
        }
    }
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
    pub filters: Vec<Filter>,
}

impl ObTableFilterList {
    pub fn new<I>(op: FilterOp, filters: I) -> Self
    where
        I: IntoIterator<Item = Filter>,
    {
        ObTableFilterList {
            op,
            filters: filters.into_iter().collect(),
        }
    }

    pub fn add_filter(&mut self, filter: Filter) {
        self.filters.push(filter)
    }

    pub fn add_filters<I>(&mut self, filters: I)
    where
        I: IntoIterator<Item = Filter>,
    {
        self.filters.extend(filters.into_iter())
    }
}

impl FilterEncoder for ObTableFilterList {
    /// Encode the filter as string.
    fn encode(&self) -> String {
        let string_op = match self.op {
            FilterOp::And => " && ",
            FilterOp::Or => " || ",
        };

        // Use an iterator with map and collect to efficiently concatenate strings
        self.filters
            .iter()
            .map(|filter| {
                let filter_string = filter.encode();
                match filter {
                    Filter::List(_) => {
                        format!("({})", filter_string)
                    }
                    Filter::Value(_) => filter_string,
                }
            })
            .collect::<Vec<_>>()
            .join(string_op)
    }

    /// Encode the filter to the buffer.
    fn encode_to(&self, buffer: &mut String) {
        let string_op = match self.op {
            FilterOp::And => " && ",
            FilterOp::Or => " || ",
        };

        for (i, filter) in self.filters.iter().enumerate() {
            if i != 0 {
                buffer.push_str(string_op);
            }
            match filter {
                Filter::List(filter_list) => {
                    buffer.push('(');
                    filter_list.encode_to(buffer);
                    buffer.push(')');
                }
                Filter::Value(value_filter) => value_filter.encode_to(buffer),
            }
        }
    }
}

/// Only support [`ObTableValueFilter`] on numeric type and string type
/// The value will be encoded into string and will be parsed into filter in the server
#[derive(Debug, Clone)]
pub struct ObTableValueFilter {
    pub op: ObCompareOperator,
    pub column_name: String,
    pub value: String,
}

impl ObTableValueFilter {
    pub fn new<V: ToString>(op: ObCompareOperator, column_name: String, value: V) -> Self {
        ObTableValueFilter {
            op,
            column_name,
            value: value.to_string(),
        }
    }
}

impl FilterEncoder for ObTableValueFilter {
    /// Encode the filter as string.
    fn encode(&self) -> String {
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

    /// Encode the filter to the buffer.
    fn encode_to(&self, buffer: &mut String) {
        if !self.column_name.is_empty() {
            if let Err(e) = write!(
                buffer,
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
    fn test_value_filter() {
        let op = ObCompareOperator::Equal;
        let column_name = "column";
        let string_column_name = "string_column";

        // create ObTableValueFilter by micro rules
        let filter_i16 = ObTableValueFilter::new(op.clone(), column_name.to_string(), 51i16);
        let filter_i32 = ObTableValueFilter::new(op.clone(), string_column_name.to_string(), 51i32);
        let filter_i64 = ObTableValueFilter::new(op.clone(), column_name.to_string(), 51i64);
        let filter_u16 = ObTableValueFilter::new(op.clone(), string_column_name.to_string(), 51u16);
        let filter_u32 = ObTableValueFilter::new(op.clone(), column_name.to_string(), 51u32);
        let filter_u64 = ObTableValueFilter::new(op.clone(), string_column_name.to_string(), 51u64);
        let filter_f32 = ObTableValueFilter::new(op.clone(), column_name.to_string(), 51.0f32);
        let filter_f64 =
            ObTableValueFilter::new(op.clone(), string_column_name.to_string(), 51.0f64);
        let filter_string =
            ObTableValueFilter::new(op.clone(), column_name.to_string(), "51".to_string());
        let filter_str = ObTableValueFilter::new(op.clone(), string_column_name.to_string(), "51");

        assert_eq!("TableCompareFilter(=,'column:51')", filter_i16.encode());
        assert_eq!(
            "TableCompareFilter(=,'string_column:51')",
            filter_i32.encode()
        );
        assert_eq!("TableCompareFilter(=,'column:51')", filter_i64.encode());
        assert_eq!(
            "TableCompareFilter(=,'string_column:51')",
            filter_u16.encode()
        );
        assert_eq!("TableCompareFilter(=,'column:51')", filter_u32.encode());
        assert_eq!(
            "TableCompareFilter(=,'string_column:51')",
            filter_u64.encode()
        );
        assert_eq!("TableCompareFilter(=,'column:51')", filter_f32.encode());
        assert_eq!(
            "TableCompareFilter(=,'string_column:51')",
            filter_f64.encode()
        );
        assert_eq!("TableCompareFilter(=,'column:51')", filter_string.encode());
        assert_eq!(
            "TableCompareFilter(=,'string_column:51')",
            filter_str.encode()
        );
    }

    #[test]
    fn test_filter_list() {
        let column_name = "column";
        let filter_list_0 = ObTableFilterList::new(
            FilterOp::And,
            vec![
                Filter::Value(ObTableValueFilter::new(
                    ObCompareOperator::Equal,
                    column_name.to_string(),
                    "0",
                )),
                Filter::Value(ObTableValueFilter::new(
                    ObCompareOperator::GreaterThan,
                    column_name.to_string(),
                    "1",
                )),
            ],
        );
        let mut filter_list_component = ObTableFilterList::new(
            FilterOp::And,
            vec![Filter::Value(ObTableValueFilter::new(
                ObCompareOperator::Equal,
                column_name.to_string(),
                2,
            ))],
        );
        filter_list_component.add_filter(Filter::Value(ObTableValueFilter::new(
            ObCompareOperator::GreaterThan,
            column_name.to_string(),
            "3",
        )));
        let mut filter_list_1 =
            ObTableFilterList::new(FilterOp::Or, vec![Filter::List(filter_list_component)]);
        filter_list_1.add_filters(vec![Filter::Value(ObTableValueFilter::new(
            ObCompareOperator::GreaterThan,
            column_name.to_string(),
            "4",
        ))]);

        assert_eq!(
            "TableCompareFilter(=,'column:0') && TableCompareFilter(>,'column:1')",
            filter_list_0.encode()
        );
        assert_eq!("(TableCompareFilter(=,'column:2') && TableCompareFilter(>,'column:3')) || TableCompareFilter(>,'column:4')", filter_list_1.encode());
    }
}
