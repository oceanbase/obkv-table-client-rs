// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, i32, i64, u8};

use mysql as my;

use super::{
    ob_part_constants, part_func_type::PartFuncType, ObHashPartDesc, ObKeyPartDesc, ObPartDesc,
    ObPartitionInfo, ObPartitionLevel, ObRangePartDesc, TableEntry,
};
use crate::{
    error::{CommonErrCode, Error::Common as CommonErr, Result},
    rpc::protocol::partition::{
        ob_column::{ObColumn, ObGeneratedColumn, ObSimpleColumn},
        ob_partition_key::{Comparable, ObPartitionKey},
    },
    serde_obkv::value::{CollationLevel, CollationType, ObjMeta, ObjType, Value},
};

pub const TEMPLATE_PART_ID: i32 = -1;

//pub const PROXY_PART_INFO_SQL: &str = "SELECT /*+READ_CONSISTENCY(WEAK)*/
// part_level, part_num, part_type, part_space, part_expr,    part_range_type,
// part_interval_bin, interval_start_bin,    sub_part_num, sub_part_type,
// sub_part_space,    sub_part_range_type, def_sub_part_interval_bin,
// def_sub_interval_start_bin, sub_part_expr,    part_key_name, part_key_type,
// part_key_idx, part_key_extra, spare1    FROM oceanbase.
// __all_virtual_proxy_partition_info    WHERE table_id = {} group by
// part_key_name order by part_key_name LIMIT {};";

pub struct LocationUtil {}

impl LocationUtil {
    pub fn fetch_partition_info(
        conn: &mut my::PooledConn,
        table_entry: &TableEntry,
    ) -> Result<ObPartitionInfo> {
        let sql = format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ part_level, part_num, part_type, part_space, part_expr
	, part_range_type, part_interval_bin, interval_start_bin, sub_part_num, sub_part_type
	, sub_part_space, sub_part_range_type, def_sub_part_interval_bin, def_sub_interval_start_bin, sub_part_expr
	, part_key_name, part_key_type, part_key_idx, part_key_extra, spare1
    FROM oceanbase.__all_virtual_proxy_partition_info
    WHERE table_id = {}
    GROUP BY part_key_name
    ORDER BY part_key_name
    LIMIT {};", table_entry.table_id, i64::MAX);

        let mut info = ObPartitionInfo::new();
        // in java sdk is parsePartitionInfo()
        let mut is_first_row = true;
        for result in conn.query(sql)? {
            result.map(|mut row| {
                    if is_first_row {
                        is_first_row = false;

                        // get part level
                        info.level = ObPartitionLevel::from_int(row.take("part_level").unwrap());

                        // get first part
                        if info.level.get_index() >= ObPartitionLevel::One.get_index() {
                            let part_desc: Option<ObPartDesc> = match LocationUtil::build_part_desc(
                                ObPartitionLevel::One,
                                row.clone(),
                            ) {
                                Ok(v) => v,
                                Err(e) => {
                                    warn!(
                                        "LocationUtil:fetch_partition_info ObPartitionLevel::One build_part_desc,err: {:?}",
                                        e
                                    );
                                    None
                                }
                            };
                            match part_desc {
                                Some(v) => info.first_part_desc = Some(v),
                                None => warn!("fail to build first part"),
                            };
                        }

                        // get sub part
                        if info.level.get_index() == ObPartitionLevel::Two.get_index() {
                            let part_desc =
                                match LocationUtil::build_part_desc(ObPartitionLevel::Two, row.clone()) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        warn!(
                                            "LocationUtil:fetch_partition_info ObPartitionLevel::Two build_part_desc,err: {:?}",
                                            e
                                        );
                                        None
                                    }
                                };
                            match part_desc {
                                Some(v) => info.sub_part_desc = Some(v),
                                None => warn!("fail to build sub part"),
                            };
                        }
                    }
                    let part_key_extra: String = row.take("part_key_extra").unwrap();
                    let part_key_name: String = row.take("part_key_name").unwrap();
                    let part_key_idx: i32 = row.take("part_key_idx").unwrap();
                    let part_key_type: u8 = row.take("part_key_type").unwrap();

                    let spare1: u8 = row.take("spare1").unwrap();

                    // get part key for each loop
                    if !part_key_extra.is_empty() {
                        // TODO: new ObGeneratedColumnExpressParser(getPlainString(partKeyExtra)).parse());
                        let column = ObGeneratedColumn::new(
                            part_key_name,
                            part_key_idx,
                            ObjType::from_u8(part_key_type)
                                .expect("LocationUtil::fetch_partition_info fail to decode obj type"),
                            CollationType::from_u8(spare1)
                                 .expect("LocationUtil::fetch_partition_info fail to decode collation type"),
                        );
                        info.part_columns.push(Box::new(column));
                    } else {
                        let column = ObSimpleColumn::new(
                            part_key_name,
                            part_key_idx,
                            ObjType::from_u8(part_key_type)
                                .expect("LocationUtil::fetch_partition_info fail to decode obj type"),
                            CollationType::from_u8(spare1)
                                 .expect("LocationUtil::fetch_partition_info fail to decode collation type"),
                        );

                        info.part_columns.push(Box::new(column));
                    }
                })?;
        }

        // get list partition column types here
        let mut ordered_parted_columns = Vec::new();
        match &info.first_part_desc {
            None => (),
            Some(first_part_desc) => {
                if first_part_desc.is_list_part() || first_part_desc.is_range_part() {
                    ordered_parted_columns =
                        LocationUtil::get_ordered_part_columns(&info.part_columns, first_part_desc);
                }
            }
        }
        if let Some(first_part_desc) = &info.first_part_desc {
            match &info.sub_part_desc {
                None => (),
                Some(sub_part_desc) => {
                    if sub_part_desc.is_list_part() || first_part_desc.is_range_part() {
                        ordered_parted_columns = LocationUtil::get_ordered_part_columns(
                            &info.part_columns,
                            sub_part_desc,
                        );
                    }
                }
            }
        }

        // set the property of first part and sub part
        LocationUtil::set_part_desc_property(
            &mut info.first_part_desc,
            &info.part_columns,
            &ordered_parted_columns,
        )?;
        LocationUtil::set_part_desc_property(
            &mut info.sub_part_desc,
            &info.part_columns,
            &ordered_parted_columns,
        )?;

        Ok(info)
    }

    fn build_part_desc(level: ObPartitionLevel, mut row: my::Row) -> Result<Option<ObPartDesc>> {
        let part_level_prefix = match level {
            ObPartitionLevel::Two => "sub_",
            _ => "",
        };
        let part_type = PartFuncType::from_i32(
            row.take(&*format!("{}part_type", part_level_prefix))
                .unwrap(),
        );
        let part_expr: String = row
            .take(&*format!("{}part_expr", part_level_prefix))
            .unwrap();

        if part_type.is_range_part() {
            let mut range_desc = ObRangePartDesc::new();
            range_desc.set_part_func_type(part_type);
            range_desc.set_part_expr(part_expr.clone());
            range_desc.set_ordered_part_column_names(
                part_expr.split(',').map(|s| s.to_string()).collect(),
            );
            let mut types: Vec<ObjType> = Vec::new();
            let obj_type_str: String = row
                .take(&*format!("{}part_range_type", part_level_prefix))
                .unwrap();
            for v in obj_type_str.split(',') {
                types.push(ObjType::from_u8(u8::from_str_radix(v, 10)?)?);
            }
            range_desc.set_ordered_compare_column_types(types);
            return Ok(Some(ObPartDesc::ObRangePartDesc(range_desc)));
        } else if part_type.is_hash_part() {
            let mut hash_desc = ObHashPartDesc::new();
            hash_desc.set_part_func_type(part_type);
            hash_desc.set_part_expr(part_expr.clone());
            hash_desc.set_ordered_part_column_names(
                part_expr.split(',').map(|s| s.to_string()).collect(),
            );

            hash_desc.set_part_num(
                row.take(&*format!("{}part_num", part_level_prefix))
                    .unwrap(),
            );
            // in java sdk, complete_works is implemented in setPartNum()
            let mut complete_works = Vec::new();
            for i in 0..hash_desc.get_part_num() {
                complete_works.push(i as i64);
            }

            hash_desc.set_complete_works(complete_works);

            hash_desc.set_part_space(
                row.take(&*format!("{}part_space", part_level_prefix))
                    .unwrap(),
            );

            hash_desc.set_part_name_id_map(LocationUtil::build_default_part_name_id_map(
                hash_desc.get_part_num(),
            ));
            return Ok(Some(ObPartDesc::ObHashPartDesc(hash_desc)));
        } else if part_type.is_key_part() {
            let mut key_part_desc = ObKeyPartDesc::new();
            key_part_desc.set_part_func_type(part_type);
            key_part_desc.set_part_expr(part_expr.clone());
            key_part_desc.set_ordered_part_column_names(
                part_expr.split(',').map(|s| s.to_string()).collect(),
            );
            key_part_desc.set_part_num(
                row.take(&*format!("{}part_num", part_level_prefix))
                    .unwrap(),
            );
            key_part_desc.set_part_space(
                row.take(&*format!("{}part_space", part_level_prefix))
                    .unwrap(),
            );
            key_part_desc.set_part_name_id_map(LocationUtil::build_default_part_name_id_map(
                key_part_desc.get_part_num(),
            ));
            return Ok(Some(ObPartDesc::ObKeyPartDesc(key_part_desc)));
        } else {
            error!(
                "LocationUtil::build_part_desc not supported part type, type = {:?}",
                part_type
            );
        }

        Ok(None)
    }

    fn get_ordered_part_columns(
        partition_key_columns: &[Box<dyn ObColumn>],
        part_desc: &ObPartDesc,
    ) -> Vec<Box<dyn ObColumn>> {
        let mut columns: Vec<Box<dyn ObColumn>> = Vec::new();
        for part_col_name in part_desc.get_ordered_part_column_names() {
            for key_column in partition_key_columns {
                if part_col_name.eq_ignore_ascii_case(&key_column.get_column_name()) {
                    columns.push(key_column.clone());
                }
            }
        }
        columns
    }

    fn set_part_desc_property(
        part_desc: &mut Option<ObPartDesc>,
        part_columns: &[Box<dyn ObColumn>],
        list_part_columns: &[Box<dyn ObColumn>],
    ) -> Result<()> {
        if let Some(v) = part_desc {
            v.set_part_columns((*part_columns).to_vec());
            if v.is_key_part() {
                if part_columns.is_empty() {
                    error!("LocationUtil::set_part_desc_property key part desc is empty");
                    return Err(CommonErr(
                        CommonErrCode::PartitionError,
                        "LocationUtil::set_part_desc_property key part desc is empty".to_owned(),
                    ));
                }
            } else if v.is_list_part() {
                // TODO: impl list part
                unimplemented!();
            // v.set_ordered_compare_columns((*part_columns).clone());
            } else if v.is_range_part() {
                v.set_ordered_compare_columns(list_part_columns.to_vec());
            }
        }
        Ok(())
    }

    fn build_default_part_name_id_map(part_num: i32) -> HashMap<String, i64> {
        // the default partition name is 'p0,p1...'
        let mut part_name_id_map = HashMap::new();
        for i in 0..part_num {
            part_name_id_map.insert(format!("p{}", i), i as i64);
        }
        part_name_id_map
    }

    pub fn build_part_name_id_map(partition_info: &ObPartitionInfo) -> HashMap<String, i64> {
        let part_name_map1 = &partition_info.part_name_id_map;
        let mut part_name_map = HashMap::new();

        for (part_name1, part_id1) in part_name_map1 {
            if let Some(sub_part_desc) = &partition_info.sub_part_desc {
                let part_name_map2: &HashMap<String, i64> = sub_part_desc.get_part_name_id_map();
                for (part_name2, part_id2) in part_name_map2 {
                    let com_part_name = format!("{}s{}", part_name1, part_name2);
                    let part_id =
                        ObPartIdCalculator::generate_part_id(Some(*part_id1), Some(*part_id2));
                    if let Some(id) = part_id {
                        part_name_map.insert(com_part_name, id);
                    }
                }
            } else {
                part_name_map.insert(part_name1.clone(), (*part_id1) as i64);
            }
        }
        part_name_map
    }

    // TODO: impl list part
    pub fn fetch_first_part(
        conn: &mut my::PooledConn,
        table_entry: &mut TableEntry,
        ob_part_func_type: PartFuncType,
    ) -> Result<()> {
        let sql = format!(
            "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, part_name, high_bound_val
                               FROM oceanbase.__all_virtual_proxy_partition
                               WHERE table_id = {} LIMIT {};",
            table_entry.table_id,
            i32::MAX
        );
        if ob_part_func_type.is_range_part() {
            // in java sdk is parseFirstPartRange
            let bounds = LocationUtil::parse_range_part(conn, sql, table_entry, false)?;
            match &mut table_entry.partition_info {
                None => {
                    error!("LocationUtil::fetch_first_part partition_info is None");
                    return Err(CommonErr(
                        CommonErrCode::PartitionError,
                        "LocationUtil:fetch_first_part partition_info is None".to_owned(),
                    ));
                }
                Some(info) => match &mut info.first_part_desc {
                    Some(ObPartDesc::ObRangePartDesc(v)) => v.set_bounds(bounds),
                    _ => error!("LocationUtil::fetch_first_part never be here"),
                },
            }
        } else if ob_part_func_type.is_list_part() {
            // TODO: impl list part
            unimplemented!();
            //            conn.query(sql).map(|result| {
            //                result
            //                    .map(|x| x.unwrap())
            //                    .map(|mut row| {})
            //                    .collect()
            //            })?;
        }
        Ok(())
    }

    // TODO: impl list part
    pub fn fetch_sub_part(
        conn: &mut my::PooledConn,
        table_entry: &mut TableEntry,
        sub_part_func_type: PartFuncType,
    ) -> Result<()> {
        let sql = format!(
            "SELECT /*+READ_CONSISTENCY(WEAK)*/ sub_part_id, part_name, high_bound_val
                FROM oceanbase.__all_virtual_proxy_sub_partition
                WHERE table_id = {} and part_id = {} LIMIT {};",
            table_entry.table_id,
            TEMPLATE_PART_ID,
            i32::MAX
        );
        if sub_part_func_type.is_range_part() {
            // in java sdk is parseFirstPartRange
            let bounds = LocationUtil::parse_range_part(conn, sql, table_entry, true)?;
            match &mut table_entry.partition_info {
                None => {
                    error!("LocationUtil::fetch_sub_part partition_info is None");
                    return Err(CommonErr(
                        CommonErrCode::PartitionError,
                        "LocationUtil::fetch_sub_part partition_info is None".to_owned(),
                    ));
                }
                Some(info) => match &mut info.sub_part_desc {
                    Some(ObPartDesc::ObRangePartDesc(v)) => v.set_bounds(bounds),
                    _ => error!("LocationUtil::fetch_sub_part never be here"),
                },
            }
        } else if sub_part_func_type.is_list_part() {
            // TODO: impl list part
            unimplemented!();
            //            conn.query(sql).map(|result| {
            //                result
            //                    .map(|x| x.unwrap())
            //                    .map(|mut row| {})
            //                    .collect()
            //            })?;
        }
        Ok(())
    }

    fn parse_range_part(
        conn: &mut my::PooledConn,
        sql: String,
        table_entry: &mut TableEntry,
        is_sub_part: bool,
    ) -> Result<Vec<(ObPartitionKey, i64)>> {
        let part_id_column_name = if is_sub_part {
            "sub_part_id"
        } else {
            "part_id"
        };
        let mut part_desc = &mut None;
        if let Some(info) = &mut table_entry.partition_info {
            if !is_sub_part {
                part_desc = &mut info.first_part_desc;
            } else {
                part_desc = &mut info.sub_part_desc;
            }
        }

        let mut order_part_columns: &Vec<Box<dyn ObColumn>> = &Vec::new();
        let mut bounds: Vec<(ObPartitionKey, i64)> = Vec::new();
        let mut part_name_id_map: HashMap<String, i64> = HashMap::new();

        if let Some(part_desc) = part_desc {
            match part_desc {
                ObPartDesc::ObRangePartDesc(v) => {
                    order_part_columns = v.get_ordered_compare_column()
                }
                _ => error!("LocationUtil::parse_range_part never be here"),
            }
        }

        for result in conn.query(sql)? {
            result.map(|mut row| {
                let high_bound_val: String = row.take("high_bound_val").unwrap();
                let splits: Vec<String> =
                    high_bound_val.split(',').map(|s| s.to_string()).collect();
                let mut part_elements: Vec<Comparable> = Vec::new();
                for i in 0..splits.len() {
                    let element_str = LocationUtil::get_plain_string(&splits[i]);
                    if element_str.eq_ignore_ascii_case("MAXVALUE") {
                        part_elements.push(Comparable::MAXVALUE);
                    } else if element_str.eq_ignore_ascii_case("MINVALUE") {
                        part_elements.push(Comparable::MINVALUE);
                    } else {
                        part_elements.push(Comparable::Value(
                            order_part_columns[i]
                                .eval_value(&[Value::String(
                                    element_str,
                                    ObjMeta::new(
                                        order_part_columns[i].get_ob_obj_type().clone(),
                                        CollationLevel::Numeric,
                                        order_part_columns[i].get_ob_collation_type().clone(),
                                        10,
                                    ),
                                )])
                                .unwrap(),
                        ));
                    }
                }
                let ob_partition_key = ObPartitionKey::new(part_elements);
                let part_id: i64 = row.take(part_id_column_name).unwrap();
                let part_name: String = row.take("part_name").unwrap();
                bounds.push((ob_partition_key, part_id));
                part_name_id_map.insert(part_name.to_lowercase(), part_id);
            })?;
        }

        if let Some(part_desc) = part_desc {
            part_desc.set_part_name_id_map(part_name_id_map);
        }
        // TODO: check a b partition_elements
        bounds.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());
        Ok(bounds)
    }

    fn get_plain_string(s: &str) -> String {
        let start = if !s.is_empty() && s[..1] == '\''.to_string() {
            1
        } else {
            0
        };
        let end = if !s.is_empty() && s[(s.len() - 1) as usize..] == '\''.to_string() {
            s.len() - 1
        } else {
            s.len()
        };
        s[start..end].to_string()
    }
}

pub struct ObPartIdCalculator {}

impl ObPartIdCalculator {
    pub fn generate_part_id(part_id1: Option<i64>, part_id2: Option<i64>) -> Option<i64> {
        if part_id1.is_none() || part_id2.is_none() {
            return None;
        }
        if let Some(id1) = part_id1 {
            if let Some(id2) = part_id2 {
                return Some(
                    id1 << ob_part_constants::PART_ID_SHIFT | id2 | ob_part_constants::MASK,
                );
            }
        }
        None
    }
}
