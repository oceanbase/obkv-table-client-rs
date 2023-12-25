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

use std::{
    collections::HashMap,
    i32, i64,
    sync::{atomic::AtomicUsize, Arc},
    u8,
};

use chrono::Utc;
use mysql as my;
use mysql::{prelude::Queryable, PooledConn, Row};

use super::{
    ob_part_constants, part_func_type::PartFuncType, ObHashPartDesc, ObKeyPartDesc, ObPartDesc,
    ObPartitionInfo, ObPartitionLevel, ObRangePartDesc, TableEntry,
};
use crate::{
    constant::ALL_DUMMY_TABLE,
    error::{CommonErrCode, Error::Common as CommonErr, Result},
    location::{
        ObPartitionEntry, ObPartitionLocation, ObReplicaType, ObServerAddr, ObServerInfo,
        ObServerRole, ObServerStatus, ReplicaLocation, TableEntryKey, TableLocation, OB_INVALID_ID,
    },
    rpc::protocol::partition::{
        ob_column::{ObColumn, ObGeneratedColumn, ObSimpleColumn},
        ob_partition_key::{Comparable, ObPartitionKey},
    },
    serde_obkv::value::{CollationLevel, CollationType, ObjMeta, ObjType, Value},
    util::obversion::{ob_vsn_major, parse_ob_vsn_from_sql},
    ResultCodes,
};

pub const TEMPLATE_PART_ID: i32 = -1;

pub struct LocationUtil {}

impl LocationUtil {
    pub fn get_ob_version_from_server(conn: &mut my::PooledConn) -> Result<()> {
        if ob_vsn_major() == 0 {
            let sql = "SELECT /*+READ_CONSISTENCY(WEAK)*/ OB_VERSION() AS CLUSTER_VERSION";
            for row in conn.query::<Row, &str>(sql)? {
                let cluster_version = match my::from_row_opt(row) {
                    Ok(version) => version,
                    Err(e) => {
                        error!("ObTableLocation::get_table_entry_from_remote: fail to do mysql row conversion, err:{}", e);
                        return Err(CommonErr(
                            CommonErrCode::ConvertFailed,
                            format!("mysql row conversion err:{e}"),
                        ));
                    }
                };
                parse_ob_vsn_from_sql(cluster_version);
            }
        }
        Ok(())
    }

    /// getTableEntryLocationFromRemote will try get location of every
    /// partition/tablet from server
    pub fn get_table_location_from_remote(
        conn: &mut my::PooledConn,
        key: &TableEntryKey,
        table_entry: &TableEntry,
    ) -> Result<ObPartitionEntry> {
        let partition_num = table_entry.partition_num;
        let mut part_str = String::with_capacity((partition_num * 7) as usize);
        let sql: String = if ob_vsn_major() >= 4 {
            if table_entry.is_partition_table() {
                let mut idx: i64 = 0;
                match table_entry.part_tablet_id_map() {
                    Some(part_tablet_id_map) => {
                        for value in part_tablet_id_map.values() {
                            if idx > 0 {
                                part_str.push(',');
                            }
                            part_str.push_str(&format!("{value}"));
                            idx += 1;
                        }
                    }
                    None => {
                        error!("Location::get_table_location_from_remote: partition table has no part_tablet_id_map, table={:?}",
                               table_entry);
                        return Err(CommonErr(
                            CommonErrCode::PartitionError,
                            format!("Location::get_table_location_from_remote: partition table has no part_tablet_id_map, table={:?}",
                                    table_entry),
                        ));
                    }
                }
            } else {
                for idx in 0..partition_num {
                    if idx > 0 {
                        part_str.push(',');
                    }
                    part_str.push_str(&format!("{idx}"));
                }
            }
            format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ A.tablet_id as tablet_id, A.svr_ip as svr_ip, A.sql_port as sql_port,
                            A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port, B.status as status, B.stop_time as stop_time,
                            A.spare1 as replica_type FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port
                            WHERE tenant_name = '{}' and database_name= '{}' and table_name = '{}' and tablet_id in ({})",
                            &key.tenant_name,
                            &key.database_name,
                            &key.table_name,
                            &part_str,
            )
        } else {
            if table_entry.is_partition_table()
                && table_entry.partition_info.is_some()
                && table_entry
                    .partition_info
                    .as_ref()
                    .unwrap()
                    .sub_part_desc
                    .is_some()
            {
                // confirm sub part num
                let first_part_num: i64 = table_entry
                    .partition_info
                    .as_ref()
                    .and_then(|info| info.first_part_desc.as_ref())
                    .map_or(0, |desc| desc.get_part_num() as i64);
                let sub_part_num: i64 = table_entry
                    .partition_info
                    .as_ref()
                    .and_then(|info| info.sub_part_desc.as_ref())
                    .map_or(0, |desc| desc.get_part_num() as i64);
                for i in 0..first_part_num {
                    for j in 0..sub_part_num {
                        if i > 0 || j > 0 {
                            part_str.push(',');
                        }
                        let part_id =
                            ObPartIdCalculator::generate_part_id(Option::from(i), Option::from(j))
                                .unwrap();
                        part_str.push_str(&format!("{part_id}"));
                    }
                }
            } else {
                for idx in 0..partition_num {
                    if idx > 0 {
                        part_str.push(',');
                    }
                    part_str.push_str(&format!("{idx}"));
                }
            }
            format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ A.partition_id as partition_id, A.svr_ip as svr_ip, A.sql_port as sql_port,
                            A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port,
                            B.status as status, B.stop_time as stop_time, A.spare1 as replica_type FROM oceanbase.__all_virtual_proxy_schema A
                            inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port
                            WHERE tenant_name = '{}' and database_name= '{}' and table_name = '{}' and partition_id in ({})",
                            &key.tenant_name,
                            &key.database_name,
                            &key.table_name,
                            &part_str,
            )
        };

        // getPartitionLocationFromResultSet in java client
        let mut partition_location = HashMap::new();

        for row in conn.query::<Row, String>(sql)? {
            let (
                part_id,
                svr_ip,
                sql_port,
                table_id,
                role,
                replica_num,
                part_num,
                svr_port,
                status,
                stop_time,
                replica_type,
            ) = match my::from_row_opt(row) {
                Ok(tuple) => tuple,
                Err(e) => {
                    error!("ObTableLocation::get_table_location_from_remote: fail to do mysql row conversion, err:{}", e);
                    return Err(CommonErr(
                        CommonErrCode::ConvertFailed,
                        format!("mysql row conversion err:{e}"),
                    ));
                }
            };

            let partition_id: i64 = if ob_vsn_major() >= 4 {
                part_id
            } else if table_entry.is_partition_table() {
                table_entry
                    .partition_info
                    .as_ref()
                    .and_then(|partition_info| partition_info.sub_part_desc.as_ref())
                    .map_or(part_id, |sub_part_desc| {
                        ObPartIdCalculator::get_part_idx(
                            part_id,
                            sub_part_desc.get_part_num() as i64,
                        )
                    })
            } else {
                part_id
            };
            let _: i64 = table_id;
            let _: i64 = replica_num;
            let _: i64 = part_num;

            // buildReplicaLocation in java client
            let role = ObServerRole::from_int(role);
            let replica_type = ObReplicaType::from_int(replica_type);
            let status = ObServerStatus::from_string(status);
            let mut observer_addr = ObServerAddr::new();
            observer_addr.address(svr_ip);
            observer_addr.set_sql_port(sql_port);
            observer_addr.set_svr_port(svr_port);

            let observer_info = ObServerInfo { stop_time, status };
            if !observer_info.is_active() {
                warn!(
                    "ObTableLocation::get_table_location_from_remote: inactive observer found, \
                    server_info:{:?}, addr:{:?}",
                    observer_info, observer_addr
                );
                continue;
            }

            let replica = ReplicaLocation {
                addr: observer_addr.clone(),
                info: observer_info.clone(),
                role: role.clone(),
                replica_type,
            };

            if !replica.is_valid() {
                warn!(
                    "ObTableLocation::get_table_location_from_remote: inactive observer replica found, \
                    server_info:{:?}, addr:{:?}",
                    observer_info, observer_addr
                );
                continue;
            }
            // end of buildReplicaLocation in java client

            let location = partition_location
                .entry(partition_id)
                .or_insert(ObPartitionLocation {
                    leader: None,
                    followers: vec![],
                });

            match role {
                ObServerRole::Leader => location.leader = Some(replica),
                ObServerRole::Follower => location.followers.push(replica),
                _ => (),
            };
        }

        // Check partition info.
        for part_idx in 0..table_entry.partition_num {
            // get real partition id
            let part_id = if ob_vsn_major() >= 4 {
                table_entry
                    .part_tablet_id_map()
                    .and_then(|m| m.get(&part_idx).copied())
                    .unwrap_or(part_idx)
            } else {
                part_idx
            };

            let location = partition_location.get(&part_id);
            if location.is_none() {
                error!("Location::get_table_location_from_remote: partition num={} is not exists, table={:?}, locations={:?}",
                   part_id, table_entry, partition_location);
                return Err(CommonErr(
                    CommonErrCode::PartitionError,
                    format!("Location::get_table_location_from_remote: Table maybe dropped. partition num={part_id} is not exists, table={table_entry:?}, locations={partition_location:?}"),
                ));
            }

            if location.unwrap().leader.is_none() {
                error!("Location::get_table_location_from_remote: partition num={} has no leader, table={:?}, locations={:?}",
                   part_id, table_entry, partition_location);

                return Err(CommonErr(
                    CommonErrCode::PartitionError,
                    format!("Location::get_table_location_from_remote: partition num={part_id} has no leader, table={table_entry:?}, locations={partition_location:?}, maybe rebalancing"),
                ));
            }
        }

        Ok(ObPartitionEntry {
            parititon_location: partition_location,
        })
    }

    pub fn get_table_entry_from_remote_inner(
        conn: &mut PooledConn,
        key: &TableEntryKey,
    ) -> Result<TableEntry> {
        let sql: String = if ob_vsn_major() >= 4 {
            match key.table_name.clone().as_str() {
                ALL_DUMMY_TABLE => format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ A.tablet_id as tablet_id, A.svr_ip as svr_ip, A.sql_port as sql_port,
                                                A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port,
                                                B.status as status, B.stop_time as stop_time, A.spare1 as replica_type FROM oceanbase.__all_virtual_proxy_schema A
                                                inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port WHERE tenant_name = '{}'
                                                and database_name= '{}' and table_name = '{}'",
                                                    &key.tenant_name,
                                                    &key.database_name,
                                                    &key.table_name),
                _ => format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ A.tablet_id as tablet_id, A.svr_ip as svr_ip, A.sql_port as sql_port,
                            A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port,
                            B.status as status, B.stop_time as stop_time, A.spare1 as replica_typ FROM oceanbase.__all_virtual_proxy_schema A
                            inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port WHERE tenant_name = '{}'
                            and database_name= '{}' and table_name = '{}' and tablet_id = 0",
                                &key.tenant_name,
                                &key.database_name,
                                &key.table_name),
            }
        } else {
            match key.table_name.clone().as_str() {
                ALL_DUMMY_TABLE => format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ A.partition_id as partition_id, A.svr_ip as svr_ip, A.sql_port as sql_port,
                                                A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port,
                                                B.status as status, B.stop_time as stop_time, A.spare1 as replica_type FROM oceanbase.__all_virtual_proxy_schema A
                                                inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port
                                                WHERE tenant_name = '{}' and database_name='{}' and table_name ='{}'",
                                                   &key.tenant_name,
                                                   &key.database_name,
                                                   &key.table_name),
                _ => format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ A.partition_id as partition_id, A.svr_ip as svr_ip, A.sql_port as sql_port,
                            A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port,
                            B.status as status, B.stop_time as stop_time, A.spare1 as replica_type FROM oceanbase.__all_virtual_proxy_schema A
                            inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port WHERE tenant_name = '{}'
                            and database_name= '{}' and table_name = '{}' and partition_id = 0",
                                &key.tenant_name,
                                &key.database_name,
                                &key.table_name),
            }
        };

        //  get table entry from result set
        let mut table_entry: TableEntry;
        match LocationUtil::get_table_entry_from_conn(conn, sql, key) {
            Ok(result_entry) => table_entry = result_entry,
            Err(e) => {
                return Err(e);
            }
        }

        // fetch partition info
        if table_entry.is_partition_table() {
            // fetch partition info
            match LocationUtil::fetch_partition_info(conn, &table_entry) {
                Ok(v) => table_entry.partition_info = Some(v),
                Err(e) => {
                    table_entry.partition_info = None;
                    error!(
                        "Location::get_table_entry_from_remote_inner fetch_partition_info error:{}",
                        e
                    );
                    return Err(CommonErr(
                        CommonErrCode::PartitionError,
                        format!(
                            "location::get_table_entry_from_remote_inner fetch_partition_info error:{e}",
                        ),
                    ));
                }
            }
            if let Some(v) = table_entry.partition_info.clone() {
                // fetch first part
                if let Some(first_part_desc) = &v.first_part_desc {
                    let ob_part_func_type = first_part_desc.get_part_func_type();
                    LocationUtil::fetch_first_part(conn, &mut table_entry, ob_part_func_type)?;
                }
                // fetch sub part
                if let Some(sub_part_desc) = &v.sub_part_desc {
                    let ob_part_func_type = sub_part_desc.get_part_func_type();
                    LocationUtil::fetch_sub_part(conn, &mut table_entry, ob_part_func_type)?;
                }
            }
            if let Some(v) = &mut table_entry.partition_info {
                v.part_name_id_map = LocationUtil::build_part_name_id_map(v);
            }
        }

        let part_entry = LocationUtil::get_table_location_from_remote(conn, key, &table_entry)?;
        table_entry.partition_entry = Some(part_entry);
        table_entry.set_refresh_time_mills(Utc::now().timestamp_millis());

        Ok(table_entry)
    }

    pub fn get_table_entry_from_conn(
        conn: &mut my::PooledConn,
        sql: String,
        key: &TableEntryKey,
    ) -> Result<TableEntry> {
        let mut table_id = OB_INVALID_ID;
        let mut replica_num = OB_INVALID_ID;
        let mut partition_num = OB_INVALID_ID;
        let mut replica_locations = Vec::new();

        for row in conn.query::<Row, String>(sql.clone())? {
            let (
                partition_id,
                svr_ip,
                sql_port,
                tbl_id,
                role,
                repl_num,
                part_num,
                svr_port,
                status,
                stop_time,
                replica_type,
            ) = match my::from_row_opt(row) {
                Ok(tuple) => tuple,
                Err(e) => {
                    error!("ObTableLocation::get_table_entry_from_conn: fail to do mysql row conversion, err:{}", e);
                    return Err(CommonErr(
                        CommonErrCode::ConvertFailed,
                        format!("mysql row conversion err:{e}"),
                    ));
                }
            };
            // for compile (type inference)
            let _part_id: i64 = partition_id;
            // for table_entry
            table_id = tbl_id;
            replica_num = repl_num;
            partition_num = part_num;
            // build  replica location
            let role: ObServerRole = ObServerRole::from_int(role);
            let status: ObServerStatus = ObServerStatus::from_string(status);
            let mut observer_addr = ObServerAddr::new();
            let replica_type = ObReplicaType::from_int(replica_type);

            observer_addr.address(svr_ip);
            observer_addr.set_sql_port(sql_port);
            observer_addr.set_svr_port(svr_port);

            let observer_info = ObServerInfo { stop_time, status };
            let replica_location = ReplicaLocation {
                addr: observer_addr.clone(),
                info: observer_info.clone(),
                role,
                replica_type,
            };
            if !replica_location.is_valid() {
                warn!(
                    "ObTableLocation::get_table_entry_from_remote: inactive observer found, \
                     server_info:{:?}, addr:{:?}",
                    observer_info, observer_addr
                );
                continue;
            }

            replica_locations.push(replica_location);
        }

        if replica_locations.is_empty() {
            if key.table_name == ALL_DUMMY_TABLE {
                return Err(CommonErr(
                    CommonErrCode::InvalidParam,
                    format!("Failed to collect dummy data from the server: Possible incorrect user credentials. tenant:{}, cluster:{}, database:{}", key.tenant_name, key.cluster_name, key.database_name),
                ));
            }

            return Err(CommonErr(
                CommonErrCode::ObException(ResultCodes::OB_ERR_UNKNOWN_TABLE),
                format!("table not found:{}", key.table_name),
            ));
        }
        let table_location = TableLocation { replica_locations };

        Ok(TableEntry {
            table_id,
            partition_num,
            replica_num,
            table_entry_key: key.clone(),
            refresh_time_mills: Arc::new(AtomicUsize::new(0)),
            partition_info: None,
            table_location,
            partition_entry: None,
            row_key_element: HashMap::new(),
        })
    }

    pub fn fetch_partition_info(
        conn: &mut PooledConn,
        table_entry: &TableEntry,
    ) -> Result<ObPartitionInfo> {
        let sql: String = if ob_vsn_major() >= 4 {
            format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ part_level, part_num, part_type, part_space, part_expr,
                part_range_type, sub_part_num, sub_part_type, sub_part_space, sub_part_range_type, sub_part_expr,
                part_key_name, part_key_type, part_key_idx, part_key_extra, part_key_collation_type
                FROM oceanbase.__all_virtual_proxy_partition_info WHERE tenant_name = '{}' and table_id = {}
                group by part_key_name order by part_key_name LIMIT {};",
                    table_entry.table_entry_key.tenant_name,
                    table_entry.table_id,
                    i64::MAX)
        } else {
            format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ part_level, part_num, part_type, part_space, part_expr,
                part_range_type, part_interval_bin, interval_start_bin, sub_part_num, sub_part_type, sub_part_space, sub_part_range_type,
                def_sub_part_interval_bin, def_sub_interval_start_bin, sub_part_expr, part_key_name, part_key_type, part_key_idx,
                part_key_extra, spare1 FROM oceanbase.__all_virtual_proxy_partition_info
                WHERE table_id = {} GROUP BY part_key_name ORDER BY part_key_name LIMIT {};",
                      table_entry.table_id,
                      i64::MAX)
        };

        let mut info = ObPartitionInfo::new();
        // in java sdk is parsePartitionInfo()
        let mut is_first_row = true;
        for mut row in conn.query::<Row, String>(sql)? {
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
                            warn!("LocationUtil:fetch_partition_info ObPartitionLevel::One build_part_desc,err: {:?}", e);
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
                    let part_desc: Option<ObPartDesc> = match LocationUtil::build_part_desc(
                        ObPartitionLevel::Two,
                        row.clone(),
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("LocationUtil:fetch_partition_info ObPartitionLevel::Two build_part_desc,err: {:?}", e);
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
            let collation_type_label: u8 = if ob_vsn_major() >= 4 {
                row.take("part_key_collation_type").unwrap()
            } else {
                row.take("spare1").unwrap()
            };

            // get part key for each loop
            if !part_key_extra.is_empty() {
                // TODO: new ObGeneratedColumnExpressParser(getPlainString(partKeyExtra)).
                // parse());
                let column = ObGeneratedColumn::new(
                    part_key_name,
                    part_key_idx,
                    ObjType::from_u8(part_key_type)
                        .expect("LocationUtil::fetch_partition_info fail to decode obj type"),
                    CollationType::from_u8(collation_type_label)
                        .expect("LocationUtil::fetch_partition_info fail to decode collation type"),
                );
                info.part_columns.push(Box::new(column));
                unimplemented!();
            } else {
                let column = ObSimpleColumn::new(
                    part_key_name,
                    part_key_idx,
                    ObjType::from_u8(part_key_type)
                        .expect("LocationUtil::fetch_partition_info fail to decode obj type"),
                    CollationType::from_u8(collation_type_label)
                        .expect("LocationUtil::fetch_partition_info fail to decode collation type"),
                );

                info.part_columns.push(Box::new(column));
            }
        }

        // get list partition column types here
        let mut first_ordered_parted_columns = Vec::new();
        match &info.first_part_desc {
            None => (),
            Some(first_part_desc) => {
                if first_part_desc.is_list_part() || first_part_desc.is_range_part() {
                    first_ordered_parted_columns =
                        LocationUtil::get_ordered_part_columns(&info.part_columns, first_part_desc);
                }
            }
        }
        let mut sub_ordered_parted_columns = Vec::new();
        if let Some(first_part_desc) = &info.first_part_desc {
            match &info.sub_part_desc {
                None => (),
                Some(sub_part_desc) => {
                    if sub_part_desc.is_list_part() || first_part_desc.is_range_part() {
                        sub_ordered_parted_columns = LocationUtil::get_ordered_part_columns(
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
            &first_ordered_parted_columns,
        )?;
        LocationUtil::set_part_desc_property(
            &mut info.sub_part_desc,
            &info.part_columns,
            &sub_ordered_parted_columns,
        )?;

        Ok(info)
    }

    fn build_part_desc(level: ObPartitionLevel, mut row: my::Row) -> Result<Option<ObPartDesc>> {
        let part_level_prefix = match level {
            ObPartitionLevel::Two => "sub_",
            _ => "",
        };
        let part_type =
            PartFuncType::from_i32(row.take(&*format!("{part_level_prefix}part_type")).unwrap());
        let mut part_expr: String = row.take(&*format!("{part_level_prefix}part_expr")).unwrap();
        part_expr = part_expr.replace('`', "");
        part_expr.retain(|c| !c.is_whitespace());

        if part_type.is_range_part() {
            let mut range_desc = ObRangePartDesc::new();
            range_desc.set_part_func_type(part_type);
            range_desc.set_part_expr(part_expr.to_string());
            range_desc.set_ordered_part_column_names(
                part_expr.split(',').map(|s| s.to_string()).collect(),
            );
            let mut types: Vec<ObjType> = Vec::new();
            let obj_type_str: String = row
                .take(&*format!("{part_level_prefix}part_range_type"))
                .unwrap();
            for v in obj_type_str.split(',') {
                types.push(ObjType::from_u8(v.parse::<u8>()?)?);
            }
            range_desc.set_ordered_compare_column_types(types);
            return Ok(Some(ObPartDesc::Range(range_desc)));
        } else if part_type.is_hash_part() {
            let mut hash_desc = ObHashPartDesc::new();
            hash_desc.set_part_func_type(part_type);
            hash_desc.set_part_expr(part_expr.to_string());
            hash_desc.set_ordered_part_column_names(
                part_expr.split(',').map(|s| s.to_string()).collect(),
            );

            hash_desc.set_part_num(row.take(&*format!("{part_level_prefix}part_num")).unwrap());
            // in java sdk, complete_works is implemented in setPartNum()
            let mut complete_works = Vec::new();
            for i in 0..hash_desc.get_part_num() {
                complete_works.push(i as i64);
            }

            hash_desc.set_complete_works(complete_works);

            hash_desc.set_part_space(
                row.take(&*format!("{part_level_prefix}part_space"))
                    .unwrap(),
            );
            if ob_vsn_major() < 4 {
                hash_desc.set_part_name_id_map(LocationUtil::build_default_part_name_id_map(
                    hash_desc.get_part_num(),
                ));
            }
            return Ok(Some(ObPartDesc::Hash(hash_desc)));
        } else if part_type.is_key_part() {
            let mut key_part_desc = ObKeyPartDesc::new();
            key_part_desc.set_part_func_type(part_type);
            key_part_desc.set_part_expr(part_expr.to_string());
            key_part_desc.set_ordered_part_column_names(
                part_expr.split(',').map(|s| s.to_string()).collect(),
            );
            key_part_desc.set_part_num(row.take(&*format!("{part_level_prefix}part_num")).unwrap());
            key_part_desc.set_part_space(
                row.take(&*format!("{part_level_prefix}part_space"))
                    .unwrap(),
            );

            if ob_vsn_major() < 4 {
                key_part_desc.set_part_name_id_map(LocationUtil::build_default_part_name_id_map(
                    key_part_desc.get_part_num(),
                ));
            }
            return Ok(Some(ObPartDesc::Key(key_part_desc)));
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
            part_name_id_map.insert(format!("p{i}"), i as i64);
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
                    let com_part_name = format!("{part_name1}s{part_name2}");
                    let part_id =
                        ObPartIdCalculator::generate_part_id(Some(*part_id1), Some(*part_id2));
                    if let Some(id) = part_id {
                        part_name_map.insert(com_part_name, id);
                    }
                }
            } else {
                part_name_map.insert(part_name1.clone(), *part_id1);
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
        let sql: String = if ob_vsn_major() >= 4 {
            format!(
                "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, part_name, tablet_id, high_bound_val, sub_part_num
                    FROM oceanbase.__all_virtual_proxy_partition WHERE tenant_name = '{}' and table_id = {} LIMIT {};",
                table_entry.table_entry_key.tenant_name,
                table_entry.table_id,
                i32::MAX
            )
        } else {
            format!(
                "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, part_name, high_bound_val
                    FROM oceanbase.__all_virtual_proxy_partition WHERE table_id = {} LIMIT {};",
                table_entry.table_id,
                i32::MAX
            )
        };
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
                    Some(ObPartDesc::Range(v)) => v.set_bounds(bounds),
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
        } else if ob_vsn_major() >= 4
            && (ob_part_func_type.is_hash_part() || ob_part_func_type.is_key_part())
        {
            // Parse the the first partition information of Key/Hash func
            match &mut table_entry.partition_info {
                None => {
                    // dont have a partition info
                    error!("LocationUtil::fetch_first_part partition_info is None");
                    return Err(CommonErr(
                        CommonErrCode::PartitionError,
                        "LocationUtil:fetch_first_part partition_info is None".to_owned(),
                    ));
                }
                Some(_) => match LocationUtil::parse_key_hash_part(conn, sql, table_entry, false) {
                    Ok(v) => {
                        let info = table_entry.partition_info.as_mut().unwrap();
                        info.set_tablet_id_map(v)
                    }
                    Err(e) => {
                        error!(
                            "LocationUtil::fetch_first_part parse_key_hash_part error:{}",
                            e
                        );
                        return Err(CommonErr(
                            CommonErrCode::PartitionError,
                            format!(
                                "LocationUtil::fetch_first_part parse_key_hash_part error:{}",
                                e
                            ),
                        ));
                    }
                },
            }
        }
        Ok(())
    }

    // TODO: impl list part
    pub fn fetch_sub_part(
        conn: &mut my::PooledConn,
        table_entry: &mut TableEntry,
        sub_part_func_type: PartFuncType,
    ) -> Result<()> {
        let sql: String = if ob_vsn_major() >= 4 {
            format!(
                "SELECT /*+READ_CONSISTENCY(WEAK)*/ sub_part_id, part_name, tablet_id, high_bound_val
                FROM oceanbase.__all_virtual_proxy_sub_partition WHERE tenant_name = '{}' and table_id = {} LIMIT {};",
                table_entry.table_entry_key.tenant_name,
                table_entry.table_id,
                i32::MAX,
            )
        } else {
            format!(
                "SELECT /*+READ_CONSISTENCY(WEAK)*/ sub_part_id, part_name, high_bound_val
                FROM oceanbase.__all_virtual_proxy_sub_partition
                WHERE table_id = {} and part_id = {} LIMIT {};",
                table_entry.table_id,
                TEMPLATE_PART_ID,
                i32::MAX,
            )
        };

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
                    Some(ObPartDesc::Range(v)) => v.set_bounds(bounds),
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
        } else if ob_vsn_major() >= 4
            && (sub_part_func_type.is_hash_part() || sub_part_func_type.is_key_part())
        {
            // parseSubPartKeyHash
            match &mut table_entry.partition_info {
                None => {
                    // dont have a partition info
                    error!("LocationUtil::fetch_first_part partition_info is None");
                    return Err(CommonErr(
                        CommonErrCode::PartitionError,
                        "LocationUtil:fetch_first_part partition_info is None".to_owned(),
                    ));
                }
                Some(_) => match LocationUtil::parse_key_hash_part(conn, sql, table_entry, true) {
                    Ok(v) => {
                        let info = table_entry.partition_info.as_mut().unwrap();
                        info.set_tablet_id_map(v)
                    }
                    Err(e) => {
                        error!(
                            "LocationUtil::fetch_first_part parse_key_hash_part error:{}",
                            e
                        );
                        return Err(CommonErr(
                            CommonErrCode::PartitionError,
                            format!(
                                "LocationUtil::fetch_first_part parse_key_hash_part error:{}",
                                e
                            ),
                        ));
                    }
                },
            }
        }
        Ok(())
    }

    /*
     * parse_key_hash_part parse tablet_id_map from sql result and return
     */
    fn parse_key_hash_part(
        conn: &mut my::PooledConn,
        sql: String,
        table_entry: &mut TableEntry,
        is_sub_part: bool,
    ) -> Result<HashMap<i64, i64>> {
        let mut part_tablet_id_map: HashMap<i64, i64> = HashMap::new();
        for (idx, mut row) in (0_i64..).zip(conn.query::<Row, String>(sql)?.into_iter()) {
            // set sub part num into first part desc, consider refactor
            match table_entry.partition_info {
                None => {
                    error!("LocationUtil::parse_key_hash_part partition_info is None");
                    return Err(CommonErr(
                        CommonErrCode::PartitionError,
                        "LocationUtil::parse_key_hash_part partition_info is None".to_owned(),
                    ));
                }
                Some(ref mut info) => {
                    if let Some(ref mut sub_part_desc) = info.sub_part_desc {
                        if !is_sub_part && sub_part_desc.get_part_num() == 0 {
                            match sub_part_desc {
                                ObPartDesc::Hash(v) => {
                                    v.set_part_num(row.take("sub_part_num").unwrap());
                                }
                                ObPartDesc::Key(v) => {
                                    v.set_part_num(row.take("sub_part_num").unwrap());
                                }
                                _ => warn!("LocationUtil::parse_key_hash_part never be here"),
                            }
                        }
                    }
                }
            }
            part_tablet_id_map.insert(idx, row.take("tablet_id").unwrap());
        }
        Ok(part_tablet_id_map)
    }

    fn parse_range_part(
        conn: &mut my::PooledConn,
        sql: String,
        table_entry: &mut TableEntry,
        is_sub_part: bool,
    ) -> Result<Vec<(ObPartitionKey, i64)>> {
        let mut sub_part_num = -1;
        let part_id_column_name = if is_sub_part {
            "sub_part_id"
        } else {
            "part_id"
        };

        // get partition info
        let info = match table_entry.partition_info {
            None => {
                error!("LocationUtil::parse_range_part partition_info is None");
                return Err(CommonErr(
                    CommonErrCode::PartitionError,
                    "LocationUtil::parse_range_part partition_info is None".to_owned(),
                ));
            }
            Some(ref mut info) => info,
        };

        let mut order_part_columns: &Vec<Box<dyn ObColumn>> = &Vec::new();
        let mut bounds: Vec<(ObPartitionKey, i64)> = Vec::new();
        let mut part_name_id_map: HashMap<String, i64> = HashMap::new();
        let mut part_tablet_id_map: HashMap<i64, i64> = HashMap::new();
        let mut idx: i64 = 0;

        if is_sub_part {
            match &info.sub_part_desc {
                Some(ObPartDesc::Range(v)) => order_part_columns = v.get_ordered_compare_column(),
                _ => error!("LocationUtil::parse_range_part never be here"),
            }
        } else {
            match &info.first_part_desc {
                Some(ObPartDesc::Range(v)) => order_part_columns = v.get_ordered_compare_column(),
                _ => error!("LocationUtil::parse_range_part never be here"),
            }
        }

        for mut row in conn.query::<Row, String>(sql)? {
            // client only support template partition table
            // set sub part num into first part desc
            if let Some(sub_part_desc) = &info.sub_part_desc {
                if !is_sub_part && sub_part_desc.get_part_num() == 0 {
                    match sub_part_desc {
                        ObPartDesc::Range(_) => {
                            sub_part_num = row.take("sub_part_num").unwrap();
                        }
                        _ => error!("LocationUtil::parse_range_part never be here"),
                    }
                }
            }

            let high_bound_val: String = row.take("high_bound_val").unwrap();
            let splits: Vec<String> = high_bound_val.split(',').map(|s| s.to_string()).collect();
            let mut part_elements: Vec<Comparable> = Vec::new();
            for i in 0..splits.len() {
                let element_str = LocationUtil::get_plain_string(&splits[i]);
                if element_str.eq_ignore_ascii_case("MAXVALUE") {
                    part_elements.push(Comparable::MaxValue);
                } else if element_str.eq_ignore_ascii_case("MINVALUE") {
                    part_elements.push(Comparable::MinValue);
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
            if ob_vsn_major() >= 4 {
                let tablet_id: i64 = row.take("tablet_id").unwrap();
                bounds.push((ob_partition_key, idx));
                part_tablet_id_map.insert(idx, tablet_id);
                idx += 1;
            } else {
                let part_id: i64 = row.take(part_id_column_name).unwrap();
                let part_name: String = row.take("part_name").unwrap();
                bounds.push((ob_partition_key, part_id));
                part_name_id_map.insert(part_name.to_lowercase(), part_id);
            }
        }

        if ob_vsn_major() >= 4 {
            info.set_tablet_id_map(part_tablet_id_map);
        } else if is_sub_part {
            // 3.x sub part
            match info.sub_part_desc {
                Some(ref mut sub_part_desc) => {
                    sub_part_desc.set_part_name_id_map(part_name_id_map);
                }
                _ => error!("LocationUtil::parse_range_part never be here"),
            }
        } else {
            // 3.x
            match info.first_part_desc {
                Some(ref mut first_part_desc) => {
                    first_part_desc.set_part_name_id_map(part_name_id_map);
                }
                _ => error!("LocationUtil::parse_range_part never be here"),
            }
        }

        // set sub_part_num
        // since we could not set sub_part_num into info in the loop
        match table_entry.partition_info {
            None => {
                error!("LocationUtil::parse_range_part partition_info is None");
                return Err(CommonErr(
                    CommonErrCode::PartitionError,
                    "LocationUtil::parse_range_part partition_info is None".to_owned(),
                ));
            }
            Some(ref mut info) => {
                if let Some(ref mut sub_part_desc) = info.sub_part_desc {
                    if !is_sub_part && sub_part_desc.get_part_num() == 0 {
                        match sub_part_desc {
                            ObPartDesc::Range(v) => {
                                v.set_part_num(sub_part_num);
                            }
                            _ => error!("LocationUtil::parse_range_part never be here"),
                        }
                    }
                }
            }
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
        let end = if !s.is_empty() && s[(s.len() - 1)..] == '\''.to_string() {
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
        Some(ob_part_constants::generate_phy_part_id(
            part_id1?, part_id2?,
        ))
    }

    pub fn get_part_idx(part_id: i64, sub_part_num: i64) -> i64 {
        ob_part_constants::extract_part_idx(part_id) * sub_part_num
            + ob_part_constants::extract_subpart_idx(part_id)
    }
}
