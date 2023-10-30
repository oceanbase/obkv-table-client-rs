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
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    str::FromStr,
    sync::{
        atomic::{AtomicIsize, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use mysql as my;
use mysql::{prelude::Queryable, PoolConstraints, PoolOpts, Row};
use rand::{seq::SliceRandom, thread_rng};

use self::ob_part_desc::{ObHashPartDesc, ObKeyPartDesc, ObPartDesc, ObRangePartDesc};
use crate::{
    client::{table_client::ServerRoster, ClientConfig},
    constant::*,
    error::{CommonErrCode, Error::Common as CommonErr, Result},
    rpc::protocol::{codes::ResultCodes, partition::ob_column::ObColumn},
    util as u,
    util::HandyRwLock,
};

pub mod ob_part_constants;
mod ob_part_desc;
mod part_func_type;
mod util;

pub const OB_INVALID_ID: i64 = -1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObServerAddr {
    ip: String,
    sql_port: i32,
    svr_port: i32,
    #[serde(skip)]
    priority: Arc<AtomicIsize>,
    #[serde(skip)]
    grant_priority_times: Arc<AtomicUsize>,
}

impl PartialEq for ObServerAddr {
    fn eq(&self, other: &ObServerAddr) -> bool {
        self.ip() == other.ip()
            && self.sql_port() == other.sql_port()
            && self.svr_port() == other.svr_port()
    }
}

impl Hash for ObServerAddr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ip.hash(state);
        self.sql_port.hash(state);
        self.svr_port.hash(state);
    }
}

impl Eq for ObServerAddr {}

impl Default for ObServerAddr {
    fn default() -> ObServerAddr {
        ObServerAddr::new()
    }
}

impl ObServerAddr {
    pub fn new() -> Self {
        Self {
            ip: "".to_owned(),
            sql_port: 0,
            svr_port: 0,
            priority: Arc::new(AtomicIsize::new(0)),
            grant_priority_times: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn decrement_priority_and_get(&self, v: isize) -> isize {
        self.priority.fetch_sub(v, Ordering::SeqCst) - v
    }

    pub fn ip(&self) -> &str {
        &self.ip
    }

    pub fn sql_port(&self) -> i32 {
        self.sql_port
    }

    pub fn set_sql_port(&mut self, port: i32) {
        self.sql_port = port
    }

    pub fn svr_port(&self) -> i32 {
        self.svr_port
    }

    pub fn set_svr_port(&mut self, port: i32) {
        self.svr_port = port
    }

    pub fn grant_priority_times(&self) -> usize {
        self.grant_priority_times.load(Ordering::Acquire)
    }

    pub fn set_grant_priority_times(&self, ts: usize) {
        self.grant_priority_times.store(ts, Ordering::Release);
    }

    pub fn priority(&self) -> isize {
        self.priority.load(Ordering::Acquire)
    }

    pub fn set_priority(&self, priority: isize) {
        self.priority.store(priority, Ordering::Release);
    }

    pub fn address(&mut self, addr: String) {
        if addr.find(':').is_some() {
            let tmps: Vec<&str> = addr.split(':').collect();
            assert!(tmps.len() >= 2);
            self.ip = tmps.first().unwrap().to_owned().to_owned();
            self.svr_port = i32::from_str(tmps[1]).unwrap();
        } else {
            self.ip = addr;
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ObServerStatus {
    Active,
    Inactive,
    Deleting,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ObPartitionLevel {
    Zero,
    One,
    Two,
    Unknown,
}

impl ObPartitionLevel {
    pub fn from_int(i: i32) -> ObPartitionLevel {
        match i {
            0 => ObPartitionLevel::Zero,
            1 => ObPartitionLevel::One,
            2 => ObPartitionLevel::Two,
            _ => ObPartitionLevel::Unknown,
        }
    }

    pub fn get_index(&self) -> i32 {
        match self {
            ObPartitionLevel::Zero => 0,
            ObPartitionLevel::One => 1,
            ObPartitionLevel::Two => 2,
            ObPartitionLevel::Unknown => 3,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ObServerRole {
    InvalidRole,
    Leader,
    Follower,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObServerInfo {
    stop_time: i64,
    status: ObServerStatus,
}

impl ObServerInfo {
    pub fn is_active(&self) -> bool {
        self.stop_time == 0 && self.status == ObServerStatus::Active
    }

    pub fn status(&self) -> ObServerStatus {
        self.status.clone()
    }

    pub fn stop_time(&self) -> i64 {
        self.stop_time
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableEntryKey {
    cluster_name: String,
    tenant_name: String,
    database_name: String,
    table_name: String,
}

#[derive(Debug, Clone)]
pub struct TableEntry {
    table_id: i64,
    partition_num: i64,
    refresh_time_mills: Arc<AtomicUsize>,
    partition_info: Option<ObPartitionInfo>,
    //table entry location
    table_location: TableLocation,
    partition_entry: Option<ObPartitionEntry>,
    row_key_element: HashMap<String, i32>,
}

impl TableEntryKey {
    pub fn new(
        cluster_name: &str,
        tenant_name: &str,
        database_name: &str,
        table_name: &str,
    ) -> TableEntryKey {
        TableEntryKey {
            cluster_name: cluster_name.to_owned(),
            tenant_name: tenant_name.to_owned(),
            database_name: database_name.to_owned(),
            table_name: table_name.to_owned(),
        }
    }

    pub fn new_root_server_key(cluster_name: &str, tenant_name: &str) -> Self {
        Self::new(
            cluster_name,
            tenant_name,
            OCEANBASE_DATABASE,
            ALL_DUMMY_TABLE,
        )
    }
}

#[derive(Clone, Debug)]
pub struct ObPartitionInfo {
    level: ObPartitionLevel,
    first_part_desc: Option<ObPartDesc>,
    sub_part_desc: Option<ObPartDesc>,
    part_columns: Vec<Box<dyn ObColumn>>,
    row_key_element: HashMap<String, i32>,
    part_name_id_map: HashMap<String, i64>,
}

impl Default for ObPartitionInfo {
    fn default() -> ObPartitionInfo {
        ObPartitionInfo::new()
    }
}

impl ObPartitionInfo {
    pub fn new() -> Self {
        Self {
            level: ObPartitionLevel::Zero,
            first_part_desc: None,
            sub_part_desc: None,
            part_columns: Vec::new(),
            row_key_element: HashMap::<String, i32>::new(),
            part_name_id_map: HashMap::<String, i64>::new(),
        }
    }

    pub fn set_row_key_element(&mut self, row_key_element: HashMap<String, i32>) {
        if let Some(ref mut part_desc) = self.first_part_desc {
            part_desc.set_row_key_element(row_key_element.clone());
        }
        if let Some(ref mut part_desc) = self.sub_part_desc {
            part_desc.set_row_key_element(row_key_element.clone());
        }

        self.row_key_element = row_key_element;
    }

    pub fn level(&self) -> ObPartitionLevel {
        self.level.clone()
    }

    pub fn first_part_desc(&self) -> &Option<ObPartDesc> {
        &self.first_part_desc
    }

    pub fn sub_part_desc(&self) -> &Option<ObPartDesc> {
        &self.sub_part_desc
    }

    pub fn prepare(&mut self) -> Result<()> {
        match self.level {
            ObPartitionLevel::One => match &mut self.first_part_desc {
                None => {
                    error!("ObPartitionInfo::prepare firstPartDesc can not be null when level above level one");
                    Err(CommonErr(
                        CommonErrCode::PartitionError,
                        "ObPartitionInfo::prepare firstPartDesc can not be null when level above level one".to_owned(),
                    ))
                }
                Some(v) => v.prepare(),
            },
            ObPartitionLevel::Two => {
                match &mut self.first_part_desc {
                    None => {
                        error!("ObPartitionInfo::prepare firstPartDesc can not be null when level above level two");
                        return Err(CommonErr(
                            CommonErrCode::PartitionError,
                            "ObPartitionInfo::prepare firstPartDesc can not be null when level above level two".to_owned(),
                        ));
                    }
                    Some(v) => v.prepare()?,
                }
                match &mut self.sub_part_desc {
                    None => {
                        error!("ObPartitionInfo::prepare subPartDesc can not be null when level above level two");
                        Err(CommonErr(
                            CommonErrCode::PartitionError,
                            "ObPartitionInfo::prepare subPartDesc can not be null when level above level two".to_owned(),
                        ))
                    }
                    Some(v) => v.prepare(),
                }
            }
            ObPartitionLevel::Unknown => {
                error!("ObPartitionInfo::prepare ObPartitionLevel is unknown");
                Err(CommonErr(
                    CommonErrCode::PartitionError,
                    "ObPartitionInfo::prepare ObPartitionLevel is unknown".to_owned(),
                ))
            }
            ObPartitionLevel::Zero => {
                error!("ObPartitionInfo::prepare ObPartitionLevel is zero");
                Err(CommonErr(
                    CommonErrCode::PartitionError,
                    "ObPartitionInfo::prepare ObPartitionLevel is zero".to_owned(),
                ))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableLocation {
    replica_locations: Vec<ReplicaLocation>,
}

impl TableLocation {
    pub fn replica_locations(&self) -> &Vec<ReplicaLocation> {
        &self.replica_locations
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObPartitionEntry {
    parititon_location: HashMap<i64 /* partition number */, ObPartitionLocation>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObPartitionLocation {
    leader: Option<ReplicaLocation>,
    followers: Vec<ReplicaLocation>,
}

impl ObPartitionLocation {
    pub fn leader(&self) -> &Option<ReplicaLocation> {
        &self.leader
    }
}

impl ObPartitionEntry {
    pub fn get_partition_location_with_part_id(
        &self,
        part_id: i64,
    ) -> Option<&ObPartitionLocation> {
        // logic_id = part_id in partition one
        self.parititon_location.get(&part_id)
    }

    pub fn get_sub_partition_location_with_part_id(
        &self,
        part_id: i64,
        sub_part_nums: i32,
    ) -> Option<&ObPartitionLocation> {
        // only for template sub partition
        let logic_id = ob_part_constants::extract_part_idx(part_id) * sub_part_nums as i64
            + ob_part_constants::extract_subpart_idx(part_id);
        self.parititon_location.get(&logic_id)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReplicaLocation {
    addr: ObServerAddr,
    info: ObServerInfo,
    role: ObServerRole,
}

impl ReplicaLocation {
    pub fn addr(&self) -> &ObServerAddr {
        &self.addr
    }

    pub fn info(&self) -> &ObServerInfo {
        &self.info
    }
}

impl ObServerRole {
    pub fn from_int(i: i32) -> ObServerRole {
        match i {
            0 => ObServerRole::InvalidRole,
            1 => ObServerRole::Leader,
            2 => ObServerRole::Follower,
            _ => panic!("Invalid index"),
        }
    }
}

impl ObServerStatus {
    pub fn from_string(s: String) -> ObServerStatus {
        match s.as_ref() {
            "active" => ObServerStatus::Active,
            "inactive" => ObServerStatus::Inactive,
            "deleting" => ObServerStatus::Deleting,
            _ => panic!("Invalid status"),
        }
    }
}

impl TableEntry {
    pub fn set_row_key_element(&mut self, row_key_element: HashMap<String, i32>) {
        self.row_key_element = row_key_element.clone();
        if let Some(ref mut partition_info) = self.partition_info {
            partition_info.set_row_key_element(row_key_element);
        }
    }

    pub fn is_partition_table(&self) -> bool {
        self.partition_num > 1
    }

    pub fn partition_entry(&self) -> &Option<ObPartitionEntry> {
        &self.partition_entry
    }

    pub fn partition_info(&self) -> &Option<ObPartitionInfo> {
        &self.partition_info
    }

    pub fn table_location(&self) -> &TableLocation {
        &self.table_location
    }

    pub fn refresh_time_mills(&self) -> i64 {
        self.refresh_time_mills.load(Ordering::Acquire) as i64
    }

    pub fn set_refresh_time_mills(&self, v: i64) {
        self.refresh_time_mills.store(v as usize, Ordering::Release);
    }

    /// Returns true when the partition info'level is expected.
    pub fn is_partition_level(&self, level: ObPartitionLevel) -> bool {
        match self.partition_info {
            Some(ref info) => info.level() == level,
            None => false,
        }
    }

    pub fn get_partition_location_with_part_id(
        &self,
        part_id: i64,
    ) -> Option<&ObPartitionLocation> {
        match self.partition_entry {
            Some(ref entry) => entry.get_partition_location_with_part_id(part_id),
            None => None,
        }
    }

    pub fn prepare(&mut self) -> Result<()> {
        if self.is_partition_table() {
            return match &mut self.partition_info {
                Some(v) => v.prepare(),
                None => {
                    error!("TableEntry::prepare partition_info is none");
                    return Err(CommonErr(
                        CommonErrCode::PartitionError,
                        "TableEntry::prepare partition_info is none".to_owned(),
                    ));
                }
            };
        }
        Ok(())
    }
}

pub struct ObTableLocation {
    config: ClientConfig,
    /// {ServerAddr} -> {User/DB} -> {Pool}
    mysql_pools: RwLock<HashMap<ObServerAddr, HashMap<String, Arc<my::Pool>>>>,
}

impl Default for ObTableLocation {
    fn default() -> Self {
        Self::new(ClientConfig::default())
    }
}

impl ObTableLocation {
    pub fn new(config: ClientConfig) -> ObTableLocation {
        ObTableLocation {
            config,
            mysql_pools: RwLock::new(HashMap::new()),
        }
    }

    fn get_pool_from_cache(
        &self,
        pools: &HashMap<ObServerAddr, HashMap<String, Arc<my::Pool>>>,
        server_addr: &ObServerAddr,
        cache_key: &str,
    ) -> Option<Arc<my::Pool>> {
        match pools.get(server_addr) {
            Some(map) => map.get(cache_key).cloned(),
            None => None,
        }
    }

    /// Invalidate expired mysql pools.
    pub fn invalidate_mysql_pools(&self, valid_addrs: &[ObServerAddr]) {
        let valid_addrs: HashSet<ObServerAddr> = valid_addrs.iter().cloned().collect();
        self.mysql_pools
            .wl()
            .retain(|addr, _| valid_addrs.contains(addr));
    }

    fn get_or_create_mysql_pool(
        &self,
        username: &str,
        password: &str,
        db_name: &str,
        server_addr: &ObServerAddr,
        connect_timeout: Option<Duration>,
        sock_timeout: Option<Duration>,
    ) -> Result<Arc<my::Pool>> {
        let cache_key = format!("{username}/{db_name}");
        {
            let pools = self.mysql_pools.rl();
            if let Some(pool) = self.get_pool_from_cache(&pools, server_addr, &cache_key) {
                return Ok(pool);
            }
        }

        {
            let mut pools = self.mysql_pools.wl();
            //double check
            if let Some(pool) = self.get_pool_from_cache(&pools, server_addr, &cache_key) {
                return Ok(pool);
            }
            let mut builder = my::OptsBuilder::new();
            builder = builder
                .ip_or_hostname(Some(server_addr.ip.to_owned()))
                .tcp_port(server_addr.sql_port as u16)
                .user(Some(username))
                .pass(Some(password))
                .db_name(Some(db_name))
                .tcp_connect_timeout(connect_timeout)
                .read_timeout(sock_timeout);
            let constraints = PoolConstraints::new(
                self.config.metadata_mysql_conn_pool_min_size,
                self.config.metadata_mysql_conn_pool_max_size,
            );
            builder = builder.pool_opts(constraints.map(|v| PoolOpts::new().with_constraints(v)));

            info!(
                "ObTableLocation::get_or_create_mysql_pool create mysql pool \
                 server {} port {} db_name {}.",
                server_addr.ip, server_addr.sql_port, db_name
            );

            let pool = Arc::new(my::Pool::new(builder)?);

            match pools.get_mut(server_addr) {
                Some(map) => {
                    map.insert(cache_key, pool.clone());
                }
                None => {
                    let mut map = HashMap::new();
                    map.insert(cache_key, pool.clone());
                    pools.insert(server_addr.to_owned(), map);
                }
            }

            Ok(pool)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn execute_sql(
        &self,
        sql: &str,
        server_addr: &ObServerAddr,
        tenant_name: &str,
        username: &str,
        password: &str,
        database: &str,
        timeout: Duration,
    ) -> Result<()> {
        info!(
            "ObTableLocation::execute_sql begin query by sql:{}, addr:{:?}",
            sql, server_addr
        );
        let full_username = format!("{username}@{tenant_name}");
        let pool = self
            .get_or_create_mysql_pool(
                &full_username,
                password,
                database,
                server_addr,
                Some(timeout),
                Some(timeout),
            )
            .map_err(|e| {
                error!(
                    "ObTableLocation::execute_sql fail to get mysql pool, addr:{:?}, \
                     password:{}, full_username:{}, err:{}",
                    server_addr, password, full_username, e
                );
                e
            })?;

        let mut conn = pool.try_get_conn(timeout).map_err(|e| {
            error!(
                "ObTableLocation::execute_sql fail to get connection, sql:{}, addr:{:?}, err:{}",
                sql, server_addr, e
            );
            e
        })?;

        let query_res = conn.query::<Row, &str>(sql).map_err(|e| {
            error!(
                "ObTableLocation::execute_sql failed, sql:{}, addr:{:?}, err:{}",
                sql, server_addr, e
            );
            e
        })?;

        info!(
            "ObTableLocation::execute_sql succeed to query by sql:{}, addr:{:?}, query_res:{:?}",
            sql, server_addr, query_res
        );

        Ok(())
    }

    pub fn load_table_entry_randomly(
        &self,
        rs_list: &[ObServerAddr],
        key: &TableEntryKey,
        connect_timeout: Duration,
        sock_timeout: Duration,
    ) -> Result<TableEntry> {
        if rs_list.is_empty() {
            return Err(CommonErr(
                CommonErrCode::InvalidParam,
                "Empty rs list".to_owned(),
            ));
        }

        let mut rng = thread_rng();
        let random_server = rs_list.choose(&mut rng).unwrap();

        self.get_table_entry_from_remote(random_server, key, connect_timeout, sock_timeout)
    }

    /// refresh table entry with callback and priority
    fn refresh_table_entry_with_priority(
        &self,
        server_roster: &ServerRoster,
        key: &TableEntryKey,
        connect_timeout: Duration,
        sock_timeout: Duration,
        priority_timeout: Duration,
        callback: impl FnOnce(
            &ObTableLocation,
            &ObServerAddr,
            &TableEntryKey,
            Duration,
            Duration,
        ) -> Result<TableEntry>,
    ) -> Result<TableEntry> {
        let mut rs_list: Vec<ObServerAddr> = vec![];
        let grade_time = u::current_time_millis() as usize;

        let max_priority = server_roster.max_priority();

        for addr in server_roster.get_members().iter() {
            if addr.priority() == max_priority
                || grade_time - addr.grant_priority_times()
                    > u::duration_to_millis(&priority_timeout) as usize
            {
                rs_list.push(addr.to_owned());
            }
        }

        if rs_list.is_empty() {
            return Err(CommonErr(
                CommonErrCode::InvalidParam,
                "Empty rs list".to_owned(),
            ));
        }

        let mut rng = thread_rng();
        let addr = rs_list.choose(&mut rng).unwrap();

        match callback(self, addr, key, connect_timeout, sock_timeout) {
            Ok(table_entry) => {
                if addr.priority() != 0 {
                    let grant_priority_times = u::current_time_millis() as usize;
                    addr.set_priority(0);
                    addr.set_grant_priority_times(grant_priority_times);
                    server_roster.upgrade_max_priority(0);
                }

                Ok(table_entry)
            }
            Err(e) => {
                let grant_priority_times = u::current_time_millis() as usize;
                addr.set_grant_priority_times(grant_priority_times);
                server_roster.downgrade_max_priority(addr.decrement_priority_and_get(1));
                Err(e)
            }
        }
    }

    /// Load table entry location from remote with priority
    pub fn load_table_location_with_priority(
        &self,
        server_roster: &ServerRoster,
        key: &TableEntryKey,
        table_entry: &TableEntry,
        connect_timeout: Duration,
        sock_timeout: Duration,
        priority_timeout: Duration,
    ) -> Result<TableEntry> {
        let callback = |location: &ObTableLocation,
                        server_addr: &ObServerAddr,
                        key: &TableEntryKey,
                        connect_timeout: Duration,
                        sock_timeout: Duration|
         -> Result<TableEntry> {
            location.load_table_location(
                server_addr,
                key,
                table_entry,
                connect_timeout,
                sock_timeout,
            )
        };
        self.refresh_table_entry_with_priority(
            server_roster,
            key,
            connect_timeout,
            sock_timeout,
            priority_timeout,
            callback,
        )
    }

    /// Load table entry from remote with priority
    pub fn load_table_entry_with_priority(
        &self,
        server_roster: &ServerRoster,
        key: &TableEntryKey,
        connect_timeout: Duration,
        sock_timeout: Duration,
        priority_timeout: Duration,
    ) -> Result<TableEntry> {
        self.refresh_table_entry_with_priority(
            server_roster,
            key,
            connect_timeout,
            sock_timeout,
            priority_timeout,
            ObTableLocation::get_table_entry_from_remote,
        )
    }

    fn load_table_location(
        &self,
        server_addr: &ObServerAddr,
        key: &TableEntryKey,
        table_entry: &TableEntry,
        connect_timeout: Duration,
        sock_timeout: Duration,
    ) -> Result<TableEntry> {
        let pool = self.get_or_create_mysql_pool(
            &self.config.sys_user_name,
            &self.config.sys_password,
            "oceanbase",
            server_addr,
            Some(connect_timeout),
            Some(sock_timeout),
        )?;

        let mut conn = pool.try_get_conn(connect_timeout)?;

        let part_entry = self.get_table_location_from_remote(&mut conn, key, table_entry)?;
        //Clone a new table entry to return.
        let mut table_entry = table_entry.clone();
        //Update partiton entry and refresh_time
        table_entry.partition_entry = Some(part_entry);
        table_entry.set_refresh_time_mills(u::current_time_millis());

        Ok(table_entry)
    }

    pub fn get_table_entry_from_remote(
        &self,
        server_addr: &ObServerAddr,
        key: &TableEntryKey,
        connect_timeout: Duration,
        sock_timeout: Duration,
    ) -> Result<TableEntry> {
        let pool = self.get_or_create_mysql_pool(
            &self.config.sys_user_name,
            &self.config.sys_password,
            "oceanbase",
            server_addr,
            Some(connect_timeout),
            Some(sock_timeout),
        )?;

        let mut table_id = OB_INVALID_ID;
        let mut partition_num = OB_INVALID_ID;
        let mut conn = pool.try_get_conn(connect_timeout)?;
        let sql = match key.table_name.as_ref() {
        ALL_DUMMY_TABLE => format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ A.partition_id as partition_id, A.svr_ip as svr_ip, A.sql_port as sql_port,
                                    A.table_id as table_id, A.role as role, A.part_num as part_num, B.svr_port as svr_port,
                                    B.status as status, B.stop_time as stop_time FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server
                                    B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port WHERE tenant_name = '{}' and database_name='{}'
                                    and table_name ='{}'",
                                   &key.tenant_name,
                                   &key.database_name,
                                   &key.table_name),
        _ => format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ A.partition_id as partition_id, A.svr_ip as svr_ip, A.sql_port as sql_port, A.table_id as table_id,
                      A.role as role, A.part_num as part_num, B.svr_port as svr_port, B.status as status, B.stop_time as
                      stop_time FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and
                      A.sql_port = B.inner_port WHERE tenant_name = '{}' and database_name='{}' and table_name = '{}' and partition_id = 0",
                     &key.tenant_name,
                     &key.database_name,
                     &key.table_name),
        };
        let mut replica_locations = Vec::new();
        for row in conn.query::<Row, String>(sql)? {
            let (id, svr_ip, sql_port, tbl_id, role, part_num, svr_port, status, stop_time) =
                match my::from_row_opt(row) {
                    Ok(tuple) => tuple,
                    Err(e) => {
                        error!("ObTableLocation::get_table_entry_from_remote: fail to do mysql row conversion, err:{}", e);
                        return Err(CommonErr(
                            CommonErrCode::ConvertFailed,
                            format!("mysql row conversion err:{e}"),
                        ));
                    }
                };
            // just for id type infer
            let _: i64 = id;
            table_id = tbl_id;
            partition_num = part_num;
            let role: ObServerRole = ObServerRole::from_int(role);
            let status: ObServerStatus = ObServerStatus::from_string(status);

            let mut observer_addr = ObServerAddr::new();

            observer_addr.address(svr_ip);
            observer_addr.set_sql_port(sql_port);
            observer_addr.set_svr_port(svr_port);

            let observer_info = ObServerInfo { stop_time, status };
            if !observer_info.is_active() {
                warn!(
                    "ObTableLocation::get_table_entry_from_remote: inactive observer found, \
                     server_info:{:?}, addr:{:?}",
                    observer_info, observer_addr
                );
                continue;
            }

            replica_locations.push(ReplicaLocation {
                addr: observer_addr,
                info: observer_info,
                role,
            });
        }

        if replica_locations.is_empty() {
            return Err(CommonErr(
                CommonErrCode::ObException(ResultCodes::OB_ERR_UNKNOWN_TABLE),
                format!("table not found:{}", key.table_name),
            ));
        }
        let table_location = TableLocation { replica_locations };
        let mut table_entry = TableEntry {
            table_id,
            partition_num,
            refresh_time_mills: Arc::new(AtomicUsize::new(0)),
            partition_info: None,
            table_location,
            partition_entry: None,
            row_key_element: HashMap::new(),
        };

        let part_entry = self.get_table_location_from_remote(&mut conn, key, &table_entry)?;
        table_entry.partition_entry = Some(part_entry);
        table_entry.set_refresh_time_mills(u::current_time_millis());

        if table_entry.is_partition_table() {
            // fetch partition info
            match util::LocationUtil::fetch_partition_info(&mut conn, &table_entry) {
                Ok(v) => table_entry.partition_info = Some(v),
                Err(e) => {
                    table_entry.partition_info = None;
                    error!(
                        "Location::get_table_entry_from_remote fetch_partition_info error:{}",
                        e
                    );
                    return Err(CommonErr(
                        CommonErrCode::PartitionError,
                        format!(
                            "location::get_table_entry_from_remote fetch_partition_info error:{e}",
                        ),
                    ));
                }
            }
            if let Some(v) = table_entry.partition_info.clone() {
                // fetch first range part
                if let Some(first_part_desc) = &v.first_part_desc {
                    let ob_part_func_type = first_part_desc.get_part_func_type();
                    if ob_part_func_type.is_range_part() || ob_part_func_type.is_list_part() {
                        util::LocationUtil::fetch_first_part(
                            &mut conn,
                            &mut table_entry,
                            ob_part_func_type,
                        )?;
                    }
                }
                // fetch sub range part
                if let Some(sub_part_desc) = &v.sub_part_desc {
                    let ob_part_func_type = sub_part_desc.get_part_func_type();
                    if ob_part_func_type.is_range_part() || ob_part_func_type.is_list_part() {
                        util::LocationUtil::fetch_sub_part(
                            &mut conn,
                            &mut table_entry,
                            ob_part_func_type,
                        )?;
                    }
                }
            }
            if let Some(v) = &mut table_entry.partition_info {
                v.part_name_id_map = util::LocationUtil::build_part_name_id_map(v);
            }
        }

        Ok(table_entry)
    }

    pub fn get_table_location_from_remote(
        &self,
        conn: &mut my::PooledConn,
        key: &TableEntryKey,
        table_entry: &TableEntry,
    ) -> Result<ObPartitionEntry> {
        let partition_num = table_entry.partition_num;
        let mut part_str = String::with_capacity((partition_num * 2) as usize);
        for i in 0..partition_num {
            if i > 0 {
                part_str.push(',');
            }
            part_str.push_str(&format!("{i}"));
        }

        let sql = format!("SELECT /*+READ_CONSISTENCY(WEAK)*/ A.partition_id as partition_id, A.svr_ip as svr_ip, A.sql_port as sql_port,
                       A.role as role, B.svr_port as svr_port, B.status as status, B.stop_time as stop_time
                       FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port
                       WHERE tenant_name = '{}' and database_name='{}' and table_name = '{}' and partition_id in ({})",
                      &key.tenant_name,
                      &key.database_name,
                      &key.table_name,
                      &part_str);

        let mut partition_location = HashMap::new();

        for row in conn.query::<Row, String>(sql)? {
            let (part_id, svr_ip, sql_port, role, svr_port, status, stop_time) =
                match my::from_row_opt(row) {
                    Ok(tuple) => tuple,
                    Err(e) => {
                        error!("ObTableLocation::get_table_location_from_remote: fail to do mysql row conversion, err:{}", e);
                        return Err(CommonErr(
                            CommonErrCode::ConvertFailed,
                            format!("mysql row conversion err:{e}"),
                        ));
                    }
                };

            let partition_id = part_id;
            let role = ObServerRole::from_int(role);
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
                addr: observer_addr,
                info: observer_info,
                role: role.clone(),
            };

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
        for part_id in 0..table_entry.partition_num {
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
}

#[cfg(test)]
mod test {
    use std::{
        sync::{
            atomic::{AtomicIsize, AtomicUsize},
            Arc,
        },
        time::Duration,
    };

    use super::*;
    use crate::serde_obkv::value::{CollationLevel, CollationType, ObjMeta, ObjType, Value};

    // TODO: use test conf to control which environments to test.
    const TEST_OB_SERVER_IP: &str = "127.0.0.1";
    const TEST_OB_CLUSTER_NAME: &str = "test";
    const TEST_OB_TENANT_NAME: &str = "test";

    #[test]
    #[ignore]
    fn test_get_table_entry_from_remote() {
        let addr = ObServerAddr {
            ip: TEST_OB_SERVER_IP.to_string(),
            sql_port: 2881,
            svr_port: 2882,
            priority: Arc::new(AtomicIsize::new(0)),
            grant_priority_times: Arc::new(AtomicUsize::new(0)),
        };

        let key = TableEntryKey {
            cluster_name: TEST_OB_CLUSTER_NAME.to_string(),
            tenant_name: TEST_OB_TENANT_NAME.to_string(),
            database_name: OCEANBASE_DATABASE.to_owned(),
            table_name: ALL_DUMMY_TABLE.to_owned(),
        };

        let location = ObTableLocation::new(ClientConfig::default());

        let _result = location
            .get_table_entry_from_remote(
                &addr,
                &key,
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .expect("fail to get table entry from remote");
    }

    #[test]
    #[ignore]
    fn test_partition() {
        let addr = ObServerAddr {
            ip: TEST_OB_SERVER_IP.to_string(),
            sql_port: 2881,
            svr_port: 2882,
            priority: Arc::new(AtomicIsize::new(0)),
            grant_priority_times: Arc::new(AtomicUsize::new(0)),
        };

        let key = TableEntryKey {
            cluster_name: TEST_OB_CLUSTER_NAME.to_string(),
            tenant_name: TEST_OB_TENANT_NAME.to_string(),
            database_name: OCEANBASE_DATABASE.to_owned(),
            table_name: ALL_DUMMY_TABLE.to_owned(),
        };

        let location = ObTableLocation::new(ClientConfig::default());

        let _result = location
            .load_table_entry_randomly(
                &[addr],
                &key,
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .expect("fail to load table entry");
    }

    #[test]
    fn test_value_cmp() {
        let v0 = Value::String(
            "p".to_string(),
            ObjMeta::new(
                ObjType::Varchar,
                CollationLevel::Numeric,
                CollationType::Binary,
                10,
            ),
        );
        let v1 = Value::String(
            "w".to_string(),
            ObjMeta::new(
                ObjType::Varchar,
                CollationLevel::Numeric,
                CollationType::Binary,
                10,
            ),
        );
        let v2 = Value::String(
            "a".to_string(),
            ObjMeta::new(
                ObjType::Varchar,
                CollationLevel::Numeric,
                CollationType::Binary,
                10,
            ),
        );
        assert!(v0 < v1);
        assert!(v0 > v2);
    }
}
