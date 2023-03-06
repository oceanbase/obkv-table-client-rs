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

use std::{
    cmp,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
    u32,
};

use prometheus::*;
use scheduled_thread_pool::ScheduledThreadPool;

use super::{Builder as ConnBuilder, Connection};
use crate::error::{CommonErrCode, Error::Common as CommonErr, Result};

lazy_static! {
    pub static ref OBKV_CONN_POOL_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "obkv_conn_pool_seconds",
        "Bucketed histogram of connection pool operations.",
        &["type"],
        exponential_buckets(0.0005, 2.0, 18).unwrap()
    )
    .unwrap();
}

const MIN_BUILD_RETRY_INTERVAL_MS: u64 = 50 * 1000;
const BUILD_RETRY_LIMIT: usize = 3;

struct PoolInner {
    conns: Vec<Arc<Connection>>,
    next_index: usize,
    max_conn_num: usize,
    pending_conn_num: usize,
}

impl PoolInner {
    fn new(max_conn_num: usize) -> Self {
        Self {
            conns: Vec::with_capacity(max_conn_num),
            next_index: 0,
            max_conn_num,
            pending_conn_num: 0,
        }
    }

    // TODO: use more random/fair policy to pick a connection
    fn try_get(&mut self) -> (Option<Arc<Connection>>, usize) {
        let mut removed = 0usize;
        while !self.conns.is_empty() {
            let idx = self.next_index % self.conns.len();
            let conn = self.conns[idx].clone();
            if conn.is_active() {
                self.advance();
                return (Some(conn), removed);
            }
            self.conns.remove(idx);
            removed += 1;
        }
        (None, removed)
    }

    #[inline]
    fn advance(&mut self) {
        self.next_index += 1;
        if self.next_index == u32::MAX as usize {
            self.next_index = 0;
        }
    }

    #[inline]
    fn idle_conn_num(&self) -> usize {
        self.conns.len()
    }

    #[inline]
    fn should_add_conn(&self) -> bool {
        self.pending_conn_num + self.conns.len() < self.max_conn_num
    }

    #[inline]
    fn pend_conn(&mut self) {
        self.pending_conn_num += 1
    }

    #[inline]
    fn unpend_conn(&mut self) {
        self.pending_conn_num -= 1
    }

    #[inline]
    // Connection should be `pend_conn` before `add_conn`
    fn add_conn(&mut self, conn: Connection) {
        assert!(self.pending_conn_num > 0);
        self.unpend_conn();
        self.conns.push(Arc::new(conn));
    }
}

#[derive(Clone)]
pub struct ConnPool {
    shared_pool: Arc<SharedPool>,
    min_build_retry_interval: Duration,
    build_retry_limit: usize,
}

impl ConnPool {
    fn internal_new(
        min_conn_num: usize,
        max_conn_num: usize,
        conn_init_thread_pool: Arc<ScheduledThreadPool>,
        builder: ConnBuilder,
    ) -> Result<Self> {
        let shared_pool = Arc::new(SharedPool::internal_new(
            min_conn_num,
            max_conn_num,
            conn_init_thread_pool,
            builder,
        )?);
        Ok(Self {
            shared_pool,
            min_build_retry_interval: Duration::from_millis(MIN_BUILD_RETRY_INTERVAL_MS),
            build_retry_limit: BUILD_RETRY_LIMIT,
        })
    }

    fn add_connections_background(
        num: usize,
        shared_pool: &Arc<SharedPool>,
        inner: &mut PoolInner,
        min_build_retry_interval: Duration,
        build_retry_limit: usize,
    ) {
        assert!(num <= inner.max_conn_num);
        for _ in 0..num {
            Self::add_connection_background(
                shared_pool,
                inner,
                min_build_retry_interval,
                build_retry_limit,
            )
        }
    }

    fn add_connection_background(
        shared_pool: &Arc<SharedPool>,
        inner: &mut PoolInner,
        min_build_retry_interval: Duration,
        build_retry_limit: usize,
    ) {
        if !inner.should_add_conn() {
            return;
        }
        inner.pend_conn();
        bg_add(
            shared_pool,
            Duration::new(0, 0),
            min_build_retry_interval,
            0,
            build_retry_limit,
        );
        fn bg_add(
            shared_pool: &Arc<SharedPool>,
            delay: Duration,
            min_build_retry_interval: Duration,
            retry_num: usize,
            build_retry_limit: usize,
        ) {
            if retry_num > build_retry_limit {
                let mut inner = shared_pool.inner.lock().unwrap();
                inner.unpend_conn();
                error!("ConnPool::add_connection_background::bg_add fail to build connection after {} retries", retry_num);
                return;
            }

            let weak_shared_pool = Arc::downgrade(shared_pool);
            shared_pool.conn_init_thread_pool.execute_after(delay, move || {
                let shared_pool = match weak_shared_pool.upgrade() {
                    None => return,
                    Some(p) => p,
                };

                match shared_pool.build_conn() {
                    Ok(conn) => {
                        let mut inner = shared_pool.inner.lock().unwrap();
                        inner.add_conn(conn);
                        shared_pool.cond.notify_all();
                    }
                    Err(e) => {
                        error!("ConnPool::add_connection_background::bg_add fail to build a connection after {} retries, err:{}", retry_num, e);
                        let delay = cmp::max(min_build_retry_interval, delay);
                        let delay = cmp::min(shared_pool.conn_builder.connect_timeout / 2, delay * 2);
                        bg_add(&shared_pool, delay, min_build_retry_interval, retry_num + 1, build_retry_limit);
                    }
                }
            });
        }
    }

    fn wait_for_initialized(&self) -> Result<()> {
        let pool = &self.shared_pool;
        let mut inner = pool.inner.lock().unwrap();
        info!(
            "ConnPool::wait_for_initialized start to initialize {}/{} connections",
            pool.min_conn_num, pool.max_conn_num
        );

        let start = Instant::now();
        Self::add_connections_background(
            pool.max_conn_num,
            pool,
            &mut inner,
            self.min_build_retry_interval,
            self.build_retry_limit,
        );

        let connect_timeout = pool.conn_builder.connect_timeout * pool.min_conn_num as u32;
        loop {
            // wait `min_conn_num` connections to be built
            if inner.idle_conn_num() >= pool.min_conn_num {
                break;
            }

            let wait_res = pool.cond.wait_timeout(inner, connect_timeout).unwrap();
            if wait_res.1.timed_out() {
                return Err(CommonErr(
                    CommonErrCode::ConnPool,
                    format!(
                        "ConnPool::wait_for_initialized create connection timeout_ms:{}",
                        connect_timeout.as_millis()
                    ),
                ));
            }
            inner = wait_res.0;
        }

        let elapsed = Instant::now() - start;
        info!(
            "ConnPool::wait_for_initialized finish initializing {} connections, cost_ms:{}",
            pool.min_conn_num,
            elapsed.as_millis()
        );
        Ok(())
    }

    pub fn get(&self) -> Result<Arc<Connection>> {
        let _timer = OBKV_CONN_POOL_HISTOGRAM_VEC
            .with_label_values(&["get_conn"])
            .start_timer();
        let pool = &self.shared_pool;

        // TODO: may use better name for the timeout here
        let end = Instant::now() + pool.conn_builder.connect_timeout;

        let mut inner = pool.inner.lock().unwrap();
        loop {
            match inner.try_get() {
                (Some(conn), removed) => {
                    if removed > 0 {
                        Self::add_connections_background(
                            removed,
                            pool,
                            &mut inner,
                            self.min_build_retry_interval,
                            self.build_retry_limit,
                        );
                    }
                    return Ok(conn);
                }
                (None, removed) => {
                    warn!("ConnPool::get fail to get active connection so have to wait for a new one, removed:{}", removed);
                }
            }

            Self::add_connections_background(
                inner.max_conn_num,
                pool,
                &mut inner,
                self.min_build_retry_interval,
                self.build_retry_limit,
            );
            let now = Instant::now();
            if now >= end {
                return Err(CommonErr(
                    CommonErrCode::ConnPool,
                    format!(
                        "ConnPool::get get a connection timeout, timeout:{:?}, addr:{}",
                        pool.conn_builder.connect_timeout, pool.conn_builder.ip
                    ),
                ));
            }
            let wait_res = pool.cond.wait_timeout(inner, end - now).unwrap();
            if wait_res.1.timed_out() {
                return Err(CommonErr(
                    CommonErrCode::ConnPool,
                    format!(
                        "ConnPool::get wait for a connection timeout, timeout:{:?}, addr:{}",
                        pool.conn_builder.connect_timeout, pool.conn_builder.ip
                    ),
                ));
            }
            inner = wait_res.0;
        }
    }

    pub fn idle_conn_num(&self) -> usize {
        self.shared_pool.inner.lock().unwrap().idle_conn_num()
    }
}

struct SharedPool {
    min_conn_num: usize,
    max_conn_num: usize,
    conn_builder: ConnBuilder,
    inner: Mutex<PoolInner>,
    cond: Condvar,
    conn_init_thread_pool: Arc<ScheduledThreadPool>,
}

impl SharedPool {
    fn internal_new(
        min_conn_num: usize,
        max_conn_num: usize,
        conn_init_thread_pool: Arc<ScheduledThreadPool>,
        builder: ConnBuilder,
    ) -> Result<Self> {
        Ok(Self {
            min_conn_num,
            max_conn_num,
            conn_builder: builder,
            inner: Mutex::new(PoolInner::new(max_conn_num)),
            cond: Condvar::new(),
            conn_init_thread_pool,
        })
    }

    fn build_conn(&self) -> Result<Connection> {
        let mut conn = self.conn_builder.clone().build()?;
        conn.connect(
            &self.conn_builder.tenant_name,
            &self.conn_builder.user_name,
            &self.conn_builder.database_name,
            &self.conn_builder.password,
        )?;
        Ok(conn)
    }
}

pub struct Builder {
    min_conn_num: usize,
    max_conn_num: usize,
    conn_init_thread_pool: Option<Arc<ScheduledThreadPool>>,
    conn_builder: Option<ConnBuilder>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            min_conn_num: 1,
            max_conn_num: 3,
            conn_init_thread_pool: None,
            conn_builder: None,
        }
    }
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn min_conn_num(mut self, min_conn_num: usize) -> Self {
        self.min_conn_num = min_conn_num;
        self
    }

    pub fn max_conn_num(mut self, max_conn_num: usize) -> Self {
        self.max_conn_num = max_conn_num;
        self
    }

    pub fn conn_builder(mut self, conn_builder: ConnBuilder) -> Self {
        self.conn_builder = Some(conn_builder);
        self
    }

    pub fn conn_init_thread_pool(mut self, thread_pool: Arc<ScheduledThreadPool>) -> Self {
        self.conn_init_thread_pool = Some(thread_pool);
        self
    }

    pub fn build(self) -> Result<ConnPool> {
        assert!(
            self.conn_builder.is_some(),
            "missing necessary conn builder"
        );
        assert!(
            self.conn_init_thread_pool.is_some(),
            "missing necessary conn init thread pool"
        );
        assert!(
            self.min_conn_num <= self.max_conn_num,
            "min_conn_num({}) must equal or less than max_conn_num({})",
            self.min_conn_num,
            self.max_conn_num
        );
        let pool = ConnPool::internal_new(
            self.min_conn_num,
            self.max_conn_num,
            self.conn_init_thread_pool.unwrap(),
            self.conn_builder.unwrap(),
        )?;
        pool.wait_for_initialized()?;
        Ok(pool)
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::Ordering;

    use super::*;

    fn gen_test_conn_builder() -> ConnBuilder {
        ConnBuilder::new()
            // TODO(xikai): use test conf to control which environments to test.
            .ip("127.0.0.1")
            .port(2882)
            .connect_timeout(Duration::from_millis(500))
            .read_timeout(Duration::from_secs(3))
            .login_timeout(Duration::from_secs(3))
            .operation_timeout(Duration::from_secs(10))
            .tenant_name("test")
            .user_name("test")
            .database_name("test")
            .password("test")
    }

    fn gen_test_conn_pool(min_conn_num: usize, max_conn_num: usize) -> ConnPool {
        let conn_builder = gen_test_conn_builder();
        let thread_pool = ScheduledThreadPool::new(2);
        let builder = Builder::new()
            .min_conn_num(min_conn_num)
            .max_conn_num(max_conn_num)
            .conn_init_thread_pool(Arc::new(thread_pool))
            .conn_builder(conn_builder);
        builder.build().expect("fail to build ConnPool")
    }

    #[test]
    #[ignore]
    fn check_conn_valid() {
        let (min_conn_num, max_conn_num) = (2, 3);
        let pool = gen_test_conn_pool(min_conn_num, max_conn_num);
        let conn_num = pool.idle_conn_num();
        assert!(
            conn_num >= min_conn_num && conn_num <= max_conn_num,
            "conn_num({}) should in the range: [{}, {}]",
            conn_num,
            min_conn_num,
            max_conn_num
        );

        let conn = pool.get().expect("fail to get connection from the pool");
        assert!(conn.is_active(), "conn should be active");
    }

    #[test]
    #[ignore]
    fn rebuild_conn() {
        let (min_conn_num, max_conn_num) = (3, 5);
        let pool = gen_test_conn_pool(min_conn_num, max_conn_num);
        for _ in 0..max_conn_num * 2 {
            let conn = pool.get().expect("fail to get connection from the pool");
            assert!(conn.is_active(), "should get active connection");
            conn.active.store(false, Ordering::SeqCst);
        }
    }

    #[test]
    #[ignore]
    fn test_pool_inner_remove() {
        let max_conn_num = 2;
        let mut pool_inner = PoolInner::new(max_conn_num);
        let conn_builder = gen_test_conn_builder();
        for _ in 0..max_conn_num {
            let conn = conn_builder
                .clone()
                .build()
                .expect("fail to build connection");
            assert!(!conn.is_active(), "should be inactive");
            pool_inner.pend_conn();
            pool_inner.add_conn(conn);
        }
        assert_eq!(
            max_conn_num,
            pool_inner.idle_conn_num(),
            "should have max_conn_num connections"
        );
        let (conn, removed) = pool_inner.try_get();
        assert!(conn.is_none(), "no connection should be active");
        assert_eq!(
            max_conn_num, removed,
            "all the connections should be removed"
        );
    }
}
