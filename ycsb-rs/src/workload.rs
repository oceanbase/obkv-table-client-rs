mod core_workload;

use std::cell::RefCell;
pub use core_workload::CoreWorkload;

use crate::db::DB;
use std::rc::Rc;
use std::sync::Arc;
use rand::rngs::SmallRng;
use crate::obkv_client::OBKVClient;

pub trait Workload {
    fn do_insert(&self, db: Rc<dyn DB>);
    fn do_transaction(&self, rng: Rc<RefCell<SmallRng>>, db: Rc<dyn DB>);
    fn ob_insert(&self, db: Arc<OBKVClient>);
    fn ob_transaction(&self, rng: Rc<RefCell<SmallRng>>, db: Arc<OBKVClient>);
}
