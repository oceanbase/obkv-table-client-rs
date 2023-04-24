mod core_workload;

use std::{cell::RefCell, rc::Rc, sync::Arc};

pub use core_workload::CoreWorkload;
use rand::rngs::SmallRng;

use crate::{db::DB, obkv_client::OBKVClient};

pub trait Workload {
    fn do_insert(&self, db: Rc<dyn DB>);
    fn do_transaction(&self, rng: Rc<RefCell<SmallRng>>, db: Rc<dyn DB>);
    fn ob_insert(&self, db: Arc<OBKVClient>);
    fn ob_transaction(&self, rng: Rc<RefCell<SmallRng>>, db: Arc<OBKVClient>);
}
