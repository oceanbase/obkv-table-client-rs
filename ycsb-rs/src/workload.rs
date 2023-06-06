mod core_workload;

use std::{cell::RefCell, rc::Rc};

pub use core_workload::CoreWorkload;
use rand::rngs::SmallRng;

use crate::db::DB;

pub trait Workload {
    fn do_insert(&self, db: Rc<dyn DB>);
    fn do_transaction(&self, rng: Rc<RefCell<SmallRng>>, db: Rc<dyn DB>);
}
