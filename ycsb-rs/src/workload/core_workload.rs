use std::{
    cell::RefCell,
    collections::HashMap,
    ops::DerefMut,
    rc::Rc,
    sync::{Arc, Mutex},
};

use rand::{
    distributions::{Alphanumeric, DistString},
    rngs::SmallRng,
    SeedableRng,
};

use crate::{
    db::DB,
    generator::{
        AcknowledgedCounterGenerator, ConstantGenerator, CounterGenerator, DiscreteGenerator,
        Generator, UniformLongGenerator, WeightPair, ZipfianGenerator,
    },
    obkv_client::OBKVClient,
    properties::Properties,
    workload::Workload,
};

#[derive(Copy, Clone, Debug)]
pub enum CoreOperation {
    Read,
    Update,
    Insert,
    Scan,
    ReadModifyWrite,
    BatchRead,
    BatchInsertUp,
}

impl std::fmt::Display for CoreOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[allow(dead_code)]
pub struct CoreWorkload {
    rng: Mutex<SmallRng>,
    table: String,
    field_count: u64,
    field_names: Vec<String>,
    batch_count: u64,
    field_length_generator: Mutex<Box<dyn Generator<u64> + Send>>,
    read_all_fields: bool,
    write_all_fields: bool,
    data_integrity: bool,
    key_sequence: Mutex<Box<dyn Generator<u64> + Send>>,
    operation_chooser: Mutex<DiscreteGenerator<CoreOperation>>,
    key_chooser: Mutex<Box<dyn Generator<u64> + Send>>,
    //field_chooser: Box<dyn Generator<String>>,
    transaction_insert_key_sequence: Mutex<AcknowledgedCounterGenerator>,
    //scan_length: Box<dyn Generator<u64>>,
    ordered_inserts: bool,
    record_count: usize,
    zero_padding: usize,
    insertion_retry_limit: u64,
    insertion_retry_interval: u64,
}

impl CoreWorkload {
    pub fn new(prop: &Properties) -> Self {
        let rng = SmallRng::from_entropy();
        let field_name_prefix = "field";
        let field_count = 10;
        let mut field_names = vec![];
        for i in 0..field_count {
            field_names.push(format!("{field_name_prefix}{i}"));
        }
        CoreWorkload {
            rng: Mutex::new(rng),
            table: String::from("usertable"),
            field_count,
            field_names,
            batch_count: prop.batch_count,
            field_length_generator: Mutex::new(get_field_length_generator(prop)),
            read_all_fields: true,
            write_all_fields: true,
            data_integrity: true,
            key_sequence: Mutex::new(Box::new(CounterGenerator::new(prop.insert_start))),
            operation_chooser: Mutex::new(create_operation_generator(prop)),
            key_chooser: Mutex::new(get_key_chooser_generator(prop)),
            //field_chooser: Box<dyn Generator<String>>,
            transaction_insert_key_sequence: Mutex::new(AcknowledgedCounterGenerator::new(1)),
            //scan_length: Box<dyn Generator<u64>>,
            ordered_inserts: true,
            record_count: 1,
            zero_padding: 1,
            insertion_retry_limit: 0,
            insertion_retry_interval: 0,
        }
    }

    fn do_transaction_insert(&self, db: Rc<dyn DB>) {
        let keynum = self.next_key_num();
        let dbkey = format!("{}", fnvhash64(keynum));
        let mut values = HashMap::new();
        for field_name in &self.field_names {
            let field_len = self
                .field_length_generator
                .lock()
                .unwrap()
                .next_value(&mut self.rng.lock().unwrap());
            let s = Alphanumeric
                .sample_string::<SmallRng>(&mut self.rng.lock().unwrap(), field_len as usize);
            values.insert(&field_name[..], s);
        }
        db.insert(&self.table, &dbkey, &values).unwrap();
    }

    async fn ob_transaction_insert(&self, db: Arc<OBKVClient>) {
        let keynum = self.next_key_num();
        let dbkey = format!("{}", fnvhash64(keynum));
        let mut values = HashMap::new();
        for field_name in &self.field_names {
            let field_len = self
                .field_length_generator
                .lock()
                .unwrap()
                .next_value(&mut self.rng.lock().unwrap());
            let s = Alphanumeric
                .sample_string::<SmallRng>(&mut self.rng.lock().unwrap(), field_len as usize);
            values.insert(&field_name[..], s);
        }
        db.insert(&self.table, &dbkey, &values).await.unwrap();
    }

    fn do_transaction_read(&self, db: Rc<dyn DB>) {
        let keynum = self.next_key_num();
        let dbkey = format!("{}", fnvhash64(keynum));
        let mut result = HashMap::new();
        db.read(&self.table, &dbkey, &mut result).unwrap();
        // TODO: verify rows
    }

    async fn ob_transaction_read(&self, db: Arc<OBKVClient>) {
        let keynum = self.next_key_num();
        let dbkey = format!("{}", fnvhash64(keynum));
        let result = HashMap::new();
        db.read(&self.table, &dbkey, &self.field_names, &result)
            .await
            .unwrap();
    }

    fn do_transaction_update(&self, db: Rc<dyn DB>) {
        let keynum = self.next_key_num();
        let dbkey = format!("{}", fnvhash64(keynum));
        let mut values = HashMap::new();
        for field_name in &self.field_names {
            let field_len = self
                .field_length_generator
                .lock()
                .unwrap()
                .next_value(&mut self.rng.lock().unwrap());
            let s = Alphanumeric
                .sample_string::<SmallRng>(&mut self.rng.lock().unwrap(), field_len as usize);
            values.insert(&field_name[..], s);
        }
        db.update(&self.table, &dbkey, &values).unwrap();
    }

    async fn ob_transaction_update(&self, db: Arc<OBKVClient>) {
        let keynum = self.next_key_num();
        let dbkey = format!("{}", fnvhash64(keynum));
        let mut values = HashMap::new();
        for field_name in &self.field_names {
            let field_len = self
                .field_length_generator
                .lock()
                .unwrap()
                .next_value(&mut self.rng.lock().unwrap());
            let s = Alphanumeric
                .sample_string::<SmallRng>(&mut self.rng.lock().unwrap(), field_len as usize);
            values.insert(&field_name[..], s);
        }
        db.update(&self.table, &dbkey, &values).await.unwrap();
    }

    async fn ob_transaction_scan(&self, db: Arc<OBKVClient>) {
        let start = self.next_key_num();
        let dbstart = format!("{}", fnvhash64(start));
        let dbend = format!("{}", fnvhash64(start));
        let result = HashMap::new();
        db.scan(&self.table, &dbstart, &dbend, &self.field_names, &result)
            .await
            .unwrap();
    }

    async fn ob_transaction_batchread(&self, db: Arc<OBKVClient>) {
        let mut keys = Vec::new();
        for _ in 0..self.batch_count {
            // generate key
            let keynum = self.next_key_num();
            let dbkey = format!("{}", fnvhash64(keynum));
            keys.push(dbkey);
        }

        let result = HashMap::new();
        db.batch_read(&self.table, &keys, &self.field_names, &result)
            .await
            .unwrap();
    }

    async fn ob_transaction_batchinsertup(&self, db: Arc<OBKVClient>) {
        let mut keys = Vec::new();
        let mut field_values = Vec::new();

        // generate value for each field
        // operation in batch will reuse field values
        for _ in 0..self.field_count {
            let field_len = self
                .field_length_generator
                .lock()
                .unwrap()
                .next_value(&mut self.rng.lock().unwrap());
            let s = Alphanumeric
                .sample_string::<SmallRng>(&mut self.rng.lock().unwrap(), field_len as usize);
            field_values.push(s);
        }

        // generate key
        for _ in 0..self.batch_count {
            let keynum = self.next_key_num();
            let dbkey = format!("{}", fnvhash64(keynum));
            keys.push(dbkey);
        }

        db.batch_insertup(&self.table, &keys, &self.field_names, &field_values)
            .await
            .unwrap();
    }

    fn next_key_num(&self) -> u64 {
        // FIXME: Handle case where keychooser is an ExponentialGenerator.
        // FIXME: Handle case where keynum is > transactioninsertkeysequence's last
        // value
        self.key_chooser
            .lock()
            .unwrap()
            .next_value(&mut self.rng.lock().unwrap())
    }

    pub async fn ob_insert(&self, db: Arc<OBKVClient>) {
        let dbkey = self
            .key_sequence
            .lock()
            .unwrap()
            .next_value(&mut self.rng.lock().unwrap());
        let dbkey = format!("{}", fnvhash64(dbkey));
        let mut values = HashMap::new();
        for field_name in &self.field_names {
            let field_len = self
                .field_length_generator
                .lock()
                .unwrap()
                .next_value(&mut self.rng.lock().unwrap());
            let s = Alphanumeric
                .sample_string::<SmallRng>(&mut self.rng.lock().unwrap(), field_len as usize);
            values.insert(&field_name[..], s);
        }
        db.insert(&self.table, &dbkey, &values).await.unwrap();
    }

    pub async fn ob_transaction(&self, rng: Arc<Mutex<SmallRng>>, db: Arc<OBKVClient>) {
        let op = self
            .operation_chooser
            .lock()
            .unwrap()
            .next_value(&mut rng.lock().unwrap());
        match op {
            CoreOperation::Insert => {
                self.ob_transaction_insert(db).await;
            }
            CoreOperation::Read => {
                self.ob_transaction_read(db).await;
            }
            CoreOperation::Update => {
                self.ob_transaction_update(db).await;
            }
            CoreOperation::Scan => {
                self.ob_transaction_scan(db).await;
            }
            CoreOperation::BatchRead => {
                self.ob_transaction_batchread(db).await;
            }
            CoreOperation::BatchInsertUp => {
                self.ob_transaction_batchinsertup(db).await;
            }
            _ => todo!(),
        }
    }
}

impl Workload for CoreWorkload {
    fn do_insert(&self, db: Rc<dyn DB>) {
        let dbkey = self
            .key_sequence
            .lock()
            .unwrap()
            .next_value(&mut self.rng.lock().unwrap());
        let dbkey = format!("{}", fnvhash64(dbkey));
        let mut values = HashMap::new();
        for field_name in &self.field_names {
            let field_len = self
                .field_length_generator
                .lock()
                .unwrap()
                .next_value(&mut self.rng.lock().unwrap());
            let s = Alphanumeric
                .sample_string::<SmallRng>(&mut self.rng.lock().unwrap(), field_len as usize);
            values.insert(&field_name[..], s);
        }
        db.insert(&self.table, &dbkey, &values).unwrap();
    }

    fn do_transaction(&self, rng: Rc<RefCell<SmallRng>>, db: Rc<dyn DB>) {
        let op = self
            .operation_chooser
            .lock()
            .unwrap()
            .next_value(rng.borrow_mut().deref_mut());
        match op {
            CoreOperation::Insert => {
                self.do_transaction_insert(db);
            }
            CoreOperation::Read => {
                self.do_transaction_read(db);
            }
            CoreOperation::Update => {
                self.do_transaction_update(db);
            }
            _ => todo!(),
        }
    }
}

// http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
fn fnvhash64(val: u64) -> u64 {
    let mut val = val;
    let prime = 0xcbf29ce484222325;
    let mut hashval = prime;
    for _ in 0..8 {
        let octet = val & 0x00ff;
        val >>= 8;
        hashval ^= octet;
        hashval = hashval.wrapping_mul(prime);
    }
    hashval
}

fn get_field_length_generator(prop: &Properties) -> Box<dyn Generator<u64> + Send> {
    match prop.field_length_distribution.to_lowercase().as_str() {
        "constant" => Box::new(ConstantGenerator::new(prop.field_length)),
        "uniform" => Box::new(UniformLongGenerator::new(1, prop.field_length)),
        "zipfian" => Box::new(ZipfianGenerator::from_range(1, prop.field_length)),
        "histogram" => unimplemented!(),
        _ => panic!(
            "unknown field length distribution {}",
            prop.field_length_distribution
        ),
    }
}

fn get_key_chooser_generator(prop: &Properties) -> Box<dyn Generator<u64> + Send> {
    let insert_count = if prop.insert_count > 1 {
        prop.insert_count
    } else {
        prop.record_count - prop.insert_start
    };
    assert!(insert_count > 1);
    match prop.request_distribution.to_lowercase().as_str() {
        "uniform" => Box::new(UniformLongGenerator::new(
            prop.insert_start,
            prop.insert_start + insert_count - 1,
        )),
        _ => todo!(),
    }
}

fn create_operation_generator(prop: &Properties) -> DiscreteGenerator<CoreOperation> {
    let mut pairs = vec![];
    if prop.read_proportion > 0.0 {
        pairs.push(WeightPair::new(prop.read_proportion, CoreOperation::Read));
    }
    if prop.update_proportion > 0.0 {
        pairs.push(WeightPair::new(
            prop.update_proportion,
            CoreOperation::Update,
        ));
    }
    if prop.insert_proportion > 0.0 {
        pairs.push(WeightPair::new(
            prop.insert_proportion,
            CoreOperation::Insert,
        ));
    }
    if prop.scan_proportion > 0.0 {
        pairs.push(WeightPair::new(prop.scan_proportion, CoreOperation::Scan));
    }
    if prop.read_modify_write_proportion > 0.0 {
        pairs.push(WeightPair::new(
            prop.read_modify_write_proportion,
            CoreOperation::ReadModifyWrite,
        ));
    }
    if prop.batch_read_proportion > 0.0 {
        pairs.push(WeightPair::new(
            prop.batch_read_proportion,
            CoreOperation::BatchRead,
        ));
    }
    if prop.batch_insertup_proportion > 0.0 {
        pairs.push(WeightPair::new(
            prop.batch_insertup_proportion,
            CoreOperation::BatchInsertUp,
        ));
    }

    DiscreteGenerator::new(pairs)
}
