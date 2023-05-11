use std::{collections::HashMap, rc::Rc, sync::Arc};

use anyhow::{anyhow, Result};

use crate::{
    obkv_client::{OBKVClient, OBKVClientInitStruct},
    sqlite::SQLite,
};

pub trait DB {
    fn init(&self) -> Result<()>;
    fn insert(&self, table: &str, key: &str, values: &HashMap<&str, String>) -> Result<()>;
    fn read(&self, table: &str, key: &str, result: &mut HashMap<String, String>) -> Result<()>;
    fn update(&self, table: &str, key: &str, values: &HashMap<&str, String>) -> Result<()>;
    fn scan(&self, table: &str, startkey: &str, endkey: &str, values: &mut HashMap<String, String>) -> Result<()>;
}

pub fn create_db(db: &str, config: Arc<OBKVClientInitStruct>) -> Result<Rc<dyn DB>> {
    match db {
        "sqlite" => Ok(Rc::new(SQLite::new()?)),
        "obkv" => Ok(Rc::new(OBKVClient::build_normal_client(config)?)),
        db => Err(anyhow!("{} is an invalid database name", db)),
    }
}

pub fn create_ob(db: &str, config: Arc<OBKVClientInitStruct>) -> Result<Arc<OBKVClient>> {
    match db {
        "obkv" => Ok(Arc::new(OBKVClient::build_normal_client(config)?)),
        db => Err(anyhow!("{} is an invalid database name", db)),
    }
}
