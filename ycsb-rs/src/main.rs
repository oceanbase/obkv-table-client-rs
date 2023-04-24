use std::{cell::RefCell, fs, rc::Rc, sync::Arc, thread, time::Instant};

use anyhow::{bail, Result};
use properties::Properties;
use rand::{rngs::SmallRng, SeedableRng};
use structopt::StructOpt;
use workload::CoreWorkload;

use crate::{
    db::DB,
    obkv_client::{OBKVClient, OBKVClientInitStruct},
    workload::Workload,
};

pub mod db;
pub mod generator;
pub mod monitor;
pub mod obkv_client;
pub mod properties;
pub mod sqlite;
pub mod workload;

#[derive(StructOpt, Debug)]
#[structopt(name = "ycsb")]
struct Opt {
    #[structopt(name = "COMMANDS")]
    commands: Vec<String>,
    #[structopt(short, long)]
    database: String,
    #[structopt(short, long)]
    workload: String,
    #[structopt(short, long, default_value = "1")]
    threads: usize,
}

fn load(wl: Arc<CoreWorkload>, db: Rc<dyn DB>, operation_count: usize) {
    for _ in 0..operation_count {
        wl.do_insert(db.clone());
    }
}

fn run(wl: Arc<CoreWorkload>, db: Rc<dyn DB>, rng: Rc<RefCell<SmallRng>>, operation_count: usize) {
    for _ in 0..operation_count {
        wl.do_transaction(rng.clone(), db.clone());
    }
}

fn load_ob(wl: Arc<CoreWorkload>, db: Arc<OBKVClient>, operation_count: usize) {
    for _ in 0..operation_count {
        wl.ob_insert(db.clone());
    }
}

fn run_ob(
    wl: Arc<CoreWorkload>,
    db: Arc<OBKVClient>,
    rng: Rc<RefCell<SmallRng>>,
    operation_count: usize,
) {
    for _ in 0..operation_count {
        wl.ob_transaction(rng.clone(), db.clone());
    }
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    // for analyze
    // let opt = Opt {
    //     commands: vec!["run".to_string()],
    //     database: "obkv".to_string(),
    //     workload: "/ycsb-rs/workloads/workload_obkv.toml".to_string(),
    //     threads: 40,
    // };

    let raw_props = fs::read_to_string(&opt.workload)?;

    let props: Properties = toml::from_str(&raw_props)?;

    let props = Arc::new(props);

    let wl = Arc::new(CoreWorkload::new(&props));

    let config = Arc::new(OBKVClientInitStruct::new(&props));

    if opt.commands.is_empty() {
        // print commands
        bail!("no command specified");
    }

    let database = opt.database.clone();
    let thread_operation_count = props.operation_count as usize / opt.threads;
    let actual_client_count = opt.threads / props.obkv_client_reuse;
    for cmd in opt.commands {
        let start = Instant::now();
        let mut threads = vec![];
        println!(
            "Database: {database}, Command: {cmd}, Counts Per Threads: {thread_operation_count}"
        );
        println!(
            "Actual Client Count: {actual_client_count}, Client Reuse Count: {}",
            props.obkv_client_reuse
        );
        if database.eq_ignore_ascii_case("obkv") {
            for _client_idx in 0..actual_client_count {
                let database = database.clone();
                let db = db::create_ob(&database, config.clone()).unwrap();
                for _ in 0..props.obkv_client_reuse {
                    let db = db.clone();
                    let wl = wl.clone();
                    let cmd = cmd.clone();
                    threads.push(thread::spawn(move || {
                        let rng = Rc::new(RefCell::new(SmallRng::from_entropy()));
                        db.init().unwrap();
                        match &cmd[..] {
                            "load" => load_ob(wl.clone(), db, thread_operation_count),
                            "run" => run_ob(wl.clone(), db, rng, thread_operation_count),
                            cmd => panic!("invalid command: {cmd}"),
                        };
                    }));
                }
            }
        } else {
            for _ in 0..opt.threads {
                let database = database.clone();
                let wl = wl.clone();
                let config = config.clone();
                let cmd = cmd.clone();
                threads.push(thread::spawn(move || {
                    let db = db::create_db(&database, config.clone()).unwrap();
                    let rng = Rc::new(RefCell::new(SmallRng::from_entropy()));

                    db.init().unwrap();

                    match &cmd[..] {
                        "load" => load(wl.clone(), db, thread_operation_count),
                        "run" => run(wl.clone(), db, rng, thread_operation_count),
                        cmd => panic!("invalid command: {cmd}"),
                    };
                }));
            }
        }
        for t in threads {
            let _ = t.join();
        }
        let runtime = start.elapsed().as_millis();
        println!("[OVERALL], ThreadCount, {}", opt.threads);
        println!("[OVERALL], RunTime(ms), {runtime}");
        let throughput = props.operation_count as f64 / (runtime as f64 / 1000.0);
        println!("[OVERALL], Throughput(ops/sec), {throughput}\n");
    }

    monitor::print_client_matrics();
    // monitor::print_proxy_matrics();
    monitor::print_rpc_matrics();
    monitor::print_rpc_num_matrics();

    Ok(())
}
