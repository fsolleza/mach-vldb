use rocksdb::{ DBWithThreadMode, SingleThreaded, WriteOptions };
use std::{
    path::PathBuf,
    sync::{ Arc, atomic::{ compiler_fence, Ordering::SeqCst } },
    collections::HashMap,
    time::Instant,
    thread,
};
use rand::prelude::*;
use common::rocksdb::{RocksDBOp, RocksDBLog};

type DB = Arc<DBWithThreadMode<SingleThreaded>>;

fn setup_db(path: PathBuf) -> DB {
    let _ = std::fs::remove_dir_all(&path);
    let db = DBWithThreadMode::<SingleThreaded>::open_default(path).unwrap();
    let mut opt = WriteOptions::default();
    opt.set_sync(false);
    opt.disable_wal(true);
    Arc::new(db)
}

fn random_data(sz: usize) -> Vec<u8> {
    let mut data: Vec<u8> = vec![0u8; sz];
    let mut rng = thread_rng();
    for d in &mut data {
        *d = rng.gen();
    }
    data
}

fn random_idx_bounds(l: usize, rng: &mut ThreadRng) -> (usize, usize) {
    let a: usize = rng.gen_range(0..l);
    let b: usize = rng.gen_range(0..l);
    let mut bounds = (a, b);
    if bounds.0 > bounds.1 {
        bounds = (bounds.1, bounds.0);
    }
    bounds
}

fn do_work(db: DB, read_ratio: f64, min_key: usize, max_key: usize) {
    let data = random_data(1024 * 4);
    let mut available_keys: HashMap<usize, &[u8]> = HashMap::new();
    let mut rng = thread_rng();
    let mut opt = WriteOptions::default();
    opt.set_sync(true);

    loop {
        let key = rng.gen_range(min_key..max_key);
        let read: bool = rng.gen::<f64>() < read_ratio;

        if read {
            if let Some(expected) = available_keys.get(&key) {
                let now = Instant::now();

                compiler_fence(SeqCst);
                let res = db.get_pinned(key.to_be_bytes()).unwrap().unwrap();
                let sl: &[u8] = &*res;
                assert_eq!(sl, *expected);
                compiler_fence(SeqCst);

                let dur = now.elapsed();
                let log = RocksDBLog {
                    op: RocksDBOp::Read,
                    bytes: expected.len(),
                    micros: dur.as_micros() as usize,
                };
                println!("{:?}", log);
                continue;
            }
        }

        let bounds = random_idx_bounds(data.len(), &mut rng);
        let slice = &data[bounds.0..bounds.1];
        let now = Instant::now();

        compiler_fence(SeqCst);
        db.put_opt(key.to_be_bytes(), &slice, &opt).unwrap();
        compiler_fence(SeqCst);

        let dur =now.elapsed();
        let log = RocksDBLog {
            op: RocksDBOp::Write,
            bytes: slice.len(),
            micros: dur.as_micros() as usize,
        };
        println!("{:?}", log);
        let _ = available_keys.insert(key, slice);
    }
}

fn main() {
    println!("Hello, world!");
    let path = PathBuf::from("/nvme/data/tmp/2024-vldb/rocksdb-app");
    let db = setup_db(path);
    do_work(db, 0.90, 0, 4096);
}
