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

fn do_read(db: &DB, key: u64, expected: &[u8]) {
    let now = Instant::now();

    compiler_fence(SeqCst);
    let res = db.get_pinned(key.to_be_bytes()).unwrap().unwrap();
    let sl: &[u8] = &*res;
    assert_eq!(sl, expected);
    compiler_fence(SeqCst);

    let dur = now.elapsed();
    let log = RocksDBLog {
        op: RocksDBOp::Read,
        bytes: expected.len(),
        micros: dur.as_micros() as usize,
    };
    println!("{:?}", log);
}

fn do_write(db: &DB, key: u64, slice: &[u8], opt: &WriteOptions) {
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
}

fn do_work(db: DB, read_ratio: f64, min_key: u64, max_key: u64) {
    let data = random_data(1024 * 4);
    let mut available_keys: HashMap<u64, &[u8]> = HashMap::new();
    let mut rng = thread_rng();
    let mut opt = WriteOptions::default();
    opt.set_sync(true);

    loop {
        let key: u64 = rng.gen_range(min_key..max_key);
        let read: bool = rng.gen::<f64>() < read_ratio;

        if read {
            if let Some(expected) = available_keys.get(&key) {
                do_read(&db, key, expected);
                continue;
            }
        }

        let bounds = random_idx_bounds(data.len(), &mut rng);
        let slice = &data[bounds.0..bounds.1];
        do_write(&db, key, slice, &opt);
        let _ = available_keys.insert(key, slice);
    }
}

fn main() {
    let threads = 8;
    let keys_per_thread = 4096;
    let read_ratio = 0.90;

    let path = PathBuf::from("/nvme/data/tmp/2024-vldb/rocksdb-app");
    let db = setup_db(path);
    let mut handles = Vec::new();

    for i in 0..threads {
        let s = i * keys_per_thread;
        let e = s + keys_per_thread;
        let db = db.clone();

        handles.push(thread::spawn(move || {
            do_work(db, read_ratio, s, e);
        }));
    }

    for h in handles {
        let _ = h.join();
    }
}
