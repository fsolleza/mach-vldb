use rocksdb::{ DBWithThreadMode, SingleThreaded, WriteOptions };
use std::{
    path::PathBuf,
    sync::{
        Mutex,
        Arc,
        atomic::{ compiler_fence, AtomicUsize, Ordering::SeqCst },
        //mpsc::{SyncSender, Receiver, sync_channel}
    },
    collections::HashMap,
    time::{ Duration, Instant },
    thread,
};
use crossbeam::channel::{Sender, Receiver, bounded};
use rand::prelude::*;
use common::{
    ipc::ipc_sender,
    data::{micros_since_epoch, KVOp, KVLog, Record, Batch},
};
use dashmap::DashMap;
use lazy_static::*;

type DB = Arc<DBWithThreadMode<SingleThreaded>>;
//type DB = Arc<DashMap<u64, Mutex<Vec<u8>>>>;

const THREADS: usize = 32;

static READ_COUNT: AtomicUsize = AtomicUsize::new(0);
static READ_TIME_NS: AtomicUsize = AtomicUsize::new(0);
static READ_BYTES: AtomicUsize = AtomicUsize::new(0);

static WRITE_COUNT: AtomicUsize = AtomicUsize::new(0);
static WRITE_TIME_NS: AtomicUsize = AtomicUsize::new(0);
static WRITE_BYTES: AtomicUsize = AtomicUsize::new(0);

static OPS_MIN_RATE: AtomicUsize = AtomicUsize::new(0);

fn counter2() {
        let ops = READ_COUNT.swap(0, SeqCst) + WRITE_COUNT.swap(0, SeqCst);
        println!("{}", ops);
}

fn counter() {
    let read_count      = READ_COUNT.swap(0, SeqCst);
    let read_time_ns    = READ_TIME_NS.swap(0, SeqCst);
    let read_bytes      = READ_BYTES.swap(0, SeqCst);

    let write_count     = WRITE_COUNT.swap(0, SeqCst);
    let write_time_ns   = WRITE_TIME_NS.swap(0, SeqCst);
    let write_bytes     = WRITE_BYTES.swap(0, SeqCst);

    let mut read_lat = 0;
    if read_count > 0 {
        read_lat = read_time_ns / read_count;
    }

    let mut write_lat = 0;
    if write_count > 0 {
        write_lat = write_time_ns / write_count;
    }

    println!("Reads: {} {} \t\t Writes: {} {}",
             read_count, read_bytes,
             write_count, write_bytes);
}

fn init_counter() {
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(1));
            counter2();
        }
    });
}

fn set_core_affinity(cpu: usize) {
    use core_affinity::{set_for_current, CoreId};
    assert!(set_for_current(CoreId { id: cpu }));
}

fn random_core_affinity<const T: usize>() -> usize {
    let mut rng = thread_rng();
    let cpu = rng.gen::<usize>() % T;
    set_core_affinity(cpu);
    cpu
}

fn init_logging() -> Sender<KVLog> {
    let (tx, rx) = bounded(1000000);
    let ipc = ipc_sender("0.0.0.0:3002", None);
    thread::spawn(move || {
        egress(rx, ipc);
    });
    tx
}

fn egress(rx: Receiver<KVLog>, ipc: Sender<Vec<Record>>) {
    let mut batch: Vec<Record> = Vec::new();
    while let Ok(item) = rx.recv() {
        batch.push(Record::KV(item));
        if batch.len() == 256 {
            ipc.send(batch);
            batch = Vec::new();
        }
    }
}

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

fn do_read(db: &DB, key: u64) -> Option<Vec<u8>> {
    let now = Instant::now();

    compiler_fence(SeqCst);
    let res = db.get(key.to_be_bytes()).unwrap()?;
    //let entry = db.get(&key)?;
    //let mut guard = entry.lock().unwrap();
    //let res = &*guard;
    compiler_fence(SeqCst);

    let dur = now.elapsed().as_nanos() as usize;
    READ_COUNT.fetch_add(1, SeqCst);
    READ_TIME_NS.fetch_add(dur, SeqCst);
    READ_BYTES.fetch_max(dur, SeqCst);
    Some(res.clone())
}

fn do_write(db: &DB, key: u64, slice: &[u8]) {
    let mut opt = WriteOptions::default();
    opt.set_sync(false);
    let now = Instant::now();
    compiler_fence(SeqCst);
    db.put_opt(key.to_be_bytes(), slice, &opt).unwrap();
    //let mut entry = db.entry(key).or_insert_with(|| { Mutex::new(Vec::new()) });
    //let mut guard = entry.lock().unwrap();
    //guard.clear();
    //guard.resize(slice.len(), 0);
    //guard.copy_from_slice(slice);
    compiler_fence(SeqCst);

    let dur = now.elapsed().as_nanos() as usize;
    WRITE_COUNT.fetch_add(1, SeqCst);
    WRITE_TIME_NS.fetch_add(dur, SeqCst);
    WRITE_BYTES.fetch_max(dur, SeqCst);
}

fn do_work(
    db: DB,
    read_ratio: f64,
    min_key: u64,
    max_key: u64,
    out: Sender<Vec<u8>>,
    log_out: Sender<KVLog>,
) {
    lazy_static! {
        static ref DATA: Vec<u8> = random_data(1024 * 4);
    }

    let data: &'static [u8] = DATA.as_slice();

    let mut rng = thread_rng();
    let mut cpu = random_core_affinity::<32>() as u64;
    loop {

        let switch_cpu: f64 = rng.gen();
        if switch_cpu < 0.3 {
            cpu = random_core_affinity::<32>() as u64;
        }

        let key: u64 = rng.gen_range(min_key..max_key);
        let read: bool = rng.gen::<f64>() < read_ratio;

        let now = Instant::now();
        if read {
            if let Some(vec) = do_read(&db, key) {
                out.send(vec).unwrap();

                let dur_nanos = now.elapsed().as_nanos() as u64;
                let timestamp = micros_since_epoch();
                let op = KVOp::Read;
                let log = KVLog { op, dur_nanos, timestamp, cpu };
                log_out.send(log).unwrap();

                continue;
            }
        }

        let bounds = random_idx_bounds(data.len(), &mut rng);
        let slice = &data[bounds.0..bounds.1];
        do_write(&db, key, slice);

        let dur_nanos = now.elapsed().as_nanos() as u64;
        let timestamp = micros_since_epoch();
        let op = KVOp::Write;
        let log = KVLog { op, dur_nanos, timestamp, cpu };
        log_out.send(log).unwrap();
    }
}

fn some_sink(rx: Receiver<Vec<u8>>) {
    let mut total = 0;
    let mut now = Instant::now();
    loop {
        if let Ok(v) = rx.try_recv() {
            total += v.len();
            if now.elapsed().as_secs_f64() > 1. {
                now = Instant::now();
                total = 0;
            }
            drop(v);
        }
    }
}

fn main() {
    let read_ratio = 0.9;
    let keys = 4096;

    let (tx, rx) = bounded(1000000);
    let log_sink = init_logging();
    for i in 0..4 {
        let rx = rx.clone();
        thread::spawn(move || {
            assert!(core_affinity::set_for_current(core_affinity::CoreId { id: 60 + i }));
            some_sink(rx)
        });
    }

    let mut handles = Vec::new();
    let mut dbs = Vec::new();
    for i in 0..THREADS/2 {
        let db_path = format!("/nvme/data/tmp/2024-vldb-{}", i);
        std::fs::remove_dir_all(&db_path);
        let db = setup_db(db_path.into()); // Arc::new(DashMap::new());
        dbs.push(db);
    }
    for i in 0..THREADS {
        //let db = db.clone();
        let db = dbs[i % dbs.len()].clone();
        let tx = tx.clone();
        let log_sink = log_sink.clone();
        handles.push(thread::spawn(move || {
            let key_low = 0;
            let key_high = 4096;
            do_work(db, read_ratio, key_low, key_high, tx, log_sink);
        }));
    }

    init_counter();

    for h in handles {
        let _ = h.join();
    }
}
