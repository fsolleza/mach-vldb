use common::{data::*, ipc::ipc_sender};
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use lazy_static::*;
use rand::prelude::*;
use rocksdb::{DBWithThreadMode, SingleThreaded, WriteOptions};
use std::{
	path::PathBuf,
	sync::{
		atomic::{compiler_fence, AtomicUsize, Ordering::SeqCst},
		//mpsc::{SyncSender, Receiver, sync_channel}
		Arc,
	},
	thread,
	time::{Duration, Instant},
};
use thread_priority::*;

type DB = Arc<DBWithThreadMode<SingleThreaded>>;
//type DB = Arc<DashMap<u64, Mutex<Vec<u8>>>>;

const THREADS: usize = 1;

static READ_COUNT: AtomicUsize = AtomicUsize::new(0);
static READ_TIME_NS: AtomicUsize = AtomicUsize::new(0);
static READ_BYTES: AtomicUsize = AtomicUsize::new(0);

static WRITE_COUNT: AtomicUsize = AtomicUsize::new(0);
static WRITE_TIME_NS: AtomicUsize = AtomicUsize::new(0);
static WRITE_BYTES: AtomicUsize = AtomicUsize::new(0);

static DROPPED: AtomicUsize = AtomicUsize::new(0);

//static OPS_MIN_RATE: AtomicUsize = AtomicUsize::new(0);

fn counter2() {
	let read_ops = READ_COUNT.swap(0, SeqCst);
	let write_ops = WRITE_COUNT.swap(0, SeqCst);
	let dropped = DROPPED.swap(0, SeqCst);
	let ops = read_ops + write_ops;
	println!(
		"Reads: {} Writes: {} Generated: {}, Dropped: {}",
		read_ops, write_ops, ops, dropped
	);
}

fn init_counter() {
	thread::spawn(move || loop {
		thread::sleep(Duration::from_secs(1));
		counter2();
	});
}

fn set_core_affinity(cpu: usize) {
	use core_affinity::{set_for_current, CoreId};
	assert!(set_for_current(CoreId { id: cpu }));
}

//fn random_core_affinity<const T: usize>() -> usize {
//    let mut rng = thread_rng();
//    let cpu = rng.gen::<usize>() % T;
//    set_core_affinity(cpu);
//    cpu
//}

//fn init_logging() -> Sender<Record> {
//    let (tx, rx) = unbounded();
//    let ipc = ipc_sender("0.0.0.0:3001", None);
//    thread::spawn(move || {
//        set_core_affinity(30);
//        egress(rx, ipc);
//    });
//    tx
//}
//
//fn egress(rx: Receiver<Record>, ipc: Sender<Vec<Record>>) {
//    let mut batch: Vec<Record> = Vec::new();
//
//    let mut hist: Histogram = Histogram::default();
//    let mut base_hist_timestamp = 0;
//
//    loop {
//        if let Ok(item) = rx.try_recv() {
//            let b = item.timestamp - item.timestamp % (1_000_000);
//            if b > base_hist_timestamp {
//                batch.push(Record {
//                    timestamp: base_hist_timestamp,
//                    data: Data::Hist(hist),
//                });
//                hist = Histogram::default();
//                base_hist_timestamp = b;
//            }
//
//            hist.max = hist.max.max(item.data.kv_log().unwrap().dur_nanos);
//            hist.cnt += 1;
//
//            batch.push(item);
//            if batch.len() >= 1024 {
//                let l = batch.len();
//                if ipc.try_send(batch).is_err() {
//                    DROPPED.fetch_add(l, SeqCst);
//                }
//                batch = Vec::new();
//            }
//        }
//    }
//}

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
	cpu: u64,
	out: Sender<Vec<u8>>,
) {
	lazy_static! {
		static ref DATA: Vec<u8> = random_data(1024 * 4);
	}

	let log_out = ipc_sender("0.0.0.0:3010", Some(16));

	let data: &'static [u8] = DATA.as_slice();
	assert!(set_current_thread_priority(ThreadPriority::Min).is_ok());

	let mut rng = thread_rng();

	let mut hist: Histogram = Histogram::default();
	let mut hist_timestamp = micros_since_epoch();
	let mut hist_start = Instant::now();
	let bounds = random_idx_bounds(data.len(), &mut rng);
	let mut tid = std::process::id() as u64;

	let mut records = Vec::new();
	let mut flush_counter = 0;
	let mut in_cpu = true;
	let mut current_cpu = cpu;
	set_core_affinity(current_cpu as usize);
	loop {
		let switch_cpu: f64 = rng.gen();
		if switch_cpu < 0.1 {
			if in_cpu {
				current_cpu = cpu + 1;
				in_cpu = false;
			} else {
				current_cpu = cpu;
				in_cpu = true;
			}
			set_core_affinity(current_cpu as usize);
		}

		let key: u64 = rng.gen_range(min_key..max_key);
		let read: bool = rng.gen::<f64>() < read_ratio;

		let now = Instant::now();
		if read {
			if let Some(vec) = do_read(&db, key) {
				out.send(vec).unwrap();

				let dur_nanos = now.elapsed().as_nanos() as u64;
				let timestamp = micros_since_epoch();

				records.push(Record {
					timestamp,
					data: Data::KV(KVLog {
						op: KVOp::Read,
						dur_nanos,
						cpu,
						tid,
					}),
				});
				let l = records.len();
				if l > 1024 {
					if log_out.try_send(records).is_err() {
						DROPPED.fetch_add(l, SeqCst);
					}
					records = Vec::new();
				}
				continue;
			}
		}

		let slice = &data[bounds.0..bounds.1];
		do_write(&db, key, slice);
		flush_counter += 1;
		//if flush_counter == 4096 {
		//    db.flush();
		//    flush_counter = 0;
		//}
		let dur_nanos = now.elapsed().as_nanos() as u64;
		let timestamp = micros_since_epoch();

		records.push(Record {
			timestamp,
			data: Data::KV(KVLog {
				op: KVOp::Write,
				dur_nanos,
				cpu,
				tid,
			}),
		});
		let l = records.len();
		if l > 1024 {
			if log_out.try_send(records).is_err() {
				DROPPED.fetch_add(l, SeqCst);
			}
			records = Vec::new();
		}
	}
}

fn some_sink(rx: Receiver<Vec<u8>>) {
	let mut now = Instant::now();
	loop {
		if let Ok(v) = rx.try_recv() {
			if now.elapsed().as_secs_f64() > 1. {
				now = Instant::now();
			}
			drop(v);
		}
	}
}

use clap::Parser;
#[derive(Parser, Debug)]
struct Args {
	#[arg(short, long)]
	cpu: u64,

	#[arg(short, long)]
	data_dir: String,
}

fn main() {
	let args = Args::parse();
	let cpu = args.cpu;
	let read_ratio = 0.9;
	let db_path = {
		let mut p = PathBuf::from(&args.data_dir);
		p.join(format!("2024-vldb-{}", cpu))
	};

	let (tx, rx) = bounded(1024);
	thread::spawn(move || some_sink(rx));

	let mut handles = Vec::new();
	//let db_path = format!("/nvme/data/tmp/2024-vldb-{}", cpu);
	let _ = std::fs::remove_dir_all(&db_path);
	let db = setup_db(db_path.into()); // Arc::new(DashMap::new());
	handles.push(thread::spawn(move || {
		let key_low = 0;
		let key_high = 4096;
		do_work(db, read_ratio, key_low, key_high, cpu, tx);
	}));

	init_counter();

	for h in handles {
		let _ = h.join();
	}
}
