//use common::{data::*, ipc::ipc_sender};
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
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
	net::TcpStream,
	io::Write,
};
use thread_priority::*;
use monitoring_application::*;
use clap::*;

type DB = Arc<DBWithThreadMode<SingleThreaded>>;

const THREADS: usize = 1;

static READ_COUNT: AtomicUsize = AtomicUsize::new(0);
static READ_TIME_NS: AtomicUsize = AtomicUsize::new(0);
static READ_BYTES: AtomicUsize = AtomicUsize::new(0);

static WRITE_COUNT: AtomicUsize = AtomicUsize::new(0);
static WRITE_TIME_NS: AtomicUsize = AtomicUsize::new(0);
static WRITE_BYTES: AtomicUsize = AtomicUsize::new(0);

static DROPPED: AtomicUsize = AtomicUsize::new(0);

//static OPS_MIN_RATE: AtomicUsize = AtomicUsize::new(0);

pub fn micros_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

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

fn sender(rx: Receiver<Vec<Record>>, addrs: Vec<String>) {
	println!("Connecting to {:?}", addrs);
	let mut streams: Vec<TcpStream> = addrs
		.into_iter()
		.map(|x| TcpStream::connect(x).unwrap())
		.collect();
	println!("Connected!");
	while let Ok(records) = rx.recv() {
		let records = RecordBatch {
			record_type: RecordType::KVOp,
			records,
		};
		let data = serialize(&records);
		let sz = data.len();
		for stream in streams.iter_mut() {
			stream.write_all(&sz.to_be_bytes()).unwrap();
			stream.write_all(&data).unwrap();
		}
	}
}

fn do_work(
	db: DB,
	read_ratio: f64,
	min_key: u64,
	max_key: u64,
	cpu: u64,
	monitoring_tx: Sender<Vec<Record>>,
	freeing_tx: Sender<Vec<u8>>,
) {
	lazy_static! {
		static ref DATA: Vec<u8> = random_data(1024 * 4);
	}

	let data: &'static [u8] = DATA.as_slice();

	let mut rng = thread_rng();

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
				freeing_tx.send(vec).unwrap();

				let dur_micros = now.elapsed().as_micros() as u64;
				let timestamp = micros_since_epoch();

				records.push({
					let mut r = Record::default();
					r.record_type = RecordType::KVOp;
					r.timestamp_micros = timestamp;
					r.kv_op = KVOp::Read;
					r.cpu = current_cpu;
					r.duration_micros = dur_micros;
					r
				});

				let l = records.len();
				if l > 1024 {
					if monitoring_tx.try_send(records).is_err() {
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
		let dur_micros = now.elapsed().as_micros() as u64;
		let timestamp = micros_since_epoch();

		records.push({
			let mut r = Record::default();
			r.record_type = RecordType::KVOp;
			r.timestamp_micros = timestamp;
			r.kv_op = KVOp::Write;
			r.cpu = current_cpu;
			r.duration_micros = dur_micros;
			r
		});

		let l = records.len();
		if l > 1024 {
			if monitoring_tx.try_send(records).is_err() {
				DROPPED.fetch_add(l, SeqCst);
			}
			records = Vec::new();
		}
	}
}

fn some_sink(rx: Receiver<Vec<u8>>) {
	let mut now = Instant::now();
	while let Ok(v) = rx.recv() {
		if now.elapsed().as_secs_f64() > 1. {
			now = Instant::now();
		}
		drop(v);
	}
}

#[derive(Parser, Debug)]
struct Args {
	#[arg(short, long)]
	cpu: u64,

	#[arg(short, long)]
	data_dir: String,

	#[arg(short, long, value_parser, num_args = 1.., value_delimiter = ' ')]
	data_addrs: Vec<String>,
}

fn main() {
	let args = Args::parse();
	let cpu = args.cpu;
	let read_ratio = 0.9;
	let db_path = {
		let mut p = PathBuf::from(&args.data_dir);
		p.join(format!("2024-vldb-{}", cpu))
	};

	let (freeing_tx, freeing_rx) = bounded(1024);
	thread::spawn(move || {
		assert!(set_current_thread_priority(ThreadPriority::Min).is_ok());
		some_sink(freeing_rx);
	});

	let (monitoring_tx, monitoring_rx) = bounded(1024);
	let addrs = args.data_addrs.clone();
	thread::spawn(move || {
		assert!(set_current_thread_priority(ThreadPriority::Min).is_ok());
		sender(monitoring_rx, addrs);
	});

	let mut handles = Vec::new();
	//let db_path = format!("/nvme/data/tmp/2024-vldb-{}", cpu);
	let _ = std::fs::remove_dir_all(&db_path);
	let db = setup_db(db_path.into()); // Arc::new(DashMap::new());
	handles.push(thread::spawn(move || {
		assert!(set_current_thread_priority(ThreadPriority::Min).is_ok());
		let key_low = 0;
		let key_high = 4096;
		do_work(db, read_ratio, key_low, key_high, cpu, monitoring_tx, freeing_tx);
	}));

	init_counter();

	for h in handles {
		let _ = h.join();
	}
}