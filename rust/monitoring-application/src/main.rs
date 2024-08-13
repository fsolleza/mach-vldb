mod storage;

use api::kv_workload;
use api::monitoring_application::*;
use clap::*;
use crossbeam::channel::{bounded, Receiver, Sender};
use std::{
	io::{Read, Write},
	net::{TcpListener, TcpStream},
	sync::{
		atomic::{AtomicUsize, Ordering::SeqCst},
		Arc,
	},
	thread,
	time::Duration,
};
use storage::*;

static TOTAL_RECEIVED: AtomicUsize = AtomicUsize::new(0);
static TOTAL_DROPPED: AtomicUsize = AtomicUsize::new(0);
static TOTAL_WRITTEN: AtomicUsize = AtomicUsize::new(0);

static RECEIVED: AtomicUsize = AtomicUsize::new(0);
static WRITTEN: AtomicUsize = AtomicUsize::new(0);
static DROPPED: AtomicUsize = AtomicUsize::new(0);

fn stat_counter() {
	loop {
		let r = RECEIVED.swap(0, SeqCst);
		let d = DROPPED.swap(0, SeqCst);
		let w = WRITTEN.swap(0, SeqCst);

		TOTAL_RECEIVED.fetch_add(r, SeqCst);
		TOTAL_DROPPED.fetch_add(d, SeqCst);
		TOTAL_WRITTEN.fetch_add(w, SeqCst);

		println!("received: {} written: {} dropped: {}", r, w, d);
		std::thread::sleep(Duration::from_secs(1));
	}
}

fn data_receiver(mut stream: TcpStream, chan: Sender<RecordBatch>) {
	let mut msg_size = [0u8; 8];
	let mut msg_bytes: Vec<u8> = Vec::new();
	loop {
		if stream.read_exact(&mut msg_size[..]).is_ok() {
			let sz = u64::from_be_bytes(msg_size);
			msg_bytes.clear();
			msg_bytes.resize(sz as usize, 0u8);
			stream.read_exact(&mut msg_bytes[..]).unwrap();
			let records = RecordBatch::from_binary(&msg_bytes[..]);
			let l = records.len();
			RECEIVED.fetch_add(l, SeqCst);
			if chan.try_send(records).is_err() {
				DROPPED.fetch_add(l, SeqCst);
			} else {
				WRITTEN.fetch_add(l, SeqCst);
			}
		}
	}
}

fn init_ingestion<S: Storage>(data_addr: &str, mut store: S) {
	let (data_tx, data_rx) = bounded::<RecordBatch>(1024);
	println!("Setting up listener for data at {:?}", data_addr);
	let data_listener = TcpListener::bind(data_addr).unwrap();

	/*
	 * Setup an ingest thread to receive data over TCP keep track of
	 * statistics. This thread passes data over to the storage writer thread
	 */
	thread::spawn(move || {
		println!("Waiting for a connection");
		for stream in data_listener.incoming() {
			println!("Got a connection");
			let data_tx = data_tx.clone();
			thread::spawn(move || {
				data_receiver(stream.unwrap(), data_tx);
			});
		}
		println!("Exiting listener");
	});

	/*
	 * Setup the storage writer thread, receiving data over the channel and
	 * writing into storage
	 */
	thread::spawn(move || {
		while let Ok(batch) = data_rx.recv() {
			store.push_batch(&*batch);
		}
	});
}

fn handle_query<R: Reader>(args: Args, mut stream: TcpStream, reader: R) {
	let mut msg_size = [0u8; 8];
	let mut msg_bytes: Vec<u8> = Vec::new();

	/*
	 * Read from the stream
	 */
	stream.read_exact(&mut msg_size[..]).unwrap();
	let sz = u64::from_be_bytes(msg_size);
	msg_bytes.clear();
	msg_bytes.resize(sz as usize, 0u8);
	stream.read_exact(&mut msg_bytes[..]).unwrap();
	let request: Request = Request::from_binary(&msg_bytes[..]);

	/*
	 * Handle the request
	 */
	let response = match request {
		Request::DataReceived => {
			Response::DataReceived(TOTAL_RECEIVED.load(SeqCst) as u64)
		}
		Request::DataCompleteness => {
			let received = TOTAL_RECEIVED.load(SeqCst) as f64;
			let written = TOTAL_WRITTEN.load(SeqCst) as f64;
			let completeness = written / received;
			Response::DataCompleteness(completeness)
		}
		Request::KvOpsPercentile { .. } => reader.handle_query(&request),
		Request::ReadSyscalls { .. } => reader.handle_query(&request),
		Request::Scheduler { .. } => reader.handle_query(&request),
	};

	/*
	 * Write the response
	 */
	let response_bytes = response.to_binary();
	stream
		.write_all(&response_bytes.len().to_be_bytes())
		.unwrap();
	stream.write_all(&response_bytes).unwrap();
}

fn init_query_handler<R: Reader>(args: Args, reader: R) {
	let query_listener = TcpListener::bind(&args.query_addr.clone()).unwrap();

	/*
	 * Setup an ingest thread to receive data over TCP keep track of
	 * statistics. This thread passes data over to the storage writer thread
	 */
	thread::spawn(move || {
		for stream in query_listener.incoming() {
			println!("Got a query connection");
			let r = reader.clone();
			let args = args.clone();
			thread::spawn(move || {
				handle_query(args, stream.unwrap(), r);
			});
		}
		println!("Exiting listener");
	});
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
	/// Receive data using this address and port
	#[arg(short, long)]
	data_addr: String,

	/// Receive queries using this address and port
	#[arg(short, long)]
	query_addr: String,

	/// mem, mach, or  influx
	#[arg(short, long)]
	storage: String,
}

fn main() {
	let args = Args::parse();
	println!("Args: {:?}", args);

	thread::spawn(stat_counter);

	match args.storage.as_str() {
		"mem" => {
			let memstorage = Memstore::new();
			init_ingestion(&args.data_addr, memstorage.clone());
			let args = args.clone();
			init_query_handler(args, memstorage);
		}
		"mach" => {
			let mach = MachStore::new("/nvme/data/tmp/vldb/mach");
			let reader = mach.reader();
			init_ingestion(&args.data_addr, mach);
			let args = args.clone();
			init_query_handler(args, reader);
		},
		_ => panic!("unhandled storage argument {}", args.storage),
	}

	loop {
		thread::sleep(Duration::from_secs(10));
	}
}
