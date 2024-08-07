mod storage;
mod api;

use api::*;
use storage::*;
use std::{
	net::{TcpListener, TcpStream},
	thread,
	io::{Read, Write},
	sync::{Arc, atomic::{AtomicUsize, Ordering::SeqCst}},
	time::Duration,
};
use clap::*;
use crossbeam::channel::{bounded, Sender, Receiver};

static RECEIVED: AtomicUsize = AtomicUsize::new(0);
static WRITTEN: AtomicUsize = AtomicUsize::new(0);
static DROPPED: AtomicUsize = AtomicUsize::new(0);

fn stat_counter() {
	loop {
		let r = RECEIVED.swap(0, SeqCst);
		let d = DROPPED.swap(0, SeqCst);
		let w = WRITTEN.swap(0, SeqCst);
		println!("received: {} written: {} dropped: {}", r, w, d);
		std::thread::sleep(Duration::from_secs(1));
	}
}


fn data_receiver(mut stream: TcpStream, chan: Sender<RecordBatch>) {
	let mut msg_size = [0u8; 8];
	let mut msg_bytes: Vec<u8>= Vec::new();
	loop {
		if stream.read_exact(&mut msg_size[..]).is_ok() {
			let sz = u64::from_be_bytes(msg_size);

			msg_bytes.clear();
			msg_bytes.resize(sz as usize, 0u8);
			stream.read_exact(&mut msg_bytes[..]).unwrap();
			let records: RecordBatch =
				bincode::deserialize(&msg_bytes[..]).unwrap();
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

//fn query_responder<R: Reader>(mut stream: TcpStream, reader: &mut R) {
//	let mut msg_size = [0u8; 8];
//	let mut msg_bytes: Vec<u8>= Vec::new();
//
//	/*
//	 * Read from the stream
//	 */
//	stream.read_exact(&mut msg_size[..]).unwrap();
//	let sz = u64::from_be_bytes(msg_size);
//	msg_bytes.clear();
//	msg_bytes.resize(sz as usize, 0u8);
//	stream.read_exact(&mut msg_bytes[..]).unwrap();
//	let request: Request =
//		bincode::deserialize(&msg_bytes[..]).unwrap();
//
//	/*
//	 * Handle the request
//	 */
//
//	println!("Handling request");
//	let response = match request {
//		Request::Statistics => unimplemented!(),
//		Request::Data(data_request) => reader.handle_request(&data_request),
//	};
//
//	/*
//	 * Write the response
//	 */
//	let response_bytes = bincode::serialize(&response).unwrap();
//	stream.write_all(&response_bytes.len().to_be_bytes()).unwrap();
//	stream.write_all(&response_bytes).unwrap();
//}

fn init_ingestion<S: Storage>(data_addr: &str, store: S) {
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

//fn init_storage<S: Storage>(data_addr: &str, query_addr: &str, store: S) {
//
//	let (data_tx, data_rx) = bounded::<RecordBatch>(1024);
//	let mut reader = store.reader();
//	println!("Setting up listener for data at {:?}", data_addr);
//	let data_listener = TcpListener::bind(data_addr).unwrap();
//	println!("Setting up listener for queries at {:?}", query_addr);
//	let query_listener = TcpListener::bind(query_addr).unwrap();
//
//	/*
//	 * Setup an ingest thread to receive data over TCP keep track of
//	 * statistics. This thread passes data over to the storage writer thread
//	 */
//	thread::spawn(move || {
//		println!("Waiting for a connection");
//		for stream in data_listener.incoming() {
//			println!("Got a connection");
//			let data_tx = data_tx.clone();
//			thread::spawn(move || {
//				data_receiver(stream.unwrap(), data_tx);
//			});
//		}
//		println!("Exiting listener");
//	});
//
//	/*
//	 * Setup the storage writer thread, receiving data over the channel and
//	 * writing into storage
//	 */
//	thread::spawn(move || {
//		while let Ok(batch) = data_rx.recv() {
//			store.push_batch(&batch.records);
//		}
//	});
//
//	/*
//	 * Setup a thread that handles queries one at a time.
//	 */
//	thread::spawn(move || {
//		for stream in query_listener.incoming() {
//			let mut stream = stream.unwrap();
//			query_responder(stream, &mut reader);
//		}
//	});
//}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
	/// Receive data using this address and port
	#[arg(short, long)]
	data_addr: String,

	/// Receive queries using this address and port
	//#[arg(short, long)]
	//query_addr: String,

	/// mem, mach, or  influx
	#[arg(short, long)]
	storage: String,
}

fn main() {
	let args = Arc::new(Args::parse());
	println!("Args: {:?}", args);

	thread::spawn(stat_counter);

	match args.storage.as_str() {
		"mem" => {
			let memstorage = Memstore::new();
			init_ingestion(&args.data_addr, memstorage);
		},
		//"mach" => {
		//	let memstorage = Memstore::new();
		//	init_storage(&args.data_addr, &args.query_addr, memstorage);
		//},
		_ => panic!("unhandled storage argument"),
	}

	loop {
		thread::sleep(Duration::from_secs(10));
	}
}
