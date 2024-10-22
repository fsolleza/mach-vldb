use crossbeam::channel::*;
use rayon::ThreadPoolBuilder;
use serde::*;
use std::{
	io::{stdout, Read, Write},
	net::{TcpListener, TcpStream, ToSocketAddrs},
	thread,
	time::Duration,
};

pub fn ipc_client_connect<A>(addr: A) -> Option<TcpStream>
where
	A: ToSocketAddrs,
{
	let mut stdout = stdout().lock();
	let mut retries = 0;
	loop {
		if let Ok(stream) = TcpStream::connect(&addr) {
			stream.set_nodelay(true).unwrap();
			println!("\nConnected!");
			return Some(stream);
		} else {
			retries += 1;
			if retries == 10 {
				break;
			}
			print!(".");
			stdout.flush().unwrap();
			thread::sleep(Duration::from_secs(1));
		}
	}
	None
}

pub fn ipc_send<S, T>(send: &S, stream: &mut TcpStream) -> Result<T, ()>
where
	S: serde::Serialize,
	for<'a> T: Deserialize<'a>,
{
	let mut bytes = bincode::serialize(send).unwrap();

	// Send
	stream.write_all(&bytes.len().to_be_bytes()).unwrap();
	stream.write_all(&bytes).unwrap();

	// Receive
	bytes.clear();
	let mut msg_sz = [0u8; 8];
	stream.read_exact(&mut msg_sz[..]).unwrap();
	let sz = usize::from_be_bytes(msg_sz);
	bytes.clear();
	bytes.resize(sz, 0u8);
	stream.read_exact(&mut bytes[..sz]).unwrap();

	let result: T = bincode::deserialize(&bytes[..]).unwrap();
	Ok(result)
}

pub fn ipc_serve<A, S, T, F>(addr: A, func: F)
//fn(s: S) -> T)
where
	A: ToSocketAddrs,
	for<'a> S: Deserialize<'a>,
	T: serde::Serialize,
	F: FnOnce(S) -> T + Send + Copy,
{
	let threadpool = ThreadPoolBuilder::new().build().unwrap();
	let listener = TcpListener::bind(addr).unwrap();
	for stream in listener.incoming() {
		threadpool.install(move || {
			handler(stream.unwrap(), func);
		});
	}
}

fn handler<S, T, F>(mut stream: TcpStream, func: F)
//fn(s: S) -> T)
where
	for<'a> S: Deserialize<'a>,
	T: serde::Serialize,
	F: FnOnce(S) -> T + Send + Copy,
{
	let mut buf = Vec::new();
	let mut msg_sz = [0u8; 8];

	// receive
	loop {
		if stream.read_exact(&mut msg_sz[..]).is_err() {
			break;
		}

		let sz = usize::from_be_bytes(msg_sz);
		buf.clear();
		buf.resize(sz, 0u8);

		if stream.read_exact(&mut buf[..sz]).is_err() {
			break;
		}

		// process
		let request: S = bincode::deserialize(&buf[..]).unwrap();
		let result = func(request);

		// Respond
		buf.clear();
		bincode::serialize_into(&mut buf, &result).unwrap();
		if stream.write_all(&buf.len().to_be_bytes()).is_err() {
			break;
		}
		if stream.write_all(&buf).is_err() {
			break;
		}
	}
}

pub type IpcReceiver<T> = Receiver<T>;
pub type IpcSender<T> = Sender<T>;

// Create a listener socket on the specified address that spawns a new thread to
// forward messages from incoming connections into the output receiver.
pub fn ipc_receiver<A, T>(addr: A) -> Receiver<T>
where
	A: ToSocketAddrs,
	for<'a> T: Deserialize<'a> + Send + 'static,
{
	let (tx, rx) = unbounded();
	let listener = TcpListener::bind(addr).unwrap();
	thread::spawn(move || tcp_listener(listener, tx));
	rx
}

pub fn ipc_sender<A, T>(addr: A, chan_sz: Option<usize>) -> Sender<T>
where
	A: ToSocketAddrs,
	T: Serialize + Send + 'static,
{
	let (tx, rx) = match chan_sz {
		Some(sz) => bounded(sz),
		None => unbounded(),
	};

	let mut stdout = stdout().lock();
	print!("Connecting to a receiver (10s timeout) ");
	stdout.flush().unwrap();
	let mut connected = false;
	let mut retries = 0;
	while !connected {
		if let Ok(stream) = TcpStream::connect(&addr) {
			stream.set_nodelay(true).unwrap();
			println!("\nConnected!");
			let rx = rx.clone();
			thread::spawn(move || sender_handler(stream, rx));
			connected = true;
		} else {
			retries += 1;
			if retries == 10 {
				break;
			}
			print!(".");
			stdout.flush().unwrap();
			thread::sleep(Duration::from_secs(1));
		}
	}
	println!();

	if !connected {
		thread::spawn(move || {
			println!("Can't connect, consuming messages to oblivion.");
			disconnected_sender_handler(rx);
		});
	}
	tx
}

fn receiver_handler<T>(mut stream: TcpStream, tx: Sender<T>)
where
	for<'a> T: Deserialize<'a> + Send + 'static,
{
	println!("Got a connection");
	let mut data = Vec::new();
	let mut msg_sz = [0u8; 8];
	let mut done = false;
	while !done {
		if stream.read_exact(&mut msg_sz[..]).is_ok() {
			let sz = usize::from_be_bytes(msg_sz);
			data.clear();
			data.resize(sz, 0u8);
			if stream.read_exact(&mut data[..sz]).is_ok() {
				let item: T = bincode::deserialize(&data).unwrap();
				if let Err(x) = tx.send(item) {
					println!("Unable to send samples to receiver, exiting");
					println!("Error: {:?}", x.to_string());
					done = true;
				}
			} else {
				done = true;
			}
		} else {
			done = true;
		}
	}
}

fn tcp_listener<T>(listener: TcpListener, tx: Sender<T>)
where
	for<'a> T: Deserialize<'a> + Send + 'static,
{
	for stream in listener.incoming() {
		let tx = tx.clone();
		thread::spawn(move || {
			receiver_handler(stream.unwrap(), tx);
		});
	}
}

fn sender_handler<T: Serialize>(mut stream: TcpStream, rx: Receiver<T>) {
	let mut bytes = Vec::new();
	loop {
		if let Ok(item) = rx.try_recv() {
			bincode::serialize_into(&mut bytes, &item).unwrap();
			stream.write_all(&bytes.len().to_be_bytes()).unwrap();
			stream.write_all(&bytes).unwrap();
			bytes.clear();
		}
	}
}

fn disconnected_sender_handler<T: Serialize>(rx: Receiver<T>) {
	while let Ok(_) = rx.recv() {}
}
