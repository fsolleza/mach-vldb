use api::monitoring_application::{Record, RecordBatch};

use lazy_static::*;
use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::Skel;
use libbpf_rs::skel::SkelBuilder;
use libbpf_rs::PerfBuffer;
use libbpf_rs::PerfBufferBuilder;
use libbpf_rs::RingBuffer;
use libbpf_rs::RingBufferBuilder;
use std::{
	mem,
	process,
	slice,
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
		Arc,
	},
	net::TcpStream,
	io::Write,
	thread,
	time::Duration,
};
use crossbeam::channel::{bounded, Receiver, Sender};

use clap::Parser;

mod sched_switch {
	include!(concat!(env!("OUT_DIR"), "/sched_switch.skel.rs"));
}

use sched_switch::*;

lazy_static! {
	static ref RECORD_CHAN: (Sender<Record>, Receiver<Record>) = bounded(4096);
}

fn bump_memlock_rlimit() -> Result<(), ()> {
	let rlimit = libc::rlimit {
		rlim_cur: 128 << 20,
		rlim_max: 128 << 20,
	};

	if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &rlimit) } != 0 {
		return Err(());
	}

	Ok(())
}

static COUNT: AtomicUsize = AtomicUsize::new(0);
static DROPPED: AtomicUsize = AtomicUsize::new(0);
static EVENTS: AtomicUsize = AtomicUsize::new(0);

fn init_counter() {
	loop {
		let c = COUNT.swap(0, SeqCst);
		let d = DROPPED.swap(0, SeqCst);
		let e = EVENTS.swap(0, SeqCst);
		println!("Count: {} {} {}", c, d, e);
		std::thread::sleep(Duration::from_secs(1));
	}
}

fn sender(rx: Receiver<Record>, addrs: Vec<String>) {
	println!("Connecting to {:?}", addrs);
	let mut streams: Vec<TcpStream> = addrs
		.into_iter()
		.map(|x| TcpStream::connect(x).unwrap())
		.collect();
	println!("Connected!");

	let mut records = Vec::new();
	while let Ok(record) = rx.recv() {
		records.push(record);
		if records.len() == 1024 {
			let r = RecordBatch {
				inner: records,
			};
			let data = r.to_binary();
			let sz = data.len();
			for stream in streams.iter_mut() {
				stream.write_all(&sz.to_be_bytes()).unwrap();
				stream.write_all(&data).unwrap();
			}
			COUNT.fetch_add(1024, SeqCst);
			records = r.inner;
			records.clear();
		}
	}
}

fn parse_comm(comm: [i8; 16]) -> [u8; 16] {
	use std::ffi::CStr;
	let mut replace = [b'\0'; 16];
	unsafe {
		let x: Vec<&str> = CStr::from_ptr(comm[..].as_ptr())
			.to_str()
			.unwrap()
			.split("/")
			.take(1)
			.collect();
		let sl = x[0].as_bytes();
		replace[..sl.len()].copy_from_slice(sl);
	};
	replace
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone)]
struct Event {
	prev_pid: u64,
	next_pid: u64,
	cpu: u64,
	timestamp_micros: u64,
	comm: [i8; 16],
}

impl Event {
	pub fn to_record(&self) -> Record {
		Record::Scheduler {
			timestamp_micros: self.timestamp_micros,
			prev_pid: self.prev_pid,
			next_pid: self.next_pid,
			comm: parse_comm(self.comm),
			cpu: self.cpu,
		}
	}
}

fn copy_from_bytes(e: &mut Event, bytes: &[u8]) {
	let sz = mem::size_of_val(e);
	if bytes.len() < sz {
		panic!("too few bytes");
	}
	unsafe {
		let bptr = e as *mut Event as *mut u8;
		slice::from_raw_parts_mut(bptr, sz).copy_from_slice(&bytes[..sz]);
	}
}

fn handler(cpu: i32, bytes: &[u8]) -> i32 {
	let sender = RECORD_CHAN.0.clone();
	let mut event = Event::default();
	copy_from_bytes(&mut event, bytes);
	EVENTS.fetch_add(1, SeqCst);
	sender.send(event.to_record()).unwrap();
	0
}

fn handle_pb_event(cpu: i32, data: &[u8]) {
	handler(cpu, data);
}

fn handle_lost_events(cpu: i32, count: u64) {
	eprintln!("Lost {count} events on CPU {cpu}");
}

fn attach(_target_pid: u32) -> Result<(), libbpf_rs::Error> {
	use core_affinity::{set_for_current, CoreId};
	assert!(set_for_current(CoreId { id: 61 }));
	let skel_builder = SchedSwitchSkelBuilder::default();
	let mut open_skel = skel_builder.open()?;
	//open_skel.rodata().target_pid1 = target_pid;
	open_skel.rodata().this_pid = std::process::id();
	let mut skel = open_skel.load()?;
	skel.attach()?;
	let perf = PerfBufferBuilder::new(skel.maps_mut().pb())
		.sample_cb(handle_pb_event)
		.lost_cb(handle_lost_events)
		.build()?;
	loop {
		perf.poll(Duration::from_secs(10))?;
	}
}

#[derive(Parser, Debug)]
struct Args {
	#[arg(short, long)]
	data_addrs: Vec<String>,
}

fn main() {
	let args = Args::parse();
	bump_memlock_rlimit().unwrap();
	thread::spawn(init_counter);

	let (_tx, rx) = RECORD_CHAN.clone();
	thread::spawn(move || sender(rx, args.data_addrs.clone()));
	thread::spawn(move || attach(0)).join().unwrap();
}
