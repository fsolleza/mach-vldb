// SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause)
use core::time::Duration;

use anyhow::bail;
use anyhow::Result;
use clap::Parser;
use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::*;
use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::Skel;
use libbpf_rs::skel::SkelBuilder;
use libbpf_rs::PerfBufferBuilder;
use std::collections;
use std::process;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering::SeqCst};
use std::thread;
use std::process::exit;
use serde::*;
use std::collections::HashMap;

mod syscall_latency {
	include!(concat!(env!("OUT_DIR"), "/syscall_latency.skel.rs"));
}
use syscall_latency::*;

#[repr(C)]
#[derive(
	Serialize, Deserialize, Eq, PartialEq, Copy, Clone, Debug, Default,
)]
pub struct SyscallEvent {
	pub pid: u32,
	pub tid: u32,
	pub syscall_number: u64,
	pub timestamp: u64,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct SyscallEventBuffer {
	pub len: u32,
	pub buffer: [SyscallEvent; 256],
}

pub enum EventBuffer {
	Enter(SyscallEventBuffer),
	Exit(SyscallEventBuffer),
}

pub struct SyscallDuration {
	syscall_number: u64,
	duration: u64,
}

lazy_static! {
	static ref ARGS: Args = Args::parse();
}

static COUNTER: AtomicUsize = AtomicUsize::new(0);
static DROPPED: AtomicUsize = AtomicUsize::new(0);
static DONE: AtomicBool = AtomicBool::new(false);

fn counter() {
	let mut sec = 0;
	while !(DONE.load(SeqCst)) {
		let count = COUNTER.swap(0, SeqCst);
		let dropped = DROPPED.swap(0, SeqCst);
		println!("{} Retrieved: {}, Dropped: {}", sec, count, dropped);
		std::thread::sleep(std::time::Duration::from_secs(1));
		sec += 1;
	}
	thread::sleep(std::time::Duration::from_secs(2));
}

pub fn bump_memlock_rlimit() -> Result<()> {
	let rlimit = libc::rlimit {
		rlim_cur: 128 << 20,
		rlim_max: 128 << 20,
	};

	if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &rlimit) } != 0 {
		bail!("Failed to increase rlimit");
	}

	Ok(())
}

fn sink(rx: Receiver<Vec<SyscallDuration>>) {
	while let Ok(events) = rx.recv() {
		COUNTER.fetch_add(events.len(), SeqCst);
	}
}

fn combiner(rx: Receiver<EventBuffer>, tx: Sender<Vec<SyscallDuration>>) {
	let mut entry_events = HashMap::new();
	let mut entry_event_key_count = HashMap::new();
	let mut exit_event_key_count = HashMap::new();

	while let Ok(events) = rx.recv() {
		match events {
			EventBuffer::Enter(x) => {
				for i in 0..x.len as usize {
					let event = x.buffer[i];
					let key = {
						let key = (event.pid, event.tid, event.syscall_number);
						let entry = entry_event_key_count.entry(key).or_insert(0);
						let cnt = *entry;
						*entry += 1;
						(key.0, key.1, key.2, cnt)
					};
					assert!(entry_events.insert(key, event.timestamp).is_none());
				}
			}

			EventBuffer::Exit(x) => {
				let mut durations = Vec::new();
				for i in 0..x.len as usize {
					let event = x.buffer[i];
					let key = {
						let key = (event.pid, event.tid, event.syscall_number);
						let entry = exit_event_key_count.entry(key).or_insert(0);
						let cnt = *entry;
						*entry += 1;
						(key.0, key.1, key.2, cnt)
					};

					if let Some(start) = entry_events.remove(&key) {
						let duration = event.timestamp - start;
						let x = SyscallDuration {
							syscall_number: event.syscall_number,
							duration,
						};
						durations.push(x);
					}
				}
				tx.send(durations).unwrap();
				durations = Vec::new();
			}
		}
	}
}

fn exit_event_handler(_cpu: i32, bytes: &[u8]) {
	let bytes_ptr = bytes.as_ptr();
	let ptr = bytes_ptr as *const SyscallEventBuffer;
	let event_buffer = unsafe { *ptr };
}


fn enter_event_handler(_cpu: i32, bytes: &[u8]) {
	let bytes_ptr = bytes.as_ptr();
	let ptr = bytes_ptr as *const SyscallEventBuffer;
	let event_buffer = unsafe { *ptr };
}

fn lost_event_handler(cpu: i32, count: u64) {
	eprintln!("Lost {count} events on CPU {cpu}");
}

fn attach(target_pids: &[u32], combiner_tx: Sender<EventBuffer>) {
	println!("Attaching");
	let skel_builder = SyscallLatencySkelBuilder::default();
	let mut open_skel = skel_builder.open().unwrap();
	for (i, p) in target_pids.iter().enumerate() {
		open_skel.rodata().TARGET_PIDS[i] = *p;
	}

	let mut skel = open_skel.load().unwrap();
	skel.attach().unwrap();

	{
		let sender = combiner_tx.clone();
		let perf = PerfBufferBuilder::new(skel.maps_mut().perf_array_syscall_enter())
			.sample_cb(move |_cpu: i32, bytes: &[u8]| {
				let bytes_ptr = bytes.as_ptr();
				let ptr = bytes_ptr as *const SyscallEventBuffer;
				let event_buffer = unsafe { *ptr };
				let x = EventBuffer::Enter(event_buffer);
				sender.send(x).unwrap();
			})
			.lost_cb(lost_event_handler)
			.build().unwrap();
		thread::spawn(move || {
			loop {
				perf.poll(Duration::from_secs(1)).unwrap();
			}
			println!("Exiting");
		});
	}

	{
		let sender = combiner_tx.clone();
		let perf = PerfBufferBuilder::new(skel.maps_mut().perf_array_syscall_exit())
			.sample_cb(move |_cpu: i32, bytes: &[u8]| {
				let bytes_ptr = bytes.as_ptr();
				let ptr = bytes_ptr as *const SyscallEventBuffer;
				let event_buffer = unsafe { *ptr };
				let x = EventBuffer::Exit(event_buffer);
				sender.send(x).unwrap();
			})
			.lost_cb(lost_event_handler)
			.build().unwrap();
		thread::spawn(move || {
			loop {
				perf.poll(Duration::from_secs(10)).unwrap();
			}
		});
	}

	while !DONE.load(SeqCst) {
		thread::sleep(Duration::from_secs(1));
	}

}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
	#[arg(short, long, num_args = 1.., value_delimiter = ' ')]
	pids: Vec<u32>,
}

fn main() {
	bump_memlock_rlimit();
	let args = (*ARGS).clone();
	thread::spawn(counter);

	let (sink_tx, sink_rx) = unbounded();
	thread::spawn(move || {
		sink(sink_rx);
	});

	let (combiner_tx, combiner_rx) = unbounded();
	thread::spawn(move || {
		combiner(combiner_rx, sink_tx);
	});

	thread::spawn(move || {
		use std::process;
		process::id();
	});

	attach(args.pids.as_slice(), combiner_tx);


	println!("Exiting done watcher ");
}