mod core;

use common::{data::*, ipc::*};
use core::*;
use fxhash::FxHashMap;
use mach_lib::*;
use std::{
	sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
	thread,
	time::Duration,
};
use clap::Parser;

static MACH_COUNT_PER_SEC: AtomicUsize = AtomicUsize::new(0);
static MACH_COUNT: AtomicUsize = AtomicUsize::new(0);

fn counter() {
	let mut last_count = 0;
	loop {
		let count = MACH_COUNT.load(SeqCst);
		println!("Mach Count {}", count - last_count);
		MACH_COUNT_PER_SEC.swap(count - last_count, SeqCst);
		last_count = count;
		thread::sleep(Duration::from_secs(1));
	}
}

fn handle_writes(mach: &mut Mach, batch: Vec<Record>) {
	let now = micros_since_epoch();
	let mut source = 0;
	let mut partition = 0;
	let mut sl = Vec::new();
	for item in batch.iter() {
		match item.data {
			Data::Hist(_) => {
				source = 0;
				partition = 0;
			}
			Data::KV(_) => {
				source = 1;
				partition = 1;
			}
			Data::Sched(_) => {
				source = 2;
				partition = 2;
			}
		}
		bincode::serialize_into(&mut sl, &item).unwrap();
		mach.push(SourcePartition::new(source, partition), now, sl.as_slice());
		sl.clear();
	}
	MACH_COUNT.fetch_add(batch.len(), SeqCst);
}

fn get_histogram(
	reader: MachReader,
	min_ts: u64,
	max_ts: u64,
) -> Vec<(u64, u64)> {
	let snapshot = reader.snapshot(&[SourcePartition::new(0, 0)]).snapshot();
	let mut iterator = snapshot.iterator();

	let mut result = Vec::new();
	while let Some(entry) = iterator.next_entry() {
		if entry.timestamp < min_ts {
			break;
		}
		if entry.timestamp >= max_ts {
			continue;
		}
		let x = entry.timestamp;
		let item: Record = bincode::deserialize_from(&entry.data[..]).unwrap();
		let y = item.data.hist().unwrap().cnt;
		result.push((x, y));
	}
	result
}

fn get_mach_op(
	reader: MachReader,
	min_ts: u64,
	_: u64,
) -> Vec<((u64, u64), u64)> {
	unimplemented!()
}

fn get_mach_cpu(
	reader: MachReader,
	min_ts: u64,
	_: u64,
) -> Vec<((u64, u64), u64)> {
	let sp = SourcePartition::new(1, 1);
	let snapshot = reader.snapshot(&[sp]).snapshot();
	let mut iter = snapshot.iterator();

	let mut group_count: FxHashMap<(u64, u64), u64> = FxHashMap::default();

	while let Some(entry) = iter.next_entry() {
		if entry.timestamp < min_ts {
			break;
		}
		let record: Record =
			bincode::deserialize_from(&entry.data[..]).unwrap();
		let kv_log = record.data.kv_log().unwrap();

		let cpu = kv_log.cpu;
		let ts = entry.timestamp - entry.timestamp % 1_000_000;

		let entry = group_count.entry((ts, cpu)).or_insert(0);
		*entry += 1;
	}

	group_count.drain().collect()
}

fn get_mach_sched(
	reader: MachReader,
	min_ts: u64,
	max_ts: u64,
) -> Vec<((u64, [u8; 16]), u64)> {
	let sp = SourcePartition::new(2, 2);
	let snapshot = reader.snapshot(&[sp]).snapshot();
	let mut iter = snapshot.iterator();

	let mut group_count: FxHashMap<(u64, [u8; 16]), u64> = FxHashMap::default();

	while let Some(entry) = iter.next_entry() {
		if entry.timestamp < min_ts {
			break;
		}
		let record: Record =
			bincode::deserialize_from(&entry.data[..]).unwrap();
		let sched = record.data.sched().unwrap();

		let ts = entry.timestamp - entry.timestamp % 1_000_000;
		let comm = sched.comm;
		let entry = group_count.entry((ts, comm)).or_insert(0);
		*entry += 1;
	}
	group_count.drain().collect()
}

fn read_mach(reader: MachReader, request: MachRequest) -> MachResponse {
	match request {
		MachRequest::ByCpu(min, max) => {
			let r = get_mach_cpu(reader, min, max);
			MachResponse::ByCpu(r)
		}
		MachRequest::ByOp(min, max) => {
			let r = get_mach_op(reader, min, max);
			MachResponse::ByCpu(r)
		}
		MachRequest::ByComm(min, max) => {
			let r = get_mach_sched(reader, min, max);
			MachResponse::ByComm(r)
		}
		MachRequest::Hist(min, max) => {
			let r = get_histogram(reader, min, max);
			MachResponse::Hist(r)
		}
		MachRequest::Count => {
			MachResponse::Count(MACH_COUNT_PER_SEC.load(SeqCst) as u64)
		}
	}
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
	/// Receive signals from driver here
	#[arg(short, long)]
	mach_path: String,
}

fn main() {
	let args = Args::parse();
	let mut mach =
		Mach::new(args.mach_path.into(), true, Duration::from_secs(1));
	let mach_reader = mach.reader();

	let mut handles = Vec::new();

	handles.push(thread::spawn(counter));

	let rx: IpcReceiver<Vec<Record>> = ipc_receiver("0.0.0.0:3020");
	handles.push(thread::spawn(move || {
		while let Ok(batch) = rx.recv() {
			handle_writes(&mut mach, batch);
		}
	}));

	handles.push(thread::spawn(move || {
		let handle_queries = |r: MachRequest| -> MachResponse {
			let reader = mach_reader.clone();
			read_mach(reader, r)
		};
		common::ipc::ipc_serve("0.0.0.0:3021", handle_queries);
	}));

	for handle in handles {
		handle.join().unwrap();
	}
}
