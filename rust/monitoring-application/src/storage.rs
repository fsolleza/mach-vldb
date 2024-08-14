use api::monitoring_application::*;
use fxhash::FxHashMap;
use std::{
	sync::{Arc, RwLock},
	time::SystemTime,
};

use influxdb_line_protocol::LineProtocolBuilder;
use influxdb_iox_client::write::Client as WriteClient;
use influxdb_iox_client::flight::Client as ReadClient;
use influxdb_iox_client::connection::Builder;
use influxdb_iox_client::format::QueryOutputFormat;
use futures::stream::TryStreamExt;
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use serde_json::Value;
use chrono::{DateTime, Utc};

use mach_lib::{Entry, Partitions, PartitionsReader};


pub trait Storage: Sync + Send + 'static {
	fn push_batch(&mut self, records: &[Record]);
	//fn reader(&self) -> impl Reader;
}

pub trait Reader: Sync + Send + 'static + Clone {
	fn handle_query(&self, query: &Request) -> Response;
}

#[derive(Clone)]
pub struct InfluxStore {
	tuple_id: u64,
	last_timestamp_nanos: u64,
	write_addr: String,
	read_addr: String,
}

impl InfluxStore {

	fn do_write(&mut self, data: &[Record]) {
		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();
		let write_addr = self.write_addr.clone();
		let connection = rt.block_on(async {
			Builder::default().build(&write_addr).await.unwrap()
		});
		let mut client = WriteClient::new(connection);
	
		let now = micros_since_epoch();
		let buf = self.build_lp(&data);
		let lp = std::str::from_utf8(&buf[..]).unwrap();
		rt.block_on(async {
			client.write_lp("vldb_demo", lp).await.unwrap();
		});
	}

	fn build_lp(&mut self, data: &[Record]) -> Vec<u8> {
		let mut lp = LineProtocolBuilder::new();
		let timestamp_nanos = micros_since_epoch() * 1000;

		// Reset tuple_id if the timestamp is now new. This way, we don't make
		// excessively too many tags
		if self.last_timestamp_nanos != timestamp_nanos {
			self.tuple_id = 0;
		}
		self.last_timestamp_nanos = timestamp_nanos;

		let mut tuple_id = self.tuple_id;
		let mut tuple_id_buf = String::new();
		for r in data {

			// Put together deduplication tuple tag
			tuple_id += 1;
			tuple_id_buf.clear();
			{
				use std::fmt::Write;
				write!(&mut tuple_id_buf, "{}", tuple_id);
			}

			match *r {
				Record::KVOp {
					cpu,
					timestamp_micros,
					duration_micros,
				} => {
					lp = lp
						.measurement("table_kvop")
						.tag("id", &tuple_id_buf)
						.field("cpu", cpu)
						.field("timestamp_micros", timestamp_micros)
						.field("duration_micros", duration_micros)
						.timestamp(timestamp_nanos as i64)
						.close_line();
				}
				Record::Syscall {
					syscall_number,
					timestamp_micros,
					duration_micros,
				} => {
					lp = lp
						.measurement("table_syscall")
						.tag("id", &tuple_id_buf)
						.field("syscall_number", syscall_number)
						.field("timestamp_micros", timestamp_micros)
						.field("duration_micros", duration_micros)
						.timestamp(timestamp_nanos as i64)
						.close_line();
				}
				Record::Scheduler {
					prev_pid,
					next_pid,
					cpu,
					timestamp_micros,
					comm,
				} => {
					lp = lp
						.measurement("table_sched")
						.tag("id", &tuple_id_buf)
						.field("prev_pid", prev_pid)
						.field("next_pid", next_pid)
						.field("cpu", cpu)
						.field("timestamp_micros", timestamp_micros)
						.timestamp(timestamp_nanos as i64)
						.close_line();
				}
			}
		}

		self.tuple_id = tuple_id;
		lp.build()
	}

	fn handle_query(&self, query: &str) -> Value {

		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();

		let query: String = query.into();
		let read_addr: String = self.read_addr.clone();

		let batches: Vec<_> = rt.block_on(async {
			let connection = Builder::default()
				.build(read_addr)
				.await
				.unwrap();
			let mut client = ReadClient::new(connection);
			client.add_header("bucket", "vldb_demo").unwrap();
			let mut query_results =
				client.influxql("vldb_demo", query.clone()).await.unwrap();
			let mut batches: Vec<_> =
				(&mut query_results).try_collect().await.unwrap();
			let schema = query_results
				.inner()
				.schema()
				.cloned()
				.ok_or(influxdb_iox_client::flight::Error::NoSchema)
				.unwrap();
			batches.push(ArrowRecordBatch::new_empty(schema));
			batches
		});

		let json_string = QueryOutputFormat::Json.format(&batches).unwrap();
		serde_json::from_str(&json_string).unwrap()
	}

	fn exec_scheduler(&self, low: u64, high: u64) -> Vec<Record> {
		let low = DateTime::from_timestamp_micros(low as i64).unwrap();
		let high = DateTime::from_timestamp_micros(high as i64).unwrap();
		let query = format!(
    	    "SELECT * FROM table_sched WHERE time >= '{}' AND time <= '{}'",
			low, high
    	);
		let result = self.handle_query(&query);
		println!("Scheduler requests: {:?}", result);
		Vec::new()
	}

	fn exec_kvop(&self, low: u64, high: u64, tile: f64) -> Vec<Record> {
		let low = DateTime::from_timestamp_micros(low as i64).unwrap();
		let high = DateTime::from_timestamp_micros(high as i64).unwrap();
		let query = format!(
    	    "SELECT * FROM table_kvop WHERE time >= '{}' AND time <= '{}'",
			low, high
    	);
		let result = self.handle_query(&query);
		println!("Scheduler requests: {:?}", result);
		Vec::new()
	}

	fn exec_read_syscalls(&self, low: u64, high: u64, tile: f64) -> Vec<Record> {
		let low = DateTime::from_timestamp_micros(low as i64).unwrap();
		let high = DateTime::from_timestamp_micros(high as i64).unwrap();
		let query = format!(
    	    "SELECT * FROM table_syscalls WHERE time >= '{}' AND time <= '{}'",
			low, high
    	);
		let result = self.handle_query(&query);
		println!("Scheduler requests: {:?}", result);
		Vec::new()
	}
}

impl Storage for InfluxStore {
	fn push_batch(&mut self, records: &[Record]) {
		self.do_write(records);
	}
}

impl Reader for InfluxStore {
	fn handle_query(&self, query: &Request) -> Response {
		match query {
			Request::KvOpsPercentile {
				low_ts,
				high_ts,
				tile,
			} => {
				let records =
					self.exec_kvop(*low_ts, *high_ts, *tile);
				Response::KvOpsPercentile(records)
			}
			Request::ReadSyscalls {
				low_ts,
				high_ts,
				tile,
			} => {
				let records = self.exec_read_syscalls(*low_ts, *high_ts, *tile);
				Response::ReadSyscalls(records)
			}
			Request::Scheduler { low_ts, high_ts } => {
				let records = self.exec_scheduler(*low_ts, *high_ts);
				Response::Scheduler(records)
			}
			Request::DataReceived => unreachable!(),
			Request::DataCompleteness => unreachable!(),
		}
	}
}

pub struct MachStore {
	inner: Partitions,
}

impl MachStore {
	pub fn new(path: &str) -> Self {
		let inner = Partitions::new(path.into());
		Self { inner }
	}

	pub fn reader(&self) -> MachReader {
		MachReader {
			inner: self.inner.reader(),
		}
	}

	fn push_kvop(
		&mut self,
		timestamp: u64,
		cpu: u64,
		timestamp_micros: u64,
		duration_micros: u64,
	) {
		let mut bytes = [0u8; 24];
		bytes[0..8].copy_from_slice(&cpu.to_be_bytes());
		bytes[8..16].copy_from_slice(&timestamp_micros.to_be_bytes());
		bytes[16..24].copy_from_slice(&duration_micros.to_be_bytes());
		self.inner.push(0, 0, timestamp, &bytes);
	}

	fn push_syscall(
		&mut self,
		timestamp: u64,
		syscall_number: u64,
		timestamp_micros: u64,
		duration_micros: u64,
	) {
		let mut bytes = [0u8; 24];
		bytes[0..8].copy_from_slice(&syscall_number.to_be_bytes());
		bytes[8..16].copy_from_slice(&timestamp_micros.to_be_bytes());
		bytes[16..24].copy_from_slice(&duration_micros.to_be_bytes());
		self.inner.push(1, 1, timestamp, &bytes);
	}

	fn push_scheduler(
		&mut self,
		timestamp: u64,
		prev_pid: u64,
		next_pid: u64,
		cpu: u64,
		timestamp_micros: u64,
		comm: [u8; 16],
	) {
		let mut bytes = [0u8; 48];
		bytes[0..8].copy_from_slice(&prev_pid.to_be_bytes());
		bytes[8..16].copy_from_slice(&next_pid.to_be_bytes());
		bytes[16..24].copy_from_slice(&cpu.to_be_bytes());
		bytes[24..32].copy_from_slice(&timestamp_micros.to_be_bytes());
		bytes[32..].copy_from_slice(&comm);
		self.inner.push(2, 1, timestamp, &bytes);
	}
}

impl Storage for MachStore {
	fn push_batch(&mut self, records: &[Record]) {
		let ts = micros_since_epoch();
		let mut sync_kvop = false;
		let mut sync_sched = false;
		let mut sync_syscall = false;
		for r in records {
			match *r {
				Record::KVOp {
					cpu,
					timestamp_micros,
					duration_micros,
				} => {
					self.push_kvop(ts, cpu, timestamp_micros, duration_micros);
					sync_kvop = true;
				}
				Record::Syscall {
					syscall_number,
					timestamp_micros,
					duration_micros,
				} => {
					self.push_syscall(
						ts,
						syscall_number,
						timestamp_micros,
						duration_micros,
					);
					sync_syscall = true;
				}
				Record::Scheduler {
					prev_pid,
					next_pid,
					cpu,
					timestamp_micros,
					comm,
				} => {
					self.push_scheduler(
						ts,
						prev_pid,
						next_pid,
						cpu,
						timestamp_micros,
						comm,
					);
					sync_sched = true;
				}
			}
		}

		if sync_kvop {
			self.inner.sync(0, 0);
		}
		if sync_syscall {
			self.inner.sync(1, 1);
		}
		if sync_sched {
			self.inner.sync(2, 1);
		}
	}
}

#[derive(Clone)]
pub struct MachReader {
	inner: PartitionsReader,
}

impl MachReader {
	fn exec_scheduler(&self, low: u64, high: u64) -> Vec<Record> {
		let mut events = Vec::new();

		let snapshot = match self.inner.snapshot(&[(2, 1)]) {
			Some(x) => x,
			None => {
				println!("No snapshot made");
				return Vec::new();
			},
		};
		let mut iterator = snapshot.iterator();

		while let Some(entry) = iterator.next_entry() {
			if entry.timestamp < low {
				break;
			}
			if entry.timestamp > high {
				continue;
			}
			let data = &entry.data;

			let prev_pid = u64::from_be_bytes(data[0..8].try_into().unwrap());
			let next_pid = u64::from_be_bytes(data[8..16].try_into().unwrap());
			let cpu = u64::from_be_bytes(data[16..24].try_into().unwrap());
			let timestamp_micros = u64::from_be_bytes(data[24..32].try_into().unwrap());
			let comm: [u8; 16] = data[32..48].try_into().unwrap();

			let rec = Record::Scheduler {
				prev_pid, next_pid, cpu, timestamp_micros, comm
			};
			events.push(rec);
		}
		events
	}

	fn exec_read_syscalls(
		&self,
		low: u64,
		high: u64,
		tile: f64,
	) -> Vec<Record> {

		let mut raw_data: Vec<(u64, u64, u64)> = Vec::new();

		let snapshot = match self.inner.snapshot(&[(1, 1)]) {
			Some(x) => x,
			None => { return Vec::new() },
		};

		let mut iterator = snapshot.iterator();

		while let Some(entry) = iterator.next_entry() {
			if entry.timestamp < low {
				break;
			}
			if entry.timestamp > high {
				continue;
			}
			let data = &entry.data;
			let syscall_number = u64::from_be_bytes(data[0..8].try_into().unwrap());
			let timestamp_micros = u64::from_be_bytes(data[8..16].try_into().unwrap());
			let duration_micros = u64::from_be_bytes(data[16..24].try_into().unwrap());
			raw_data.push((syscall_number, timestamp_micros, duration_micros));
		}

		raw_data.sort_by_key(|x| x.2);
		let tile_idx = (raw_data.len() as f64 * tile) as usize;
		let mut raw_data: Vec<_> = raw_data[tile_idx..].into();
		raw_data.sort_by_key(|x| x.1);

		let mut events = Vec::new();
		for item in raw_data.iter() {
			let rec = Record::Syscall {
				syscall_number: item.0,
				timestamp_micros: item.1,
				duration_micros: item.2
			};
			events.push(rec);
		}
		events

	}

	fn exec_kvops_percentile(
		&self,
		low: u64,
		high: u64,
		tile: f64,
	) -> Vec<Record> {
		let mut raw_data: Vec<(u64, u64, u64)> = Vec::new();

		let snapshot = match self.inner.snapshot(&[(0, 0)]) {
			Some(x) => x,
			None => { return Vec::new() },
		};
		let mut iterator = snapshot.iterator();

		while let Some(entry) = iterator.next_entry() {
			if entry.timestamp < low {
				break;
			}
			if entry.timestamp > high {
				continue;
			}
			let data = &entry.data;
			let cpu = u64::from_be_bytes(data[0..8].try_into().unwrap());
			let timestamp_micros = u64::from_be_bytes(data[8..16].try_into().unwrap());
			let duration_micros = u64::from_be_bytes(data[16..24].try_into().unwrap());
			raw_data.push((cpu, timestamp_micros, duration_micros));
		}

		raw_data.sort_by_key(|x| x.2);
		let tile_idx = (raw_data.len() as f64 * tile) as usize;
		let mut raw_data: Vec<_> = raw_data[tile_idx..].into();
		raw_data.sort_by_key(|x| x.1);

		let mut events = Vec::new();
		for item in raw_data.iter() {
			let rec = Record::KVOp {
				cpu: item.0,
				timestamp_micros: item.1,
				duration_micros: item.2
			};
			events.push(rec);
		}
		events
	}
}

impl Reader for MachReader {
	fn handle_query(&self, query: &Request) -> Response {
		match query {
			Request::KvOpsPercentile {
				low_ts,
				high_ts,
				tile,
			} => {
				let records =
					self.exec_kvops_percentile(*low_ts, *high_ts, *tile);
				Response::KvOpsPercentile(records)
			}
			Request::ReadSyscalls {
				low_ts,
				high_ts,
				tile,
			} => {
				let records = self.exec_read_syscalls(*low_ts, *high_ts, *tile);
				Response::ReadSyscalls(records)
			}
			Request::Scheduler { low_ts, high_ts } => {
				let records = self.exec_scheduler(*low_ts, *high_ts);
				Response::Scheduler(records)
			}
			Request::DataReceived => unreachable!(),
			Request::DataCompleteness => unreachable!(),
		}
	}
}


#[derive(Clone)]
pub struct Memstore {
	pub data: Arc<RwLock<Vec<(u64, Record)>>>,
}

impl Memstore {
	pub fn new() -> Self {
		Self {
			data: Arc::new(RwLock::new(Vec::new())),
		}
	}

	fn exec_scheduler(&self, low: u64, high: u64) -> Vec<Record> {
		let mut events = Vec::new();
		let guard = self.data.read().unwrap();
		for item in guard.iter().rev() {
			if item.0 < low {
				break;
			}
			if item.0 > high {
				continue;
			}

			match item.1 {
				Record::Scheduler { .. } => {
					events.push(item.1);
				}
				_ => {}
			}
		}
		events
	}

	fn exec_read_syscalls(
		&self,
		low: u64,
		high: u64,
		tile: f64,
	) -> Vec<Record> {
		let mut durations = Vec::new();

		let guard = self.data.read().unwrap();
		for (idx, item) in guard.iter().enumerate().rev() {
			match item.1 {
				Record::Syscall {
					duration_micros, ..
				} => {
					if item.0 < low {
						break;
					}

					if item.0 > high {
						continue;
					}

					durations.push((idx, duration_micros));
				}
				_ => {}
			}
		}
		drop(guard);

		durations.sort_by_key(|x| x.1);
		let tile_idx = (durations.len() as f64 * tile) as usize;
		let mut slow_queries: Vec<(usize, u64)> = durations[tile_idx..].into();
		slow_queries.sort_by_key(|x| x.0);

		let mut result = Vec::new();
		let guard = self.data.read().unwrap();
		for (idx, _) in slow_queries {
			result.push(guard[idx].1);
		}
		result
	}

	fn exec_kvops_percentile(
		&self,
		low: u64,
		high: u64,
		tile: f64,
	) -> Vec<Record> {
		let mut durations = Vec::new();

		let guard = self.data.read().unwrap();
		for (idx, item) in guard.iter().enumerate().rev() {
			match item.1 {
				Record::KVOp {
					duration_micros, ..
				} => {
					if item.0 < low {
						break;
					}

					if item.0 > high {
						continue;
					}

					durations.push((idx, duration_micros));
				}
				_ => {}
			}
		}
		drop(guard);

		durations.sort_by_key(|x| x.1);
		let tile_idx = (durations.len() as f64 * tile) as usize;
		let mut slow_queries: Vec<(usize, u64)> = durations[tile_idx..].into();
		slow_queries.sort_by_key(|x| x.0);

		let mut result = Vec::new();
		let guard = self.data.read().unwrap();
		for (idx, _) in slow_queries {
			result.push(guard[idx].1);
		}
		result
	}
}

impl Storage for Memstore {
	fn push_batch(&mut self, records: &[Record]) {
		let ts = micros_since_epoch();
		let mut guard = self.data.write().unwrap();
		for r in records {
			guard.push((ts, *r));
		}
	}
}

impl Reader for Memstore {
	fn handle_query(&self, query: &Request) -> Response {
		match query {
			Request::KvOpsPercentile {
				low_ts,
				high_ts,
				tile,
			} => {
				let records =
					self.exec_kvops_percentile(*low_ts, *high_ts, *tile);
				Response::KvOpsPercentile(records)
			}
			Request::ReadSyscalls {
				low_ts,
				high_ts,
				tile,
			} => {
				let records = self.exec_read_syscalls(*low_ts, *high_ts, *tile);
				Response::ReadSyscalls(records)
			}
			Request::Scheduler { low_ts, high_ts } => {
				let records = self.exec_scheduler(*low_ts, *high_ts);
				Response::Scheduler(records)
			}
			Request::DataReceived => unreachable!(),
			Request::DataCompleteness => unreachable!(),
		}
	}
}

pub fn micros_since_epoch() -> u64 {
	SystemTime::now()
		.duration_since(SystemTime::UNIX_EPOCH)
		.unwrap()
		.as_micros() as u64
}
