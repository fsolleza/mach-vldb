use api::monitoring_application::*;
use fxhash::FxHashMap;
use std::{
	sync::{Arc, RwLock},
	time::{Instant, Duration, SystemTime},
	fmt::Write,
};

use influxdb_line_protocol::LineProtocolBuilder;
use influxdb3_client::{Format, Client};
use chrono::{DateTime, Utc};

use mach_lib::{Entry, Partitions, PartitionsReader};


pub trait Storage: Sync + Send + 'static + Sized {
	fn push_batch(&mut self, records: &[Record]);
	fn duplicate(&self) -> Self {
		unimplemented!()
	}
}

pub trait Reader: Sync + Send + 'static + Clone {
	fn handle_query(&self, query: &Request) -> Response;
}

async fn influx_write(addr: &str, db_name: &str, body: &str) {
	let addr = format!("http://{}", addr);
	let client = Client::new(addr).unwrap();
	let body: String = body.into();
	client
		.api_v3_write_lp(db_name)
		.body(body)
		.send()
		.await
		.expect("send write_lp request");
}

async fn influx_read(addr: &str, db_name: &str, query: &str) -> serde_json::Value {
	let client = Client::new(addr).unwrap();
	let response_bytes = client
		.api_v3_query_sql(db_name, query)
		.format(Format::Json)
		.send()
		.await
		.expect("send query_sql request");
	let response_str = std::str::from_utf8(&response_bytes).unwrap();
	serde_json::from_str(response_str).unwrap()
}

#[derive(Clone)]
pub struct InfluxStore {
	host_addr: String,
	buf: Vec<Record>
}

impl InfluxStore {

	pub fn new(host_addr: &str) -> Self {
		Self {
			host_addr: host_addr.into(),
			buf: Vec::new(),
		}
	}

	fn do_write(&mut self, data: &[Record]) {
		self.buf.extend_from_slice(data);
		if self.buf.len() < 1024 * 1024 {
			return;
		}

		let buf = self.build_lp(&self.buf);

		let addr = self.host_addr.clone();

		//let client = reqwest::blocking::Client::new();
		//let url = format!("http://{base}/api/v3/write", base = self.host_addr);
		//let params = &[("db", "foo")];

		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();

		let addr = self.host_addr.clone();
		rt.block_on(influx_write(&addr, "vldb_demo", &buf));
		self.buf.clear();
	}

	fn build_lp(&self, data: &[Record]) -> String {
		let mut lp = LineProtocolBuilder::new();

		let mut cpu_str = String::new();
		for r in data {
			match *r {
				Record::KVOp {
					cpu,
					timestamp_micros,
					duration_micros,
				} => {
					cpu_str.clear();
					write!(cpu_str, "{}", cpu);
					lp = lp
						.measurement("table_kvop")
						.tag("cpu", &cpu_str)
						.field("duration_micros", duration_micros)
						.timestamp(timestamp_micros as i64)
						.close_line();
				}
				Record::Syscall {
					cpu,
					syscall_number,
					timestamp_micros,
					duration_micros,
				} => {
					cpu_str.clear();
					write!(cpu_str, "{}", cpu);
					lp = lp
						.measurement("table_syscall")
						.tag("cpu", &cpu_str)
						.field("syscall_number", syscall_number)
						.field("duration_micros", duration_micros)
						.timestamp(timestamp_micros as i64)
						.close_line();
				}
				Record::Scheduler {
					prev_pid,
					next_pid,
					cpu,
					timestamp_micros,
					comm,
				} => {
					cpu_str.clear();
					write!(cpu_str, "{}", cpu);
					lp = lp
						.measurement("table_sched")
						.tag("cpu", &cpu_str)
						.field("prev_pid", prev_pid)
						.field("next_pid", next_pid)
						.timestamp(timestamp_micros as i64)
						.close_line();
				}
			}
		}

		std::str::from_utf8(&lp.build()).unwrap().into()
	}

	fn handle_query(&self, query: &str) -> serde_json::Value {
		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();

		let query: String = query.into();
		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();
		let addr = self.host_addr.clone();
		let result = rt.block_on(influx_read(&addr, "vldb_demo", &query));
		result
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
		println!("KVOP requests: {:?}", result);
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
		println!("Syscall requests: {:?}", result);
		Vec::new()
	}
}

impl Storage for InfluxStore {
	fn push_batch(&mut self, records: &[Record]) {
		self.do_write(records);
	}

	fn duplicate(&self) -> Self {
		self.clone()
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
		cpu: u64,
		syscall_number: u64,
		timestamp_micros: u64,
		duration_micros: u64,
	) {
		let mut bytes = [0u8; 32];
		bytes[0..8].copy_from_slice(&syscall_number.to_be_bytes());
		bytes[8..16].copy_from_slice(&cpu.to_be_bytes());
		bytes[16..24].copy_from_slice(&timestamp_micros.to_be_bytes());
		bytes[24..32].copy_from_slice(&duration_micros.to_be_bytes());
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
					cpu,
					timestamp_micros,
					duration_micros,
				} => {
					self.push_syscall(
						ts,
						cpu,
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
				prev_pid,
				next_pid,
				cpu,
				timestamp_micros,
				comm
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

		let mut raw_data: Vec<(u64, u64, u64, u64)> = Vec::new();

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
			let cpu = u64::from_be_bytes(data[8..16].try_into().unwrap());
			let timestamp_micros = u64::from_be_bytes(data[16..24].try_into().unwrap());
			let duration_micros = u64::from_be_bytes(data[24..32].try_into().unwrap());
			raw_data.push((syscall_number, cpu, timestamp_micros, duration_micros));
		}

		raw_data.sort_by_key(|x| x.2);
		let tile_idx = (raw_data.len() as f64 * tile) as usize;
		let mut raw_data: Vec<_> = raw_data[tile_idx..].into();
		raw_data.sort_by_key(|x| x.1);

		let mut events = Vec::new();
		for item in raw_data.iter() {
			let rec = Record::Syscall {
				syscall_number: item.0,
				cpu: item.1,
				timestamp_micros: item.2,
				duration_micros: item.3,
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
				duration_micros: item.2,
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
	pub rps: Option<f64>
}

impl Memstore {
	pub fn new() -> Self {
		Self {
			data: Arc::new(RwLock::new(Vec::new())),
			rps: None,
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
		let now = Instant::now();

		let ts = micros_since_epoch();
		let mut guard = self.data.write().unwrap();
		for r in records {
			guard.push((ts, *r));
		}
		drop(guard);

		if let Some(rps) = self.rps {
			let expected = Duration::from_secs_f64((records.len() as f64 * (1.0 / rps)));
			while now.elapsed() < expected {}
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
