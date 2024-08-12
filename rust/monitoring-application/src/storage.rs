use api::monitoring_application::*;
use fxhash::FxHashMap;
use std::{
	sync::{Arc, RwLock},
	time::SystemTime,
};

pub trait Storage: Sync + Send + 'static {
	fn push_batch(&self, records: &[Record]);
	//fn reader(&self) -> impl Reader;
}

pub trait Reader: Sync + Send + 'static + Clone {
	fn handle_query(&self, query: &Request) -> Response;
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
				},
				_ => {}
			}
		}
		events
	}

	fn exec_read_syscalls(&self, low: u64, high: u64, tile: f64) -> Vec<Record> {
		let mut durations = Vec::new();

		let guard = self.data.read().unwrap();
		for (idx, item) in guard.iter().enumerate().rev() {
			match item.1 {
				Record::Syscall { duration_micros, .. } => {
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

	fn exec_kvops_percentile(&self, low: u64, high: u64, tile: f64) -> Vec<Record> {
		let mut durations = Vec::new();

		let guard = self.data.read().unwrap();
		for (idx, item) in guard.iter().enumerate().rev() {
			match item.1 {
				Record::KVOp { duration_micros, .. } => {
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
	fn push_batch(&self, records: &[Record]) {
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
			Request::KvOpsPercentile { low_ts, high_ts, tile } => {
				let records = self.exec_kvops_percentile(*low_ts, *high_ts, *tile);
				Response::KvOpsPercentile(records)
			}
			Request::ReadSyscalls { low_ts, high_ts, tile } => {
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
