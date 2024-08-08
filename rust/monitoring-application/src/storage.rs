use crate::api::*;
use fxhash::FxHashMap;
use std::{
	sync::{Arc, Mutex},
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
	pub data: Arc<Mutex<Vec<(u64, Record)>>>,
}

impl Memstore {
	pub fn new() -> Self {
		Self {
			data: Arc::new(Mutex::new(Vec::new())),
		}
	}
}

impl Storage for Memstore {
	fn push_batch(&self, records: &[Record]) {
		let ts = micros_since_epoch();
		let mut guard = self.data.lock().unwrap();
		for r in records {
			guard.push((ts, *r));
		}
	}
}

impl Reader for Memstore {
	fn handle_query(&self, query: &Request) -> Response {
		match query {
			Request::KvOps { low_ts, high_ts } => {
				let mut result = Vec::new();
				for item in self.data.lock().unwrap().iter().rev() {

					if item.0 < *low_ts {
						break;
					}

					if item.0 > *high_ts {
						continue;
					}

					result.push(item.1);
				}
				Response::KvOpRecords(result)
			},
			_ => unreachable!(),
		}
	}
}

pub fn micros_since_epoch() -> u64 {
	SystemTime::now()
		.duration_since(SystemTime::UNIX_EPOCH)
		.unwrap()
		.as_micros() as u64
}
