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

pub trait Reader: Sync + Send + 'static { }

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

	//fn reader(&self) -> Self {
	//	self.clone()
	//}
}

impl Reader for Memstore {
}

pub fn micros_since_epoch() -> u64 {
	SystemTime::now()
		.duration_since(SystemTime::UNIX_EPOCH)
		.unwrap()
		.as_micros() as u64
}
