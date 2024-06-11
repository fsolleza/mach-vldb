use crate::api::*;
use fxhash::FxHashMap;
use std::{
	sync::{Arc, Mutex},
	time::SystemTime,
};

pub trait Storage: Sync + Send + 'static {
	fn push_batch(&self, records: &[Record]);
	fn reader(&self) -> impl Reader;
}

pub trait Reader: Sync + Send + 'static {
	fn handle_request(&self, requests: &DataRequest) -> Response;
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

	fn reader(&self) -> Self {
		self.clone()
	}
}

impl Reader for Memstore {
	fn handle_request(&self, request: &DataRequest) -> Response {

		let field_grouping: Vec<Field> =
			request.grouping.iter().copied().collect();
		let mut group_count: FxHashMap<(u64, [FieldValue; 16]), f64> =
			FxHashMap::default();
		let mut guard = self.data.lock().unwrap();

		for (ts, entry) in guard.iter().rev() {
			if *ts < request.min_ts_micros {
				break;
			}

			let mut value_group = empty_field_value_buffer();
			for (idx, field) in field_grouping.iter().enumerate() {
				value_group[idx] = entry.get_field_value(*field);
			}

			let ts = (*ts) - *ts % 1_000_000;

			let val = group_count.entry((ts, value_group)).or_insert(0.0);

			match request.aggregation {
				Aggregation::Sum => {
					let v = entry.get_field_value(request.field);
					*val += v.as_uint() as f64;
				},
				Aggregation::Count => *val += 1.,
			}
		}

		let mut result = Vec::new();
		for (k, v) in group_count {
			result.push((k, v));
		}
		result.sort_by_key(|x| x.0.0);

		Response::Data { data: result }
	}
}

fn empty_field_value_buffer() -> [FieldValue; 16] {
	[ FieldValue::None; 16 ]
}

pub fn micros_since_epoch() -> u64 {
	SystemTime::now()
		.duration_since(SystemTime::UNIX_EPOCH)
		.unwrap()
		.as_micros() as u64
}
