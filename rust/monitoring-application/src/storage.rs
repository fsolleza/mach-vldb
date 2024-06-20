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

		// Keep values for count, sum, max
		let mut group_count:
			FxHashMap<(u64, [FieldValue; 16]), (u64, u64, u64)> =
			FxHashMap::default();
		let mut guard = self.data.lock().unwrap();

		let mut count: u64 = 0;

		for (ts, entry) in guard.iter().rev() {
			if *ts > request.max_ts_micros {
				continue;
			}
			if *ts < request.min_ts_micros {
				break;
			}

			count += 1;

			let mut value_group = empty_field_value_buffer();
			for (idx, field) in field_grouping.iter().enumerate() {
				value_group[idx] = entry.get_field_value(*field);
			}

			let ts = (*ts) - *ts % 1_000_000;

			let val = group_count
				.entry((ts, value_group))
				.or_insert((0u64, 0u64, 0u64));

			val.0 += 1;
			if let Some(x) = entry.get_field_value(request.field).as_uint() {
				val.1 += x;
				val.2 = val.2.max(x);
			}
		}
		drop(guard);

		let mut group_count: FxHashMap<(u64, [FieldValue; 16]), f64> =
			match request.aggregation {
				Aggregation::Sum => group_count
					.drain()
					.map(|(k, v)| (k, v.1 as f64))
					.collect(),
				Aggregation::Count => group_count
					.drain()
					.map(|(k, v)| (k, v.0 as f64))
					.collect(),
				Aggregation::Avg => group_count
					.drain()
					.map(|(k, v)| (k, (v.1 as f64) / (v.0 as f64)))
					.collect(),
				Aggregation::Max => group_count
					.drain()
					.map(|(k, v)| (k, v.2 as f64))
					.collect(),
			};

		let mut result = FxHashMap::default();
		for ((ts, group), v) in group_count {
			let vec = result.entry(group).or_insert_with(Vec::new);
			vec.push((ts, v));
		}
		for (_, v) in result.iter_mut() {
			v.sort_by_key(|x| x.0);
		}
		println!("number of groups: {}", result.len());

		let mut vec = Vec::new();
		for (k, v) in result {
			vec.push(DataResponse { group: k.into(), data: v });
		}

		Response::Data(vec)
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
