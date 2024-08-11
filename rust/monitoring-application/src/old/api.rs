use fxhash::FxHashMap;
use serde::*;
use std::collections::{HashMap, HashSet};

//pub fn to_binary<T: Serialize>(data: &T) -> Vec<u8> {
//	bincode::serialize(data).unwrap()
//}
//
//pub fn to_json<T: Serialize>(data: &T) -> String {
//	serde_json::to_string(data).unwrap()
//}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub enum Record {
	KVOp {
		cpu: u64,
		timestamp_micros: u64,
		duration_micros: u64,
	},

	Scheduler {
		prev_pid: u64,
		next_pid: u64,
		cpu: u64,
		timestamp_micros: u64,
		comm: [u8; 16],
	}
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RecordBatch {
	pub inner: Vec<Record>,
}

impl std::ops::Deref for RecordBatch {
	type Target = [Record];
	fn deref(&self) -> &Self::Target {
		self.inner.as_slice()
	}
}

impl RecordBatch {
	pub fn from_binary(bin: &[u8]) -> Self {
		bincode::deserialize(bin).unwrap()
	}

	pub fn to_binary(&self) -> Vec<u8> {
		bincode::serialize(self).unwrap()
	}
}


#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
	SetQps { rate: u64 },
	DataReceived,
	DataCompleteness,
	KvOpsPercentile { low_ts: u64, high_ts: u64, tile: f64 },
}

impl Request {
	pub fn from_binary(bin: &[u8]) -> Self {
		bincode::deserialize(bin).unwrap()
	}

	pub fn to_binary(&self) -> Vec<u8> {
		bincode::serialize(self).unwrap()
	}
}

#[derive(Serialize, Deserialize)]
pub enum Response {
  SetQps,
	DataReceived(u64),
	DataCompleteness(f64),
	KvOpsPercentile(Vec<Record>),
}

impl Response {
	pub fn from_binary(bin: &[u8]) -> Self {
		bincode::deserialize(bin).unwrap()
	}

	pub fn to_binary(&self) -> Vec<u8> {
		bincode::serialize(self).unwrap()
	}
}
