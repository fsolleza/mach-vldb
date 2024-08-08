use fxhash::FxHashMap;
use serde::*;
use std::collections::{HashMap, HashSet};

pub fn to_binary<T: Serialize>(data: &T) -> Vec<u8> {
	bincode::serialize(data).unwrap()
}

pub fn to_json<T: Serialize>(data: &T) -> String {
	serde_json::to_string(data).unwrap()
}

#[derive(Serialize, Deserialize, Copy, Clone)]
pub enum Record {
	KVOp {
		cpu: u64,
		timestamp_micros: u64,
		duration_micros: u64,
	},
}

#[derive(Serialize, Deserialize)]
pub struct RecordBatch {
	pub inner: Vec<Record>,
}

impl std::ops::Deref for RecordBatch {
	type Target = [Record];
	fn deref(&self) -> &Self::Target {
		self.inner.as_slice()
	}
}

#[derive(Serialize, Deserialize)]
pub enum Request {
	DataReceived,
	DataCompleteness,
	KvOps { low_ts: u64, high_ts: u64 },
}

#[derive(Serialize, Deserialize)]
pub enum Response {
	DataReceived(u64),
	DataCompleteness(f64),
	KvOpRecords(Vec<Record>),
}

//#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
//pub enum StorageEngine {
//	Mem,
//	Mach
//}

//#[derive(Default, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
//pub enum Field {
//	KvOp,
//	DurationMicros,
//	Cpu,
//	TimestampMicros,
//	SchedulerEvent,
//	Tid,
//	Comm,
//
//	#[default]
//	None
//}
//
//#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
//pub enum FieldValue {
//	Cpu(u64),
//	KvOp(u64),
//	DurationMicros(u64),
//	TimestampMicros(u64),
//	None,
//}
//
//impl Default for FieldValue {
//	fn default() -> Self {
//		Self::None
//	}
//}
//
//impl FieldValue {
//	pub fn as_uint(&self) -> Option<u64> {
//		match self {
//			FieldValue::Cpu(x) => Some(*x),
//			FieldValue::KvOp(x) => Some(*x),
//			FieldValue::DurationMicros(x) => Some(*x),
//			FieldValue::TimestampMicros(x) => Some(*x),
//			_ => None,
//		}
//	}
//}
//
//#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
//pub enum Aggregation {
//	Max,
//	Count,
//	Avg,
//	Sum,
//}
//
//#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
//pub enum Request {
//	Statistics,
//	Data(DataRequest),
//}
//
//#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
//pub struct DataRequest {
//	pub storage: StorageEngine,
//	pub field: Field,
//	pub min_ts_micros: u64,
//	pub max_ts_micros: u64,
//	pub aggregation: Aggregation,
//	pub grouping: HashSet<Field>,
//}
//
//#[derive(Default, Debug, PartialEq, Serialize, Deserialize, Clone)]
//pub enum Response {
//	#[default]
//	None,
//	Statistics {
//		percent_complete: u64,
//	},
//	Data(Vec<DataResponse>),
//}
//
//#[derive(Default, Debug, PartialEq, Serialize, Deserialize, Clone)]
//pub struct DataResponse {
//	pub group: Vec<FieldValue>,
//	pub data: Vec<(u64, f64)>,
//}
//
//#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
//pub enum KVOp {
//	Read = 0,
//	Write = 1,
//}
//
//impl Default for KVOp {
//	fn default() -> Self {
//		Self::Read
//	}
//}
//#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
//pub enum RecordType {
//	KVOp,
//	//Scheduler,
//	//None,
//}
//
//impl Default for RecordType {
//	fn default() -> Self {
//		Self::None
//	}
//}
//

/*
 * To simplify, we shove everything into a single record type and note the
 * schema using the RecordType enum
 */
//#[derive(Default, Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy)]
//pub struct Record {
//	pub record_type: RecordType,
//	pub timestamp_micros: u64,
//	pub duration_micros: u64,
//	pub cpu: u64,
//	pub kv_op: KVOp,
//	pub prev_pid: u64,
//	pub next_pid: u64,
//	pub comm: [u8; 16],
//}
//
//impl Record {
//	pub fn get_field_value(&self, field: Field) -> FieldValue {
//		match field {
//			Field::KvOp => FieldValue::KvOp(self.kv_op as u64),
//			Field::Cpu => FieldValue::Cpu(self.cpu),
//			Field::DurationMicros => FieldValue::DurationMicros(self.duration_micros),
//			Field::TimestampMicros => FieldValue::TimestampMicros(self.timestamp_micros),
//			_ => unimplemented!(),
//		}
//	}
//}
//
//#[derive(Serialize, Deserialize, Clone)]
//pub struct RecordBatch {
//	pub record_type: RecordType,
//	pub records: Vec<Record>
//}
//

//pub fn deserialize<'a, T>(bytes: &'a [u8]) -> T
//where
//    T: Deserialize<'a>,
//{
//	bincode::deserialize(bytes).unwrap()
//}
