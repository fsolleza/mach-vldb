use serde::*;
use std::collections::HashSet;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub enum StorageEngine {
	Mem
}

#[derive(Default, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub enum Field {
	KvOp,
	DurationMicros,
	Cpu,
	TimestampMicros,

	#[default]
	None
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub enum FieldValue {
	Uint(u64),
	None,
}

impl Default for FieldValue {
	fn default() -> Self {
		Self::None
	}
}

impl FieldValue {
	pub fn as_uint(&self) -> u64 {
		match self {
			FieldValue::Uint(x) => *x,
			_ => panic!("Error casting Field value to int"),
		}
	}
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum Aggregation {
	Sum,
	Count,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum Request {
	Statistics,
	Data(DataRequest),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct DataRequest {
	pub storage: StorageEngine,
	pub field: Field,
	pub min_ts_micros: u64,
	pub max_ts_micros: u64,
	pub aggregation: Aggregation,
	pub grouping: HashSet<Field>,
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Response {
	#[default]
	None,
	Statistics {
		percent_complete: u64,
	},
	Data {
		data: Vec<((u64, [FieldValue; 16]), f64)>,
	}
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub enum KVOp {
	Read = 0,
	Write = 1,
}

impl Default for KVOp {
	fn default() -> Self {
		Self::Read
	}
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub enum RecordType {
	KVOp,
	Scheduler,
	None,
}

impl Default for RecordType {
	fn default() -> Self {
		Self::None
	}
}

/*
 * To simplify, we shove everything into a single record type and note the
 * schema using the RecordType enum
 */
#[derive(Default, Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy)]
pub struct Record {
	pub record_type: RecordType,
	pub timestamp_micros: u64,
	pub duration_micros: u64,
	pub cpu: u64,
	pub kv_op: KVOp,
	pub prev_pid: u64,
	pub next_pid: u64,
	pub comm: [u8; 16],
}

impl Record {
	pub fn get_field_value(&self, field: Field) -> FieldValue {
		match field {
			Field::KvOp => FieldValue::Uint(self.kv_op as u64),
			Field::Cpu => FieldValue::Uint(self.cpu),
			Field::DurationMicros => FieldValue::Uint(self.duration_micros),
			Field::TimestampMicros => FieldValue::Uint(self.timestamp_micros),
			_ => unimplemented!(),
		}
	}
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RecordBatch {
	pub record_type: RecordType,
	pub records: Vec<Record>
}

pub fn serialize<T: Serialize>(data: &T) -> Vec<u8> {
	bincode::serialize(data).unwrap()
}

pub fn deserialize<'a, T>(bytes: &'a [u8]) -> T
where
    T: Deserialize<'a>,
{
	bincode::deserialize(bytes).unwrap()
}
