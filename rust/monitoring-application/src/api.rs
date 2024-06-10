use serde::*;
use std::collections::HashSet;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub enum Field {
	kv_op,
	kv_duration_micros,
	kv_cpu,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum FieldValue {
	kv_op(KVOp),
	kv_duration_micros(u64),
	kv_cpu(u64),
	none,
}

impl FieldValue {
	pub fn as_uint(&self) -> u64 {
		match self {
			FieldValue::kv_duration_micros(x) => *x,
			FieldValue::kv_cpu(x) => *x,
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
	pub field: Field,
	pub min_ts_micros: u64,
	pub max_ts_micros: u64,
	pub aggregation: Aggregation,
	pub grouping: HashSet<Field>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Response {
	Statistics {
		percent_complete: u64,
	},
	Data {
		data: Vec<((u64, [FieldValue; 16]), f64)>,
	}
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub enum KVOp {
	Read,
	Write,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub enum RecordType {
	KVOp,
	Sched
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy)]
pub struct Record {
	pub record_type: RecordType,
	pub kv_op: Option<KVOp>,
	pub kv_cpu: Option<u64>,
	pub kv_duration_micros: Option<u64>,
	pub timestamp_micros: u64
}

impl Record {
	pub fn get_field_value(&self, field: Field) -> FieldValue {
		match field {
			Field::kv_op => FieldValue::kv_op(self.kv_op.unwrap()),
			Field::kv_cpu => FieldValue::kv_cpu(self.kv_cpu.unwrap()),
			Field::kv_duration_micros => FieldValue::kv_duration_micros(
				self.kv_duration_micros.unwrap()
			),
		}
	}
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
