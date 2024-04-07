use serde::*;
use std::io::Write;
use std::time::SystemTime;

pub fn micros_since_epoch() -> u64 {
	SystemTime::now()
		.duration_since(SystemTime::UNIX_EPOCH)
		.unwrap()
		.as_micros() as u64
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Default)]
pub struct Histogram {
	pub max: u64,
	pub cnt: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Batch {
	pub records: Vec<Record>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Record {
	pub timestamp: u64,
	pub data: Data,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Data {
	Sched(Sched),
	KV(KVLog),
	Hist(Histogram),
}

impl Data {
	pub fn is_scheduler(&self) -> bool {
		match self {
			Self::Sched(_) => true,
			_ => false,
		}
	}

	pub fn is_kv_log(&self) -> bool {
		match self {
			Self::KV(_) => true,
			_ => false,
		}
	}

	pub fn hist(&self) -> Option<&Histogram> {
		match self {
			Self::Hist(x) => Some(&x),
			_ => None,
		}
	}
	pub fn sched(&self) -> Option<&Sched> {
		match self {
			Self::Sched(x) => Some(&x),
			_ => None,
		}
	}

	pub fn kv_log(&self) -> Option<&KVLog> {
		match self {
			Self::KV(x) => Some(&x),
			_ => None,
		}
	}

	pub fn is_histogram(&self) -> bool {
		match self {
			Self::Hist(_) => true,
			_ => false,
		}
	}
}

pub unsafe fn parse_comm(comm: &[u8]) -> &str {
	let i8sl = &*(comm as *const _ as *const [i8]);
	std::ffi::CStr::from_ptr(i8sl[..].as_ptr())
		.to_str()
		.unwrap()
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Default)]
#[repr(C)]
pub struct Sched {
	pub prev_pid: u64,
	pub next_pid: u64,
	pub cpu: u64,
	pub comm: [u8; 16],
}

impl Sched {
	pub fn parse_comm(&self) -> &str {
		unsafe { parse_comm(&self.comm) }
	}
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Eq, PartialEq)]
pub enum KVOp {
	Read = 0,
	Write = 1,
}

impl Default for KVOp {
	fn default() -> Self {
		Self::Read
	}
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub struct KVLog {
	pub op: KVOp,
	pub cpu: u64,
	pub tid: u64,
	pub dur_nanos: u64,
}

impl KVLog {
	pub fn serialize_into<W: Write>(
		&self,
		writer: W,
	) -> Result<(), bincode::Error> {
		bincode::serialize_into(writer, self)
	}

	pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
		bincode::serialize(self)
	}
}
