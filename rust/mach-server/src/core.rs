use fxhash::FxHashMap;
use serde::*;

#[derive(Serialize, Deserialize, Clone)]
pub enum MachRequest {
	Hist(u64, u64),
	ByCpu(u64, u64),
	ByOp(u64, u64),
	ByComm(u64, u64),
	Count,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum MachResponse {
	Hist(Vec<(u64, u64)>),
	ByCpu(Vec<((u64, u64), u64)>),
	ByOp(Vec<((u64, u64), u64)>),
	ByComm(Vec<((u64, [u8; 16]), u64)>),
	Count(u64),
}

impl MachResponse {
	pub fn from_comm(self) -> Option<Vec<((u64, [u8; 16]), u64)>> {
		match self {
			MachResponse::ByComm(x) => Some(x),
			_ => None,
		}
	}

	pub fn from_cpu(self) -> Option<Vec<((u64, u64), u64)>> {
		match self {
			MachResponse::ByCpu(x) => Some(x),
			_ => None,
		}
	}
	pub fn from_hist(self) -> Option<Vec<(u64, u64)>> {
		match self {
			MachResponse::Hist(x) => Some(x),
			_ => None,
		}
	}
	pub fn from_count(self) -> Option<u64> {
		match self {
			MachResponse::Count(x) => Some(x),
			_ => None,
		}
	}
}
