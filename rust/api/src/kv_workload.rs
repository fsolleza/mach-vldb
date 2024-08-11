use serde::*;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Request {
	SetQps(u64),
	OpsPerSec,
}

impl Request {
	pub fn from_binary(data: &[u8]) -> Self {
		bincode::deserialize(data).unwrap()
	}

	pub fn to_binary(&self) -> Vec<u8> {
		bincode::serialize(self).unwrap()
	}
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Response {
	SetQps,
	OpsPerSec(u64),
}

impl Response {
	pub fn from_binary(data: &[u8]) -> Self {
		bincode::deserialize(data).unwrap()
	}

	pub fn to_binary(&self) -> Vec<u8> {
		bincode::serialize(self).unwrap()
	}
}
