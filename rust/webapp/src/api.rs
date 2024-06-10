use serde::*;
use std::collections::{HashMap, HashSet};


#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct Request {
	pub requests: Vec<DataRequest>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct DataRequest {
	pub source: String,
	pub engine: String,
	pub min_ts_millis: u64,
	pub max_ts_millis: u64,
	pub aggregation: String,
	pub grouping: Vec<String>,
	pub type_: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct Response {
	pub responses: Vec<Vec<u64>>
}
