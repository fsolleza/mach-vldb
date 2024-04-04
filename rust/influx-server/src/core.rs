use serde::*;
use common::data::Record;

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum InfluxRequest {
    Query(String),
    Count,
}

impl InfluxRequest {
    pub fn query_request(self) -> Option<String> {
        match self {
            Self::Query(x) => Some(x),
            _ => None
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum InfluxResponse {
    Err(String),
    Json(String),
    Count(u64),
}

impl InfluxResponse {
    pub fn to_json_string(self) -> Option<String> {
        match self {
            Self::Json(x) => Some(x),
            _ => None
        }
    }
    pub fn count(self) -> Option<u64> {
        match self {
            Self::Count(x) => Some(x),
            _ => None
        }
    }
}

