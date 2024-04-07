use serde::*;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum AppRequest {
	Enable,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum AppResponse {
	Ok,
}
