use serde::*;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum SchedRequest {
    Enable
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum SchedResponse {
    Ok
}


