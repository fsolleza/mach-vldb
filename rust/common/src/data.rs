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
    pub min: u64,
    pub median: u64,
    pub avg: f64,
    pub cnt: u64,
    pub op: KVOp,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Batch {
    pub records: Vec<Record>,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub enum Record {
    Sched(Sched),
    KV(KVLog),
    HistSecond(Histogram),
    HistMillisecond(Histogram),
    Hist100Micros(Histogram),
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Default)]
#[repr(C)]
pub struct Sched {
    pub prev_pid: u64,
    pub next_pid: u64,
    pub cpu: u64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, Eq, PartialEq)]
pub enum KVOp {
    Read,
    Write,
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
    pub timestamp: u64,
    pub dur_nanos: u64,
}

impl KVLog {
    pub fn serialize_into<W: Write>(&self, writer: W)
        -> Result<(), bincode::Error>
    {
        bincode::serialize_into(writer, self)
    }

    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }
}


