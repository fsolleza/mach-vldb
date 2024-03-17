use serde::*;
use std::io::Write;

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub enum RocksDBOp {
    Read,
    Write,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub struct RocksDBOpLog {
    pub op: RocksDBOp,
    pub bytes: usize,
    pub micros: usize,
}

impl RocksDBOpLog {
    pub fn serialize_into<W: Write>(&self, writer: W)
        -> Result<(), bincode::Error>
    {
        bincode::serialize_into(writer, self)
    }

    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }
}

