use serde::*;
use std::io::Write;

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub enum KVOp {
    Read,
    Write,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub struct KVLog {
    pub op: KVOp,
    pub bytes: usize,
    pub nanos: usize,
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


