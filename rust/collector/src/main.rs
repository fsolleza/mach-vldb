use common::{
    ipc::ipc_receiver,
    data::*,
};
use crossbeam::channel::Receiver;

fn init_server() -> Receiver<Vec<Record>> {
    ipc_receiver("0.0.0.0::3001")
}


fn main() {
    println!("Hello, world!");
}
