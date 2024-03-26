use common::{
    ipc::{ipc_receiver, IpcReceiver},
    data::*,
};
use mach_lib::{Mach, MachReader, SourcePartition};
use crossbeam::channel;
use std::{
    thread,
    time::Duration,
};

fn init_mach() -> MachReader {
    let mut mach = Mach::new("/nvme/data/tmp/vldb".into(), true, Duration::from_secs(1));
    let reader = mach.reader();

    let server_rx: IpcReceiver<Vec<Record>> = ipc_receiver("0.0.0.0::3001");

    thread::spawn(move || {
        let mut sl = Vec::new();
        while let Ok(data) = server_rx.try_recv() {
            let now = micros_since_epoch();

            let mut source = 0;
            let mut partition = 0;

            for item in data.iter() {
                match item {
                    Record::Sched(_) => source = 0,
                    Record::KV(_) => source = 1,
                }
                bincode::serialize_into(&mut sl, &data).unwrap();
                mach.push(
                    SourcePartition::new(source, partition),
                    now,
                    sl.as_slice(),
                );
            }
        }
    });
    reader
}

fn main() {
    println!("Hello, world!");
}
