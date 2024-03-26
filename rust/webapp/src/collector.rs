use common::{
    ipc::{ipc_receiver, IpcReceiver},
    data::*,
};
//use mach_lib::{Mach, MachReader, SourcePartition};
use crossbeam::channel::*;
use std::{
    thread,
    time::Duration,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
};

//static MACH_COUNT: AtomicUsize = AtomicUsize::new(0);
//static MACH_READER: Mutex<Option<MachReader>> = Mutex::new(None);

static VEC_COUNT: AtomicUsize = AtomicUsize::new(0);
static VEC_DROP: AtomicUsize = AtomicUsize::new(0);
static VEC_READER: Mutex<Option<Arc<Mutex<Vec<(u64, Record)>>>>> = Mutex::new(None);

pub fn init_collector() {

    let server_rx: IpcReceiver<Vec<Record>> = ipc_receiver("localhost:3001");
    let mut sinks: Vec<(Sender<Arc<[Record]>>, &'static AtomicUsize)> = Vec::new();

    //let mut mach = Mach::new("/nvme/data/tmp/vldb".into(), true, Duration::from_secs(1));
    //let mach_reader = mach.reader();
    //let (mach_tx, mach_rx) = bounded(1028);
    //thread::spawn(move || {
    //    init_mach_sink(mach, mach_rx);
    //});
    //*MACH_READER.lock().unwrap() = Some(mach_reader);
    //sinks.push(mach_tx);

    let vec = Arc::new(Mutex::new(Vec::new()));
    let (vec_tx, vec_rx) = bounded(1028);
    let v = vec.clone();
    thread::spawn(move || {
        init_vec_sink(v, vec_rx);
    });
    sinks.push((vec_tx, &VEC_DROP));
    *VEC_READER.lock().unwrap() = Some(vec.clone());

    loop {
        if let Ok(data) = server_rx.try_recv() {
            let item: Arc<[Record]> = data.into();
            for (sink, drops) in &sinks {
                if sink.try_send(item.clone()).is_err() {
                    drops.fetch_add(item.len(), SeqCst);
                }
            }
        }
    }
}

fn init_vec_sink(
    vec: Arc<Mutex<Vec<(u64, Record)>>>,
    rx: Receiver<Arc<[Record]>>,
) {
    thread::spawn(move || {
        loop {
            let count = VEC_COUNT.swap(0, SeqCst);
            let drop = VEC_COUNT.swap(0, SeqCst);
            println!("Vec Count {}\t Drop {}", count, drop);
            thread::sleep(Duration::from_secs(1));
        }
    });
    loop {
        if let Ok(data) = rx.try_recv() {
            let now = micros_since_epoch();
            let mut guard = vec.lock().unwrap();
            for item in data.iter() {
                guard.push((now, *item));
            }
            VEC_COUNT.fetch_add(data.len(), SeqCst);
        }
    }
}

//fn init_mach_sink(mut mach: Mach, rx: Receiver<Arc<[Record]>>) {
//    let mut sl = Vec::new();
//    thread::spawn(move || {
//        loop {
//            let count = MACH_COUNT.swap(0, SeqCst);
//            println!("Mach Count {}", count);
//            thread::sleep(Duration::from_secs(1));
//        }
//    });
//
//    loop {
//        if let Ok(data) = rx.try_recv() {
//            let now = micros_since_epoch();
//
//            let mut source = 0;
//            let mut partition = 0;
//
//            for item in data.iter() {
//                match item {
//                    Record::Sched(_) => source = 0,
//                    Record::KV(_) => source = 1,
//                }
//                bincode::serialize_into(&mut sl, &item).unwrap();
//                mach.push(
//                    SourcePartition::new(source, partition),
//                    now,
//                    sl.as_slice(),
//                    );
//                sl.clear();
//            }
//            MACH_COUNT.fetch_add(data.len(), SeqCst);
//        }
//    }
//}
