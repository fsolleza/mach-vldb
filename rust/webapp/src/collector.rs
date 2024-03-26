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
        atomic::{AtomicUsize, AtomicBool, Ordering::SeqCst},
    },
};

//static MACH_COUNT: AtomicUsize = AtomicUsize::new(0);
//static MACH_READER: Mutex<Option<MachReader>> = Mutex::new(None);

static VEC_COUNT: AtomicUsize = AtomicUsize::new(0);
static VEC_DROP: AtomicUsize = AtomicUsize::new(0);
static VEC_READER: Mutex<Option<Arc<Mutex<Vec<(u64, Record)>>>>> = Mutex::new(None);

static QUERY: AtomicUsize = AtomicUsize::new(2);

fn filter(data: Vec<Record>) -> Arc<[Record]> {
    match QUERY.load(SeqCst) {

        0 => {
            let mut v = Vec::new();
            for item in data {
                match item {
                    Record::HistSecond(_) => v.push(item),
                    _ => {},
                }
            }
            return v.into();
        },

        1 => {
            let mut v = Vec::new();
            for item in data {
                match item {
                    Record::HistMillisecond(_) => v.push(item),
                    _ => {},
                }
            }
            return v.into();
        },

        2 => {
            let mut v = Vec::new();
            for item in data {
                match item {
                    Record::Hist100Micros(_) => v.push(item),
                    _ => {},
                }
            }
            return v.into();
        },

        _ => { return data.into(); }

    }
}

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
            let item = filter(data);
            if item.len() > 0 {
                for (sink, drops) in &sinks {
                    if sink.try_send(item.clone()).is_err() {
                        drops.fetch_add(item.len(), SeqCst);
                    }
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

fn vec_query_seconds_histogram(min_ts: u64, max_ts: u64) -> Arc<[Record]> {
    let guard = VEC_READER.lock().unwrap();
    let inner_guard = guard.as_ref().unwrap().lock().unwrap();

    let mut result = Vec::new();
    for (ts, item) in inner_guard.iter().rev() {
        if *ts < min_ts{
            break;
        }
        if *ts > max_ts {
            continue;
        }
        match item {
            Record::HistSecond(_) => result.push(*item),
            _ => {},
        }
    }
    result.into()
}

fn vec_query_millis_histogram(min_ts: u64, max_ts: u64) -> Arc<[Record]> {
    let guard = VEC_READER.lock().unwrap();
    let inner_guard = guard.as_ref().unwrap().lock().unwrap();

    let mut result = Vec::new();
    for (ts, item) in inner_guard.iter().rev() {
        if *ts < min_ts{
            break;
        }
        if *ts > max_ts {
            continue;
        }
        match item {
            Record::HistMillisecond(_) => result.push(*item),
            _ => {},
        }
    }
    result.into()
}

fn vec_query_100micros_histogram(min_ts: u64, max_ts: u64) -> Arc<[Record]> {
    let guard = VEC_READER.lock().unwrap();
    let inner_guard = guard.as_ref().unwrap().lock().unwrap();

    let mut result = Vec::new();
    for (ts, item) in inner_guard.iter().rev() {
        if *ts < min_ts{
            break;
        }
        if *ts > max_ts {
            continue;
        }
        match item {
            Record::Hist100Micros(_) => result.push(*item),
            _ => {},
        }
    }
    result.into()
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
