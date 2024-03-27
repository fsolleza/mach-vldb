use common::{
    ipc::{ipc_receiver, IpcReceiver},
    data::*,
};
use mach_lib::{Mach, MachReader, SourcePartition};
use crossbeam::channel::*;
use std::{
    thread,
    time::{Instant, Duration},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, AtomicBool, Ordering::SeqCst},
    },
    collections::{HashMap, BTreeMap, HashSet},
};
use lazy_static::*;
use dashmap::DashMap;

static MACH_COUNT: AtomicUsize = AtomicUsize::new(0);
static MACH_DROP: AtomicUsize = AtomicUsize::new(0);
static MACH_READER: Mutex<Option<MachReader>> = Mutex::new(None);

static VEC_COUNT: AtomicUsize = AtomicUsize::new(0);
static VEC_DROP: AtomicUsize = AtomicUsize::new(0);
static VEC_READER: Mutex<Option<Arc<Mutex<Vec<(u64, Record)>>>>> = Mutex::new(None);

static KVLOGS_ENABLED: AtomicBool = AtomicBool::new(false);
static SCHEDULER_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn enable_kvlogs() {
    KVLOGS_ENABLED.store(true, SeqCst);
}

pub fn disable_kvlogs() {
    KVLOGS_ENABLED.store(false, SeqCst);
}

pub fn enable_scheduler() {
    SCHEDULER_ENABLED.store(true, SeqCst);
}

pub fn disable_scheduler() {
    SCHEDULER_ENABLED.store(false, SeqCst);
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Field {
    KVOp,
    KVCpu,
    KVTid,
    KVDur,
    HistMax,
    HistCnt,
}

impl Field {
    pub fn from_str(x: &str) -> Option<Self> {
        if x == "kvop" { return Some(Self::KVOp); }
        if x == "kvcpu" { return Some(Self::KVCpu); }
        if x == "kvtid" { return Some(Self::KVTid); }
        if x == "kvdur" { return Some(Self::KVDur); }
        if x == "histmax" { return Some(Self::HistMax); }
        if x == "histcnt" { return Some(Self::HistCnt); }
        return None;
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum FieldValue {
    KVOp(u64),
    KVCpu(u64),
    KVTid(u64),
    KVDur(u64),
    HistMax(u64),
    HistCnt(u64),
    None,
}

impl FieldValue {
    pub fn as_string(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum AggregateFunc {
    Max,
    Count,
    None,
}

impl AggregateFunc {
    pub fn from_str(x: &str) -> Option<Self> {
        if x == "max" { return Some(Self::Max); }
        if x == "count" { return Some(Self::Count); }
        if x == "none" { return Some(Self::None); }
        return None
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Storage {
    //Vec,
    Mach,
}

impl Storage {
    pub fn from_str(x: &str) -> Option<Self> {
        if x == "mach" { return Some(Self::Mach); }
        return None;
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Source {
    Hist,
    KV,
    Sched,
}

impl Source {
    pub fn from_str(x: &str) -> Option<Self> {
        if x == "hist" { return Some(Self::Hist); }
        if x == "kv" { return Some(Self::KV); }
        if x == "sched" { return Some(Self::Sched); }
        return None;
    }
}

#[derive(Clone, Debug)]
pub struct QuerySeries {
    pub storage: Storage,
    pub source: Source,
    pub grouping: Vec<Field>,
    pub variable: Field,
    pub aggr: AggregateFunc,
}

#[derive(Clone, Debug)]
pub struct Query {
    pub series: Vec<QuerySeries>,
    pub min_ts: u64,
    pub max_ts: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Point {
    pub x: u64,
    pub y: u64
}

impl Query {

    fn get_data(&self) -> HashMap<(Storage, Source), Vec<Record>> {
        let mut storage_source = HashMap::new();
        for s in &self.series {
            storage_source
                .entry(s.storage)
                .or_insert_with(Vec::new)
                .push(s.source);
        }

        let mut result = HashMap::new();
        for (storage, sources) in storage_source.iter() {
            let mut scan_result = match storage {
                //Storage::Vec => vec_query(&sources, self.min_ts, self.max_ts),
                Storage::Mach => mach_query(&sources, self.min_ts, self.max_ts),
            };
            for (source, data) in scan_result.drain() {
                assert!(result.insert((*storage, source), data).is_none());
            }
        }
        result
    }

    pub fn execute(&self) -> Vec<HashMap<Vec<FieldValue>, Vec<Point>>> {
        let raw_data = self.get_data();

        let mut result = Vec::new();

        for s in self.series.iter() {

            let source_data: &Vec<Record> = {
                let k = (s.storage, s.source);
                raw_data.get(&k).unwrap()
            };

            // First, group records by key
            let by_key = {
                if s.grouping.len() > 0 {
                    group_by_key(&s.grouping, source_data)
                } else {
                    let mut map = HashMap::new();
                    map.insert(vec![FieldValue::None], source_data.clone());
                    map
                }
            };

            // For each grouping by key, group it by time
            let mut by_key_time = HashMap::new();
            for (group, r) in by_key.iter() {
                let mut aggregations = Vec::new();
                for (ts, v) in group_by_time(1_000_000, &r) {

                    // Then, for each of this, aggregate each grouping
                    let result = aggregate(s.aggr, s.variable, &v);
                    aggregations.push(Point { x: ts, y: result });
                }
                by_key_time.insert(group.clone(), aggregations);
            }
            result.push(by_key_time);
        }
        result
    }
}

fn aggregate(
    func: AggregateFunc,
    field: Field,
    records: &[Record]
) -> u64 {

    let mut uint: u64 = 0;

    match func {
        AggregateFunc::Max => {
            let mut max = 0;
            for r in records {
                match field {
                    Field::KVDur => {
                        max = max.max(r.data.kv_log().unwrap().dur_nanos);
                    },
                    Field::HistMax => {
                        max = max.max(r.data.hist().unwrap().max);
                    }
                    Field::HistCnt => {
                        max = max.max(r.data.hist().unwrap().cnt);
                    }
                    _ => panic!("Unhandled field to aggregate {:?}", field),
                }
            }
            return max;
        },

        AggregateFunc::Count => {
            return records.len() as u64;
        },
        _ => panic!("Unhandled aggregate func {:?}", func),
    }
}

fn group_by_time(
    group_micros: u64,
    records: &[Record],
) -> BTreeMap<u64, Vec<Record>> {
    let mut map = BTreeMap::new();
    let mut base = 0;
    for record in records {
        let b = record.timestamp - record.timestamp % group_micros;
        if b - base >= group_micros {
            base = b;
        }
        map.entry(base).or_insert_with(Vec::new).push(*record);
    }
    map
}

fn group_by_key(
    fields: &[Field],
    records: &[Record]
) -> HashMap<Vec<FieldValue>, Vec<Record>> {
    let mut map: HashMap<Vec<FieldValue>, Vec<Record>> = HashMap::new();
    let mut key_vec = Vec::new();
    for record in records {
        for field in fields {
            let field_value = match field {
                Field::KVOp => {
                    FieldValue::KVOp(record.data.kv_log().unwrap().op as u64)
                },
                Field::KVCpu => {
                    FieldValue::KVCpu(record.data.kv_log().unwrap().cpu)
                },
                Field::KVTid => {
                    FieldValue::KVTid(record.data.kv_log().unwrap().cpu)
                },
                _ => panic!("Unhandled group by field {:?}", field),
            };
            key_vec.push(field_value);
        }
        if let Some(x) = map.get_mut(&key_vec) {
            x.push(*record);
        } else {
            let k = key_vec.clone();
            let mut v = Vec::new();
            v.push(*record);
            assert!(map.insert(k, v).is_none());
        }
        key_vec.clear();
    }
    map
}

fn mach_query(
    sources: &[Source],
    min_ts: u64,
    max_ts: u64
) -> HashMap<Source, Vec<Record>> {

    let mut mach_sources = Vec::new();
    let mut result: HashMap<Source, Vec<Record>> = HashMap::new();
    for s in sources {
        let source = match s {
            Source::Hist => 0,
            Source::KV => 1,
            Source::Sched => 2,
        };
        let partition = 0;
        mach_sources.push(SourcePartition::new(source, partition));
        result.insert(*s, Vec::new());
    }

    let reader = MACH_READER.lock().unwrap().as_ref().unwrap().clone();
    let snapshot = reader.snapshot(&mach_sources).snapshot();
    let mut iterator = snapshot.iterator();


    while let Some(entry) = iterator.next_entry() {
        if entry.timestamp < min_ts {
            break;
        }
        if entry.timestamp >= max_ts {
            continue;
        }
        let source = entry.source;
        let item: Record = bincode::deserialize_from(&entry.data[..]).unwrap();
        let source = match source {
            0 => Source::Hist,
            1 => Source::KV,
            2 => Source::Sched,
            _ => panic!("Unhandled source in Mach"),
        };
        result.get_mut(&source).unwrap().push(item);
    }
    result
}

fn vec_query(
    sources: &[Source],
    min_ts: u64,
    max_ts: u64
) -> HashMap<Source, Vec<Record>> {
    let guard = VEC_READER.lock().unwrap();
    let inner_guard = guard.as_ref().unwrap().lock().unwrap();

    let now = Instant::now();
    let mut map = HashMap::new();
    for source in sources {
        map.insert(*source, Vec::new());
    }

    for (ts, item) in inner_guard.iter().rev() {
        if *ts < min_ts{
            break;
        }
        if *ts > max_ts {
            continue;
        }
        for source in sources {
            match (source, item.data) {

                (Source::Hist, Data::Hist(_)) => {
                    map.get_mut(&Source::Hist).unwrap().push(*item);
                    break;
                },

                (Source::KV, Data::KV(_)) => {
                    map.get_mut(&Source::KV).unwrap().push(*item);
                    break;
                },

                //(Source::Sched, Data::Sched(_)) => {
                //    map.get_mut(&Source::Sched).unwrap().push(*item);
                //    break;
                //},

                _ => {},
            }
        }
    }
    println!("Scan: {:?}", now.elapsed());
    map
}

fn filter(data: Vec<Record>) -> Arc<[Record]> {
    let mut v = Vec::new();

    let kvlogs_enabled = KVLOGS_ENABLED.load(SeqCst);
    let scheduler_enabled = SCHEDULER_ENABLED.load(SeqCst);

    for item in data {
        if item.data.is_kv_log() && kvlogs_enabled {
            v.push(item);
        }

        else if item.data.is_scheduler() && scheduler_enabled {
            v.push(item);
        }

        else if item.data.is_histogram() {
            v.push(item);
        }

    }
    v.into()
}

pub fn init_collector() {
    let server_rx: IpcReceiver<Vec<Record>> = ipc_receiver("localhost:3001");
    let mut sinks: Vec<(Sender<Arc<[Record]>>, &'static AtomicUsize)> = Vec::new();

    let mut mach = Mach::new("/nvme/data/tmp/vldb".into(), true, Duration::from_secs(1));
    let mach_reader = mach.reader();
    let (mach_tx, mach_rx) = bounded(1028);
    thread::spawn(move || {
        init_mach_sink(mach, mach_rx);
    });
    *MACH_READER.lock().unwrap() = Some(mach_reader);
    sinks.push((mach_tx, &MACH_DROP));

    //let vec = Arc::new(Mutex::new(Vec::new()));
    //let (vec_tx, vec_rx) = bounded(1028);
    //let v = vec.clone();
    //thread::spawn(move || {
    //    init_vec_sink(v, vec_rx);
    //});
    //sinks.push((vec_tx, &VEC_DROP));
    //*VEC_READER.lock().unwrap() = Some(vec.clone());

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

fn init_mach_sink(mut mach: Mach, rx: Receiver<Arc<[Record]>>) {
    let mut sl = Vec::new();
    thread::spawn(move || {
        loop {
            let count = MACH_COUNT.swap(0, SeqCst);
            let drop = MACH_COUNT.swap(0, SeqCst);
            println!("Mach Count {}\t Drop {}", count, drop);
            thread::sleep(Duration::from_secs(1));
        }
    });

    loop {
        if let Ok(data) = rx.try_recv() {
            let now = micros_since_epoch();

            let mut source = 0;
            let mut partition = 0;

            for item in data.iter() {
                match item.data {
                    Data::Hist(_) => source = 0,
                    Data::KV(_) => source = 1,
                    Data::Sched(_) => source = 2,
                }
                bincode::serialize_into(&mut sl, &item).unwrap();
                mach.push(
                    SourcePartition::new(source, partition),
                    now,
                    sl.as_slice(),
                    );
                sl.clear();
            }
            MACH_COUNT.fetch_add(data.len(), SeqCst);
        }
    }
}
