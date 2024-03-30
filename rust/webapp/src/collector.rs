use common::{
    ipc::{ipc_receiver, IpcReceiver},
    data::*,
};
use mach_lib::{Mach, MachSnapshotIterator, MachReader, SourcePartition};
use crossbeam::channel::*;
use std::{
    thread,
    time::{Instant, Duration},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, AtomicU64, AtomicBool, Ordering::SeqCst},
    },
    collections::{HashMap, BTreeMap, HashSet},
};
use lazy_static::*;
use serde::*;
use dashmap::DashMap;
use chrono::{DateTime, Utc, NaiveDateTime, format::SecondsFormat};
use serde_json::Value;
use tokio::time::timeout;
use influxdb_iox_client::{
    write::Client,
    connection::Builder,
    flight,
    connection::Connection,
    format::QueryOutputFormat,
};
use influxdb_line_protocol::LineProtocolBuilder;
use futures::{stream::TryStreamExt, Future};
use arrow::record_batch::RecordBatch;

static BYTES_COUNT: AtomicUsize = AtomicUsize::new(0);

pub static MACH_COUNT_PER_SEC: AtomicUsize = AtomicUsize::new(0);
pub static MACH_DROPS_PER_SEC: AtomicUsize = AtomicUsize::new(0);

pub static INFLUX_COUNT_PER_SEC: AtomicUsize = AtomicUsize::new(0);
pub static INFLUX_DROPS_PER_SEC: AtomicUsize = AtomicUsize::new(0);

static MACH_COUNT: AtomicUsize = AtomicUsize::new(0);
static MACH_DROP: AtomicUsize = AtomicUsize::new(0);
static MACH_READER: Mutex<Option<MachReader>> = Mutex::new(None);

static INFLUX_COUNT: AtomicUsize = AtomicUsize::new(0);
static INFLUX_DROP: AtomicUsize = AtomicUsize::new(0);

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
        println!("Can't convert field from str {}", x);
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
    TS(u64),
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
    Mach,
    Influx,
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

#[derive(Copy, PartialEq, Eq, Deserialize, Serialize, Debug, Clone)]
pub struct Point {
    pub x: u64,
    pub y: u64
}

pub fn get_histogram(min_ts: u64, max_ts: u64) -> Vec<Point> {
    let reader = MACH_READER.lock().unwrap().as_ref().unwrap().clone();
    let snapshot = reader.snapshot(&[SourcePartition::new(0, 0)]).snapshot();
    let mut iterator = snapshot.iterator();

    let mut result = Vec::new();
    while let Some(entry) = iterator.next_entry() {
        if entry.timestamp < min_ts {
            break;
        }
        if entry.timestamp >= max_ts {
            continue;
        }
        let x = entry.timestamp;
        let item: Record = bincode::deserialize_from(&entry.data[..]).unwrap();
        let y = item.data.hist().unwrap().cnt;
        result.push(Point { x, y });
    }
    result
}

type QueryResult = HashMap<(u64, Vec<FieldValue>), u64>;

struct MachQuerySpec {
    source: u64,
    min_ts: u64,
    max_ts: u64,
    grouping_func: fn(u64, &Record, &mut (u64, Vec<FieldValue>)),
    aggregate_func: fn(&Record, &mut u64),
    resp: Sender<QueryResult>
}

fn mach_query_worker(rx: Receiver<MachQuerySpec>) {
    while let Ok(q) = rx.recv() {
        let MachQuerySpec {
            source,
            min_ts,
            max_ts,
            grouping_func,
            aggregate_func,
            resp
        } = q;
        let result = inner_mach_query(
            source,
            min_ts,
            max_ts,
            grouping_func,
            aggregate_func
        );
        resp.send(result).unwrap();
    }
}

lazy_static! {
    static ref MACH_QUERY_WORKER: Sender<MachQuerySpec> = {
        let (tx, rx) = bounded(100);
        thread::spawn(move || {
            mach_query_worker(rx);
        });
        tx
    };
}

pub fn mach_query(
    source: u64,
    min_ts: u64,
    max_ts: u64,
    grouping_func: fn(u64, &Record, &mut (u64, Vec<FieldValue>)),
    aggregate_func: fn(&Record, &mut u64)
) -> HashMap<(u64, Vec<FieldValue>), u64> {
    let (resp, rx) = bounded(1);
    let spec = MachQuerySpec {
        source, min_ts, max_ts, grouping_func, aggregate_func, resp
    };
    MACH_QUERY_WORKER.clone().send(spec).unwrap();
    rx.recv().unwrap()
}

fn inner_mach_query(
    source: u64,
    min_ts: u64,
    max_ts: u64,
    grouping_func: fn(u64, &Record, &mut (u64, Vec<FieldValue>)),
    aggregate_func: fn(&Record, &mut u64)
) -> HashMap<(u64, Vec<FieldValue>), u64> {
    println!("Range: {}", max_ts - min_ts);

    let now = Instant::now();
    let reader = MACH_READER.lock().unwrap().as_ref().unwrap().clone();
    let sp = SourcePartition::new(source, 0);
    let snapshot = reader.snapshot(&[sp]).snapshot();

    let mut iterator = MachIterator {
        iter: snapshot.iterator(),
        buf: Vec::new(),
        min_ts: min_ts,
        max_ts: max_ts,
        done: false,
    };

    let mut grouper = Grouping {
        buf: Vec::new(),
        map: HashMap::new(),
        group_id: 0,
        grouper: grouping_func,
        group_buf: (u64::MAX, Vec::new()),
    };

    let mut aggregator = Aggregate {
        aggregate: HashMap::new(),
        aggregate_func,
    };

    while !(iterator.done) {
        iterator.fill_buf();
        grouper.fill_buf(&iterator);
        aggregator.gather(&grouper, &iterator);
    }

    let mut results = HashMap::new();
    for (group, group_id) in grouper.map {
        let v = *aggregator.aggregate.get(&group_id).unwrap();
        results.insert(group, v);
    }
    println!("Iterator style execution {:?}", now.elapsed());
    results
}

struct Aggregate {
    pub aggregate: HashMap<u64, u64>,
    aggregate_func: fn(&Record, &mut u64)
}

impl Aggregate {
    fn gather(&mut self, grouper: &Grouping, iterator: &MachIterator) {
        let mut zipper = grouper
            .buf
            .iter()
            .zip(iterator.buf.iter());

        for (group, record) in zipper {
            let agg: &mut u64 = self.aggregate.entry(*group).or_insert(0);
            (self.aggregate_func)(&record.2, agg);
        }
    }
}

struct Grouping {
    buf: Vec<u64>,
    map: HashMap<(u64, Vec<FieldValue>), u64>,
    group_id: u64,
    grouper: fn(u64, &Record, &mut (u64, Vec<FieldValue>)),
    group_buf: (u64, Vec<FieldValue>),
}

impl Grouping {
    fn fill_buf(&mut self, iterator: &MachIterator) {
        self.buf.clear();
        for (ts, _, item) in iterator.buf.iter() {
            self.group_buf.0 = u64::MAX;
            self.group_buf.1.clear();
            (self.grouper)(*ts, item, &mut self.group_buf);
            let id = if let Some(x) = self.map.get(&self.group_buf) {
                *x
            } else {
                let id = self.group_id;
                self.group_id += 1;
                assert!(self.map.insert(self.group_buf.clone(), id).is_none());
                id
            };
            self.buf.push(id);
        }
    }
}

struct MachIterator {
    iter: MachSnapshotIterator,
    buf: Vec<(u64, Source, Record)>,
    min_ts: u64,
    max_ts: u64,
    done: bool,
}

impl MachIterator {
    fn fill_buf(&mut self) {
        self.buf.clear();
        while self.buf.len() < 256 {
            if let Some(entry) = self.iter.next_entry() {
                if entry.timestamp < self.min_ts {
                    self.done = true;
                    break;
                }
                if entry.timestamp >= self.max_ts {
                    continue;
                }
                let item: Record =
                    bincode::deserialize_from(&entry.data[..]).unwrap();
                let source = match entry.source {
                    0 => Source::Hist,
                    1 => Source::KV,
                    2 => Source::Sched,
                    _ => panic!("Unhandled source in Mach"),
                };
                self.buf.push((entry.timestamp, source, item));
            } else {
                self.done = true;
                break;
            }
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct Item {
    count: u64,
    cpu: u64,
    time: u64,
}

impl Item {
    fn parse_object(v: &serde_json::Value) -> Self {
        let count = v.get("count").unwrap().as_u64().unwrap();
        let cpu = v.get("cpu").unwrap().as_f64().unwrap() as u64;
        let time = {
            let time = v.get("time").unwrap().as_str().unwrap();
            let parse_fmt = "%Y-%m-%dT%H:%M:%S";
            let p = NaiveDateTime::parse_from_str(time, parse_fmt).unwrap();
            p.and_utc().timestamp_micros() as u64
        };

        Self {
            count,
            cpu,
            time
        }
    }
}

//fn get_influx_cpu_worker(rx: Receiver<(u64, u64, Sender<QueryResult>)>) {
//    let rt = tokio::runtime::Builder::new_multi_thread()
//        .worker_threads(64)
//        .enable_all()
//        .build()
//        .unwrap();
//    let client = influxdb3_client::Client::new("http://127.0.0.1:8181").unwrap();
//    while let Ok((min_ts, max_ts, tx)) = rx.recv() {
//        let now = Instant::now();
//        let r = rt.block_on(async {
//            let r = inner_get_influx_cpu(min_ts, max_ts, &client).await;
//            r
//        });
//        tx.send(r).unwrap();
//    }
//}
//
//lazy_static! {
//    static ref INFLUX_QUERY_WORKER: Sender<(u64, u64, Sender<QueryResult>)> = {
//        let (tx, rx) = bounded(100);
//        thread::spawn(move || {
//            get_influx_cpu_worker(rx);
//        });
//        tx
//    };
//}

pub async fn get_influx_cpu(
    min_ts: u64,
    max_ts: u64
) -> HashMap<(u64, Vec<FieldValue>), u64> {

    let min_formatted = {
        let dt = DateTime::from_timestamp_micros(min_ts as i64).unwrap();
        dt.to_rfc3339()
    };
    let max_formatted = {
        let dt = DateTime::from_timestamp_micros(max_ts as i64).unwrap();
        dt.to_rfc3339()
    };
    let query = format!(
        "SELECT COUNT(op) FROM kv WHERE time > '{}' GROUP BY time(1s),cpu fill(none)",
        min_formatted
    );
    println!("Query: {}", query);

    let now = Instant::now();

    // This entire bit is adapted from 
    // https://github.com/influxdata/influxdb/blob/bb6a5c0bf6968117251617cda99cb39a5274b6dd/influxdb_iox/src/commands/query.rs#L77
    let connection = Builder::default()
        .build("http://127.0.0.1:8082")
        .await
        .unwrap();
    let mut client = flight::Client::new(connection);
    client.add_header("bucket", "vldb_demo").unwrap();
    let mut query_results = client.influxql("vldb_demo", query.clone()).await.unwrap();
    let mut batches: Vec<_> = (&mut query_results).try_collect().await.unwrap();
    println!("Influx query {} took: {:?}", query, now.elapsed());

    let schema = query_results
        .inner()
        .schema()
        .cloned()
        .ok_or(influxdb_iox_client::flight::Error::NoSchema).unwrap();
    batches.push(RecordBatch::new_empty(schema));
    let formatted_result = QueryOutputFormat::Json.format(&batches).unwrap();

    let v: Value = serde_json::from_str(&formatted_result).unwrap();

    let mut map = HashMap::new();

    let arr = v.as_array().unwrap();
    for item in arr {
        let parsed_item = Item::parse_object(item);
        let key = {
            let group = vec![FieldValue::KVCpu(parsed_item.cpu)];
            (parsed_item.time, group)
        };
        map.insert(key, parsed_item.count);
    }

    map
}

//async fn inner_get_influx_cpu(
//    min_ts: u64,
//    max_ts: u64,
//    client: &influxdb3_client::Client
//) -> HashMap<(u64, Vec<FieldValue>), u64> {
//
//    let min_formatted = {
//        let dt = DateTime::from_timestamp_micros(min_ts as i64).unwrap();
//        dt.to_rfc3339()
//    };
//    let max_formatted = {
//        let dt = DateTime::from_timestamp_micros(max_ts as i64).unwrap();
//        dt.to_rfc3339()
//    };
//    let query = format!(
//        "SELECT * FROM kv WHERE time > '{}'",
//        min_formatted
//    );
//    println!("Query: {}", query);
//
//    let now = Instant::now();
//    let mut resp_bytes = client
//        .api_v3_query_sql("vldb-demo", query.clone())
//        .format(influxdb3_client::Format::Json) // could be Json
//        .send()
//        .await
//        .unwrap();
//    println!("Influx query {} took: {:?}", query, now.elapsed());
//
//    let data: &str = std::str::from_utf8(&resp_bytes).unwrap();
//    let v: Value = serde_json::from_str(data).unwrap();
//
//    //let arr = v.as_array().unwrap();
//    let mut map = HashMap::new();
//    //for item in arr {
//    //    let parsed_item = Item::parse_object(item);
//    //    let key = {
//    //        let group = vec![FieldValue::KVCpu(parsed_item.cpu)];
//    //        (parsed_item.time, group)
//    //    };
//    //    map.insert(key, parsed_item.count);
//    //}
//    map
//}


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

/***************************
COLLECTORS
***************************/


pub fn init_collector() {
    let server_rx: IpcReceiver<Vec<Record>> = ipc_receiver("localhost:3001");

    let mut sinks: Vec<(Sender<Arc<[Record]>>, &'static AtomicUsize)> = Vec::new();

    // Setup mach sink
    let mut mach = Mach::new("/nvme/data/tmp/vldb".into(), true, Duration::from_secs(1));
    let mach_reader = mach.reader();
    let (mach_tx, mach_rx) = bounded(1028);
    thread::spawn(move || {
        init_mach_sink(mach, mach_rx);
    });
    *MACH_READER.lock().unwrap() = Some(mach_reader);
    sinks.push((mach_tx, &MACH_DROP));

    // Setup influx sink
    let (influx_tx, influx_rx) = bounded(1028);
    thread::spawn(move || {
        init_influx_sink(influx_rx);
    });
    sinks.push((influx_tx, &INFLUX_DROP));

    loop {
        if let Ok(data) = server_rx.try_recv() {
            let item = filter(data);
            let len = item.len();
            if len > 0 {
                for (sink, drop) in &sinks {
                    if sink.try_send(item.clone()).is_err() {
                        drop.fetch_add(len, SeqCst);
                    }
                }
            }
        }
    }
}

fn make_lp_bytes(samples: &[Record]) -> Vec<u8> {
    let mut lp = LineProtocolBuilder::new();

    for r in samples {
        match r.data {
            Data::Sched(x) => panic!("Unimplmented line protocol"),
                Data::Hist(x) => {
                    lp = lp
                        .measurement("hist")
                        .tag("source","hist")
                        .field("max",x.max)
                        .field("ts",r.timestamp)
                        .close_line();
                },
                Data::KV(x) => {
                    lp = lp
                        .measurement("kv")
                        .tag("source","kv")
                        .field("op", x.op as u64)
                        .field("cpu", x.cpu)
                        .field("tid", x.tid)
                        .field("dur_nanos", x.dur_nanos)
                        .field("ts",r.timestamp)
                        .close_line();
                },
        }
    }
    lp.build()
}

fn append_to_lp(lp: &mut String, timestamp: u64, r: &Record) {
    use std::fmt::Write;

    let ts = r.timestamp;
    if lp.len() > 0 {
        lp.push('\n');
    }
    match r.data {
        Data::Sched(x) => panic!("Unimplmented line protocol"),
        Data::Hist(x) => {
            lp.push_str("hist,");
            lp.push_str("source=hist ");
            lp.push_str("max=");
            write!(lp, "{},", x.max);
            lp.push_str("cnt=");
            write!(lp, "{},", x.cnt);
        },
        Data::KV(x) => {
            lp.push_str("kv,");
            lp.push_str("source=hist ");

            lp.push_str("op=");
            write!(lp, "{},", x.op as u64);

            lp.push_str("cpu=");
            write!(lp, "{},", x.cpu);

            lp.push_str("tid=");
            write!(lp, "{},", x.tid);

            lp.push_str("dur_nanos=");
            write!(lp, "{},", x.dur_nanos);
        },
    }

    // generation timestamp
    lp.push_str("ts=");
    write!(lp, "{}",ts);

    write!(lp, " {}", timestamp);
}

static INFLUX_MAX: AtomicU64 = AtomicU64::new(0);
static INFLUX_WRITE_ADDR: &str = "http://127.0.0.1:8080";

fn init_influx_sink(rx: Receiver<Arc<[Record]>>) {
    thread::spawn(move || {
        let mut last_count = 0;
        let mut last_drop = 0;
        loop {
            let count = INFLUX_COUNT.load(SeqCst);
            let drop = INFLUX_DROP.load(SeqCst);
            println!(
                "Influx Count {}\t Drop {}",
                count-last_count,
                drop-last_drop
            );
            INFLUX_COUNT_PER_SEC.swap(count - last_count, SeqCst);
            INFLUX_DROPS_PER_SEC.swap(drop - last_drop, SeqCst);
            last_count = count;
            last_drop = drop;
            thread::sleep(Duration::from_secs(1));
        }
    });

    let mut handles = Vec::new();
    for _ in 0..1 {
        let rx = rx.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connection = rt.block_on(async {
            Builder::default()
                .build(INFLUX_WRITE_ADDR)
                .await
                .unwrap()
        });
        let mut client = Client::new(connection);
        //let mut buf = String::new();
        handles.push(thread::spawn(move || {
            loop {
                if let Ok(data) = rx.try_recv() {
                    let now = micros_since_epoch();
                    //buf.clear();
                    let buf = make_lp_bytes(&data);
                    //for item in data.iter() {
                    //    append_to_lp(&mut buf, now, item);
                    //}
                    let lp = std::str::from_utf8(&buf[..]).unwrap();
                    //let mut req = client.api_v3_write_lp("vldb-demo");
                    rt.block_on(async {
                        client.write_lp("vldb_demo", lp).await.unwrap();
                    });
                    INFLUX_COUNT.fetch_add(data.len(), SeqCst);
                    INFLUX_MAX.store(now, SeqCst);
                }
            }
        }));
    }
    for h in handles {
        h.join();
    }
}

fn init_mach_sink(mut mach: Mach, rx: Receiver<Arc<[Record]>>) {
    let mut sl = Vec::new();
    thread::spawn(move || {
        let mut last_count = 0;
        let mut last_drop = 0;
        loop {
            let count = MACH_COUNT.load(SeqCst);
            let drop = MACH_DROP.load(SeqCst);
            println!(
                "Mach Count {}\t Drop {}",
                count-last_count,
                drop-last_drop
            );
            MACH_COUNT_PER_SEC.swap(count-last_count, SeqCst);
            MACH_DROPS_PER_SEC.swap(drop-last_drop, SeqCst);
            last_count = count;
            last_drop = drop;
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
