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
use fxhash::FxHashMap;
use std::fmt::Write;

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
    println!("ENABLING KV LOGS IN COLLECTOR");
    let mut connection = common::ipc::ipc_client_connect("0.0.0.0:3011").unwrap();
    let request = workload::AppRequest::Enable;
    let _response: workload::AppResponse = common::ipc::ipc_send(&request, &mut connection).unwrap();
}

pub fn disable_kvlogs() {
    KVLOGS_ENABLED.store(false, SeqCst);
}

pub fn enable_scheduler() {
    let mut connection = common::ipc::ipc_client_connect("0.0.0.0:3050").unwrap();
    let request = sched_switch::SchedRequest::Enable;
    let _response: sched_switch::SchedResponse = common::ipc::ipc_send(&request, &mut connection).unwrap();
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum FieldValue {
    KVOp(u64),
    KVCpu(u64),
    KVTid(u64),
    KVDur(u64),
    HistMax(u64),
    HistCnt(u64),
    TS(u64),
    SchedComm(String),
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
    let mut connection = common::ipc::ipc_client_connect("0.0.0.0:3021").unwrap();
    let request = mach_server::MachRequest::Hist(min_ts, max_ts);
    let response: mach_server::MachResponse = common::ipc::ipc_send(&request, &mut connection).unwrap();
    let result = response.from_hist().unwrap();
    result.iter().copied().map(|(x, y)| Point {x, y}).collect()
}

pub fn get_mach_cpu(min_ts: u64, max_ts: u64) -> FxHashMap<(u64, Vec<FieldValue>), u64> {
    let mut connection = common::ipc::ipc_client_connect("0.0.0.0:3021").unwrap();
    let request = mach_server::MachRequest::ByCpu(min_ts, max_ts);
    let response: mach_server::MachResponse = common::ipc::ipc_send(&request, &mut connection).unwrap();
    let result = response.from_cpu().unwrap();

    let mut map = FxHashMap::default();
    for ((ts, cpu), v) in result.iter() {
        map.insert((*ts, vec![FieldValue::KVCpu(*cpu)]), *v);
    }
    map
}

pub fn get_mach_sched(min_ts: u64, max_ts: u64) -> FxHashMap<(u64, Vec<FieldValue>), u64> {
    let mut connection = common::ipc::ipc_client_connect("0.0.0.0:3021").unwrap();
    let request = mach_server::MachRequest::ByComm(min_ts, max_ts);
    let response: mach_server::MachResponse = common::ipc::ipc_send(&request, &mut connection).unwrap();
    let result = response.from_comm().unwrap();

    let mut map = FxHashMap::default();
    for ((ts, comm), v) in result.iter() {
        let comm = unsafe { common::data::parse_comm(comm) };
        map.insert((*ts, vec![FieldValue::SchedComm(comm.into())]), *v);
    }
    map
}

fn do_influx_query(
    query: String
) -> Value {
    // This entire bit is adapted from 
    // https://github.com/influxdata/influxdb/blob/bb6a5c0bf6968117251617cda99cb39a5274b6dd/influxdb_iox/src/commands/query.rs#L77

    let mut connection = common::ipc::ipc_client_connect("0.0.0.0:3041").unwrap();
    let request = influx_server::InfluxRequest::Query(query);
    let response: influx_server::InfluxResponse = common::ipc::ipc_send(&request, &mut connection).unwrap();
    let json = response.to_json_string().unwrap();
    serde_json::from_str(&json).unwrap()
}

fn rfc3339_from_micros(ts: u64) -> String {
    let dt = DateTime::from_timestamp_micros(ts as i64).unwrap();
    dt.to_rfc3339()
}

pub async fn get_influx_sched(
    min_ts: u64,
    max_ts: u64
) -> FxHashMap<(u64, Vec<FieldValue>), u64> {

    let min_formatted = rfc3339_from_micros(min_ts);
    let query = format!(
        "SELECT COUNT(sched_cpu) FROM table_kv WHERE time > '{}' AND source = 'sched' GROUP BY time(1s),sched_comm fill(none)",
        min_formatted
    );
    let v = do_influx_query(query);
    let mut map = FxHashMap::default();
    let arr = v.as_array().unwrap();
    for item in arr {
        let parsed_item = InfluxSchedComm::parse_object(item);
        let key = {
            let group = vec![FieldValue::SchedComm(parsed_item.comm)];
            (parsed_item.time, group)
        };
        map.insert(key, parsed_item.count);
    }
    map
}

pub async fn get_influx_cpu(
    min_ts: u64,
    max_ts: u64
) -> FxHashMap<(u64, Vec<FieldValue>), u64> {
    let min_formatted = rfc3339_from_micros(min_ts);
    let query = format!(
        "SELECT COUNT(kv_op) FROM table_kv WHERE time > '{}' AND source = 'kv' GROUP BY time(1s),kv_cpu fill(none)",
        min_formatted
    );
    let v = do_influx_query(query);
    let mut map = FxHashMap::default();
    let arr = v.as_array().unwrap();
    for item in arr {
        let parsed_item = InfluxQPSCPU::parse_object(item);
        let key = {
            let group = vec![FieldValue::KVCpu(parsed_item.cpu)];
            (parsed_item.time, group)
        };
        map.insert(key, parsed_item.count);
    }
    map
}

#[derive(Copy, Clone, Debug)]
struct InfluxQPSCPU {
    count: u64,
    cpu: u64,
    time: u64,
}

impl InfluxQPSCPU {
    fn parse_object(v: &serde_json::Value) -> Self {
        let count = v.get("count").unwrap().as_u64().unwrap();
        let cpu = v.get("kv_cpu").unwrap().as_f64().unwrap() as u64;
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


#[derive(Clone, Debug)]
struct InfluxSchedComm {
    count: u64,
    comm: String,
    time: u64,
}

impl InfluxSchedComm {
    fn parse_object(v: &serde_json::Value) -> Self {
        let count = v.get("count").unwrap().as_u64().unwrap();
        let comm: String = v.get("sched_comm").unwrap().as_str().unwrap().into();
        let time = {
            let time = v.get("time").unwrap().as_str().unwrap();
            let parse_fmt = "%Y-%m-%dT%H:%M:%S";
            let p = NaiveDateTime::parse_from_str(time, parse_fmt).unwrap();
            p.and_utc().timestamp_micros() as u64
        };

        Self {
            count,
            comm,
            time
        }
    }
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

/***************************
COLLECTORS
***************************/

static TOTAL_COUNT: AtomicU64 = AtomicU64::new(0);
//static INFLUX_COUNT: AtomicU64 = AtomicU64::new(0);
//static MACH_COUNT: AtomicU64 = AtomicU64::new(0);

static TOTAL_COUNT_PER_SEC: AtomicU64 = AtomicU64::new(0);
//static INFLUX_COUNT_PER_SEC: AtomicU64 = AtomicU64::new(0);
//static MACH_COUNT_PER_SEC: AtomicU64 = AtomicU64::new(0);

fn count_per_sec(last_count: &mut u64, cnt: &AtomicU64, ps: &AtomicU64) {
    let count = cnt.load(SeqCst);
    let diff = count - *last_count;
    ps.swap(diff, SeqCst);
    *last_count = count;
}

fn run_count_per_sec() {
    let mut last_total_count = 0;
    let mut last_influx_count = 0;
    let mut last_mach_count = 0;
    loop {
        count_per_sec(&mut last_total_count, &TOTAL_COUNT, &TOTAL_COUNT_PER_SEC);
        //count_per_sec(&mut last_influx_count, &INFLUX_COUNT, &INFLUX_COUNT_PER_SEC);
        //count_per_sec(&mut last_mach_count, &MACH_COUNT, &MACH_COUNT_PER_SEC);
        thread::sleep(Duration::from_secs(1));
    }
}

pub fn init_count_receiver() {
    thread::spawn(run_count_per_sec);
    thread::spawn(move || {
        let server_rx: IpcReceiver<u64> = ipc_receiver("localhost:3001");
        while let Ok(count) = server_rx.recv() {
            println!("COUNT: {}", count);
            TOTAL_COUNT.fetch_add(count, SeqCst);
        }
    });
}

pub fn get_total_count() -> u64 {
    TOTAL_COUNT_PER_SEC.load(SeqCst)
}

pub fn get_influx_count() -> u64 {
    let mut connection = common::ipc::ipc_client_connect("0.0.0.0:3041").unwrap();
    let request = influx_server::InfluxRequest::Count;
    let response: influx_server::InfluxResponse = common::ipc::ipc_send(&request, &mut connection).unwrap();
    response.count().unwrap()
}

pub fn get_mach_count() -> u64 {
    let mut connection = common::ipc::ipc_client_connect("0.0.0.0:3021").unwrap();
    let request = mach_server::MachRequest::Count;
    let response: mach_server::MachResponse = common::ipc::ipc_send(&request, &mut connection).unwrap();
    response.from_count().unwrap()
}


//pub fn init_collector() {
//    let server_rx: IpcReceiver<Vec<Record>> = ipc_receiver("localhost:3001");
//
//    let mut sinks: Vec<(Sender<Arc<[Record]>>, &'static AtomicUsize)> = Vec::new();
//
//    // Setup mach sink
//    let mut mach = Mach::new("/nvme/data/tmp/vldb".into(), true, Duration::from_secs(1));
//    let mach_reader = mach.reader();
//    let (mach_tx, mach_rx) = bounded(1028);
//    thread::spawn(move || {
//        init_mach_sink(mach, mach_rx);
//    });
//    *MACH_READER.lock().unwrap() = Some(mach_reader);
//    sinks.push((mach_tx, &MACH_DROP));
//
//    // Setup influx sink
//    //let (influx_tx, influx_rx) = bounded(1028);
//    //thread::spawn(move || {
//    //    init_influx_sink(influx_rx);
//    //});
//    //sinks.push((influx_tx, &INFLUX_DROP));
//
//    loop {
//        if let Ok(data) = server_rx.try_recv() {
//            let item = filter(data);
//            let len = item.len();
//            if len > 0 {
//                for (sink, drop) in &sinks {
//                    if sink.try_send(item.clone()).is_err() {
//                        drop.fetch_add(len, SeqCst);
//                    }
//                }
//            }
//        }
//    }
//}

//fn make_lp_bytes(samples: &[Record], time_micros: u64) -> Vec<u8> {
//    let mut lp = LineProtocolBuilder::new();
//    let time_nanos = (time_micros * 1000) as i64;
//
//    let mut tuple_ids = INFLUX_TUPLE_ID.fetch_add(samples.len() as u64, SeqCst);
//    let mut buf = String::new();
//    for r in samples {
//        write!(&mut buf, "{}", tuple_ids);
//        tuple_ids += 1;
//        match &r.data {
//                Data::Hist(x) => {
//                    lp = lp
//                        .measurement("table_hist")
//                        .tag("hist_source","hist")
//                        .tag("hist_id", &buf)
//                        .field("hist_max",x.max)
//                        .field("hist_ts",r.timestamp)
//                        .timestamp(time_nanos)
//                        .close_line();
//                },
//                Data::KV(x) => {
//                    lp = lp
//                        .measurement("table_kv")
//                        .tag("source","kv")
//                        .tag("id", &buf)
//                        .field("kv_op", x.op as u64)
//                        .field("kv_cpu", x.cpu)
//                        .field("kv_tid", x.tid)
//                        .field("kv_dur_nanos", x.dur_nanos)
//                        .field("kv_ts",r.timestamp)
//                        .timestamp(time_nanos)
//                        .close_line();
//                },
//                Data::Sched(x) => {
//                    lp = lp
//                        .measurement("table_kv")
//                        .tag("source","sched")
//                        .tag("id", &buf)
//                        .field("sched_cpu", x.cpu as u64)
//                        .field("sched_comm", x.comm.as_str())
//                        .field("sched_ts",r.timestamp)
//                        .timestamp(time_nanos)
//                        .close_line();
//                },
//        }
//        buf.clear();
//    }
//    lp.build()
//}

//static INFLUX_TUPLE_ID: AtomicU64 = AtomicU64::new(0);
//static INFLUX_WRITE_ADDR: &str = "http://127.0.0.1:8080";
//
//fn init_influx_sink(rx: Receiver<Arc<[Record]>>) {
//    thread::spawn(move || {
//        let mut last_count = 0;
//        let mut last_drop = 0;
//        loop {
//            let count = INFLUX_COUNT.load(SeqCst);
//            let drop = INFLUX_DROP.load(SeqCst);
//            //println!(
//            //    "Influx Count {}\t Drop {}",
//            //    count-last_count,
//            //    drop-last_drop
//            //);
//            INFLUX_COUNT_PER_SEC.swap(count - last_count, SeqCst);
//            INFLUX_DROPS_PER_SEC.swap(drop - last_drop, SeqCst);
//            last_count = count;
//            last_drop = drop;
//            thread::sleep(Duration::from_secs(1));
//        }
//    });
//
//    let mut handles = Vec::new();
//    for _ in 0..4 {
//        let rx = rx.clone();
//        let rt = tokio::runtime::Builder::new_current_thread()
//            .enable_all()
//            .build()
//            .unwrap();
//        let connection = rt.block_on(async {
//            Builder::default()
//                .build(INFLUX_WRITE_ADDR)
//                .await
//                .unwrap()
//        });
//        let mut client = Client::new(connection);
//        //let mut buf = String::new();
//        handles.push(thread::spawn(move || {
//            loop {
//                if let Ok(data) = rx.try_recv() {
//                    let now = micros_since_epoch();
//                    let buf = make_lp_bytes(&data, now);
//                    let lp = std::str::from_utf8(&buf[..]).unwrap();
//                    rt.block_on(async {
//                        client.write_lp("vldb_demo", lp).await.unwrap();
//                    });
//                    INFLUX_COUNT.fetch_add(data.len(), SeqCst);
//                    //INFLUX_MAX.store(now, SeqCst);
//                }
//            }
//        }));
//    }
//    for h in handles {
//        h.join();
//    }
//}
//
//fn init_mach_sink(mut mach: Mach, rx: Receiver<Arc<[Record]>>) {
//    let mut sl = Vec::new();
//    thread::spawn(move || {
//        let mut last_count = 0;
//        let mut last_drop = 0;
//        loop {
//            let count = MACH_COUNT.load(SeqCst);
//            let drop = MACH_DROP.load(SeqCst);
//            println!(
//                "Mach Count {}\t Drop {}",
//                count-last_count,
//                drop-last_drop
//            );
//            MACH_COUNT_PER_SEC.swap(count-last_count, SeqCst);
//            MACH_DROPS_PER_SEC.swap(drop-last_drop, SeqCst);
//            last_count = count;
//            last_drop = drop;
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
//                match item.data {
//                    Data::Hist(_) => source = 0,
//                    Data::KV(_) => source = 1,
//                    Data::Sched(_) => { source = 2; partition = 1; },
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
