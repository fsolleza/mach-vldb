#![allow(non_snake_case)]

mod collector;

use axum::{
    extract::{State, Json},
    response::{Html, IntoResponse},
    routing::{post, get},
    Router
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, atomic::{AtomicU64, Ordering::SeqCst}},
    time::{Instant, Duration, SystemTime},
    thread,
};
use rand::prelude::*;
use serde::*;
use tokio::runtime;
use common::data::{Record, micros_since_epoch};
use lazy_static::*;
use std::cmp::Reverse;
use collector::*;
use tokio::sync::mpsc;
use fxhash::FxHashMap;

fn main() {
    let mut handles = Vec::new();
    handles.push(thread::spawn(move || {
        let builder = runtime::Builder::new_multi_thread()
            .worker_threads(64)
            .enable_all()
            .build()
            .unwrap();
        builder.block_on(async {
            web_server().await;
        });
    }));
    //collector::init_collector();
    for h in handles {
        h.join().unwrap();
    }
}

#[derive(PartialEq, Eq, Deserialize, Debug, Clone)]
struct ScatterRequest {
    lines: Vec<ScatterLine>,
    min_ts_millis: u64,
    max_ts_millis: u64,
}

#[derive(PartialEq, Eq, Deserialize, Debug, Clone, Hash)]
struct ScatterLine {
    source: String,
    agg: String,
    field: String,
    group: Vec<String>,
}

#[derive(PartialEq, Eq, Serialize, Debug, Clone)]
struct ScatterResponse {
    data: Vec<ScatterSeries>,
}

#[derive(PartialEq, Eq, Serialize, Debug, Clone)]
struct ScatterSeries {
    source: String,
    data: Vec<ScatterPoint>
}

#[derive(PartialEq, Eq, Serialize, Debug, Clone, Copy)]
struct ScatterPoint {
    x: u64,
    y: u64,
}

#[derive(PartialEq, Eq, Deserialize, Serialize, Debug, Clone)]
struct SetCollectionRequest {
    collectKv: bool,
    collectSched: bool,
}

#[derive(PartialEq, Eq, Deserialize, Serialize, Debug, Clone)]
struct HistogramRequest {
    min_ts_millis: u64,
    max_ts_millis: u64,
}

fn group_by_time_op(ts: u64, r: &Record, v: &mut (u64, Vec<FieldValue>)) {
    let b = r.timestamp - r.timestamp % 1_000_000;
    let log = r.data.kv_log().unwrap();
    v.0 = b;
    v.1.push(FieldValue::KVOp(log.op as u64));
}

fn group_by_time_cpu(ts: u64, r: &Record, v: &mut (u64, Vec<FieldValue>)) {
    let b = r.timestamp - r.timestamp % 1_000_000;
    let log = r.data.kv_log().unwrap();
    v.0 = b;
    v.1.push(FieldValue::KVCpu(log.cpu as u64));
}

fn aggregate_func_cnt(r: &Record, o: &mut u64) {
    *o += 1;
}

fn source_to_int(s: &str) -> u64 {
    if s == "hist" { return 0; }
    if s == "kv" { return 1; }
    if s == "sched" { return 2; }
    panic!("Unhandled source");
}

async fn web_server() {
    init_count_receiver();
    let app = Router::new()
        .route("/", get(index))
        .route("/machHandler", post(machHandler))
        .route("/samplesPerSecond", get(samplesPerSecHandler))
        .route("/influxHandler", post(influxHandler))
        .route("/histogram", post(get_histogram_handler))
        .route("/setCollection", post(set_collection_handler));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .unwrap();
    println!("Listening!");
    axum::serve(listener, app).await.unwrap();
}

async fn index() -> Html<&'static str> {
    Html(std::include_str!("../index.html"))
}

async fn samplesPerSecHandler() -> impl IntoResponse {
    println!("samples per second handler");
    let mach_count = get_mach_count() as f64;
    let influx_count = get_influx_count() as f64;
    let total_count = get_total_count() as f64;

    let m = 100. * (1. - (mach_count / total_count));
    let i = 100. * (1. - (influx_count / total_count));

    /*
     * TODO: we set this to zero because it should be. Mach keeps up but we need
     * a better way of calculating this...
     */
    Json((0, i))
}

async fn set_collection_handler(
    msg: Json<SetCollectionRequest>,
) -> impl IntoResponse {
    let Json(req) = msg;
    if req.collectKv {
        collector::enable_kvlogs();
    } else {
        collector::disable_kvlogs();
    }

    if req.collectSched {
        collector::enable_scheduler();
    } else {
        collector::disable_scheduler();
    }

    Json(())
}

async fn get_histogram_handler(
    msg: Json<HistogramRequest>
) -> impl IntoResponse {
    let Json(req) = msg;
    let min_ts = req.min_ts_millis * 1000;
    let max_ts = req.max_ts_millis * 1000;
    let mut result = get_histogram(min_ts, max_ts);
    for r in result.iter_mut() {
        r.x = r.x / 1000 // need to change to millis
    }
    Json(result)
}

type QueryResult = FxHashMap<(u64, Vec<FieldValue>), u64>;

struct Cached {
    result: QueryResult,
    min_ts: u64,
    max_ts: u64,
}

impl Cached {
    fn new_empty() -> Self {
        Self {
            result: FxHashMap::default(),
            min_ts: u64::MAX,
            max_ts: u64::MAX
        }
    }
}

fn cache_merge_update(
    cache: &mut HashMap<ScatterLine, Cached>,
    query: &ScatterLine,
    new: &mut QueryResult,
    min_ts: u64,
    max_ts: u64
) {
    //let mut guard = MACH_CACHED.lock().unwrap();
    let mut map = match cache.get_mut(query) {
        Some(x) => x,
        None => {
            cache.entry(query.clone()).or_insert_with(Cached::new_empty)
        },
    };
    for (k, v) in &map.result {
        if k.0 >= min_ts && k.0 <= max_ts {
            if let Some(x) = new.get_mut(k) {
                *x = (*x).max(*v);
            } else {
                new.insert(k.clone(), *v);
            }
        }
    }
    map.result = new.clone();
    map.min_ts = min_ts;
    map.max_ts = max_ts;
}

lazy_static! {
    static ref MACH_CACHED: Mutex<HashMap<ScatterLine, Cached>> = {
        Mutex::new(HashMap::new())
    };
    static ref INFLUX_CACHED: Arc<tokio::sync::Mutex<HashMap<ScatterLine, Cached>>> = {
        Arc::new(tokio::sync::Mutex::new(HashMap::new()))
    };
}

async fn get_influx_data(line: &ScatterLine, min_ts: u64, max_ts: u64) -> FxHashMap<(u64, Vec<FieldValue>), u64> {
    let now = Instant::now();
    let r = if line.source == "kv" && line.field == "kvdur" && line.agg == "count" {
        let r = get_influx_cpu(min_ts, max_ts).await;
        println!("Influx get cpu ops: {:?}", now.elapsed());
        r
    } else if line.source == "sched" && line.field == "schedcpu" && line.agg == "count" {
        let r = get_influx_sched(min_ts, max_ts).await;
        println!("Influx get sched: {:?}", now.elapsed());
        r
    } else {
        panic!("Unhandled line {:?}", line);
    };
    r
}


async fn influxHandler(
    msg: Json<ScatterRequest>,
) -> impl IntoResponse {
        let Json(req) = msg;
        let min_ts = req.min_ts_millis * 1000;
        let max_ts = req.max_ts_millis * 1000;
        let influx_cached = INFLUX_CACHED.clone();
        let mut cache = influx_cached.lock().await;

        let mut result = Vec::new();
        for line in req.lines.iter() {
            let truncated_min_ts = match cache.get(line) {
                Some(m) => m.max_ts - 1_000_000,
                None => max_ts - 1_000_000,
            };
            let interm = {
                let mut r = get_influx_data(line, truncated_min_ts, max_ts).await;
                cache_merge_update(&mut *cache, line, &mut r, min_ts, max_ts);
                r
            };
            let mut map: HashMap<Vec<FieldValue>, Vec<Point>> = HashMap::new();
            for ((ts, group), val) in interm {
                map.entry(group)
                    .or_insert_with(Vec::new)
                    .push(Point { x: ts, y: val });
            }
            for (g, v) in map.iter_mut() {
                v.sort_by_key(|p| std::cmp::Reverse(p.x));
            }
            result.push(map);
        }

        let mut series = Vec::new();
        for r in result { // 0..query.series.len() {
            //let q = &query.series[i];
            //let r = &result[i];
            for (k, v) in r.iter() {
                let mut group = String::new();
                for g in k.iter() {
                    group.push_str(&format!("{}, ", g.as_string()));
                }
                let mut data: Vec<ScatterPoint> = v.iter().map(|x| ScatterPoint {
                    x: x.x / 1_000,
                    y: x.y
                }).collect();
                data.sort_by_key(|k| k.x);
                let ser = ScatterSeries {
                    source: group,
                    data,
                };
                series.push(ser);
            }
        }
        series.sort_by(|a, b| a.source.cmp(&b.source));
        let response = ScatterResponse { data: series };
        Json(response)
}

fn get_mach_data(line: &ScatterLine, min_ts: u64, max_ts: u64) -> FxHashMap<(u64, Vec<FieldValue>), u64> {
    let now = Instant::now();
    let r = if line.source == "kv" && line.field == "kvdur" && line.agg == "count" {
        let r = get_mach_cpu(min_ts, max_ts);
        println!("Mach get cpu ops: {:?}", now.elapsed());
        r
    } else if line.source == "sched" && line.field == "schedcpu" && line.agg == "count" {
        let r = get_mach_sched(min_ts, max_ts);
        println!("Mach get sched: {:?}", now.elapsed());
        r
    } else {
        panic!("Unhandled line {:?}", line);
    };
    r
}

async fn machHandler(
    msg: Json<ScatterRequest>,
) -> impl IntoResponse {

    let now = Instant::now();

    let Json(req) = msg;

    let min_ts = req.min_ts_millis * 1000;
    let max_ts = req.max_ts_millis * 1000;
    let mut cache = MACH_CACHED.lock().unwrap();

    let mut result = Vec::new();
    for line in req.lines.iter() {

        let truncated_min_ts = match cache.get(line) {
            Some(m) => m.max_ts - 1_000_000,
            None => max_ts - 1_000_000,
        };
        let interm = {
            let mut r = get_mach_data(line, truncated_min_ts, max_ts);
            cache_merge_update(&mut *cache, line, &mut r, min_ts, max_ts);
            r
        };
        let mut map: HashMap<Vec<FieldValue>, Vec<Point>> = HashMap::new();
        for ((ts, group), val) in interm {
            map.entry(group)
                .or_insert_with(Vec::new)
                .push(Point { x: ts, y: val });
        }

        for (g, v) in map.iter_mut() {
            v.sort_by_key(|p| std::cmp::Reverse(p.x));
        }
        result.push(map);
    }

    let mut series = Vec::new();
    for r in result { // 0..query.series.len() {
        //let q = &query.series[i];
        //let r = &result[i];
        for (k, v) in r.iter() {
            let mut group = String::new();
            for g in k.iter() {
                group.push_str(&format!("{}, ", g.as_string()));
            }
            let mut data: Vec<ScatterPoint> = v.iter().map(|x| ScatterPoint {
                x: x.x / 1_000,
                y: x.y
            }).collect();
            data.sort_by_key(|k| k.x);
            let ser = ScatterSeries {
                source: group,
                data,
            };
            series.push(ser);
        }
    }
    series.sort_by(|a, b| a.source.cmp(&b.source));

    let response = ScatterResponse { data: series };
    Json(response)
}
