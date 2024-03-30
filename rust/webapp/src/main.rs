#![allow(non_snake_case)]

mod collector;

use axum::{
    extract::Json,
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

fn main() {
    let h = thread::spawn(move || {
        let builder = runtime::Builder::new_multi_thread()
            .worker_threads(64)
            .enable_all()
            .build()
            .unwrap();
        builder.block_on(async {
            println!("HERE");
            web_server().await;
        });
    });
    collector::init_collector();
    h.join().unwrap();
}

#[derive(PartialEq, Eq, Deserialize, Debug, Clone)]
struct ScatterRequest {
    lines: Vec<ScatterLine>,
    min_ts_millis: u64,
    max_ts_millis: u64,
}

#[derive(PartialEq, Eq, Deserialize, Debug, Clone)]
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
    panic!("Unhandled source");
}

async fn web_server() {
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
    let mach_count = collector::MACH_COUNT_PER_SEC.load(SeqCst);
    let influx_count = collector::INFLUX_COUNT_PER_SEC.load(SeqCst);
    let mach_dropped = collector::MACH_DROPS_PER_SEC.load(SeqCst);
    let influx_dropped = collector::INFLUX_DROPS_PER_SEC.load(SeqCst);

    let m_total = mach_count + mach_dropped;
    let m = (100. * (mach_dropped as f64 / (m_total) as f64)) as u64;

    let i_total = influx_count + influx_dropped;
    let i = (100. * (influx_dropped as f64 / (i_total) as f64)) as u64;

    Json((m, i))
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

type QueryResult = HashMap<(u64, Vec<FieldValue>), u64>;

struct Cached {
    result: QueryResult,
    min_ts: u64,
    max_ts: u64,
}

fn mach_cache_merge_update(
    mut new: QueryResult,
    min_ts: u64,
    max_ts: u64
) -> QueryResult {
    let mut guard = MACH_CACHED.lock().unwrap();
    for (k, v) in &guard.result {
        if k.0 >= min_ts && k.0 <= max_ts {
            if let Some(x) = new.get_mut(k) {
                *x = (*x).max(*v);
            } else {
                new.insert(k.clone(), *v);
            }
        }
    }
    guard.result = new.clone();
    guard.min_ts = min_ts;
    guard.max_ts = max_ts;
    new
}

fn influx_cache_merge_update(
    mut new: QueryResult,
    min_ts: u64,
    max_ts: u64
) -> QueryResult {
    let mut guard = INFLUX_CACHED.lock().unwrap();
    for (k, v) in &guard.result {
        if k.0 >= min_ts && k.0 <= max_ts {
            if let Some(x) = new.get_mut(k) {
                *x = (*x).max(*v);
            } else {
                new.insert(k.clone(), *v);
            }
        }
    }
    guard.result = new.clone();
    guard.min_ts = min_ts;
    guard.max_ts = max_ts;
    new
}


lazy_static! {
    static ref MACH_CACHED: Mutex<Cached> = {
        Mutex::new(Cached {
            result: HashMap::new(),
            min_ts: u64::MAX,
            max_ts: u64::MAX
        })
    };
    static ref INFLUX_CACHED: Mutex<Cached> = {
        Mutex::new(Cached {
            result: HashMap::new(),
            min_ts: u64::MAX,
            max_ts: u64::MAX
        })
    };
}

static MACH_LAST_MAX: AtomicU64 = AtomicU64::new(0);
static INFLUX_LAST_MAX: AtomicU64 = AtomicU64::new(0);

async fn influxHandler(
    msg: Json<ScatterRequest>,
) -> impl IntoResponse {

    let now = Instant::now();

    let Json(req) = msg;

    let min_ts = req.min_ts_millis * 1000;
    let max_ts = req.max_ts_millis * 1000;

    let last_max = INFLUX_LAST_MAX.load(SeqCst);
    let truncated_min_ts = {
        if last_max == 0 {
            max_ts - 1_000_000
        } else {
            last_max - 1_000_000
        }
    };
    INFLUX_LAST_MAX.store(max_ts, SeqCst);

    let mut result = Vec::new();
    for line in req.lines.iter() {
        let interm = {
            let r = get_influx_cpu(truncated_min_ts, max_ts).await;
            influx_cache_merge_update(r, min_ts, max_ts)
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

async fn machHandler(
    msg: Json<ScatterRequest>,
) -> impl IntoResponse {

    let now = Instant::now();

    let Json(req) = msg;

    let min_ts = req.min_ts_millis * 1000;
    let max_ts = req.max_ts_millis * 1000;

    let last_max = MACH_LAST_MAX.load(SeqCst);
    let truncated_min_ts = {
        if last_max == 0 {
            max_ts - 1_000_000
        } else {
            last_max - 1_000_000
        }
    };
    MACH_LAST_MAX.store(max_ts, SeqCst);

    let mut result = Vec::new();
    for line in req.lines.iter() {
        let source = source_to_int(line.source.as_str());
        let interm = {
            let r = mach_query(
                source,
                truncated_min_ts,
                max_ts,
                group_by_time_cpu,
                aggregate_func_cnt
            );
            mach_cache_merge_update(r, min_ts, max_ts)
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

    //let mut cached = CACHED.lock().unwrap();

    //let mut adjusted_req = req.clone();
    //adjusted_req.min_ts_millis = adjusted_req.max_ts_millis - 3000;
    //let mut repeated = false;

    // Adjust query if repeated and there's a cache
    //if let Some(x) = cached.as_mut() {
    //    if adjusted_req.is_repeat(&x.request) {
    //        repeated = true;
    //        adjusted_req.min_ts_millis = x.request.max_ts_millis - 1;
    //    }
    //}

    //let query = adjusted_req.to_query();
    //let mut result: Vec<HashMap<Vec<FieldValue>, Vec<Point>>> = query.execute();

    // Merge results if repeated
    //if repeated {

    //    let req_min_ts = req.min_ts_millis * 1000;
    //    let cached_result = cached.as_ref().unwrap().result.clone();

    //    for i in 0..result.len() {
    //        let r = &mut result[i];
    //        let c = &cached_result[i];

    //        for (key, vec) in r.iter_mut() {

    //            let mut min_ts = u64::MAX;
    //            for item in vec.iter() {
    //                if min_ts > item.x { min_ts = item.x; }
    //            }
    //            let cached_vec = c.get(key).unwrap();
    //            //println!("Result vec {:?}", vec);
    //            for j in 0..cached_vec.len() {
    //                let cached_val = &cached_vec[j];
    //                if cached_val.x < min_ts && cached_val.x > req_min_ts {
    //                    vec.push(*cached_val);
    //                }
    //            }
    //        }
    //    }
    //}

    // Ensure every vector is sorted by decreasing time
    //for map in result.iter_mut() {
    //    for (k, vec) in map.iter_mut() {
    //        vec.sort_by_key(|x| std::cmp::Reverse(x.x))
    //    }
    //}

    // Cache this result, replacing the cached one
    //{
    //    let to_cache = Cached {
    //        result: Arc::new(result.clone()),
    //        request: req,
    //    };
    //    *cached = Some(to_cache);
    //}

