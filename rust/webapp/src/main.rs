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
    sync::{Arc, Mutex},
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
        let builder = runtime::Builder::new_current_thread()
            .worker_threads(1)
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

impl ScatterRequest {
    fn to_query(&self) -> collector::Query {
        collector::Query {
            series: self.lines.iter().map(|x| x.to_query_series()).collect(),
            min_ts: self.min_ts_millis * 1000,
            max_ts: self.max_ts_millis * 1000,
        }
    }

    fn is_repeat(&self, other: &Self) -> bool {
        self.lines == other.lines
    }
}

#[derive(PartialEq, Eq, Deserialize, Debug, Clone)]
struct ScatterLine {
    storage: String,
    source: String,
    agg: String,
    field: String,
    group: Vec<String>,
}

impl ScatterLine {
    fn to_query_series(&self) -> collector::QuerySeries {
        let storage = collector::Storage::from_str(&self.storage).unwrap();
        let source = collector::Source::from_str(&self.source).unwrap();
        let variable = collector::Field::from_str(&self.field).unwrap();
        let aggr = collector::AggregateFunc::from_str(&self.agg).unwrap();
        let grouping: Vec<collector::Field> = self.group
            .iter()
            .map(|x| collector::Field::from_str(x).unwrap())
            .collect();
        collector::QuerySeries {
            storage,
            source,
            grouping,
            variable,
            aggr,
        }
    }
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

async fn web_server() {
    let app = Router::new()
        .route("/", get(index))
        .route("/scatterPlot", post(scatter_plot_handler))
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

struct Cached {
    request: ScatterRequest,
    result: Arc<Vec<HashMap<Vec<FieldValue>, Vec<Point>>>>,
}

lazy_static! {
    static ref CACHED: Mutex<Option<Cached>> = Mutex::new(None);
}

async fn scatter_plot_handler(
    msg: Json<ScatterRequest>,
) -> impl IntoResponse {


    let now = Instant::now();

    let Json(req) = msg;

    let mut cached = CACHED.lock().unwrap();

    let mut adjusted_req = req.clone();
    adjusted_req.min_ts_millis = adjusted_req.max_ts_millis - 3000;
    let mut repeated = false;

    // Adjust query if repeated and there's a cache
    //if let Some(x) = cached.as_mut() {
    //    if adjusted_req.is_repeat(&x.request) {
    //        repeated = true;
    //        adjusted_req.min_ts_millis = x.request.max_ts_millis - 1;
    //    }
    //}

    let query = adjusted_req.to_query();
    let mut result: Vec<HashMap<Vec<FieldValue>, Vec<Point>>> = query.execute();

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
    for map in result.iter_mut() {
        for (k, vec) in map.iter_mut() {
            vec.sort_by_key(|x| std::cmp::Reverse(x.x))
        }
    }

    // Cache this result, replacing the cached one
    //{
    //    let to_cache = Cached {
    //        result: Arc::new(result.clone()),
    //        request: req,
    //    };
    //    *cached = Some(to_cache);
    //}

    let mut series = Vec::new();
    for i in 0..query.series.len() {
        let q = &query.series[i];
        let r = &result[i];
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

    let response = ScatterResponse { data: series };
    Json(response)
}
