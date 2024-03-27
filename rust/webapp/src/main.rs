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
    sync::Mutex,
    time::{Instant, Duration, SystemTime},
    thread,
};
use rand::prelude::*;
use serde::*;
use tokio::runtime;
use common::data::{Record, micros_since_epoch};
use lazy_static::*;

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

async fn scatter_plot_handler(
    msg: Json<ScatterRequest>,
) -> impl IntoResponse {

    lazy_static! {
        static ref CACHE:
            Mutex<Option<(ScatterRequest, ScatterResponse)>> =
            Mutex::new(None);
    }

    let now = Instant::now();

    let Json(req) = msg;

    let mut cache_guard = CACHE.lock().unwrap();
    let mut adjusted_req = req.clone();
    let mut repeat = false;
    let mut min_ts_millis = req.min_ts_millis;
    if let Some(x) = cache_guard.as_mut() {
        if req.is_repeat(&x.0) {
            repeat = true;
            adjusted_req.min_ts_millis = x.0.max_ts_millis + 1;
            println!(
                "This is a repeat, queries for timestamp {} {}",
                adjusted_req.min_ts_millis,
                adjusted_req.max_ts_millis
            );
        }
    }

    let query = adjusted_req.to_query();
    let result = query.execute();
    let mut series = Vec::new();
    for i in 0..query.series.len() {
        let q = &query.series[i];
        let r = &result[i];
        for (k, v) in r.iter() {
            let mut group = String::new();
            for g in k.iter() {
                group.push_str(&format!("{}, ", g.as_string()));
            }
            let ser = ScatterSeries {
                source: group,
                data: v.iter().map(|x| ScatterPoint {
                    x: x.x / 1_000,
                    y: x.y
                }).collect(),
            };
            series.push(ser);
        }
    }

    // merge the results with the cache
    if let Some(x) = cache_guard.as_mut() {
        if repeat {
            println!("Merging");
            for i in 0..series.len() {
                let s = &mut series[i];

                let mut j = usize::MAX;
                for (idx, cached) in x.1.data.iter().enumerate() {
                    if cached.source == s.source {
                        j = idx;
                        break;
                    }
                }
                if j == usize::MAX {
                    break;
                }
                let cached = &mut x.1.data[j];
                assert_eq!(s.source, cached.source);

                let last_ts = s.data[s.data.len() - 1].x;
                for c in cached.data.iter() {
                    if c.x >= last_ts {
                        continue;
                    }
                    if c.x < min_ts_millis {
                        break;
                    }
                    s.data.push(*c);
                }
            }
        }
    }

    println!("Elapsed: {:?}", now.elapsed());
    let response = ScatterResponse { data: series };
    *cache_guard = Some((req, response.clone()));

    Json(response)
}
