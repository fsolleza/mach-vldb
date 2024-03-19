#![allow(non_snake_case)]

use axum::{
    extract::Json,
    response::{Html, IntoResponse},
    routing::{post, get},
    Router
};
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, SystemTime},
};
use rand::prelude::*;
use serde::*;

fn dur_since_epoch() -> Duration {
    (SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)).unwrap()
}

#[derive(PartialEq, Eq, Deserialize, Debug, Clone)]
struct ScatterRequest {
    sources: Vec<String>,
}

#[derive(PartialEq, Eq, Serialize, Debug, Clone)]
struct ScatterResponse {
    data: Vec<ScatterSourceData>,
}

#[derive(PartialEq, Eq, Serialize, Debug, Clone)]
struct ScatterPoint {
    x: u64,
    y: i64,
}

#[derive(PartialEq, Eq, Serialize, Debug, Clone)]
struct ScatterSourceData {
    source: String,
    data: Vec<ScatterPoint>
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/scatterPlot", post(scatter_plot_handler));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn index() -> Html<&'static str> {
    Html(std::include_str!("../index.html"))
}

fn get_scatter_data(min_ms: u64, max_ms: u64, src: String) -> Vec<ScatterPoint> {

    static LOCKED_SCATTER_DATA:
        Mutex<Option<HashMap<String, Vec<ScatterPoint>>>> = Mutex::new(None);
    let mut guard = LOCKED_SCATTER_DATA.lock().unwrap();

    // If not initialized, initialize it
    if guard.is_none() {
        *guard = Some(HashMap::new());
    }

    let scatter_data: &mut HashMap<_, _> = guard.as_mut().unwrap();

    let data: &mut Vec<ScatterPoint> = scatter_data
            .entry(src)
            .or_insert(Vec::new());

    let mut rng = thread_rng();
    let mut cutoff = 0;
    for d in data.iter() {
        if d.x < min_ms {
            cutoff += 1;
        } else {
            break;
        }
    }
    for _ in data.drain(0..cutoff) {}

    let (src_last_millis, mut src_last_value) = match data.last() {
        Some(point) => (point.x, point.y),
        None => (min_ms, 0),
    };

    for millis in src_last_millis..max_ms {
        src_last_value += rng.gen_range(-5..5);
        if src_last_value < 0 {
            src_last_value = 0;
        }
        data.push(ScatterPoint { x: millis, y: src_last_value });
    }
    data.clone()
}

async fn scatter_plot_handler(
    msg: Json<ScatterRequest>,
) -> impl IntoResponse {

    let Json(req) = msg;
    println!("Request: {:?}", req);

    let now = dur_since_epoch();
    let max_ms = now.as_secs() * 1000;
    let min_ms = (now.as_secs() - 300) * 1000;

    let mut response = Vec::new();
    for source in req.sources {
        let data = get_scatter_data(min_ms, max_ms, source.clone());
        response.push(ScatterSourceData { source, data });
    }

    Json(ScatterResponse { data: response })
}
