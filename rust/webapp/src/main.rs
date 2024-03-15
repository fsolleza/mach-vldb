#![allow(non_snake_case)]

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Json,
    },
    response::{Html, IntoResponse},
    routing::{post, get},
    Router
};
use std::{
    collections::HashMap,
    sync::Mutex,
};
use futures::{sink::SinkExt, stream::StreamExt};
use rand::prelude::*;
use serde::*;
use lazy_static::*;

lazy_static! {
    static ref CONFIG: Mutex<Config> = {
        let conf = Config {
            activeSources: vec![],
            pause: false,
        };
        Mutex::new(conf)
    };
}

#[derive(PartialEq, Eq, Deserialize, Debug, Clone)]
struct Config {
    activeSources: Vec<String>,
    pause: bool,
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/config", post(config_handler))
        .route("/scatter", get(scatter_handler));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn index() -> Html<&'static str> {
    Html(std::include_str!("../index.html"))
}

async fn config_handler(
    msg: Json<Config>,
) -> impl IntoResponse {
    let Json(config) = msg;
    *CONFIG.lock().unwrap() = config.clone();
    println!("Set config {:?}", config);
    Json(())
}

async fn scatter_handler(
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| scatter_stream(socket))
}

fn try_update_config(conf: &mut Config) -> Result<(), ()> {
    if let Ok(c) = CONFIG.try_lock() {
        if &*c != conf {
            *conf = c.clone();
        }
        return Ok(());
    }
    Err(())
}

struct DummyGenerator {
    val: i32,
}

impl DummyGenerator {
    fn new(val: i32) -> Self {
        DummyGenerator {
            val,
        }
    }

    fn next_value(&mut self, rng: &mut ThreadRng) -> i32 {
        self.val += rng.gen_range(-5..5);
        if self.val < 0 {
            self.val = 0;
        }
        self.val
    }
}

async fn scatter_stream(stream: WebSocket) {
    // Split the stream so send and receive at the same time
    let (mut sender, _) = stream.split();
    let mut conf = CONFIG.lock().unwrap().clone();

    // Some dummy sources
    let mut sources = HashMap::new();
    sources.insert("A", DummyGenerator::new(20));
    sources.insert("B", DummyGenerator::new(15));

    let mut data = Vec::new();
    loop {
        let _ = try_update_config(&mut conf);
        data.clear();

        if !conf.pause {
            for source in &conf.activeSources {
                let val = sources
                    .get_mut(source.as_str())
                    .unwrap()
                    .next_value(&mut thread_rng());
                let source = source.to_owned();
                data.push((source, val));
            }

            let s = serde_json::to_string(&data).unwrap();
            if let Err(x) = sender.send(Message::Text(s)).await {
                println!("error {:?}", x);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}

