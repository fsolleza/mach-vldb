#![allow(non_snake_case)]

mod api;

use axum::{
	extract::{Json, State},
	response::{Html, IntoResponse},
	routing::{get, post},
	Router,
};
use api::*;
use fxhash::FxHashMap;
use lazy_static::*;
use rand::prelude::*;
use serde::*;
use std::cmp::Reverse;
use std::{
	collections::HashMap,
	sync::{
		atomic::{AtomicU64, Ordering::SeqCst},
		Arc, Mutex,
	},
	thread,
	time::{Duration, Instant, SystemTime},
};
use tokio::runtime;
use tokio::sync::mpsc;

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
	for h in handles {
		h.join().unwrap();
	}
}

async fn web_server() {
	let app = Router::new()
		.route("/", get(index))
		.route("/dataRequest", post(request_handler));
	let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
	println!("Listening!");
	axum::serve(listener, app).await.unwrap();
}

async fn index() -> Html<&'static str> {
	Html(std::include_str!("../index.html"))
}

async fn request_handler(
	msg: Json<Request>,
) -> impl IntoResponse {

	let Json(req) = msg;
	println!("Got request {:?}", req);
	let mut responses = Vec::new();
	for v in req.requests {
		let r = responses.push(vec![0]);
	}
	let r = Response { responses };
	Json(r)
}
