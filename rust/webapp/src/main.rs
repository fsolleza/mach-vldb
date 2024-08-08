#![allow(non_snake_case)]

use axum::{
	extract::{Json, State},
	response::{Html, IntoResponse},
	routing::{get, post},
	Router,
};
use clap::*;
use fxhash::FxHashMap;
use lazy_static::*;
use rand::prelude::*;
use serde::*;
use std::cmp::Reverse;
use std::{
	collections::HashMap,
	io::{Read, Write},
	sync::{
		atomic::{AtomicU64, Ordering::SeqCst},
		Arc, Mutex,
	},
	thread,
	time::{Duration, Instant, SystemTime},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	addr_memstore: String,

	#[arg(short, long)]
	addr_mach: String,
}

lazy_static! {
	static ref ARGS: Args = Args::parse();
}

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


#[derive(Serialize, Deserialize, Debug)]
enum WebRequest {
	Mem(monitoring_application::Request),
}

impl WebRequest {
	fn request(&self) -> &monitoring_application::Request {
		match self {
			Self::Mem(x) => &x,
			_ => unimplemented!(),
		}
	}
}

async fn request_handler(msg: Json<WebRequest>) -> impl IntoResponse {
	let Json(web_req) = msg;

	println!("Got request {:?}", web_req);

	let mut stream = match web_req {
		WebRequest::Mem(_) => {
			TcpStream::connect(&ARGS.addr_memstore).await.unwrap()
		}
	};

	let bytes = web_req.request().to_binary();

	stream.write_all(&bytes.len().to_be_bytes()).await.unwrap();
	stream.write_all(&bytes).await.unwrap();

	let mut sz = [0u8; 8];
	stream.read_exact(&mut sz).await.unwrap();
	let sz = u64::from_be_bytes(sz);

	let mut v = vec![0u8; sz as usize];
	stream.read_exact(&mut v).await.unwrap();
	let response = monitoring_application::Response::from_binary(&v[..]);

	Json(response)
}
