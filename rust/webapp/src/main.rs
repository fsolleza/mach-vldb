#![allow(non_snake_case)]

use monitoring_application::*;

use axum::{
	extract::{Json, State},
	response::{Html, IntoResponse},
	routing::{get, post},
	Router,
};
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
	io::{Read, Write},
};
use tokio::runtime;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use clap::*;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	addr_memstore: String,
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

async fn request_handler(
	msg: Json<Request>,
) -> impl IntoResponse {

	let Json(req) = msg;
	println!("Got request {:?}", req);

	let response: Response = match req {
		Request::Statistics => unimplemented!(),
		Request::Data(ref x) => {
			// Connect based on the storage engine
			let mut stream = match x.storage {
				StorageEngine::Mem => 
					TcpStream::connect(&ARGS.addr_memstore).await.unwrap(),
			};

			// Send request
			let req = bincode::serialize(&req).unwrap();
			stream.write_all(&req.len().to_be_bytes()).await.unwrap();
			stream.write_all(&req).await.unwrap();

			// Take response
			let mut sz = [0u8; 8];
			stream.read_exact(&mut sz).await.unwrap();
			let sz = u64::from_be_bytes(sz);
			let mut v = vec![0u8; sz as usize];
			stream.read_exact(&mut v).await.unwrap();
			let resp: Response = bincode::deserialize_from(&v[..]).unwrap();
			resp
		}
	};
	
	Json(response)
}
