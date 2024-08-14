#![allow(non_snake_case)]

use api::monitoring_application;
use api::kv_workload;
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

	#[arg(short, long)]
	addr_influx: String,

	/// Send commands to RDB instances through these addresses and ports
	#[arg(short, long, value_parser, num_args = 1.., value_delimiter = ' ')]
    workload_addrs: Vec<String>,
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
		.route("/charts", get(charts))
		.route("/dataRequest", post(request_handler));
	let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
	println!("Listening!");
	axum::serve(listener, app).await.unwrap();
}

async fn index() -> Html<&'static str> {
	Html(std::include_str!("../index.html"))
}

async fn charts() -> Html<&'static str> {
	Html(std::include_str!("../charts.html"))
}

#[derive(Serialize, Deserialize, Debug)]
enum WebRequest {
	Mem(monitoring_application::Request),
	Mach(monitoring_application::Request),
	Influx(monitoring_application::Request),
	SetQps { rate: u64 },
	OpsPerSec,
}

impl WebRequest {
	fn request(&self) -> &monitoring_application::Request {
		match self {
			Self::Mem(x) => &x,
			Self::Mach(x) => &x,
			Self::Influx(x) => &x,
			Self::SetQps { .. } => unimplemented!(),
			Self::OpsPerSec => unimplemented!(),
		}
	}
}

#[derive(Serialize, Deserialize)]
enum WebResponse {
	SetQps,
	OpsPerSec(u64),
	DataReceived(u64),
	DataCompleteness(f64),
	KvOpsPercentile(Vec<api::monitoring_application::Record>),
	ReadSyscalls(Vec<api::monitoring_application::Record>),
	Scheduler(Vec<api::monitoring_application::Record>),
}

impl Into<WebResponse> for api::monitoring_application::Response {
	fn into(self) -> WebResponse  {
		match self {
			Self::DataReceived(x) => WebResponse::DataReceived(x),
			Self::DataCompleteness(x) => WebResponse::DataCompleteness(x),
			Self::KvOpsPercentile(x) => WebResponse::KvOpsPercentile(x),
			Self::ReadSyscalls(x) => WebResponse::ReadSyscalls(x),
			Self::Scheduler(x) => WebResponse::Scheduler(x),
		}
	}
}

impl Into<WebResponse> for api::kv_workload::Response {
	fn into(self) -> WebResponse  {
		match self {
			Self::SetQps => WebResponse::SetQps,
			Self::OpsPerSec(x) => WebResponse::OpsPerSec(x),
		}
	}
}

async fn request_handler(msg: Json<WebRequest>) -> impl IntoResponse {
	let Json(web_req) = msg;

	println!("Got request {:?}", web_req);

	let resp = match web_req {
		WebRequest::Mem(_) => {
			handle_storage_request(web_req).await
		}
		WebRequest::Mach(_) => {
			handle_storage_request(web_req).await
		}
		WebRequest::Influx(_) => {
			handle_storage_request(web_req).await
		}
		WebRequest::SetQps{ rate }=> {
			handle_set_qps_request(rate).await
		}
		WebRequest::OpsPerSec => {
			handle_ops_per_sec_request().await
		}
	};

	Json(resp)
}

async fn handle_storage_request(web_req: WebRequest) -> WebResponse {
	let mut stream = match web_req {
		WebRequest::Mem(_) => {
			TcpStream::connect(&ARGS.addr_memstore).await.unwrap()
		},
		WebRequest::Mach(_) => {
			TcpStream::connect(&ARGS.addr_mach).await.unwrap()
		}
		WebRequest::Influx(_) => {
			TcpStream::connect(&ARGS.addr_influx).await.unwrap()
		}
		WebRequest::SetQps { .. } => unreachable!(),
		WebRequest::OpsPerSec { .. } => unreachable!(),
	};

	let bytes = web_req.request().to_binary();

	stream.write_all(&bytes.len().to_be_bytes()).await.unwrap();
	stream.write_all(&bytes).await.unwrap();

	let mut sz = [0u8; 8];
	stream.read_exact(&mut sz).await.unwrap();
	let sz = u64::from_be_bytes(sz);

	let mut v = vec![0u8; sz as usize];
	stream.read_exact(&mut v).await.unwrap();
	monitoring_application::Response::from_binary(&v[..]).into()
}

async fn handle_set_qps_request(qps: u64) -> WebResponse {
	println!("Workload addrs {:?}", ARGS.workload_addrs);
	let qps = qps / ARGS.workload_addrs.len() as u64;
	for addr in ARGS.workload_addrs.iter() {
		println!("Sending set qps request to addr {}", addr);

		let req = kv_workload::Request::SetQps(qps);
		let binary = req.to_binary();

		let mut stream = TcpStream::connect(addr).await.unwrap();
		stream.write_all(&binary.len().to_be_bytes()).await.unwrap();
		stream.write_all(&binary).await.unwrap();

		let sz = {
			let mut sz_bytes = [0u8; 8];
			stream.read_exact(&mut sz_bytes).await.unwrap();
			usize::from_be_bytes(sz_bytes)
		};

		let mut vec = vec![0u8; sz];
		stream.read_exact(&mut vec).await.unwrap();
		let resp = kv_workload::Response::from_binary(&vec);
		println!("set qps response: {:?}", resp);
	}

	WebResponse::SetQps
}

async fn handle_ops_per_sec_request() -> WebResponse {
	let mut total = 0;
	for addr in ARGS.workload_addrs.iter() {
		let req = kv_workload::Request::OpsPerSec;
		let binary = req.to_binary();

		let mut stream = TcpStream::connect(addr).await.unwrap();
		stream.write_all(&binary.len().to_be_bytes()).await.unwrap();
		stream.write_all(&binary).await.unwrap();

		let sz = {
			let mut sz_bytes = [0u8; 8];
			stream.read_exact(&mut sz_bytes).await.unwrap();
			usize::from_be_bytes(sz_bytes)
		};

		let mut vec = vec![0u8; sz];
		stream.read_exact(&mut vec).await.unwrap();
		let resp = kv_workload::Response::from_binary(&vec);
		match resp {
			kv_workload::Response::OpsPerSec(x) => total += x,
			_ => unreachable!(),
		}
	}
	WebResponse::OpsPerSec(total)
}

