mod core;

use arrow::record_batch::RecordBatch;
use common::{data::*, ipc::*};
use core::*;
use futures::stream::TryStreamExt;
use influxdb_iox_client::{
	connection::Builder, connection::Connection, flight,
	format::QueryOutputFormat, write::Client,
};
use influxdb_line_protocol::LineProtocolBuilder;
use std::fmt::Write;
use std::{
	sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
	thread,
	time::Duration,
};

static INFLUX_COUNT: AtomicUsize = AtomicUsize::new(0);
static INFLUX_COUNT_PER_SEC: AtomicUsize = AtomicUsize::new(0);
static INFLUX_TUPLE_ID: AtomicU64 = AtomicU64::new(0);
static INFLUX_WRITE_ADDR: &str = "http://127.0.0.1:8080";

fn counter() {
	let mut last_count = 0;
	loop {
		let count = INFLUX_COUNT.load(SeqCst);
		let last_cps = INFLUX_COUNT_PER_SEC.swap(count - last_count, SeqCst);
		last_count = count;
		println!("Count: {}", last_cps);
		thread::sleep(Duration::from_secs(1));
	}
}

fn handle_writes(data: Vec<Record>) {
	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();
	let connection = rt.block_on(async {
		Builder::default().build(INFLUX_WRITE_ADDR).await.unwrap()
	});
	let mut client = Client::new(connection);

	let now = micros_since_epoch();
	let buf = make_lp_bytes(data.as_slice(), now);
	let lp = std::str::from_utf8(&buf[..]).unwrap();
	rt.block_on(async {
		client.write_lp("vldb_demo", lp).await.unwrap();
	});
	INFLUX_COUNT.fetch_add(data.len(), SeqCst);
}

fn handle_query_string(query: String) -> InfluxResponse {
	let rt = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	let batches: Vec<_> = rt.block_on(async {
		let connection: Connection = Builder::default()
			.build("http://127.0.0.1:8082")
			.await
			.unwrap();
		let mut client = flight::Client::new(connection);
		client.add_header("bucket", "vldb_demo").unwrap();
		let mut query_results =
			client.influxql("vldb_demo", query.clone()).await.unwrap();
		let mut batches: Vec<_> =
			(&mut query_results).try_collect().await.unwrap();
		let schema = query_results
			.inner()
			.schema()
			.cloned()
			.ok_or(influxdb_iox_client::flight::Error::NoSchema)
			.unwrap();
		batches.push(RecordBatch::new_empty(schema));
		batches
	});

	let formatted_result = match QueryOutputFormat::Json.format(&batches) {
		Ok(j) => InfluxResponse::Json(j),
		Err(_) => InfluxResponse::Err("Can't get anything from Influx".into()),
	};
	formatted_result
}

fn handle_queries(r: InfluxRequest) -> InfluxResponse {
	match r {
		InfluxRequest::Count => {
			let influx_count = INFLUX_COUNT_PER_SEC.load(SeqCst);
			return InfluxResponse::Count(influx_count as u64);
		}
		InfluxRequest::Query(query) => handle_query_string(query),
	}
}

fn parse_comm(comm: &[u8]) -> &str {
	let i8sl = unsafe { &*(comm as *const _ as *const [i8]) };
	unsafe {
		std::ffi::CStr::from_ptr(i8sl[..].as_ptr())
			.to_str()
			.unwrap()
	}
}

fn make_lp_bytes(samples: &[Record], time_micros: u64) -> Vec<u8> {
	let mut lp = LineProtocolBuilder::new();
	let time_nanos = (time_micros * 1000) as i64;

	let mut tuple_ids = INFLUX_TUPLE_ID.fetch_add(samples.len() as u64, SeqCst);
	let mut buf = String::new();
	for r in samples {
		write!(&mut buf, "{}", tuple_ids).unwrap(); // deal with duplicates
		tuple_ids += 1;
		match &r.data {
			Data::Hist(x) => {
				lp = lp
					.measurement("table_hist")
					.tag("hist_source", "hist")
					.tag("id", &buf)
					.field("hist_max", x.max)
					.field("hist_ts", r.timestamp)
					.timestamp(time_nanos)
					.close_line();
			}
			Data::KV(x) => {
				lp = lp
					.measurement("table_kv")
					.tag("source", "kv")
					.tag("id", &buf)
					.field("kv_op", x.op as u64)
					.field("kv_cpu", x.cpu)
					.field("kv_tid", x.tid)
					.field("kv_dur_nanos", x.dur_nanos)
					.field("kv_ts", r.timestamp)
					.timestamp(time_nanos)
					.close_line();
			}
			Data::Sched(x) => {
				lp = lp
					.measurement("table_kv")
					.tag("source", "sched")
					.tag("id", &buf)
					.field("sched_cpu", x.cpu as u64)
					.field("sched_comm", parse_comm(&x.comm))
					.field("sched_ts", r.timestamp)
					.timestamp(time_nanos)
					.close_line();
			}
		}
		buf.clear();
	}
	lp.build()
}

fn main() {
	let mut handles = Vec::new();
	handles.push(thread::spawn(counter));

	let rx: IpcReceiver<Vec<Record>> = ipc_receiver("0.0.0.0:3040");
	for _ in 0..2 {
		let rx = rx.clone();
		handles.push(thread::spawn(move || {
			while let Ok(batch) = rx.recv() {
				handle_writes(batch);
			}
		}));
	}

	handles.push(thread::spawn(move || {
		common::ipc::ipc_serve("0.0.0.0:3041", handle_queries);
	}));
	for handle in handles {
		handle.join().unwrap();
	}
}
