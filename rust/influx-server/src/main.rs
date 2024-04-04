use common::{
    ipc::{ipc_receiver, IpcReceiver},
    data::*,
};
use mach_lib::{Mach, MachSnapshotIterator, MachReader, SourcePartition};
use crossbeam::channel::*;
use std::{
    thread,
    time::{Instant, Duration},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, AtomicU64, AtomicBool, Ordering::SeqCst},
    },
    collections::{HashMap, BTreeMap, HashSet},
};
use lazy_static::*;
use serde::*;
use dashmap::DashMap;
use chrono::{DateTime, Utc, NaiveDateTime, format::SecondsFormat};
use serde_json::Value;
use tokio::time::timeout;
use influxdb_iox_client::{
    write::Client,
    connection::Builder,
    flight,
    connection::Connection,
    format::QueryOutputFormat,
};
use influxdb_line_protocol::LineProtocolBuilder;
use futures::{stream::TryStreamExt, Future};
use arrow::record_batch::RecordBatch;
use std::fmt::Write;

static INFLUX_COUNT: AtomicUsize = AtomicUsize::new(0);
static INFLUX_DROP: AtomicUsize = AtomicUsize::new(0);
static INFLUX_COUNT_PER_SEC: AtomicUsize = AtomicUsize::new(0);
static INFLUX_DROPS_PER_SEC: AtomicUsize = AtomicUsize::new(0);
static INFLUX_TUPLE_ID: AtomicU64 = AtomicU64::new(0);
static INFLUX_WRITE_ADDR: &str = "http://127.0.0.1:8080";

fn init_influx_sink(rx: Receiver<Arc<[Record]>>) {
    thread::spawn(move || {
        let mut last_count = 0;
        let mut last_drop = 0;
        loop {
            let count = INFLUX_COUNT.load(SeqCst);
            let drop = INFLUX_DROP.load(SeqCst);
            //println!(
            //    "Influx Count {}\t Drop {}",
            //    count-last_count,
            //    drop-last_drop
            //);
            INFLUX_COUNT_PER_SEC.swap(count - last_count, SeqCst);
            INFLUX_DROPS_PER_SEC.swap(drop - last_drop, SeqCst);
            last_count = count;
            last_drop = drop;
            thread::sleep(Duration::from_secs(1));
        }
    });

    let mut handles = Vec::new();
    for _ in 0..4 {
        let rx = rx.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connection = rt.block_on(async {
            Builder::default()
                .build(INFLUX_WRITE_ADDR)
                .await
                .unwrap()
        });
        let mut client = Client::new(connection);
        //let mut buf = String::new();
        handles.push(thread::spawn(move || {
            loop {
                if let Ok(data) = rx.try_recv() {
                    let now = micros_since_epoch();
                    let buf = make_lp_bytes(&data, now);
                    let lp = std::str::from_utf8(&buf[..]).unwrap();
                    rt.block_on(async {
                        client.write_lp("vldb_demo", lp).await.unwrap();
                    });
                    INFLUX_COUNT.fetch_add(data.len(), SeqCst);
                    //INFLUX_MAX.store(now, SeqCst);
                }
            }
        }));
    }
    for h in handles {
        h.join();
    }
}

fn make_lp_bytes(samples: &[Record], time_micros: u64) -> Vec<u8> {
    let mut lp = LineProtocolBuilder::new();
    let time_nanos = (time_micros * 1000) as i64;

    let mut tuple_ids = INFLUX_TUPLE_ID.fetch_add(samples.len() as u64, SeqCst);
    let mut buf = String::new();
    for r in samples {
        write!(&mut buf, "{}", tuple_ids);
        tuple_ids += 1;
        match &r.data {
                Data::Hist(x) => {
                    lp = lp
                        .measurement("table_hist")
                        .tag("hist_source","hist")
                        .tag("hist_id", &buf)
                        .field("hist_max",x.max)
                        .field("hist_ts",r.timestamp)
                        .timestamp(time_nanos)
                        .close_line();
                },
                Data::KV(x) => {
                    lp = lp
                        .measurement("table_kv")
                        .tag("source","kv")
                        .tag("id", &buf)
                        .field("kv_op", x.op as u64)
                        .field("kv_cpu", x.cpu)
                        .field("kv_tid", x.tid)
                        .field("kv_dur_nanos", x.dur_nanos)
                        .field("kv_ts",r.timestamp)
                        .timestamp(time_nanos)
                        .close_line();
                },
                Data::Sched(x) => {
                    lp = lp
                        .measurement("table_kv")
                        .tag("source","sched")
                        .tag("id", &buf)
                        .field("sched_cpu", x.cpu as u64)
                        .field("sched_comm", x.comm.as_str())
                        .field("sched_ts",r.timestamp)
                        .timestamp(time_nanos)
                        .close_line();
                },
        }
        buf.clear();
    }
    lp.build()
}

fn main() {
    println!("Hello, world!");
}
