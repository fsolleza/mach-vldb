use influx_server::*;
use common::{
    ipc::*,
    data::*,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering::SeqCst},
    time::{Duration, Instant},
    thread
};

static COUNT: AtomicUsize = AtomicUsize::new(0);
static DROPPED: AtomicUsize = AtomicUsize::new(0);

fn init_print_counter() {
    let count_tx: IpcSender<u64> = ipc_sender("0.0.0.0:3001", Some(32));
    thread::spawn(move || {
        loop {
            let c = COUNT.swap(0, SeqCst);
            let d = DROPPED.swap(0, SeqCst);
            println!("Count: {} {}", c, d);
            count_tx.send(c as u64).unwrap();
            thread::sleep(Duration::from_secs(1));
        }
    });
}

fn main() {
    init_print_counter();

    let influx_tx: IpcSender<Vec<Record>> = ipc_sender("0.0.0.0:3040", Some(32));
    let mach_tx: IpcSender<Vec<Record>> = ipc_sender("0.0.0.0:3020", Some(1024));
    let rx: IpcReceiver<Vec<Record>> = ipc_receiver("0.0.0.0:3010");

    let mut hist: Histogram = Histogram::default();
    let mut hist_timestamp = {
        let ts = micros_since_epoch();
        ts - ts % 1_000_000
    };
    let mut hist_interval = Instant::now();

    while let Ok(mut batch) = rx.recv() {
        // Update the histogram
        for item in &batch {
            let dur = item.data.kv_log().unwrap().dur_nanos;
            hist.max = hist.max.max(dur);
        }
        hist.cnt += batch.len() as u64;

        // If histogram interval is done, push into the batch and regenerate the
        // histogram
        if hist_interval.elapsed().as_micros() > 1_000_000 {
            batch.push(Record {
                timestamp: hist_timestamp,
                data: Data::Hist(hist),
            });
            hist = Histogram::default();
            hist_timestamp = {
                let ts = micros_since_epoch();
                ts - ts % 1_000_000
            };
            hist_interval = Instant::now();
        }

        let len = batch.len();

        COUNT.fetch_add(len, SeqCst);

        if influx_tx.try_send(batch.clone()).is_err() {
            DROPPED.fetch_add(len, SeqCst);
            continue;
        }

        if mach_tx.try_send(batch).is_err() {
            DROPPED.fetch_add(len, SeqCst);
            continue;
        }

        //let request = InfluxRequest::Write(batch);
        //let resp: InfluxResponse = ipc_send(&request, &mut influx_client).unwrap();
        //match resp {
        //    InfluxResponse::Err(_) => DROPPED.fetch_add(len, SeqCst),
        //    InfluxResponse::Ok => COUNT.fetch_add(len, SeqCst),
        //    _ => unreachable!(),
        //};

    }
}
