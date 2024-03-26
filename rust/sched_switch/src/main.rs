use core::time::Duration;

use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::Skel;
use libbpf_rs::skel::SkelBuilder;
use libbpf_rs::RingBufferBuilder;
use libbpf_rs::RingBuffer;
use libbpf_rs::PerfBufferBuilder;
use libbpf_rs::PerfBuffer;
use std::process;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering::SeqCst}};
use std::mem;
use std::slice;
use std::thread;
use lazy_static::*;

use crossbeam::channel::{Sender, Receiver, bounded};
use common::{
    ipc::*,
    data::{Sched, Record},
};

use clap::Parser;

mod sched_switch {
    include!(concat!(env!("OUT_DIR"), "/sched_switch.skel.rs"));
}

use sched_switch::*;

lazy_static! {
    static ref RECORD_SENDER: Sender<Sched> = init_egress();
}

fn bump_memlock_rlimit() -> Result<(), ()> {
    let rlimit = libc::rlimit {
        rlim_cur: 128 << 20,
        rlim_max: 128 << 20,
    };

    if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &rlimit) } != 0 {
        return Err(());
    }

    Ok(())
}

static COUNT: AtomicUsize = AtomicUsize::new(0);

fn init_counter() {
    loop {
        let x = COUNT.swap(0, SeqCst);
        println!("Count: {}", x);
        std::thread::sleep(Duration::from_secs(1));
    }
}

fn init_egress() -> Sender<Sched> {
    let (tx, rx) = bounded(1000000);
    let ipc = ipc_sender("0.0.0.0:3001", None);
    thread::spawn(move || {
        egress(rx, ipc);
    });
    tx
}

fn egress(rx: Receiver<Sched>, ipc: Sender<Vec<Record>>) {
    let mut batch: Vec<Record> = Vec::new();
    println!("Beginning egress loop");
    while let Ok(item) = rx.recv() {
        batch.push(Record::Sched(item));
        if batch.len() == 1024 {
            ipc.send(batch).unwrap();
            batch = Vec::new();
            println!("Sending");
            COUNT.fetch_add(1024, SeqCst);
        }
    }
}

fn copy_from_bytes(e: &mut Sched, bytes: &[u8]) {
    let sz = mem::size_of_val(e);
    if bytes.len() < sz {
        panic!("too few bytes");
    }
    unsafe {
        let bptr = e as *mut Sched as *mut u8;
        slice::from_raw_parts_mut(bptr, sz).copy_from_slice(&bytes[..sz]);
    }
}

fn handler(cpu: i32, bytes: &[u8]) -> i32 {
    let sender = RECORD_SENDER.clone();
    let mut event = Sched::default();
    copy_from_bytes(&mut event, bytes);
    sender.send(event).unwrap();
    0
}

fn handle_pb_event(cpu: i32, data: &[u8]) {
    handler(cpu, data);
}

fn handle_lost_events(cpu: i32, count: u64) {
    eprintln!("Lost {count} events on CPU {cpu}");
}

fn attach(target_pid: u32) -> Result<(), libbpf_rs::Error> {
    let skel_builder = SchedSwitchSkelBuilder::default();
    let mut open_skel = skel_builder.open()?;
    open_skel.rodata().target_pid = target_pid;
    let mut skel = open_skel.load()?;
    skel.attach()?;
    let perf = PerfBufferBuilder::new(skel.maps_mut().pb())
        .sample_cb(handle_pb_event)
        .lost_cb(handle_lost_events)
        .build()?;
    loop {
        perf.poll(Duration::from_secs(10))?;
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Target pid
    #[arg(short, long)]
    pid: u32,
}

fn main() {
    let args = Args::parse();
    bump_memlock_rlimit().unwrap();
    std::thread::spawn(init_counter);
    let _ = RECORD_SENDER.clone();
    std::thread::spawn(move || attach(args.pid)).join().unwrap();
}
