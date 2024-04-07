mod core;

use core::*;
use std::time::Duration;

use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::Skel;
use libbpf_rs::skel::SkelBuilder;
use libbpf_rs::RingBufferBuilder;
use libbpf_rs::RingBuffer;
use libbpf_rs::PerfBufferBuilder;
use libbpf_rs::PerfBuffer;
use std::process;
use std::sync::{Arc, atomic::{AtomicUsize, AtomicBool, Ordering::SeqCst}};
use std::mem;
use std::slice;
use std::thread;
use lazy_static::*;

use crossbeam::channel::{Sender, Receiver, bounded};
use common::{
    ipc::*,
    data::{Sched, Data, Record},
};

use clap::Parser;

mod sched_switch {
    include!(concat!(env!("OUT_DIR"), "/sched_switch.skel.rs"));
}

use sched_switch::*;

lazy_static! {
    static ref RECORD_SENDER: Sender<Record> = init_egress();
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
static DROPPED: AtomicUsize = AtomicUsize::new(0);
static EVENTS: AtomicUsize = AtomicUsize::new(0);
static ENABLE: AtomicBool = AtomicBool::new(false);

fn init_counter() {
    let count_ipc = ipc_sender("0.0.0.0:3001", Some(32));
    loop {
        let c = COUNT.swap(0, SeqCst);
        let d = DROPPED.swap(0, SeqCst);
        let e = EVENTS.swap(0, SeqCst);
        println!("Count: {} {} {}", c, d, e);
        count_ipc.send(c).unwrap();
        std::thread::sleep(Duration::from_secs_f64(0.25));
    }
}

fn init_egress() -> Sender<Record> {
    let (tx, rx) = bounded(4096);
    let mach_ipc = ipc_sender("0.0.0.0:3020", Some(32));
    let infl_ipc = ipc_sender("0.0.0.0:3040", Some(32));
    thread::spawn(move || {
        egress(rx, mach_ipc, infl_ipc);
    });
    tx
}

fn egress(rx: Receiver<Record>, mach_ipc: Sender<Vec<Record>>, infl_ipc: Sender<Vec<Record>>) {
    let mut batch: Vec<Record> = Vec::new();
    println!("Beginning egress loop");
    while let Ok(item) = rx.recv() {
        batch.push(item);
        if batch.len() == 1024 {
            if ENABLE.load(SeqCst) {
                if mach_ipc.try_send(batch.clone()).is_err() {
                    DROPPED.fetch_add(1024, SeqCst);
                    continue;
                }
                if infl_ipc.try_send(batch).is_err() {
                    DROPPED.fetch_add(1024, SeqCst);
                }
                COUNT.fetch_add(1024, SeqCst);
            }
            batch = Vec::new();
        }
    }
}

fn parse_comm(comm: [i8; 16]) -> [u8; 16] {
    use std::ffi::CStr;
    let mut replace = [b'\0'; 16];
    unsafe {
        let x: Vec<&str> = CStr::from_ptr(comm[..].as_ptr())
            .to_str()
            .unwrap()
            .split("/")
            .take(1)
            .collect();
        let sl = x[0].as_bytes();
        replace[..sl.len()].copy_from_slice(sl);
    };
    replace
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone)]
struct Event {
    prev_pid: u64,
    next_pid: u64,
    cpu: u64,
    timestamp: u64,
    comm: [i8; 16],
}

impl Event {
    pub fn to_record(&self) -> Record {
        Record {
            timestamp: self.timestamp,
            data: Data::Sched(Sched {
                prev_pid: self.prev_pid,
                next_pid: self.next_pid,
                comm: parse_comm(self.comm),
                cpu: self.cpu,
            }),
        }
    }
}

fn copy_from_bytes(e: &mut Event, bytes: &[u8]) {
    let sz = mem::size_of_val(e);
    if bytes.len() < sz {
        panic!("too few bytes");
    }
    unsafe {
        let bptr = e as *mut Event as *mut u8;
        slice::from_raw_parts_mut(bptr, sz).copy_from_slice(&bytes[..sz]);
    }
}

fn handler(cpu: i32, bytes: &[u8]) -> i32 {
    let sender = RECORD_SENDER.clone();
    let mut event = Event::default();
    copy_from_bytes(&mut event, bytes);
    EVENTS.fetch_add(1, SeqCst);
    sender.send(event.to_record()).unwrap();
    0
}

fn handle_pb_event(cpu: i32, data: &[u8]) {
    handler(cpu, data);
}

fn handle_lost_events(cpu: i32, count: u64) {
    eprintln!("Lost {count} events on CPU {cpu}");
}

fn attach(_target_pid: u32) -> Result<(), libbpf_rs::Error> {
    use core_affinity::{set_for_current, CoreId};
    assert!(set_for_current(CoreId { id: 61 }));
    let skel_builder = SchedSwitchSkelBuilder::default();
    let mut open_skel = skel_builder.open()?;
    //open_skel.rodata().target_pid1 = target_pid;
    open_skel.rodata().this_pid = std::process::id();
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

//#[derive(Parser, Debug)]
//#[command(version, about, long_about = None)]
//struct Args {
//    /// Target pid
//    #[arg(short, long)]
//    pid: u32,
//}

fn main() {
    //let args = Args::parse();
    bump_memlock_rlimit().unwrap();

    thread::spawn(move || {
        let handle_queries = |r: SchedRequest| -> SchedResponse {
            match r {
                SchedRequest::Enable => {
                    ENABLE.store(true, SeqCst);
                    SchedResponse::Ok
                }
            }
        };
        common::ipc::ipc_serve("0.0.0.0:3050", handle_queries);
    });


    std::thread::spawn(init_counter);
    let _ = RECORD_SENDER.clone();
    //std::thread::spawn(move || attach(args.pid)).join().unwrap();
    std::thread::spawn(move || attach(0)).join().unwrap();
}
