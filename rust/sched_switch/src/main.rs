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

use clap::Parser;

mod sched_switch {
    include!(concat!(env!("OUT_DIR"), "/sched_switch.skel.rs"));
}

use sched_switch::*;

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


fn handler(cpu: i32, _bytes: &[u8]) -> i32 {
    //let mut event = Event::default();
    //copy_from_bytes(&mut event, bytes);
    //println!("cpu: {}, pid: {}, syscall: {}, timestamp: {}", cpu, event.pid, event.syscall_number, event.timestamp);
    COUNT.fetch_add(1, SeqCst);
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
    std::thread::spawn(move || attach(args.pid)).join().unwrap();
}
