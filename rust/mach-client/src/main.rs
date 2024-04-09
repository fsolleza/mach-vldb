use std::thread;
use std::time::Duration;
use common::data::*;

pub fn enable_kvlogs() {
	println!("ENABLING KV LOGS IN COLLECTOR");
	let mut connection =
		common::ipc::ipc_client_connect("0.0.0.0:3011").unwrap();
	let request = workload::AppRequest::Enable;
	let _response: workload::AppResponse =
		common::ipc::ipc_send(&request, &mut connection).unwrap();
}

fn requests_by_cpu(min_ts: u64, max_ts: u64) {
	let mut connection =
		common::ipc::ipc_client_connect("0.0.0.0:3021").unwrap();
	let request = mach_server::MachRequest::ByCpu(min_ts, max_ts);
	let response: mach_server::MachResponse =
		common::ipc::ipc_send(&request, &mut connection).unwrap();
	let result = response.from_cpu().unwrap();
	println!("{:?}", result);
}

fn requests_by_op(min_ts: u64, max_ts: u64) {
	let mut connection =
		common::ipc::ipc_client_connect("0.0.0.0:3021").unwrap();
	let request = mach_server::MachRequest::ByOp(min_ts, max_ts);
	let response: mach_server::MachResponse =
		common::ipc::ipc_send(&request, &mut connection).unwrap();
	let result = response.from_cpu().unwrap();
	println!("{:?}", result);
}


fn main() {
	enable_kvlogs();
	thread::sleep(Duration::from_secs(2));
	loop {
		let now = micros_since_epoch();
		let min_ts = now - 10 * 1_000_000;
		requests_by_cpu(min_ts, now);
		/* This will break */
		// request_by_op(min_ts, now);
		thread::sleep(Duration::from_secs(1));
	}
}
