# Webapp and collector

$ ./run_influx.sh
$ ./setup_influx.sh
$ cd webapp; cargo run --release

# Workload

$ cd workload
$ cargo build --release
$ cargo run release --bin gather

# These are currently hardcoded

$ cargo run --release --bin kv-workload -- --cpu 15
$ cargo run --release --bin kv-workload -- --cpu 1
$ sudo ./target/release/matmul-workload
