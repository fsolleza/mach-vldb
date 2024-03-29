Executing workload:

cargo run --release --bin gather
sudo ./target/release/matmul-workload # sudo needed because of thread priority
cargo run --release --bin kv-workload -- --cpu 1 # this will collide with matmul
cargo run --release --bin kv-workload -- --cpu 16
