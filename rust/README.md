
## Repo Setup

'''
git clone git@github.com:fsolleza/mach-vldb.git
cd mach-vldb
git submodule update --init --recursive

# build all components
make build-all

# runs the application.
# See Makefile for configuration and individual make commands
make application

# run interfering application (probably in a separate terminal)
cd matrix-multiply
cargo run --release

# running syscall latency BPF
cd syscall-latency
cargo build --release
./target/release/syscall-latency --pids [pids to monitor]
'''
