#! /usr/bin/env bash

cd influxdb
cargo run --release -- namespace create vldb_demo  --host http://localhost:8081
cargo run --release -- table create vldb_demo table_kvop --host http://localhost:8081
cargo run --release -- table create vldb_demo table_sched --host http://localhost:8081
cargo run --release -- table create vldb_demo table_syscalls --host http://localhost:8081
