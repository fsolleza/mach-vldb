#! /usr/bin/env bash

cd influxdb
cargo run --release -- namespace create vldb_demo  --host http://localhost:8081
cargo run --release -- table create vldb_demo kv --host http://localhost:8081
cargo run --release -- table create vldb_demo hist --host http://localhost:8081
