#! /usr/bin/env sh

DATADIR=/nvme/data/tmp/influx
BINDADDR=127.0.0.1:8181
RAMBYTES=1073741824

cd influxdb
cargo run --release --bin influxdb3 -- \
    serve \
    --object-store file \
    --data-dir $DATADIR \
    --ram-pool-data-bytes $RAMBYTES \
    --http-bind $BINDADDR
