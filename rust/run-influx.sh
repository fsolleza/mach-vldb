#! /usr/bin/env sh

DATADIR=/nvme/data/tmp/influx
HTTPHOST=http://127.0.0.1:8080
RAMBYTES=1073741824
EXECPOOLBYTES=17179869184

rm -rf $DATADIR

cd influxdb
#CARGO_PROFILE_RELEASE_DEBUG=true cargo run --release --bin influxdb3 -- \
#    serve \
#    --object-store file \
#    --data-dir $DATADIR \
#    --ram-pool-data-bytes $RAMBYTES \
#    --http-bind $BINDADDR


#CARGO_PROFILE_RELEASE_DEBUG=true cargo run --release --bin influxdb_iox -- \
#    run \
#    --object-store file \
#    --data-dir $DATADIR


cargo run --release \
    -- run all-in-one \
    --object-store=file \
    --data-dir $DATADIR \
    --http-host $HTTPHOST

# In a different terminal, run the following - note the 8081 endpoint
# cargo run --release -- namespace create vldb_demo  --host http://localhost:8081
# cargo run --release -- table create vldb_demo kv --host http://localhost:8081
# cargo run --release -- table create vldb_demo hist --host http://localhost:8081

