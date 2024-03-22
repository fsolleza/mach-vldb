#! /usr/bin/env bash

echo "Validating sudo access..."
sudo echo "sudo access confirmed"

while true
do
    sleep_time=$(( RANDOM % (20) ))
    echo "sleeping for $sleep_time seconds"
    sleep $sleep_time
    echo "will flush in 3s"
    sleep 3
    echo 3 | sudo tee /proc/sys/vm/drop_caches
    echo "flushed cache at $(date)"
done
