Executing workload:

cargo run --release --bin kv-workload

kv-workload:
    90% reads
    32 threads
    16 RocksDB instances, each used by 2 threads
    swap threads randomly

matmul
    randomly chooses one thread
    does matmul on that thread
    affects two RDB threads
        (the thread shared by this process, and the other thread sharing the DB)

run the sched swith watcher
