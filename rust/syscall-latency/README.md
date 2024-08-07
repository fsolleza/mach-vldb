## Building this

* First build zlib in `../zlib`

```
RUSTFLAGS="-L ../zlib -lz" cargo build --release
```

## Running

Need `sudo` for bumping memlock rlimit

```
sudo ../target/release/syscall-latency-bpf --pid [PID]
```



