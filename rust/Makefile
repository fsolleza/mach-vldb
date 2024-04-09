ifndef VERBOSE
.SILENT:
endif

INFLUX_DATADIR := /nvme/data/tmp/influx
INFLUX_HTTPHOST := http://127.0.0.1:8080
INFLUX_CTLHOST := http://127.0.0.1:8081
INFLUX_BIN := ./influxdb/target/release/influxdb_iox

GATHER_CPU := 15-20
SCHED_CPU := 21-25
INFLUX_CPU := 26-36
WEBAPP_CPU := 37-47
MACH_CPU := 47-50

foo:
	echo "foo here"

.PHONY: start
start: webapp workload
	echo "+ all setup, visit localhost:3000"

.PHONY: kv-workload
kv-workload: tmp_dir
	cd workload; cargo build --release --bin kv-workload > ../tmp/build-kv-workload 2>&1;
	./workload/target/release/kv-workload --cpu 1 > tmp/log-kv-workload-1 2>&1 &
	./workload/target/release/kv-workload --cpu 3 > tmp/log-kv-workload-3 2>&1 &
	./workload/target/release/kv-workload --cpu 5 > tmp/log-kv-workload-5 2>&1 &
	./workload/target/release/kv-workload --cpu 7 > tmp/log-kv-workload-7 2>&1 &
	./workload/target/release/kv-workload --cpu 9 > tmp/log-kv-workload-9 2>&1 &
	echo "+ launched kv workload"

.PHONY: matmul-workload
matmul-workload: make_sudo tmp_dir
	cd workload; cargo build --release --bin matmul-workload > ../tmp/build-matmul-workload 2>&1;
	sudo ./workload/target/release/matmul-workload > tmp/log-matmul-workload 2>&1 &
	echo "+ launched matmul workload"

.PHONY: gather
gather: tmp_dir
	cd workload; cargo build --release --bin gather > ../tmp/build-gather 2>&1
	taskset --cpu-list $(GATHER_CPU) ./workload/target/release/gather > tmp/log-gather 2>&1 &
	echo "+ launched the gatherer, sleeping for 15s"
	sleep 15s;
	echo "+ done sleeping"

.PHONY: sched
sched: tmp_dir make_sudo
	cd sched_switch; cargo build --release > ../tmp/build-sched-switch 2>&1;
	taskset --cpu-list $(SCHED_CPU) sudo ./sched_switch/target/release/sched_switch > tmp/log-sched-switch 2>&1 &
	echo "+ scheduler events running"

.PHONY: make_sudo
make_sudo:
	echo "+ checking for sudo"
	sudo echo "+ sudo works"

.PHONY: workload
workload: tmp_dir make_sudo gather kv-workload matmul-workload
	echo "+ workload running"

.PHONY: influx
influx: tmp_dir
	echo "+ removing old influx dir at $(INFLUX_DATADIR)"
	rm -rf $(INFLUX_DATADIR);
	echo "+ building influxdb"
	cd influxdb; \
	cargo build --release --bin influxdb_iox > ../tmp/build-influx 2> ../tmp/build-influx
	taskset --cpu-list $(INFLUX_CPU) $(INFLUX_BIN) all-in-one \
		-vv \
		--querier-max-concurrent-queries 256 \
		--object-store file \
		--data-dir $(INFLUX_DATADIR) \
		--http-host $(INFLUX_HTTPHOST) > tmp/log-influx 2> tmp/log-influx &
	echo "+ creating influx resources"
	sleep 3s;
	$(INFLUX_BIN) namespace create vldb_demo --host $(INFLUX_CTLHOST) > /dev/null 2>&1
	$(INFLUX_BIN) table create vldb_demo table_kv --host $(INFLUX_CTLHOST) > /dev/null 2>&1
	$(INFLUX_BIN) table create vldb_demo table_hist --host $(INFLUX_CTLHOST) > /dev/null 2>&1
	$(INFLUX_BIN) table create vldb_demo table_sched --host $(INFLUX_CTLHOST) > /dev/null 2>&1
	echo "+ running influx server"
	cd influx-server; cargo build --release > ../tmp/build-influx-server 2>&1
	taskset --cpu-list $(INFLUX_CPU) ./influx-server/target/release/influx-server > tmp/log-influx-server 2>&1 &

.PHONY: influx
mach: tmp_dir
	cd mach-server; cargo build --release --bin mach-server \
		> ../tmp/build-mach-server 2>&1
	taskset --cpu-list $(MACH_CPU) ./mach-server/target/release/mach-server \
		> tmp/log-mach-server 2>&1 &
	echo "+ mach-server running"

.PHONY: webapp
webapp: tmp_dir influx mach
	echo "+ building and running webapp"
	cd webapp; cargo build --release > ../tmp/build-webapp 2>&1
	taskset --cpu-list $(WEBAPP_CPU) ./webapp/target/release/webapp > tmp/log-webapp 2>&1 &

.PHONY: mach-client
mach-test-client: tmp_dir
	cd mach-client; cargo build --release > ../tmp/build-mach-client 2>&1
	echo "+ mach client built, launching now"
	 ./mach-client/target/release/mach-client > tmp/log-mach-client 2>&1 &
	echo "+ mach client running"
	echo "+ use \"tail -f tmp/log-mach-client\" to see prints and logs"

.PHONY: mach-test-workload
mach-test-workload: tmp_dir mach gather
	cd workload; cargo build --release --bin kv-workload > ../tmp/build-kv-workload 2>&1;
	./workload/target/release/kv-workload --cpu 1 > tmp/log-kv-workload-1 2>&1 &
	./workload/target/release/kv-workload --cpu 2 > tmp/log-kv-workload-2 2>&1 &
	echo "+ launched kv workload"

.PHONY: mach-test
mach-test: mach-test-workload mach-test-client
	echo "+ run \"make mach-test-stop\" to stop everything"

.PHONY: tmp_dir
tmp_dir:
	mkdir -p tmp

.PHONY: clean
clean:
	echo "+ cleaning"
	rm -rf tmp;
	rm -rf $(INFLUX_DATADIR);

.PHONY: mach-test-stop
mach-test-stop:
	echo "+ attempting to kill kv-workloads"
	-pkill kv-workload >/dev/null 2>&1 || true
	echo "+ attempting to kill gather"
	-pkill gather > /dev/null 2>&1 || true
	echo "+ attempting to kill mach"
	-pkill mach-server > /dev/null 2>&1 || true
	echo "+ attempting to kill mach-client"
	-pkill -9 mach-client > /dev/null 2>&1 || true
	ps -a

.PHONY: stop
stop: make_sudo
	echo "+ attempting to kill kv-workloads"
	-pkill kv-workload >/dev/null 2>&1 || true
	echo "+ attempting to kill matmul-workload"
	-sudo pkill matmul-workload > /dev/null 2>&1 || true
	echo "+ attempting to kill gather"
	-sudo pkill gather > /dev/null 2>&1 || true
	echo "+ attempting to kill sched switch"
	-sudo pkill -9 sched_switch > /dev/null 2>&1 || true
	echo "+ attempting to kill influx"
	-sudo pkill -9 influxdb_iox > /dev/null 2>&1 || true
	-sudo pkill -9 influx-server > /dev/null 2>&1 || true
	echo "+ attempting to kill mach"
	-sudo pkill mach-server > /dev/null 2>&1 || true
	echo "+ attempting to kill webapp"
	-sudo pkill -9 webapp > /dev/null 2>&1 || true
	echo "+ attempting to kill mach-client"
	-sudo pkill -9 mach-client > /dev/null 2>&1 || true
	ps -a
