################################################
## Makefile configuration flags and variables ##
################################################

MAKEFLAGS=--warn-undefined-variables

# define the variable V to make verbose (e.g., make [target] V=1)
ifndef V
.SILENT:
endif

###############
## VARIABLES ##
###############

ADDR_MEMSTORE_DATA  := 0.0.0.0:3501
ADDR_MEMSTORE_QUERY := 0.0.0.0:3502

ADDR_MACH_DATA  := 0.0.0.0:3601
ADDR_MACH_QUERY := 0.0.0.0:3602

ADDR_INFLUX_DATA  := 0.0.0.0:3401
ADDR_INFLUX_QUERY := 0.0.0.0:3402

ADDRS_DATA := $(ADDR_INFLUX_DATA)

ADDR_KV_2 := 0.0.0.0:3702
ADDR_KV_4 := 0.0.0.0:3704
ADDR_KV_6 := 0.0.0.0:3706
ADDR_KV_8 := 0.0.0.0:3708

WORKLOAD_ADDRS := $(ADDR_KV_2)

CPUS_MEMSTORE         := 50-53
CPUS_MACH             := 54-55
CPUS_INFLUX           := 55-60
CPUS_WEBAPP           := 10-15
CPUS_KV_WORKLOAD      := 1-10
CPUS_SCHEDULER_EVENTS := 21-25
CPUS_SYSCALL_LATENCY  := 21-25

BIN_WEBAPP           := ./webapp/target/release/webapp
BIN_MONITORING_APP   := ./monitoring-application/target/release/monitoring-application
BIN_KV_WORKLOAD      := ./kv-workload/target/release/kv-workload
BIN_MATRIX_MULTIPLY  := ./matrix-multiply/target/release/matrix-multiply
BIN_SCHEDULER_EVENTS := ./scheduler-events/target/release/scheduler-events
BIN_SYSCALL_LATENCY  := ./syscall-latency/target/release/syscall-latency

PATH_BG_PIDS  := tmp/bg_pids
PATH_WORKLOAD := /nvme/data/tmp/vldb/workload

###############
## Functions ##
###############

pprint := printf "+ %s\n"

define bg
	$(shell stty sane)
	$(shell $(1) > tmp/log-$@$(2) 2>&1 & \
		echo $@,$$! >> $(PATH_BG_PIDS))
	$(shell stty sane)
	$(pprint) "Running $@"
	$(pprint) "Outputting to tmp/log-$@$(2)"
endef

define rust_build
	$(info + building $(1))
	stty sane
	$(eval $@_r = \
		$(shell cd $(1); \
			cargo build --release $(2) >../tmp/build-$(1) 2>&1; \
			test $$? -eq 0 && echo foo || echo ; ))
	# stop build if error
	$(if $($@_r),,$(error build $(1) failed))
	stty sane
endef

.PHONY: sane-terminal
sane-terminal:
	stty sane

#################
## RUN TARGETS ##
#################

.PHONY: application
application: \
	stop \
	tmp-dir \
	monitoring-application-influx \
	kv-workload \
	webapp \
	matrix-multiply \
	sane-terminal

.PHONY: webapp
webapp: tmp-dir
	$(pprint) "Running webapp"
	$(call bg, \
		taskset --cpu-list $(CPUS_WEBAPP) $(BIN_WEBAPP) \
		--addr-memstore $(ADDR_MEMSTORE_QUERY) \
		--addr-mach $(ADDR_MACH_QUERY) \
		--addr-influx $(ADDR_INFLUX_QUERY) \
		--workload-addrs $(WORKLOAD_ADDRS) \
	,)

.PHONY: monitoring-application-influx
monitoring-application-influx: tmp-dir
	$(pprint) "Running monitoring application with influx"
	$(call bg, \
		taskset --cpu-list $(CPUS_INFLUX) \
		$(BIN_MONITORING_APP) \
		--storage mem \
		--data-addr $(ADDR_INFLUX_DATA) \
		--query-addr $(ADDR_INFLUX_QUERY) \
	,)

.PHONY: monitoring-application-memstore
monitoring-application-memstore: tmp-dir
	$(pprint) "Running monitoring application with memstore"
	$(call bg, \
		taskset --cpu-list $(CPUS_MEMSTORE) \
		$(BIN_MONITORING_APP) \
		--storage mem \
		--data-addr $(ADDR_MEMSTORE_DATA) \
		--query-addr $(ADDR_MEMSTORE_QUERY) \
	,)

.PHONY: monitoring-application-mach
monitoring-application-mach: tmp-dir
	$(pprint) "Running monitoring application with mach"
	$(call bg, \
		taskset --cpu-list $(CPUS_MACH) \
		$(BIN_MONITORING_APP) \
		--storage mach \
		--data-addr $(ADDR_MACH_DATA) \
		--query-addr $(ADDR_MACH_QUERY) \
	,)

.PHONY: matrix-multiply
matrix-multiply: tmp-dir do-sudo
	$(pprint) "Running monitoring application with memstore"
	$(call bg, $(BIN_MATRIX_MULTIPLY),)

.PHONY: kv-workload
kv-workload: tmp-dir
	$(pprint) "Running kv-workload processes"
	$(call bg, $(BIN_KV_WORKLOAD) \
		--cpu 2 \
		--data-dir $(PATH_WORKLOAD)2 \
		--data-addrs $(ADDRS_DATA) \
		--rate-addr $(ADDR_KV_2) \
	,2)

.PHONY: scheduler-events
scheduler-events: tmp-dir do-sudo
	sudo taskset --cpu-list $(CPUS_SCHEDULER_EVENTS) $(BIN_SCHEDULER_EVENTS) \
		--target-pids $$(cat tmp/bg_pids | grep matrix | cut -d, -f2) \
		--data-addrs $(ADDRS_DATA)

.PHONY: syscall-latency
syscall-latency: tmp-dir do-sudo
	sudo taskset --cpu-list $(CPUS_SYSCALL_LATENCY) $(BIN_SYSCALL_LATENCY) \
		--data-addrs $(ADDRS_DATA)

.PHONY: influx-run
influx-run:
	$(call bg, $(BIN_KV_WORKLOAD) \

###################
## BUILD TARGETS ##
###################

.PHONY: build-webapp
build-webapp: tmp-dir
	$(call rust_build,webapp,,)

.PHONY: build-monitoring-application
build-monitoring-application: tmp-dir
	$(call rust_build,monitoring-application,,)

.PHONY: build-kv-workload
build-kv-workload:
	$(call rust_build,kv-workload,,)

.PHONY: build-matrix-multiply
build-matrix-multiply:
	$(call rust_build,matrix-multiply,,)

.PHONY: build-scheduler-events
build-scheduler-events:
	$(call rust_build,scheduler-events,,)

.PHONY: build-syscall-latency
build-syscall-latency:
	$(call rust_build,syscall-latency,,)

.PHONY: build-all
build-all: tmp-dir \
	build-webapp \
	build-monitoring-application \
	build-kv-workload \
	build-matrix-multiply \
	build-scheduler-events \
	build-syscall-latency
	$(pprint) "building done"


#############
## HELPERS ##
#############

.PHONY: clean-code
clean-code:
	cd common; cargo clean;
	cd influxdb; cargo clean;
	cd influx_example; cargo clean;
	cd influx-server; cargo clean;
	cd kv-workload; cargo clean;
	cd mach-client; cargo clean;
	cd mach-lib; cargo clean;
	cd mach-server; cargo clean;
	cd matrix-multiply; cargo clean;
	cd monitoring-application; cargo clean;
	cd scheduler-events; cargo clean;
	cd webapp; cargo clean;

.PHONY: tmp-dir
tmp-dir:
	mkdir -p tmp

.PHONY: stop
stop:
	stty sane
	test ! -e $(PATH_BG_PIDS) && $(pprint) "$(PATH_BG_PIDS) does not exist" || true

	test -e $(PATH_BG_PIDS) && \
		test $$(cut $(PATH_BG_PIDS) -d, -f2 | wc -l) -eq 0 && \
		$(pprint) "No pids found" || \
		true

	test -e $(PATH_BG_PIDS) && \
		test $$(cut $(PATH_BG_PIDS) -d, -f2 | wc -l) -gt 0 && \
		( \
			$(pprint) "Killing the following pids"; \
			cat $(PATH_BG_PIDS) | xargs $(pprint); \
			cut $(PATH_BG_PIDS) -d, -f2 | xargs kill -9 \
		) ; \
		rm -f $(PATH_BG_PIDS)

	$(pprint) \
		"verify no stray processes (make ps)"
	$(pprint) \
		"stop otel (make otel-stop)"

	$(pprint) \
		"clean files (make clean-files)"

.PHONY: ps
ps:
	ps -U $$(whoami) -o pid,comm,cmd --sort comm

.PHONY: do-sudo
do-sudo:
	stty sane
	sudo $(pprint) "confirmed sudo"
	stty sane
