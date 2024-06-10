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
ADDR_MEMSTORE_QUERY := 0.0.0.0:3511

CPUS_MEMSTORE := 50-53
CPUS_WEBAPP := 2-4

BIN_WEBAPP         := ./webapp/target/release/webapp
BIN_MONITORING_APP := ./monitoring-application/target/release/monitoring-application

PATH_BG_PIDS := tmp/bg_pids

###############
## Functions ##
###############

pprint := printf "+ %s\n"

define bg
	$(shell stty sane)
	$(shell $(1) > tmp/log-$@ 2>&1 & \
		echo $@,$$! >> $(PATH_BG_PIDS))
	$(shell stty sane)
	$(pprint) "Running $@"
	$(pprint) "Outputting to tmp/log-$@"
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

#################
## RUN TARGETS ##
#################

.PHONY: webapp
webapp: tmp-dir
	$(pprint) "Running webapp"
	$(call bg, taskset --cpu-list $(CPUS_WEBAPP) $(BIN_WEBAPP))

.PHONY: monitoring-application-memstore
monitoring-application-memstore: tmp-dir
	$(pprint) "Running monitoring application with memstore"
	$(call bg, \
		taskset --cpu-list $(CPUS_MEMSTORE) \
		$(BIN_MONITORING_APP) \
		--storage mem \
		--data-addr $(ADDR_MEMSTORE_DATA) \
		--query-addr $(ADDR_MEMSTORE_QUERY))

###################
## BUILD TARGETS ##
###################

.PHONY: build-webapp
build-webapp: tmp-dir
	$(call rust_build,webapp,,)

.PHONY: build-monitoring-application
build-monitoring-application: tmp-dir
	$(call rust_build,monitoring-application,,)

#############
## HELPERS ##
#############

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
