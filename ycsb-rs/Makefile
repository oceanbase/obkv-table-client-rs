MODE ?= release
ROOT = $(shell pwd)

export OBKV_YCSB_BINARY ?= $(ROOT)/../target/$(MODE)/obkv-ycsb

database ?= obkv
workload ?= $(ROOT)/workloads/workload_obkv.toml
threads ?= 200

build-test:
	cargo build --$(MODE)

build:build-test

load: build
	$(OBKV_YCSB_BINARY) load --database $(database) --threads $(threads) --workload $(workload)

run: build
	$(OBKV_YCSB_BINARY) run --database $(database) --threads $(threads) --workload $(workload)

clean:
	rm -rf $(ROOT)/../target/

install-flamegraph:
	wget https://github.com/brendangregg/FlameGraph/archive/master.zip && unzip master.zip && sudo mkdir $(ROOT)/opt && sudo mv FlameGraph-master/ $(ROOT)/opt/FlameGraph && rm master.zip

record-flamegraph:
	sudo perf record --call-graph=dwarf -F 99 -p $$(ps -ef | grep obkv-ycsb | grep -v grep | awk '{print $$2}') -g -- sleep 30

out-flamegraph:
	sudo perf script -i perf.data > out.perf

folded-flamegraph:
	$(ROOT)/opt/FlameGraph/stackcollapse-perf.pl out.perf > out.folded

gengerate-flamegraph:
	$(ROOT)/opt/FlameGraph/flamegraph.pl out.folded > obkv.svg

flamegraph:
	$(MAKE) out-flamegraph && $(MAKE) folded-flamegraph && $(MAKE) gengerate-flamegraph
