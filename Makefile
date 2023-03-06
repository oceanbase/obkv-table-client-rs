SHELL = /bin/bash

DIR=$(shell pwd)

check-license:
	cd $(DIR); sh scripts/check-license.sh

check-cargo-toml:
	cd $(DIR); cargo sort --workspace --check

clippy:
	cd $(DIR); cargo clippy --all-targets --all-features --workspace -- -D warnings

test-ut: 
	cd $(DIR); cargo test --lib --all-features --workspace

test-all:
	cd $(DIR); cargo test --all --all-features --workspace

