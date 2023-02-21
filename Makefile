SHELL = /bin/bash

DIR=$(shell pwd)

check-license:
	cd $(DIR); sh scripts/check-license.sh

check-cargo-toml:
	cd $(DIR); cargo sort --workspace --check

clippy:
	cd $(DIR); cargo clippy --all-targets --all-features --workspace -- -D warnings
