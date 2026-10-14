#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# Native disposable services only. The binary fixture clears its environment and
# runs in a temporary directory, so .env.local and real credentials are unused.
PATH="${PATH}:/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/local/sbin"
export PATH
cargo test --locked --test signal_workflow_e2e -- --ignored --nocapture --test-threads=1
