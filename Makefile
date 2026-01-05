# mqlite Makefile
# Build, test, and run conformance tests

.PHONY: all build build-release build-profiling test test-unit test-integration test-conformance \
        conformance conformance-v3 conformance-v5 conformance-ci \
        broker-start broker-stop run run-release run-profiling clean help install-conformance \
        perf perf-record perf-report perf-top heaptrack heaptrack-report clean-profile

# Configuration
BROKER_ADDR ?= localhost
BROKER_PORT ?= 1883
PERF_DURATION ?= 30
CONFORMANCE_VERSION := 0.1.2
CONFORMANCE_URL := https://github.com/vibesrc/mqttconformance/releases/download/v$(CONFORMANCE_VERSION)/mqtt-conformance-x86_64-unknown-linux-musl.tar.gz
CONFORMANCE_BIN := ./bin/mqtt-conformance

# Default target
all: build test

# Build targets
build:
	cargo build

build-release:
	cargo build --release

build-profiling:
	cargo build --profile profiling

# Test targets (cargo tests)
test: test-unit

test-unit:
	cargo test --workspace

# Install mqtt-conformance binary
install-conformance: $(CONFORMANCE_BIN)

$(CONFORMANCE_BIN):
	@mkdir -p ./bin
	@echo "Downloading mqtt-conformance $(CONFORMANCE_VERSION)..."
	@curl -sL $(CONFORMANCE_URL) | tar -xz -C ./bin
	@chmod +x $(CONFORMANCE_BIN)
	@echo "mqtt-conformance installed to $(CONFORMANCE_BIN)"

# External conformance tests using mqtt-conformance
# These require a running broker at BROKER_ADDR:BROKER_PORT
conformance: conformance-v3 conformance-v5

conformance-v3: $(CONFORMANCE_BIN)
	@echo "Running MQTT v3.1.1 conformance tests..."
	$(CONFORMANCE_BIN) run -H $(BROKER_ADDR) -p $(BROKER_PORT) -v 3 --parallel

conformance-v5: $(CONFORMANCE_BIN)
	@echo "Running MQTT v5.0 conformance tests..."
	$(CONFORMANCE_BIN) run -H $(BROKER_ADDR) -p $(BROKER_PORT) -v 5 --parallel

# Run specific section (usage: make conformance-section SECTION="connect" VERSION=3)
conformance-section: $(CONFORMANCE_BIN)
	$(CONFORMANCE_BIN) run -H $(BROKER_ADDR) -p $(BROKER_PORT) -v $(VERSION) --section $(SECTION) --parallel

# List available tests
conformance-list: $(CONFORMANCE_BIN)
	$(CONFORMANCE_BIN) list -v 3
	$(CONFORMANCE_BIN) list -v 5

# CI targets for running conformance tests
conformance-ci: build-release $(CONFORMANCE_BIN) broker-start
	@sleep 1
	@$(MAKE) conformance BROKER_ADDR=127.0.0.1 BROKER_PORT=1883 || ($(MAKE) broker-stop && exit 1)
	@$(MAKE) broker-stop

broker-start:
	@echo "Starting broker..."
	@./target/release/mqlite -b 127.0.0.1:1883 & echo $$! > /tmp/mqlite.pid
	@sleep 2
	@echo "Broker started (PID: $$(cat /tmp/mqlite.pid))"

broker-stop:
	@echo "Stopping broker..."
	-@kill $$(cat /tmp/mqlite.pid 2>/dev/null) 2>/dev/null || true
	-@rm -f /tmp/mqlite.pid
	@echo "Broker stopped"

# Run the broker
run:
	cargo run -- -b 0.0.0.0:1883

run-release:
	cargo run --release -- -b 0.0.0.0:1883

run-profiling:
	cargo run --profile profiling -- -b 0.0.0.0:1883

# Clean
clean:
	cargo clean
	rm -rf ./bin

clean-profile:
	rm -f perf.data perf.data.old report.txt top.txt
	rm -f heaptrack.mqlite.*.zst

# Profiling targets (requires running broker)
perf: perf-record perf-report

perf-record:
	@echo "Recording CPU profile for $(PERF_DURATION)s..."
	@PID=$$(pgrep -x mqlite) && \
	if [ -z "$$PID" ]; then echo "Error: mqlite not running"; exit 1; fi && \
	sudo perf record -g -p $$PID --call-graph dwarf sleep $(PERF_DURATION)
	@echo "Profile saved to perf.data"

perf-report:
	@echo "Generating perf reports..."
	sudo perf report --stdio --no-children --sort=symbol --percent-limit 0.5 > report.txt
	sudo perf report --stdio --no-children -g none --percent-limit 0.5 > top.txt
	@echo "Reports saved to report.txt and top.txt"

perf-top:
	@PID=$$(pgrep -x mqlite) && \
	if [ -z "$$PID" ]; then echo "Error: mqlite not running"; exit 1; fi && \
	sudo perf top -p $$PID

heaptrack: build-profiling
	@echo "Starting broker with heaptrack..."
	heaptrack ./target/profiling/mqlite -b 0.0.0.0:1883

heaptrack-report:
	@FILE=$$(ls -t heaptrack.mqlite.*.zst 2>/dev/null | head -1) && \
	if [ -z "$$FILE" ]; then echo "No heaptrack files found"; exit 1; fi && \
	echo "Analyzing $$FILE..." && \
	heaptrack_print --print-peak 1 "$$FILE" 2>/dev/null | \
	  perl -0777 -ne 'while (/(\d+) calls with ([\d.]+[KMG]?B?) peak.*?mqlite::(\S+?)(?:::h[a-f0-9]+)?\s/gs) { print "$$2\t$$1 calls\t$$3\n" }' | \
	  sort -hr | head -20

# Help
help:
	@echo "mqlite Makefile"
	@echo ""
	@echo "Build targets:"
	@echo "  build          - Debug build"
	@echo "  build-release  - Release build (optimized, no debug symbols)"
	@echo "  build-profiling - Profiling build (optimized + debug symbols)"
	@echo ""
	@echo "Test targets (cargo tests):"
	@echo "  test           - Run all cargo tests"
	@echo "  test-unit      - Run unit tests"
	@echo ""
	@echo "External conformance tests (requires running broker):"
	@echo "  conformance    - Run all conformance tests (v3 + v5)"
	@echo "  conformance-v3 - Run MQTT v3.1.1 tests"
	@echo "  conformance-v5 - Run MQTT v5.0 tests"
	@echo "  conformance-ci - Build, start broker, run tests, stop broker"
	@echo "  conformance-list - List all available tests"
	@echo ""
	@echo "  conformance-section VERSION=3 SECTION='connect'"
	@echo "                 - Run specific test section"
	@echo ""
	@echo "Profiling targets:"
	@echo "  perf           - Record and generate CPU profile reports"
	@echo "  perf-record    - Record CPU profile (requires running broker)"
	@echo "  perf-report    - Generate report.txt and top.txt from perf.data"
	@echo "  perf-top       - Live CPU profiling (interactive)"
	@echo "  heaptrack      - Start broker with heap profiling"
	@echo "  heaptrack-report - Analyze most recent heaptrack file"
	@echo "  clean-profile  - Remove profiling artifacts"
	@echo ""
	@echo "Other targets:"
	@echo "  install-conformance - Download mqtt-conformance binary"
	@echo "  run            - Run broker (debug)"
	@echo "  run-release    - Run broker (release)"
	@echo "  run-profiling  - Run broker (profiling, for perf)"
	@echo "  clean          - Clean build artifacts"
	@echo ""
	@echo "Configuration:"
	@echo "  BROKER_ADDR    - Broker address (default: localhost)"
	@echo "  BROKER_PORT    - Broker port (default: 1883)"
	@echo "  PERF_DURATION  - Perf record duration in seconds (default: 30)"
