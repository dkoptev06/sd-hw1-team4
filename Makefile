.DEFAULT_GOAL := check

GO_MODULES := services/rental services/source services/payment
BUILD_DIR := $(CURDIR)/build
# VCS stamping fails where git is missing or the checkout has another owner (containers, CI).
GOFLAGS_COMMON := -buildvcs=false
RENTAL_TEST_DATABASE_URL ?= postgres://rental:rental@127.0.0.1:$(or $(RENTAL_POSTGRES_PORT),55432)/rental?sslmode=disable

.PHONY: check test test-integration test-functional vet build up down logs db-up \
	loadtest-build loadtest loadtest-demo loadtest-history-ids

check: test vet build

# Unit tests; PostgreSQL integration tests are skipped.
test:
	set -e; for module in $(GO_MODULES); do \
		(cd "$$module" && RENTAL_TEST_DATABASE_URL= GOWORK=off go test $(GOFLAGS_COMMON) -race ./...); \
	done

# Rental tests including PostgreSQL integration tests (start the database with `make db-up`).
test-integration:
	cd services/rental && RENTAL_TEST_DATABASE_URL='$(RENTAL_TEST_DATABASE_URL)' GOWORK=off go test $(GOFLAGS_COMMON) -race -count=1 ./...

test-functional:
	cd services/rental && RENTAL_TEST_DATABASE_URL='$(RENTAL_TEST_DATABASE_URL)' GOWORK=off go test $(GOFLAGS_COMMON) -race -count=1 -tags=functional -v ./tests/...

vet:
	set -e; for module in $(GO_MODULES); do \
		(cd "$$module" && GOWORK=off go vet $(GOFLAGS_COMMON) ./...); \
	done

build:
	mkdir -p "$(BUILD_DIR)"
	cd services/rental && GOWORK=off go build $(GOFLAGS_COMMON) -o "$(BUILD_DIR)/rental" ./cmd/rental
	cd services/source && GOWORK=off go build $(GOFLAGS_COMMON) -o "$(BUILD_DIR)/source" ./cmd
	cd services/payment && GOWORK=off go build $(GOFLAGS_COMMON) -o "$(BUILD_DIR)/payment" ./cmd

db-up:
	docker compose up -d --wait postgres

up:
	docker compose up -d --build --wait

down:
	docker compose down

logs:
	docker compose logs -f rental-api rental-worker rental-maintenance

# Load emulator (tools/loadtest) against the running stack (`make up`), through nginx.
# Defaults are the full team-4 profile: 100 trips/s, 50 GET/trip, 7m warm-up + 30m window.
# Override any parameter, e.g. `make loadtest LOADTEST_RATE=150 LOADTEST_WINDOW=5m`;
# extra emulator flags go to LOADTEST_ARGS. Warm-up must be at least the maximum ride.
LOADTEST_COMPOSE := docker compose -f compose.yaml -f compose.loadtest.yaml --profile loadtest
LOADTEST_RATE ?= 100
LOADTEST_READS ?= 50
LOADTEST_WARMUP ?= 7m
LOADTEST_WINDOW ?= 30m
LOADTEST_MIN_RIDE ?= 4m
LOADTEST_MAX_RIDE ?= 6m
LOADTEST_ARGS ?=
LOADTEST_OUT ?= build/loadtest.json
# Mixed profile: a file of archived order UUIDs (from prepare-emulator-data.py mixed --ids
# or `make loadtest-history-ids`); LOADTEST_HISTORICAL of the LOADTEST_READS go to them.
LOADTEST_HISTORY ?=
LOADTEST_HISTORICAL ?= 15
LOADTEST_HISTORY_MOUNT = $(if $(LOADTEST_HISTORY),-v "$(abspath $(LOADTEST_HISTORY)):/history.ids:ro")
LOADTEST_HISTORY_ARGS = $(if $(LOADTEST_HISTORY),-historical $(LOADTEST_HISTORICAL) -history-file /history.ids)

loadtest-build:
	$(LOADTEST_COMPOSE) build loadtest

loadtest: loadtest-build
	@docker compose ps -q --status running nginx | grep -q . || { echo "Стенд не запущен: выполните make up" >&2; exit 1; }
	@mkdir -p "$(dir $(LOADTEST_OUT))"
	$(LOADTEST_COMPOSE) run --rm --no-deps $(LOADTEST_HISTORY_MOUNT) loadtest -url http://nginx:8080 \
		-rate $(LOADTEST_RATE) -reads $(LOADTEST_READS) -warmup $(LOADTEST_WARMUP) -window $(LOADTEST_WINDOW) \
		-min-ride $(LOADTEST_MIN_RIDE) -max-ride $(LOADTEST_MAX_RIDE) $(LOADTEST_HISTORY_ARGS) $(LOADTEST_ARGS) \
		> "$(LOADTEST_OUT)"; \
	status=$$?; echo; python3 tools/loadtest/scripts/summarize.py "$(LOADTEST_OUT)"; echo "report: $(LOADTEST_OUT)"; exit $$status

# Short profile for a live demo, about 5.5 minutes: 100s warm-up, 2m window, 60-90s rides.
loadtest-demo:
	@$(MAKE) --no-print-directory loadtest LOADTEST_WARMUP=100s LOADTEST_WINDOW=2m \
		LOADTEST_MIN_RIDE=60s LOADTEST_MAX_RIDE=90s LOADTEST_OUT=build/loadtest-demo.json

# Write UUIDs of orders already archived on the running stack to build/history.ids
# (e.g. after `make loadtest-demo`), so the next run also reads the archive:
# `make loadtest-demo LOADTEST_HISTORY=build/history.ids`.
loadtest-history-ids:
	@mkdir -p build
	docker compose exec -T postgres psql -XqAt -U rental -d rental \
		-c "SELECT order_id FROM orders_history WHERE delete_after > now() + interval '1 day' ORDER BY random() LIMIT 50000" \
		> build/history.ids
	@echo "$$(wc -l < build/history.ids) archived order IDs in build/history.ids"
