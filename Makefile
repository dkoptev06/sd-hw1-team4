.DEFAULT_GOAL := check

GO_MODULES := services/rental services/source services/payment
BUILD_DIR := $(CURDIR)/build
# VCS stamping fails where git is missing or the checkout has another owner (containers, CI).
GOFLAGS_COMMON := -buildvcs=false
RENTAL_TEST_DATABASE_URL ?= postgres://rental:rental@127.0.0.1:$(or $(RENTAL_POSTGRES_PORT),55432)/rental?sslmode=disable

.PHONY: check test test-integration vet build up down logs db-up

check: test vet build

# Unit tests; PostgreSQL integration tests are skipped.
test:
	set -e; for module in $(GO_MODULES); do \
		(cd "$$module" && RENTAL_TEST_DATABASE_URL= GOWORK=off go test $(GOFLAGS_COMMON) -race ./...); \
	done

# Rental tests including PostgreSQL integration tests (start the database with `make db-up`).
test-integration:
	cd services/rental && RENTAL_TEST_DATABASE_URL='$(RENTAL_TEST_DATABASE_URL)' GOWORK=off go test $(GOFLAGS_COMMON) -race -count=1 ./...

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
