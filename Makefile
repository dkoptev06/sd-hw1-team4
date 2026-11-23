.DEFAULT_GOAL := check

GO_MODULES := services/rental services/source services/payment
BUILD_DIR := $(CURDIR)/build

.PHONY: check test test-integration vet build up

check: test vet build

test:
	set -e; for module in $(GO_MODULES); do \
		(cd "$$module" && RENTAL_TEST_DATABASE_URL= GOWORK=off go test -buildvcs=false -race ./...); \
	done

test-integration:
	@test -n "$$RENTAL_TEST_DATABASE_URL" || { echo "Set RENTAL_TEST_DATABASE_URL to run PostgreSQL integration tests." >&2; exit 1; }
	cd services/rental && GOWORK=off go test -buildvcs=false -race -count=1 ./...

vet:
	set -e; for module in $(GO_MODULES); do \
		(cd "$$module" && GOWORK=off go vet -buildvcs=false ./...); \
	done

build:
	mkdir -p "$(BUILD_DIR)"
	cd services/rental && GOWORK=off go build -buildvcs=false -o "$(BUILD_DIR)/rental-api" ./cmd/api
	cd services/rental && GOWORK=off go build -buildvcs=false -o "$(BUILD_DIR)/rental-worker" ./cmd/worker
	cd services/rental && GOWORK=off go build -buildvcs=false -o "$(BUILD_DIR)/rental-maintenance" ./cmd/maintenance
	cd services/source && GOWORK=off go build -buildvcs=false -o "$(BUILD_DIR)/source" ./cmd
	cd services/payment && GOWORK=off go build -buildvcs=false -o "$(BUILD_DIR)/payment" ./cmd

up:
	docker compose config --quiet
	docker compose build rental source payment
	docker compose up -d rental source payment
