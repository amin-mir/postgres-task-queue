.PHONY: start-postgres stop-postgres init-postgres psql
.PHONY: migrate-up migrate-down migrate-version migrate-create

PG_NAME := queue-pg
PG_PORT := 5432
PG_VERSION := 18-alpine
POSTGRES_PASSWORD ?= mysecretpassword
POSTGRES_USER ?= postgres
POSTGRES_DB ?= postgres
NET_NAME := queue-net
MIGRATIONS_DIR := migrations

DB_DSN := postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(PG_NAME):$(PG_PORT)/$(POSTGRES_DB)?sslmode=disable

ensure-network:
	docker network inspect $(NET_NAME) >/dev/null 2>&1 || docker network create $(NET_NAME) >/dev/null

start-postgres: ensure-network
# 	docker rm -f $(PG_NAME) >/dev/null 2>&1 || true
	docker run --name $(PG_NAME) -e POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		-e POSTGRES_USER=$(POSTGRES_USER) -e POSTGRES_DB=$(POSTGRES_DB) \
		-p $(PG_PORT):5432 --network $(NET_NAME) -d postgres:$(PG_VERSION)
	@echo "Waiting for postgres to be ready..."
	@until docker exec $(PG_NAME) pg_isready -U $(POSTGRES_USER) >/dev/null 2>&1; do \
		sleep 0.2; \
	done
	@echo "Postgres is ready."

stop-postgres:
	docker rm -f $(PG_NAME) >/dev/null 2>&1 || true
	docker network rm $(NET_NAME) >/dev/null 2>&1 || true

start-psql:
	docker run -it --rm --network $(NET_NAME) postgres:$(PG_VERSION) \
		psql -h $(PG_NAME) -U $(POSTGRES_USER) -d $(POSTGRES_DB)

migrate-up:
	docker run --rm --network $(NET_NAME) -v $(PWD)/$(MIGRATIONS_DIR):/migrations migrate/migrate:latest \
		-path=/migrations -database "$(DB_DSN)" up

migrate-down:
	docker run --rm --network $(NET_NAME) -v $(PWD)/$(MIGRATIONS_DIR):/migrations migrate/migrate:latest \
		-path=/migrations -database "$(DB_DSN)" down 1

migrate-version:
	docker run --rm --network $(NET_NAME) -v $(PWD)/$(MIGRATIONS_DIR):/migrations migrate/migrate:latest \
		-path=/migrations -database "$(DB_DSN)" version

migrate-create:
	@test -n "$(name)" || (echo "Usage: make migrate-create name=your_migration_name" && exit 1)
	docker run --rm -v $(PWD)/$(MIGRATIONS_DIR):/migrations migrate/migrate:latest \
		create -ext sql -dir /migrations -seq "$(name)"

.PHONY: gen
gen:
	go generate ./...