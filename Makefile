SHELL := /bin/bash
GOBIN := $(shell go env GOPATH)/bin
export PATH := $(PATH):$(GOBIN)

# Локальные переменные окружения: cp .env.example .env (файл опционален).
# Экспортируются во все команды make: goose, тесты, go run и т.д.
-include .env
export

DB_DSN ?= postgres://qrok:qrok@127.0.0.1:5432/qrok?sslmode=disable
# --project-directory: интерполяция ${VAR} в compose-файле читает .env из корня репо.
COMPOSE := docker compose -f deploy/docker-compose.yml --project-directory .

.DEFAULT_GOAL := help

.PHONY: help
help: ## Список таргетов
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: tools
tools: ## Установить dev-инструменты (buf, goose, gremlins, golangci-lint, protoc-плагины)
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/pressly/goose/v3/cmd/goose@latest
	go install github.com/avito-tech/go-mutesting/cmd/go-mutesting@latest
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

.PHONY: build
build: ## Собрать бинари в ./bin
	go build -o bin/qrok ./cmd/qrok
	go build -o bin/server ./cmd

.PHONY: run-server
run-server: ## Запустить сервер локально (env из .env, конфиг из qrok.yaml при наличии)
	@test -f qrok.yaml || (echo "Создайте конфиг: cp deploy/server.example.yaml qrok.yaml" && exit 1)
	go run ./cmd

.PHONY: doctor
doctor: ## Проверить dev-окружение перед запуском
	@echo "==> Postgres"
	@pg_isready -h 127.0.0.1 -p 5432 -U qrok -d qrok >/dev/null 2>&1 && echo "  OK" || echo "  FAIL — make compose-up"
	@echo "==> HTTP :8080 (сервер)"
	@curl -sf http://127.0.0.1:8080/health >/dev/null && echo "  OK — открой http://127.0.0.1:8080/dashboard/" || echo "  FAIL — в отдельном терминале: make run-server"

.PHONY: test
test: ## Unit-тесты
	go test -race ./...

.PHONY: verify
verify: ## Проверка после рефакторинга: сборка всех пакетов + unit-тесты
	go build ./...
	go test -race ./...

.PHONY: test-integration
test-integration: ## Интеграционные тесты (нужен make compose-up)
	go test -race -tags integration -count=1 ./...

.PHONY: test-e2e
test-e2e: ## Функциональные e2e-тесты (docker-compose стек)
	go test -tags e2e -count=1 -timeout 10m ./...

.PHONY: mutation
mutation: ## Мутационные тесты критичных пакетов (go-mutesting)
	go-mutesting ./pkg/fault/ ./internal/config/ ./internal/agent/source/kafka/

.PHONY: bench
bench: ## Бенчмарки с аллокациями; сравнение прогонов — benchstat
	go test -bench=. -benchmem -run='^$$' ./...

.PHONY: cover
cover: ## Отчёт покрытия
	go test -coverprofile=coverage.out ./... && go tool cover -html=coverage.out -o coverage.html
	@echo "открой coverage.html"

.PHONY: lint
lint: ## golangci-lint + buf lint
	golangci-lint run ./...
	buf lint

.PHONY: proto
proto: ## Сгенерировать gRPC-код из proto (с lint'ом)
	buf lint
	buf generate

.PHONY: proto-breaking
proto-breaking: ## Проверить обратную совместимость контракта против main
	buf breaking --against '.git#branch=main'

.PHONY: seed-dev
seed-dev: ## Создать dev org/tunnel/agent_token в Postgres
	go run ./cmd/seed

.PHONY: sync-skills
sync-skills: ## Синхронизировать .claude/skills → .cursor/skills (discovery Cursor)
	rsync -a --delete .claude/skills/ .cursor/skills/

.PHONY: run-agent
run-agent: ## Запустить агента (нужен --config deploy/agent.example.yaml с token)
	go run ./cmd/qrok agent start --config deploy/agent.example.yaml

.PHONY: run-listen
run-listen: ## Запустить dev-клиент
	go run ./cmd/qrok listen --config deploy/listen.example.yaml

.PHONY: migrate-up
migrate-up: ## Накатить миграции
	goose -dir migrations postgres "$(DB_DSN)" up

.PHONY: migrate-down
migrate-down: ## Откатить последнюю миграцию
	goose -dir migrations postgres "$(DB_DSN)" down

.PHONY: migrate-status
migrate-status: ## Статус миграций
	goose -dir migrations postgres "$(DB_DSN)" status

.PHONY: compose-up
compose-up: ## Поднять dev-окружение (Kafka, Postgres, Redis, MinIO)
	$(COMPOSE) up -d --wait

.PHONY: compose-down
compose-down: ## Остановить dev-окружение
	$(COMPOSE) down

.PHONY: compose-nuke
compose-nuke: ## Остановить и удалить данные dev-окружения
	$(COMPOSE) down -v
