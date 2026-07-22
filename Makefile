# Common tasks for the streaming stack. The README explains the why; this is
# the how, in one place, so the many manual steps are not copy-pasted by hand.
#
# Quick path:  make up  ->  make topic  ->  make produce  ->  make consume
.DEFAULT_GOAL := help

TOPIC       ?= retail.events
BOOTSTRAP   ?= localhost:9092
COUNT       ?= 500
SEED        ?= 42
CDC_TOPIC   ?= retail.cdc.public.products

.PHONY: help
help: ## List the available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

# ── Platform ────────────────────────────────────────────────────────────────
.PHONY: up
up: ## Start the stack (Kafka, Schema Registry, Postgres, Connect)
	docker compose up -d

.PHONY: ps
ps: ## Show service status (wait for healthy)
	docker compose ps

.PHONY: down
down: ## Stop and remove the stack
	docker compose down

.PHONY: topic
topic: ## Create the events topic (auto-creation is off by design)
	docker compose exec kafka kafka-topics --bootstrap-server $(BOOTSTRAP) \
		--create --if-not-exists --topic $(TOPIC) --partitions 3

.PHONY: topics
topics: ## List topics
	docker compose exec kafka kafka-topics --bootstrap-server $(BOOTSTRAP) --list

# ── Event pipeline ──────────────────────────────────────────────────────────
.PHONY: produce
produce: ## Produce synthetic events (COUNT=500 SEED=42)
	python -m retail_stream.producer --topic $(TOPIC) --count $(COUNT) --seed $(SEED)

.PHONY: consume
consume: ## Consume and print events
	python -m retail_stream.consumer --topic $(TOPIC)

.PHONY: aggregate
aggregate: ## Windowed revenue (WINDOW=tumbling|sliding)
	python -m retail_stream.consumer --aggregate $(or $(WINDOW),tumbling)

# ── CDC ─────────────────────────────────────────────────────────────────────
.PHONY: cdc-register
cdc-register: ## Register the Debezium Postgres connector
	curl -s -X POST -H "Content-Type: application/json" \
		--data @cdc/register-postgres.json http://localhost:8083/connectors

.PHONY: cdc-watch
cdc-watch: ## Tail the CDC change-event topic
	docker compose exec kafka kafka-console-consumer \
		--bootstrap-server $(BOOTSTRAP) --topic $(CDC_TOPIC) --from-beginning

# ── Dev ─────────────────────────────────────────────────────────────────────
.PHONY: install
install: ## Install Python dependencies
	pip install -r requirements.txt

.PHONY: test
test: ## Run the test suite
	pytest -q

.PHONY: validate-compose
validate-compose: ## Validate docker-compose.yml (what CI checks)
	docker compose -f docker-compose.yml config --quiet
