# Makefile - sobe, valida e derruba a pilha rust-datalake com um comando.
# Compativel com GNU Make 3.81 (macOS) e 4.x. Segredos ficam no arquivo .env; o make nunca os le.
SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

COMPOSE  ?= docker compose
ENV_FILE ?= .env
WORKERS  ?= 3
SERVICE  ?=
DB       ?= jobs
CONFIRM  ?=

DC = $(COMPOSE) --env-file $(ENV_FILE)

.PHONY: help env check build up up-query ps logs scale psql backup smoke test down down-volumes

help: ## Lista os alvos disponiveis
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

env: ## Gera $(ENV_FILE) a partir de env.example com segredos aleatorios (nao sobrescreve)
	@if [ -e "$(ENV_FILE)" ]; then echo "$(ENV_FILE) ja existe; nada a fazer"; exit 0; fi; \
	umask 077; \
	while IFS= read -r line; do \
	  if [[ "$$line" =~ ^([A-Z0-9_]+)=$$ ]]; then \
	    k="$${BASH_REMATCH[1]}"; \
	    case "$$k" in \
	      LAKE_S3_ACCESS_KEY) v=$$(openssl rand -hex 10) ;; \
	      LAKE_S3_SECRET_KEY) v=$$(openssl rand -hex 20) ;; \
	      *) v=$$(openssl rand -hex 24) ;; \
	    esac; \
	    printf '%s=%s\n' "$$k" "$$v"; \
	  else printf '%s\n' "$$line"; fi; \
	done < env.example > "$(ENV_FILE)"; \
	chmod 600 "$(ENV_FILE)"; \
	echo "escrito $(ENV_FILE) (0600). Guarde LAKEKEEPER_ENCRYPTION_KEY fora do host: sem ela o catalogo nao e restauravel."

$(ENV_FILE):
	@echo "falta $(ENV_FILE): rode 'make env' primeiro"; exit 1

check: ## Renderiza o compose offline e imprime o fact sheet (sem subir nada)
	bash scripts/check-compose.sh --out -

build: $(ENV_FILE) ## Constroi as imagens (jobs-db, lake-worker, lake-query)
	$(DC) --profile query build

up: $(ENV_FILE) ## Sobe o core: rustfs, postgres, lakekeeper, lake-worker
	$(DC) up -d --build

up-query: $(ENV_FILE) ## Sobe o core + lake-query (Flight SQL em 127.0.0.1:50051)
	$(DC) --profile query up -d --build

ps: $(ENV_FILE) ## Estado dos servicos
	$(DC) --profile query --profile ops ps -a

logs: $(ENV_FILE) ## Segue os logs (SERVICE=lake-worker para filtrar)
	$(DC) --profile query logs -f $(SERVICE)

scale: $(ENV_FILE) ## Escala o worker (WORKERS=3)
	$(DC) up -d --no-build --scale lake-worker=$(WORKERS) lake-worker

psql: $(ENV_FILE) ## Abre psql no Postgres (DB=jobs|catalog)
	$(DC) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d $(DB)'

backup: $(ENV_FILE) ## pg_dump de catalog e jobs + copia do bucket do lake em ./backups
	$(DC) --profile ops run --rm pg-backup
	$(DC) --profile ops run --rm lake-backup

smoke: ## Sobe a pilha em projeto isolado, valida tudo e derruba
	bash scripts/smoke.sh

test: ## Self-test do checker e contrato dos documentos da avaliacao (se docs/assessment existir)
	bash tests/check-assessment-test.sh
	@if [ -d docs/assessment ]; then python3 scripts/check-assessment.py all docs/assessment; \
	else echo "docs/assessment ausente: contrato dos documentos nao verificado"; fi

down: $(ENV_FILE) ## Para os servicos, mantendo os volumes
	$(DC) --profile query --profile ops down

down-volumes: $(ENV_FILE) ## Para e APAGA volumes (dados e catalogo); exige CONFIRM=yes
ifneq ($(CONFIRM),yes)
	@echo "recusado: apaga rustfs-data e pg-data. Rode 'make down-volumes CONFIRM=yes' se for isso mesmo."; exit 1
else
	$(DC) --profile query --profile ops down -v --remove-orphans
endif
