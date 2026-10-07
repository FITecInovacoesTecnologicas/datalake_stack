# rust-datalake

> Data lake / lakehouse enxuto para um unico host, em Docker Compose, com componentes em Rust e tabelas Apache Iceberg.

![status](https://img.shields.io/badge/status-PoC-orange) ![compose](https://img.shields.io/badge/docker%20compose-v2-blue) ![rust](https://img.shields.io/badge/rust-1.88%2B-informational) ![iceberg](https://img.shields.io/badge/table%20format-Apache%20Iceberg-lightgrey)

## Visao geral

`rust-datalake` sobe, com um comando, uma pilha minima de lakehouse: object storage S3, catalogo Iceberg REST, Postgres com fila de jobs e agendamento, um worker de transformacao e um endpoint SQL. Tudo roda em um unico host e expoe portas apenas em `127.0.0.1`.

O objetivo e ter um ambiente de desenvolvimento / prova de conceito pequeno, rapido de subir e facil de entender, sem Spark, Trino ou JVM. O processamento e a consulta usam DataFusion e iceberg-rust compilados em binarios Rust proprios.

E indicado para quem precisa experimentar pipelines raw -> clean -> curated sobre Iceberg, ou validar uma arquitetura lakehouse antes de escolher infraestrutura definitiva.

> **Estado:** `lake-worker` e `lake-query` sao *scaffolds*. Eles compilam, sobem e cumprem o contrato operacional, mas a logica de transformacao ainda nao existe. Veja [Limitacoes conhecidas](#limitacoes-conhecidas).

## Funcionalidades

- **Object storage S3 (RustFS):** bucket `lake` com chave de servico restrita ao bucket. A chave root so e usada pelo job `storage-init`.
- **Catalogo Iceberg REST (Lakekeeper):** bootstrap e warehouse criados automaticamente e de forma idempotente, com metadados no Postgres e credenciais cifradas por `LAKEKEEPER_ENCRYPTION_KEY`.
- **Postgres 16 + pg_cron:** uma instancia com dois bancos isolados por role, `catalog` (Lakekeeper) e `jobs` (fila `jobs.job_queue` + agendamento).
- **lake-worker:** consome a fila com `FOR UPDATE SKIP LOCKED` e escala horizontalmente (`--scale lake-worker=N`). Usa memory pool e spill do DataFusion e faz shutdown gracioso em SIGTERM (grace period de 2 min).
- **lake-query (perfil `query`):** endpoint Arrow Flight SQL sobre DataFusion, com o catalogo Iceberg registrado como `lake`.
- **pg-backup (perfil `ops`):** `pg_dump` sob demanda dos dois bancos em `./backups`.
- **Operacao por Makefile:** gera segredos, sobe, escala, faz backup, roda o smoke test e derruba a pilha.
- **Smoke test isolado:** sobe a pilha inteira em um projeto Compose separado, com portas e segredos proprios, e valida cada componente.

## Arquitetura

```text
                         host (portas so em 127.0.0.1)
 ┌──────────────────────────────────────────────────────────────────────┐
 │                                                                      │
 │  ┌──────────────┐   S3 (chave do bucket)   ┌──────────────────────┐  │
 │  │   RustFS     │◀─────────────────────────│  Lakekeeper          │  │
 │  │ :9000 S3     │                          │  Iceberg REST :8181  │  │
 │  │ :9001 console│◀──────┐                  └──────────┬───────────┘  │
 │  └──────▲───────┘       │                             │ catalog DB   │
 │         │ storage-init  │ S3 (dados)                  ▼              │
 │         │ (bucket+chave)│            ┌──────────────────────────────┐│
 │         │               │            │ Postgres 16 + pg_cron :5433  ││
 │  ┌──────┴───────────────┴──┐ jobs DB │  ├─ catalog (Lakekeeper)     ││
 │  │ lake-worker  (xN)       │◀───────▶│  └─ jobs   (job_queue, cron) ││
 │  │ DataFusion + iceberg-rs │         └──────────────────────────────┘│
 │  └─────────────────────────┘                     ▲                   │
 │  ┌─────────────────────────┐                     │ pg_dump           │
 │  │ lake-query  [query]     │──▶ Lakekeeper       │                   │
 │  │ Flight SQL :50051       │──▶ RustFS     ┌─────┴──────┐            │
 │  └─────────────────────────┘               │ pg-backup  │ [ops]      │
 │                                            └────────────┘            │
 └──────────────────────────────────────────────────────────────────────┘
```

Ordem de inicializacao: `rustfs` -> `storage-init` | `postgres` -> `lakekeeper-migrate` -> `lakekeeper` -> `lakekeeper-init` -> `lake-worker` / `lake-query`.

**Modelo de acesso ao storage:** STS esta desligado. Lakekeeper e os servicos Rust usam a mesma chave restrita ao bucket (`X-Iceberg-Access-Delegation: client-managed`). Clientes externos (PyIceberg, Spark, Trino, DuckDB) devem usar `remote-signing`.

| Servico | Imagem / build | Perfil | Porta (host) |
|---|---|---|---|
| `rustfs` | `rustfs/rustfs:${RUSTFS_VERSION}` | core | 9000 (S3), 9001 (console) |
| `storage-init` | `rustfs/rc:${RC_VERSION}` | core (one-shot) | - |
| `postgres` | build `./jobs-db` (Postgres 16.15 + pg_cron) | core | 5433 |
| `lakekeeper-migrate` | `quay.io/lakekeeper/catalog:${LAKEKEEPER_VERSION}` | core (one-shot) | - |
| `lakekeeper` | `quay.io/lakekeeper/catalog:${LAKEKEEPER_VERSION}` | core | 8181 (REST + UI) |
| `lakekeeper-init` | `curlimages/curl:${CURL_VERSION}` | core (one-shot) | - |
| `lake-worker` | build `./lake-worker` | core | - |
| `lake-query` | build `./lake-query` | `query` | 50051 (Flight SQL) |
| `pg-backup` | `rust-datalake/postgres:local` | `ops` (sob demanda) | - |

## Quick start

### Pre-requisitos

- Docker Engine 24+ com Compose v2 e BuildKit habilitado.
- `openssl`, `curl` e `bash` no host. `make` e opcional, mas recomendado.
- Nao e preciso toolchain Rust: `lake-worker` e `lake-query` compilam dentro do `docker build`.

### Subir a pilha

```bash
make env        # gera .env (0600) a partir de env.example com segredos nos tamanhos certos
make up         # core: rustfs, postgres, lakekeeper, lake-worker
make ps         # confere o estado dos servicos
```

A primeira execucao compila os binarios Rust e pode levar varios minutos.

> **Importante:** guarde `LAKEKEEPER_ENCRYPTION_KEY` fora do host. Sem ela, o dump do banco `catalog` nao e restauravel.

### Enfileirar um job

```bash
make psql       # psql no banco jobs (DB=catalog para o catalogo)
```

```sql
insert into jobs.job_queue (kind, payload) values ('refresh-curated', '{"table": "sales"}');

-- agendar com pg_cron (UTC):
select cron.schedule('nightly', '0 3 * * *',
  $$insert into jobs.job_queue(kind) values ('refresh-curated')$$);

select id, kind, status, attempts, last_error from jobs.job_queue order by id desc limit 10;
```

O worker verifica a fila a cada `POLL_INTERVAL_SECS` (5 s), marca o job como `running` e, ao terminar, como `done` ou `failed` (com `last_error`).

## Uso

### Comandos do Makefile

Rode `make help` para listar todos os alvos.

| Comando | O que faz |
|---|---|
| `make env` | Gera `.env` com segredos aleatorios (nao sobrescreve um existente) |
| `make check` | Renderiza o compose offline e imprime o fact sheet (nao sobe nada) |
| `make build` | Constroi as imagens `jobs-db`, `lake-worker` e `lake-query` |
| `make up` | Sobe o core |
| `make up-query` | Sobe o core + `lake-query` (Flight SQL em `127.0.0.1:50051`) |
| `make ps` | Mostra o estado de todos os servicos, incluindo perfis |
| `make logs SERVICE=lake-worker` | Segue os logs (sem `SERVICE`, de todos) |
| `make scale WORKERS=3` | Escala o `lake-worker` |
| `make psql DB=jobs` | Abre `psql` (`DB=jobs` ou `DB=catalog`) |
| `make backup` | Faz `pg_dump -Fc` de `catalog` e `jobs` em `./backups` |
| `make smoke` | Smoke test em projeto isolado (veja [Testes](#testes)) |
| `make down` | Para os servicos e mantem os volumes |
| `make down-volumes CONFIRM=yes` | Para os servicos e **apaga** `rustfs-data` e `pg-data` |

Para usar outro arquivo de ambiente, rode `ENV_FILE=outro.env make up`.

### Sem Makefile

```bash
cp env.example .env && chmod 600 .env
# Preencha cada chave vazia. Tamanhos:
#   LAKE_S3_ACCESS_KEY -> openssl rand -hex 10   (20 chars; RustFS aceita 3-20)
#   LAKE_S3_SECRET_KEY -> openssl rand -hex 20   (40 chars; RustFS aceita 8-40)
#   demais segredos    -> openssl rand -hex 24   (48 chars)

docker compose up -d --build                  # core
docker compose --profile query up -d --build  # + lake-query
docker compose up -d --scale lake-worker=3    # mais workers
docker compose --profile ops run --rm pg-backup
```

### Endpoints locais

| Endpoint | Endereco |
|---|---|
| S3 API (RustFS) | `http://127.0.0.1:9000` |
| Console RustFS | `http://127.0.0.1:9001` |
| Lakekeeper REST + UI | `http://127.0.0.1:8181` (catalogo em `/catalog`, gestao em `/management/v1`) |
| Postgres | `127.0.0.1:5433` (bancos `jobs` e `catalog`) |
| Flight SQL | `grpc://127.0.0.1:50051` (perfil `query`) |

## Configuracao

Todas as variaveis ficam em `.env`, gerado a partir de [env.example](env.example). O Compose falha na inicializacao se alguma variavel obrigatoria estiver vazia.

### Versoes de imagem (sempre fixadas, nunca `latest`)

| Variavel | Default |
|---|---|
| `RUSTFS_VERSION` | `1.0.0` |
| `RC_VERSION` | `v0.1.36` |
| `LAKEKEEPER_VERSION` | `v0.13.1` |
| `CURL_VERSION` | `8.22.0` |

### Storage, catalogo e banco

| Variavel | Descricao | Default |
|---|---|---|
| `RUSTFS_ROOT_USER` / `RUSTFS_ROOT_PASSWORD` | Credencial root do RustFS (usada so por `rustfs` e `storage-init`) | segredo |
| `LAKE_BUCKET` | Bucket do lake | `lake` |
| `LAKE_S3_REGION` | Regiao S3 | `local-01` |
| `LAKE_S3_ACCESS_KEY` | Access key restrita ao bucket (3-20 chars) | segredo |
| `LAKE_S3_SECRET_KEY` | Secret key restrita ao bucket (8-40 chars) | segredo |
| `LAKE_WAREHOUSE` | Nome do warehouse no Lakekeeper (tambem e o prefixo no bucket) | `lake` |
| `LAKEKEEPER_ENCRYPTION_KEY` | Cifra as credenciais guardadas no catalogo. **Faca backup fora do host** | segredo |
| `PG_ADMIN_USER` / `PG_ADMIN_PASSWORD` | Superusuario do Postgres | `lake_admin` / segredo |
| `CATALOG_DB_PASSWORD` | Senha da role `lakekeeper` (banco `catalog`) | segredo |
| `JOBS_WORKER_PASSWORD` | Senha da role `lake_worker` (banco `jobs`). Use hex: ela entra em uma URL | segredo |

### Limites de recursos (opcionais)

O pool de memoria do DataFusion deve ficar abaixo do limite do container. O excedente vai para spill em `/spill`.

| Variavel | Descricao | Default |
|---|---|---|
| `WORKER_MEM_LIMIT` | Limite de memoria do container `lake-worker` | `2g` |
| `WORKER_POOL_MB` | Memory pool do DataFusion no worker | `1400` |
| `WORKER_CPUS` | CPUs do `lake-worker` | `2` |
| `QUERY_MEM_LIMIT` | Limite de memoria do container `lake-query` | `4g` |
| `QUERY_POOL_MB` | Memory pool do DataFusion no query | `2800` |
| `QUERY_CPUS` | CPUs do `lake-query` | `4` |

## Desenvolvimento

### Estrutura do repositorio

```text
docker-compose.yml       pilha completa (perfis: query, ops)
env.example              variaveis obrigatorias e pins de versao
Makefile                 atalhos de operacao (make help)
jobs-db/                 imagem Postgres 16 + pg_cron
  init/00-catalog-db.sh  cria banco/role catalog e extensoes do Lakekeeper
  init/01-jobs-db.sh     cria pg_cron, role lake_worker e jobs.job_queue
lake-worker/             worker Rust (DataFusion 53, iceberg-rust 0.10, tokio-postgres)
lake-query/              endpoint Flight SQL (arrow-flight 58, DataFusion 53, tonic 0.14)
scripts/
  smoke.sh               smoke test ponta a ponta em projeto isolado
  check-compose.sh       fact sheet JSON do compose, sem subir nada
  check-assessment.py    valida o contrato dos documentos de avaliacao
tests/
  check-assessment-test.sh  self-test do check-assessment.py
```

### Implementar transformacoes

A logica de negocio do worker fica no hook `run_job` em [lake-worker/src/main.rs](lake-worker/src/main.rs). Ele recebe o `SessionContext` do DataFusion (com o catalogo Iceberg), o `kind` e o `payload` do job. Um retorno `Ok(())` marca o job como `done` e um `Err` o marca como `failed`.

Depois de alterar o codigo, recompile so o servico:

```bash
docker compose up -d --build lake-worker
```

As imagens Rust usam build multi-stage com cache do cargo (BuildKit), entao rebuilds incrementais sao rapidos.

### Testes

```bash
make check      # valida o compose offline (docker compose config) e imprime fatos em JSON
make smoke      # ou: bash scripts/smoke.sh
```

O smoke test sobe a pilha inteira no projeto `rust-datalake-smoke`, com portas 19000/19001/15433/18181/50052 e segredos descartaveis. Ele verifica:

- a saude do RustFS;
- o bucket criado;
- o bootstrap e o warehouse do Lakekeeper;
- o `pg_cron` no banco `jobs`;
- o worker processando um job ate `done`;
- a porta Flight SQL.

Ao final, derruba a pilha com os volumes e imprime `SMOKE=OK`. Use `KEEP=1` para manter a pilha no ar e `SMOKE_TIMEOUT_SECS` para ajustar a espera (default `600`).

> `make test` depende de `docs/assessment/`, que nao esta neste repositorio. Alem disso, hoje `tests/check-assessment-test.sh` falha com `cp: cannot copy a directory ... into itself`.

## Limitacoes conhecidas

- **Scaffolds:** hoje `run_job` executa apenas `SELECT 1`, e o Flight SQL responde somente a statements simples. Os demais metodos retornam `unimplemented`.
- **Sem `Cargo.lock` commitado:** gere o arquivo dentro do container e faca commit, para ter builds reproduziveis.
- **Escrita Iceberg limitada:** iceberg-rust 0.10 faz append / `INSERT INTO`. Overwrite, delete e compaction precisam de outra estrategia.
- **Manutencao de tabelas:** expiracao de snapshots, limpeza de arquivos orfaos e compaction nao estao cobertas.
- **Lakekeeper sem autenticacao:** so e aceitavel enquanto a porta 8181 estiver em localhost. Antes de expor a porta, configure `LAKEKEEPER__OPENID_PROVIDER_URI` / `LAKEKEEPER__OPENID_AUDIENCE` e faca um novo bootstrap.
- **Backups parciais:** `pg-backup` cobre apenas os bancos. O volume `rustfs-data` ainda nao tem rotina de backup.

## Contribuindo

1. Crie uma branch a partir de `main` e vincule o trabalho ao item correspondente no Jira.
2. Rode `make check` e `make smoke` antes de abrir o PR.
3. Mantenha versoes de imagem fixadas em `env.example` e portas expostas apenas em `127.0.0.1`.
4. Nunca faca commit de `.env`, dumps em `backups/` ou qualquer segredo.
5. Todo merge exige revisao de par, inclusive para codigo gerado com apoio de IA.
