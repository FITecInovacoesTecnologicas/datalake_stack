# rust-datalake

> Data lake / lakehouse enxuto para um unico host, em Docker Compose, com componentes em Rust e tabelas Apache Iceberg.

![status](https://img.shields.io/badge/status-PoC-orange) ![compose](https://img.shields.io/badge/docker%20compose-v2-blue) ![rust](https://img.shields.io/badge/rust-1.88%2B-informational) ![iceberg](https://img.shields.io/badge/table%20format-Apache%20Iceberg-lightgrey)

## Visao geral

`rust-datalake` sobe, com um comando, uma pilha minima de lakehouse: object storage S3, catalogo Iceberg REST, Postgres com fila de jobs e agendamento, um worker de transformacao e um endpoint SQL. Tudo roda em um unico host e expoe portas apenas em `127.0.0.1`.

O objetivo e ter um ambiente de desenvolvimento / prova de conceito pequeno, rapido de subir e facil de entender, sem Spark, Trino ou JVM. O processamento e a consulta usam DataFusion e iceberg-rust compilados em binarios Rust proprios.

E indicado para quem precisa experimentar pipelines raw -> clean -> curated sobre Iceberg, ou validar uma arquitetura lakehouse antes de escolher infraestrutura definitiva.

> **Estado:** o `lake-worker` executa jobs `sql` (DDL e append sobre Iceberg), mas ainda nao tem jobs de negocio. O `lake-query` atende consultas so de leitura. Veja [Limitacoes conhecidas](#limitacoes-conhecidas) e o passo a passo em [docs/funcionamento.md](docs/funcionamento.md).

## Funcionalidades

- **Object storage S3 (RustFS):** bucket `lake` com chave de servico restrita ao bucket. A chave root so e usada pelo job `storage-init`.
- **Catalogo Iceberg REST (Lakekeeper):** bootstrap e warehouse criados automaticamente e de forma idempotente, com metadados no Postgres e credenciais cifradas por `LAKEKEEPER_ENCRYPTION_KEY`.
- **Postgres 16 + pg_cron:** uma instancia com dois bancos isolados por role, `catalog` (Lakekeeper) e `jobs` (fila `jobs.job_queue` + agendamento).
- **lake-worker:** consome a fila com `FOR UPDATE SKIP LOCKED` e escala horizontalmente (`--scale lake-worker=N`). A entrega e *at-least-once*: o worker envia heartbeat, um reaper recupera jobs de workers mortos, ha retry com backoff exponencial e o worker reconecta ao Postgres sozinho. Executa SQL sobre o catalogo Iceberg (`lake`), usa memory pool e spill do DataFusion e faz shutdown gracioso em SIGTERM (grace period de 2 min).
- **lake-query (perfil `query`):** endpoint Arrow Flight SQL **somente leitura** sobre DataFusion, com o catalogo Iceberg registrado como `lake` e atualizado a cada `LAKE_METADATA_CACHE_TTL_SECS`. Exige token (`LAKE_QUERY_TOKEN`) e responde aos metadados que ferramentas JDBC/ADBC consultam.
- **pg-backup e lake-backup (perfil `ops`):** `pg_dump` dos dois bancos e copia do bucket do lake em `./backups`, sob demanda.
- **Operacao por Makefile ou Taskfile:** gera segredos, sobe, escala, faz backup, roda o smoke test e derruba a pilha.
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
| `lake-worker` | build `./Dockerfile`, target `lake-worker` | core | - |
| `lake-query` | build `./Dockerfile`, target `lake-query` | `query` | 50051 (Flight SQL) |
| `pg-backup` | `rust-datalake/postgres:local` | `ops` (sob demanda) | - |
| `lake-backup` | `rustfs/rc:${RC_VERSION}` | `ops` (sob demanda) | - |

## Quick start

### Pre-requisitos

- Docker Engine 24+ com Compose v2 e BuildKit habilitado.
- `openssl`, `curl` e `bash` no host. `make` ou [Task](https://taskfile.dev) sao opcionais, mas recomendados.
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
-- job sql: uma instrucao ou uma lista, executadas em ordem sobre o catalogo `lake`
insert into jobs.job_queue (kind, payload) values ('sql', '{"sql": [
  "CREATE SCHEMA IF NOT EXISTS lake.raw",
  "CREATE TABLE IF NOT EXISTS lake.raw.vendas (id BIGINT, produto VARCHAR, valor DOUBLE)",
  "INSERT INTO lake.raw.vendas VALUES (1, ''cafe'', 12.5)"
]}');

-- agendar com pg_cron (UTC):
select cron.schedule('nightly', '0 3 * * *',
  $$insert into jobs.job_queue(kind, payload) values ('sql', '{"sql": "INSERT INTO lake.curated.t SELECT ..."}')$$);

select id, kind, status, attempts, max_attempts, last_error from jobs.job_queue order by id desc limit 10;
```

O worker verifica a fila a cada `POLL_INTERVAL_SECS` (5 s), marca o job como `running` e, ao terminar, como `done` ou `failed` (com `last_error`).

Kinds disponiveis:

| `kind` | O que faz |
|---|---|
| `sql` | Executa `payload.sql` (string ou lista) no DataFusion. `CREATE SCHEMA lake.x` cria um namespace Iceberg; `CREATE TABLE` e `INSERT INTO` (append) gravam no lake. |
| `noop`, `smoke` | Nao faz nada; serve para testes. |
| qualquer outro | Falha na hora, sem retry (`unknown job kind`). |

Como a fila se comporta em falhas:

- **Erro no job:** o job volta para `queued` com `available_at = now() + JOB_RETRY_BACKOFF_SECS * 2^(tentativa-1)` (no maximo 1 h), ate `max_attempts` (padrao 3; pode ser definido por job no `insert`). Erros de payload e `kind` desconhecido nao sao repetidos.
- **Worker morto (OOM, kill -9):** o job em `running` para de receber heartbeat. Depois de `JOB_LEASE_SECS` (60 s), qualquer replica o devolve para `queued`.
- **Postgres reiniciado:** o worker reconecta na proxima consulta a fila.
- Como um job pode rodar mais de uma vez, ele precisa ser seguro para repetir (`IF NOT EXISTS`, cargas idempotentes).

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
| `make backup` | Faz `pg_dump -Fc` de `catalog` e `jobs` e copia o bucket do lake para `./backups` |
| `make smoke` | Smoke test em projeto isolado (veja [Testes](#testes)) |
| `make down` | Para os servicos e mantem os volumes |
| `make down-volumes CONFIRM=yes` | Para os servicos e **apaga** `rustfs-data` e `pg-data` |

Para usar outro arquivo de ambiente, rode `ENV_FILE=outro.env make up`.

### Comandos do Taskfile

O [Taskfile.yml](Taskfile.yml) tem as mesmas tarefas do Makefile, com os mesmos nomes e variaveis. Use o que preferir: os dois chamam os mesmos comandos do Docker Compose e o mesmo script de geracao do `.env`.

```bash
task                              # lista as tarefas (o mesmo que task --list)
task env                          # gera .env
task up-query                     # core + lake-query
task logs SERVICE=lake-worker     # variaveis vao depois do nome da tarefa
task scale WORKERS=3
task psql DB=catalog
task down-volumes CONFIRM=yes     # recusa sem CONFIRM=yes
ENV_FILE=outro.env task up        # ou: task up ENV_FILE=outro.env
```

Requer Task 3.30 ou mais recente. Para ver o comando que uma tarefa executaria sem roda-lo, use `task --dry <tarefa>`.

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
docker compose --profile ops run --rm lake-backup
```

### Endpoints locais

| Endpoint | Endereco |
|---|---|
| S3 API (RustFS) | `http://127.0.0.1:9000` |
| Console RustFS | `http://127.0.0.1:9001` |
| Lakekeeper REST + UI | `http://127.0.0.1:8181` (catalogo em `/catalog`, gestao em `/management/v1`) |
| Postgres | `127.0.0.1:5433` (bancos `jobs` e `catalog`) |
| Flight SQL | `grpc://127.0.0.1:50051` (perfil `query`; header `authorization: Bearer $LAKE_QUERY_TOKEN`, ou usuario qualquer com senha = token) |

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
| `LAKE_QUERY_TOKEN` | Token exigido em toda chamada Flight SQL ao `lake-query` | segredo |
| `JOB_LEASE_SECS` | Tempo sem heartbeat ate um job `running` voltar para a fila | `60` |
| `JOB_RETRY_BACKOFF_SECS` | Atraso base do retry (dobra a cada tentativa, maximo 1 h) | `30` |

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
Taskfile.yml             as mesmas tarefas para o Task (task --list)
Cargo.toml / Cargo.lock  workspace Rust (lake-worker + lake-query) e lock unico
Dockerfile               build das imagens Rust (targets lake-worker e lake-query)
jobs-db/                 imagem Postgres 16 + pg_cron
  init/00-catalog-db.sh  cria banco/role catalog e extensoes do Lakekeeper
  init/01-jobs-db.sh     cria pg_cron, role lake_worker e jobs.job_queue
lake-worker/             worker Rust (DataFusion 53, iceberg-rust 0.10, tokio-postgres)
lake-query/              endpoint Flight SQL (arrow-flight 58, DataFusion 53, tonic 0.14)
scripts/
  smoke.sh               smoke test ponta a ponta em projeto isolado
  gen-env.sh             gera o .env com segredos aleatorios (make env / task env)
  check-compose.sh       fact sheet JSON do compose, sem subir nada
  check-assessment.py    valida o contrato dos documentos de avaliacao
tests/
  check-assessment-test.sh  self-test do check-assessment.py
```

### Implementar transformacoes

A logica de negocio do worker fica no hook `run_job` em [lake-worker/src/main.rs](lake-worker/src/main.rs), com um ramo do `match` por `kind`. O hook recebe o `SessionContext` do DataFusion (com o catalogo Iceberg registrado como `lake`) e o job (`kind`, `payload`, tentativa).

- Um retorno `Ok(())` marca o job como `done`.
- Um `Err` agenda um retry ou, esgotadas as tentativas, marca o job como `failed`.
- Um erro `permanent(...)` falha o job na hora.

Antes de executar SQL, chame `lake.refresh()` para enxergar tabelas criadas por outros jobs.

Para casos simples, um job `sql` ja resolve sem codigo novo.

Depois de alterar o codigo, recompile so o servico:

```bash
docker compose up -d --build lake-worker
```

As imagens Rust saem de um unico [Dockerfile](Dockerfile) na raiz, que compila o workspace Cargo inteiro numa so execucao do cargo. As dependencias (DataFusion, Arrow, iceberg-rust) sao compiladas uma vez para os dois binarios, e o `target/` fica num cache do BuildKit, entao rebuilds incrementais sao rapidos. Por isso o build de `lake-worker` tambem compila `lake-query`, e vice-versa.

O LTO fica desligado por padrao para encurtar o build. Para uma imagem de producao, rode `RUST_RELEASE_LTO=thin make build` (ou `task build`) ou defina a variavel no `.env`.

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

> `make test` roda o self-test do `check-assessment.py`. O contrato de `docs/assessment/` so e verificado se o diretorio existir; ele nao faz parte deste repositorio.

## Limitacoes conhecidas

- **Sem jobs de negocio:** o worker so conhece `sql` e `noop`. Transformacoes que nao cabem em SQL precisam de um novo `kind` em `run_job`.
- **Flight SQL parcial:** prepared statements, chaves primarias e estrangeiras e tipos XDBC respondem `unimplemented`. O endpoint e somente leitura e usa um token compartilhado, sem usuarios nem TLS.
- **Tabelas de metadados Iceberg:** `SELECT * FROM lake.ns."t$snapshots"` funciona, mas escolher colunas ou agregar gera erro interno (bug do `iceberg-datafusion` 0.10.1, que ignora a projecao).
- **Atualizar dependencias Rust:** o `Cargo.lock` do workspace, na raiz, esta commitado e o build usa `--locked`. Para atualizar, rode `cargo update` dentro de um container `rust:1.99-bookworm` e faca commit do lock.
- **Escrita Iceberg limitada:** iceberg-rust 0.10 faz append / `INSERT INTO`. Overwrite, delete e compaction precisam de outra estrategia.
- **Manutencao de tabelas:** expiracao de snapshots, limpeza de arquivos orfaos e compaction nao estao cobertas.
- **Lakekeeper sem autenticacao:** so e aceitavel enquanto a porta 8181 estiver em localhost. Antes de expor a porta, configure `LAKEKEEPER__OPENID_PROVIDER_URI` / `LAKEKEEPER__OPENID_AUDIENCE` e faca um novo bootstrap.
- **Backup sem agendamento:** `make backup` copia bancos e bucket, mas nao ha rotina periodica nem copia para fora do host. Rode `pg-backup` e `lake-backup` em sequencia (o alvo do Makefile ja faz isso) para que catalogo e dados correspondam ao mesmo momento.

## Contribuindo

1. Crie uma branch a partir de `main` e vincule o trabalho ao item correspondente no Jira.
2. Rode `make check` e `make smoke` (ou `task check` e `task smoke`) antes de abrir o PR.
3. Mantenha versoes de imagem fixadas em `env.example` e portas expostas apenas em `127.0.0.1`.
4. Nunca faca commit de `.env`, dumps em `backups/` ou qualquer segredo.
5. Todo merge exige revisao de par, inclusive para codigo gerado com apoio de IA.
