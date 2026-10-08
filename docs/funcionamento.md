# Como o rust-datalake funciona hoje

Este documento descreve, etapa por etapa, o que acontece quando a pilha sobe e como um job percorre o sistema. Ele se baseia em uma execucao real de `make env` + `make up-query` feita em 2026-10-07 sobre o commit `662b3c0`, atualizada com as correcoes das [lacunas](#lacunas-e-riscos-observados) aplicadas na mesma data.

Para instalacao e comandos, veja o [README](../README.md). Aqui o foco e o *porque* e o *como* de cada peca.

## Sumario

1. [Visao geral](#1-visao-geral)
2. [Etapa 0: gerar o `.env`](#etapa-0-gerar-o-env)
3. [Etapa 1: build das imagens](#etapa-1-build-das-imagens)
4. [Etapa 2: `rustfs` (object storage)](#etapa-2-rustfs-object-storage)
5. [Etapa 3: `storage-init` (bucket e chave restrita)](#etapa-3-storage-init-bucket-e-chave-restrita)
6. [Etapa 4: `postgres` (bancos `catalog` e `jobs`)](#etapa-4-postgres-bancos-catalog-e-jobs)
7. [Etapa 5: `lakekeeper-migrate`](#etapa-5-lakekeeper-migrate)
8. [Etapa 6: `lakekeeper` (catalogo Iceberg REST)](#etapa-6-lakekeeper-catalogo-iceberg-rest)
9. [Etapa 7: `lakekeeper-init` (bootstrap e warehouse)](#etapa-7-lakekeeper-init-bootstrap-e-warehouse)
10. [Etapa 8: `lake-worker`](#etapa-8-lake-worker)
11. [Etapa 9: `lake-query` (Flight SQL)](#etapa-9-lake-query-flight-sql)
12. [Etapa extra: `pg-backup` e `lake-backup`](#etapa-extra-pg-backup-e-lake-backup)
13. [Ciclo de vida de um job, com dados reais](#ciclo-de-vida-de-um-job-com-dados-reais)
14. [Fluxo completo de um evento: carga incremental de vendas](#fluxo-completo-de-um-evento-carga-incremental-de-vendas)
15. [Lacunas e riscos observados](#lacunas-e-riscos-observados)
16. [Como verificar cada etapa](#como-verificar-cada-etapa)

---

## 1. Visao geral

A pilha e um lakehouse minimo para um unico host, com tres camadas:

| Camada | Componente | Papel |
|---|---|---|
| Armazenamento | **RustFS** | Guarda os arquivos (Parquet e metadados Iceberg) no bucket `lake`, via API S3 |
| Catalogo | **Lakekeeper** + Postgres (`catalog`) | Sabe quais tabelas Iceberg existem e qual e o arquivo de metadados atual de cada uma |
| Processamento | **lake-worker** + Postgres (`jobs`) | Le uma fila de jobs e executa transformacoes com DataFusion |
| Consulta | **lake-query** | Expoe SQL (Arrow Flight SQL) sobre o catalogo |

Ordem de subida, definida pelos `depends_on` do [docker-compose.yml](../docker-compose.yml):

```text
rustfs ──(healthy)──▶ storage-init ──(exit 0)──────────────────────┐
                                                                   ▼
postgres ──(healthy)──▶ lakekeeper-migrate ──(exit 0)──▶ lakekeeper ──(healthy)──▶ lakekeeper-init
    │                                                                                   │ (exit 0)
    │                                                                   ┌───────────────┴───────────────┐
    └──────────────────────(healthy)──────────────────────────────────▶ lake-worker              lake-query [perfil query]
```

Tres servicos sao **one-shot**: `storage-init`, `lakekeeper-migrate` e `lakekeeper-init` rodam, terminam com codigo 0 e ficam `Exited (0)`. Isso e o esperado. Os tres sao idempotentes: rodar `make up` de novo nao duplica nada.

Estado observado apos a subida:

```text
SERVICE              STATE     STATUS                      PORTS
lake-query           running   Up                          127.0.0.1:50051->50051/tcp
lake-worker          running   Up
lakekeeper           running   Up (healthy)                127.0.0.1:8181->8181/tcp
lakekeeper-init      exited    Exited (0)
lakekeeper-migrate   exited    Exited (0)
postgres             running   Up (healthy)                127.0.0.1:5433->5432/tcp
rustfs               running   Up (healthy)                127.0.0.1:9000-9001->9000-9001/tcp
storage-init         exited    Exited (0)
```

Todas as portas estao presas a `127.0.0.1`: nada fica acessivel pela rede, so pela propria maquina.

---

## Etapa 0: gerar o `.env`

**Comando:** `make env` (alvo `env` do [Makefile](../Makefile))

O que acontece:

1. Se `.env` ja existe, o alvo nao faz nada. Ele nunca sobrescreve segredos.
2. Com `umask 077`, le o [env.example](../env.example) linha a linha.
3. Cada linha `CHAVE=` (valor vazio) recebe um segredo aleatorio em hexadecimal:
   - `LAKE_S3_ACCESS_KEY` -> `openssl rand -hex 10` (20 caracteres; o RustFS aceita de 3 a 20)
   - `LAKE_S3_SECRET_KEY` -> `openssl rand -hex 20` (40 caracteres; o RustFS aceita de 8 a 40)
   - todo o resto -> `openssl rand -hex 24` (48 caracteres)
4. Linhas que ja tem valor, como as versoes de imagem, sao copiadas como estao.
5. O arquivo final recebe `chmod 600`.

O formato hex foi escolhido porque o valor pode ir para uma URL (`JOBS_DATABASE_URL`), para JSON (`lakekeeper-init`) ou para o shell, e hex nao precisa de escape em nenhum desses casos.

O Compose usa a sintaxe `${VAR:?mensagem}`. Se qualquer variavel obrigatoria estiver vazia, `docker compose up` falha antes de criar containers.

> **Segredo critico:** `LAKEKEEPER_ENCRYPTION_KEY` cifra a credencial S3 que o Lakekeeper guarda no banco `catalog`. Sem ela, um backup do `catalog` nao serve para nada. Guarde essa chave fora do host.

**Antes de subir (opcional):** `make check` roda [scripts/check-compose.sh](../scripts/check-compose.sh), que renderiza o compose com `docker compose config` (sem subir nada) e imprime um JSON com servicos, politicas de restart, limites de recurso e variaveis exigidas.

---

## Etapa 1: build das imagens

**Comando:** `make up-query` (equivale a `docker compose --env-file .env --profile query up -d --build`)

Tres imagens sao construidas localmente:

| Imagem | Origem | O que faz o build | Tamanho final |
|---|---|---|---|
| `rust-datalake/postgres:local` | [jobs-db/Dockerfile](../jobs-db/Dockerfile) | `postgres:16.15-bookworm` + `apt install postgresql-16-cron` | 445 MB |
| `rust-datalake/lake-worker:local` | [lake-worker/Dockerfile](../lake-worker/Dockerfile) | `cargo build --release` em `rust:1.99-bookworm`; copia so o binario para `debian:bookworm-slim` | 240 MB |
| `rust-datalake/lake-query:local` | [lake-query/Dockerfile](../lake-query/Dockerfile) | Idem, com `arrow-flight` + `iceberg-datafusion` | 258 MB |

As imagens Rust usam **build multi-stage**:

- **Estagio `builder`:**
  1. copia `Cargo.toml` e `Cargo.lock` e roda `cargo fetch --locked` com um `main.rs` provisorio. As dependencias ficam nessa camada da imagem, que so e refeita quando o `Cargo.lock` muda;
  2. copia `src/` e compila com `cargo build --release --locked --offline`. O diretorio `target/` fica num cache do BuildKit proprio de cada servico (`id=lake-worker-target` / `id=lake-query-target`), entao so o primeiro build e lento e os dois builds rodam em paralelo sem se bloquear.
- **Estagio final:** Debian slim com `ca-certificates`, um usuario sem privilegios (`lake`, uid 10001) e o diretorio `/spill`. O toolchain Rust nao vai para a imagem final.

Tempos da primeira compilacao nesta maquina (12 CPUs):

- `lake-worker`: **7m47s**
- `lake-query`: **14m05s** (mais dependencias: Flight, tonic, `iceberg-datafusion`)

Os dois builds rodam em paralelo, entao o tempo total ficou perto de 15 minutos. Rebuilds incrementais, como `docker compose up -d --build lake-worker` depois de editar o `main.rs`, recompilam so o crate alterado.

Os `Cargo.lock` estao versionados e o build usa `--locked`, entao builds em datas diferentes geram os mesmos binarios. Para atualizar dependencias, rode `cargo update` dentro de um container `rust:1.99-bookworm` e faca commit do lock.

> Historico: na primeira versao, os dois builds compartilhavam o cache do registry do cargo. Quando surgiu um crate novo, os dois tentaram desempacota-lo ao mesmo tempo e um falhou com `File exists`. A camada de dependencias por servico eliminou a disputa.

---

## Etapa 2: `rustfs` (object storage)

**Imagem:** `rustfs/rustfs:1.0.0`. RustFS e um servidor compativel com S3, escrito em Rust e semelhante ao MinIO.

- Sobe com a credencial **root** (`RUSTFS_ROOT_USER` / `RUSTFS_ROOT_PASSWORD`).
- Guarda os dados no volume nomeado `rustfs-data` (`/data`).
- Porta `9000`: API S3. Porta `9001`: console web.
- **Healthcheck:** `curl http://127.0.0.1:9000/health` a cada 10 s, com ate 12 tentativas e `start_period` de 20 s.
- Opcao `no-new-privileges:true` ativada.

Resultado observado: `/health` responde `200`. O console fica em `http://127.0.0.1:9001/rustfs/console/`. A raiz `http://127.0.0.1:9001/` responde `403`, o que e normal.

---

## Etapa 3: `storage-init` (bucket e chave restrita)

**Imagem:** `rustfs/rc:v0.1.36` (cliente de linha de comando do RustFS). **Espera:** `rustfs` healthy.

Este e o **unico** servico que usa a chave root. O script inline no compose:

1. `rc alias set lake http://rustfs:9000 <root>`: configura o cliente.
2. `rc bucket create lake/lake --ignore-existing`: cria o bucket `lake`. Se ele ja existe, nao faz nada.
3. Escreve uma politica IAM em `/tmp/lake-policy.json` com duas regras:
   - no **bucket**: `ListBucket`, `GetBucketLocation`, `ListBucketMultipartUploads`;
   - nos **objetos** (`lake/*`): `GetObject`, `PutObject`, `DeleteObject`, `AbortMultipartUpload`, `ListMultipartUploadParts`.
4. Cria a *service account* `LAKE_S3_ACCESS_KEY` com essa politica. Se a conta ja existe, atualiza o secret e a politica.
5. Imprime `bucket lake ready` e termina.

Na pratica, todos os outros servicos (Lakekeeper, worker, query) usam essa chave restrita. Mesmo que ela vaze, nao da acesso a outros buckets nem a operacoes administrativas.

Log observado:

```text
✓ Bucket 'lake/lake' created successfully.
lake access key created
bucket lake ready
```

---

## Etapa 4: `postgres` (bancos `catalog` e `jobs`)

**Imagem:** `rust-datalake/postgres:local` (Postgres 16.15 + pg_cron). **Volume:** `pg-data`.

Uma unica instancia com dois bancos isolados por role:

| Banco | Dono | Quem acessa | Para que serve |
|---|---|---|---|
| `catalog` | role `lakekeeper` | so `lakekeeper` e superusuarios (`REVOKE CONNECT ... FROM PUBLIC`) | metadados do catalogo Iceberg |
| `jobs` | superusuario `lake_admin` | `lake_worker` (schema `jobs`) | fila de jobs e agendamento com pg_cron |

### Parametros do servidor (campo `command` do compose)

- `shared_preload_libraries=pg_cron`: carrega o pg_cron no boot.
- `cron.database_name=jobs`: o pg_cron vive no banco `jobs`.
- `cron.use_background_workers=on`: os jobs agendados rodam em background workers, sem abrir conexao via libpq.
- `cron.timezone=UTC`: os horarios do `cron.schedule` sao em UTC.
- `max_worker_processes=20`: folga para os background workers do pg_cron.

### Scripts de inicializacao

Os scripts de `jobs-db/init/` rodam **so no primeiro boot, com o volume vazio**. Mudar um script depois disso nao tem efeito, a menos que o volume seja apagado (`make down-volumes CONFIRM=yes`).

[00-catalog-db.sh](../jobs-db/init/00-catalog-db.sh):

1. Cria a role `lakekeeper` com `CATALOG_DB_PASSWORD`.
2. Cria o banco `catalog`, tendo `lakekeeper` como dono.
3. Revoga `CONNECT` de `PUBLIC` no `catalog`.
4. Instala as extensoes que as migracoes do Lakekeeper exigem: `uuid-ossp`, `pgcrypto`, `pg_trgm`, `btree_gin`, `btree_gist`.

[01-jobs-db.sh](../jobs-db/init/01-jobs-db.sh):

1. `CREATE EXTENSION pg_cron` no banco `jobs`.
2. Cria a role `lake_worker` com `JOBS_WORKER_PASSWORD`.
3. Cria o schema `jobs`, tendo `lake_worker` como dono.
4. Cria a fila:

```sql
CREATE TABLE jobs.job_queue (
    id           bigserial PRIMARY KEY,
    kind         text        NOT NULL,                 -- tipo do job (ex.: 'refresh-curated')
    payload      jsonb       NOT NULL DEFAULT '{}',    -- parametros livres
    status       text        NOT NULL DEFAULT 'queued' -- queued | running | done | failed
    attempts     integer     NOT NULL DEFAULT 0,       -- quantas vezes foi reivindicado (token de fencing)
    max_attempts integer     NOT NULL DEFAULT 3,       -- limite de tentativas com retry
    available_at timestamptz NOT NULL DEFAULT now(),   -- so pode ser pego a partir deste instante
    claimed_at   timestamptz,                          -- quando um worker pegou
    claimed_by   text,                                 -- hostname do worker que pegou
    heartbeat_at timestamptz,                          -- ultimo sinal de vida do worker durante o job
    finished_at  timestamptz,                          -- quando terminou (done ou failed)
    last_error   text                                  -- mensagem de erro se failed
);
CREATE INDEX job_queue_status_available_idx ON jobs.job_queue (status, available_at);
```

5. Concede `USAGE` no schema `cron` a `lake_worker`.

As colunas `max_attempts`, `claimed_by` e `heartbeat_at` foram adicionadas depois da primeira versao. Em volumes criados antes delas, o proprio `lake-worker` as cria na inicializacao, com `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`. Isso e possivel porque `lake_worker` e dono da tabela.

**Healthcheck:** `pg_isready -h 127.0.0.1`. O `-h 127.0.0.1` forca TCP. Durante os scripts de init, o Postgres temporario escuta so no socket unix, entao o healthcheck **nao passa antes** dos scripts terminarem. Isso evita que o Lakekeeper conecte com o banco pela metade.

Verificacao observada: bancos `postgres`, `jobs` e `catalog`; extensao `pg_cron 1.6` em `jobs`.

---

## Etapa 5: `lakekeeper-migrate`

**Imagem:** `quay.io/lakekeeper/catalog:v0.13.1`, comando `migrate`. **Espera:** `postgres` healthy.

Aplica o schema do Lakekeeper no banco `catalog`. A conexao usa campos separados (`LAKEKEEPER__PG_HOST_W`, `..._PORT`, `..._USER`, `..._PASSWORD`, `..._DATABASE`) em vez de uma URL, para que a senha nao precise de URL-encoding. Ao terminar, sai com codigo 0.

Resultado observado: **47 tabelas** no schema `public` do `catalog`.

---

## Etapa 6: `lakekeeper` (catalogo Iceberg REST)

**Mesma imagem**, comando `serve`. **Espera:** `lakekeeper-migrate` concluido com sucesso.

- Implementa a **Iceberg REST Catalog API** em `/catalog` e a API de gestao em `/management/v1`. A UI fica em `/ui/`.
- Guarda tudo no banco `catalog`. Credenciais de storage sao cifradas com `LAKEKEEPER__PG_ENCRYPTION_KEY`.
- **Sem autenticacao:** `authz-backend: allow-all`. So e aceitavel porque a porta 8181 esta em localhost.
- **Healthcheck:** o proprio binario (`/home/nonroot/lakekeeper healthcheck`).

`GET /management/v1/info` observado:

```json
{"version":"0.13.1","bootstrapped":true,"authz-backend":"allow-all",
 "queues":["tabular_expiration","tabular_purge","task_log_cleanup"], ...}
```

As filas internas do Lakekeeper open-source cuidam so de expiracao/purge de tabelas removidas e de limpeza de logs. **Nao** fazem compaction, expiracao de snapshots nem remocao de arquivos orfaos.

---

## Etapa 7: `lakekeeper-init` (bootstrap e warehouse)

**Imagem:** `curlimages/curl:8.22.0`. **Espera:** `lakekeeper` healthy **e** `storage-init` concluido. O bucket e a chave precisam existir antes de o warehouse apontar para eles.

O script faz duas chamadas HTTP, cada uma protegida por uma verificacao de idempotencia:

1. **Bootstrap.** Se `GET /management/v1/info` ainda nao mostra `"bootstrapped": true`, envia `POST /management/v1/bootstrap` com `{"accept-terms-of-use": true}`. O bootstrap inicializa o servidor e cria o projeto padrao.
2. **Warehouse.** Se `GET /management/v1/warehouse` ainda nao lista `lake`, envia `POST /management/v1/warehouse` com:
   - `storage-profile`: S3 no bucket `lake`, com prefixo `lake/`, endpoint `http://rustfs:9000`, `path-style-access: true`, `flavor: s3-compat`, **`sts-enabled: false`** e **`remote-signing-enabled: true`**;
   - `storage-credential`: a chave restrita da etapa 3, que o Lakekeeper cifra e guarda;
   - `delete-profile: hard`: um `DROP TABLE` apaga os dados de verdade.

Resultado observado:

```text
lakekeeper bootstrapped
warehouse lake created
```

O warehouse `lake` ficou `active` (id `7733a534-...`). Os dados das tabelas vao para `s3://lake/lake/...`.

### Modelo de acesso ao storage

Um cliente Iceberg pode obter acesso ao S3 de tres formas. Esta pilha usa duas:

| Quem | Modo | Como funciona |
|---|---|---|
| `lake-worker`, `lake-query` | `client-managed` | Enviam o header `X-Iceberg-Access-Delegation: client-managed`. O Lakekeeper nao devolve credenciais: a resposta traz endpoint, regiao e path-style (e tambem campos de remote signing, que estes servicos nao usam). O cliente assina as requisicoes S3 com a chave restrita que recebeu por variavel de ambiente. |
| Clientes externos (PyIceberg, Spark, Trino, DuckDB) | `remote-signing` | O cliente nao conhece a chave. Ele pede ao Lakekeeper que assine cada requisicao S3. |
| (desligado) | `vended-credentials` / STS | Exigiria STS no RustFS. |

---

## Etapa 8: `lake-worker`

**Imagem:** `rust-datalake/lake-worker:local`. **Espera:** `postgres` healthy **e** `lakekeeper-init` concluido. Codigo em [lake-worker/src/main.rs](../lake-worker/src/main.rs).

### Configuracao recebida (ambiente)

- Bloco compartilhado `x-lake-client-env`: `LAKE_CATALOG_URI=http://lakekeeper:8181/catalog`, `LAKE_WAREHOUSE`, `LAKE_S3_*` e `LAKE_ACCESS_DELEGATION=client-managed`.
- `JOBS_DATABASE_URL=postgresql://lake_worker:<senha>@postgres:5432/jobs`
- `POLL_INTERVAL_SECS=5`, `JOB_LEASE_SECS=60`, `JOB_RETRY_BACKOFF_SECS=30`
- `LAKE_MEMORY_LIMIT_MB=1400`, `LAKE_SPILL_DIR=/spill`
- Limites do container: `mem_limit: 2g`, `cpus: 2`. O pool do DataFusion (1400 MB) fica abaixo do limite do container (2 GB), e o que passar disso vai para spill em disco.
- `/spill` e um **volume anonimo**, entao cada replica tem o seu.
- `init: true` e `stop_grace_period: 2m`: o `tini` repassa o SIGTERM ao processo, que tem ate 2 minutos para terminar o job em andamento.

### Sequencia de inicializacao

Os logs sao JSON estruturado (`tracing-subscriber`):

1. Le e valida as variaveis. Se faltar uma obrigatoria, encerra com erro.
2. Cria o diretorio de spill e monta o **runtime do DataFusion** (memory pool limitado, spill em `/spill`).
   → `datafusion session ready {memory_mb:1400, spill_dir:"/spill"}`
3. Cria o cliente do **catalogo REST** (iceberg-rust) com o `OpenDalStorageFactory::S3`, que e o FileIO usado para ler e gravar Parquet e metadados no RustFS. Em seguida, registra o catalogo no DataFusion como **`lake`**. Se o Lakekeeper estiver fora, o worker sobe mesmo assim e tenta registrar de novo no proximo job.
   → `iceberg catalog registered as \`lake\` {namespaces:N}`
4. Conecta ao banco `jobs` (timeout de 10 s) e aplica a migracao idempotente das colunas novas.
   → `connected to jobs database`, `lake-worker started {lease_secs:60, worker_id:<hostname>}`
5. Entra no loop.

### Loop de polling

Cada volta do loop executa:

1. **Reaper**, no maximo uma vez a cada `lease/3` (20 s). Jobs `running` sem heartbeat ha mais de `JOB_LEASE_SECS` voltam para `queued`, ou viram `failed` se ja esgotaram `max_attempts`:

   ```sql
   UPDATE jobs.job_queue
      SET status = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END, ...
          last_error = 'lease expired: worker <id> stopped heartbeating'
    WHERE status = 'running'
      AND coalesce(heartbeat_at, claimed_at) < now() - make_interval(secs => $lease);
   ```

2. **Claim** de um job, de forma atomica:

   ```sql
   UPDATE jobs.job_queue
      SET status = 'running', claimed_at = now(), heartbeat_at = now(),
          claimed_by = $worker, attempts = attempts + 1, finished_at = NULL
    WHERE id = (SELECT id FROM jobs.job_queue
                 WHERE status = 'queued' AND available_at <= now()
                 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
   RETURNING id, kind, payload, attempts, max_attempts;
   ```

   - `FOR UPDATE SKIP LOCKED` garante que **duas replicas nunca pegam o mesmo job**. E isso que permite `make scale WORKERS=3`.
   - `ORDER BY id` produz uma ordem FIFO. `available_at <= now()` permite agendar jobs para o futuro e e o que implementa o backoff do retry.

3. **Execucao com heartbeat.** O job roda enquanto, em paralelo, o worker atualiza `heartbeat_at` a cada `lease/3`. Se o heartbeat afetar 0 linhas, significa que outra replica ja reaproveitou o job; nesse caso a execucao local e abandonada.

4. **Registro do resultado**, sempre com *fencing* por `(id, attempts)`:

   | Resultado do job | Efeito |
   |---|---|
   | `Ok` | `status='done'`, `finished_at=now()` |
   | erro comum, com tentativas restantes | `status='queued'`, `available_at = now() + backoff * 2^(tentativa-1)` (no maximo 1 h), `last_error` |
   | erro comum, sem tentativas restantes | `status='failed'`, `finished_at`, `last_error` |
   | erro permanente (payload invalido, `kind` desconhecido) | `status='failed'` na hora, sem retry |
   | lease perdido (outro worker ja tem o job) | nada e gravado: o `UPDATE` com fencing nao encontra a linha |

5. **Fila vazia:** loga `polling jobs.job_queue: idle` e dorme `POLL_INTERVAL_SECS` (5 s), acordando antes se chegar um SIGTERM. Depois de concluir um job, o worker volta direto ao claim, sem dormir.

### Conexao com o Postgres

O worker mantem uma conexao e a recria quando ela cai. Se o Postgres reiniciar, a proxima consulta a fila detecta `is_closed()`, reconecta e segue normalmente. Se a conexao cair durante um job, o resultado e gravado apos a reconexao. Se nem isso for possivel, o reaper recupera o job quando o lease expirar.

### O hook `run_job`

Ele despacha por `kind`:

| `kind` | Comportamento |
|---|---|
| `sql` | `payload.sql` e uma string ou uma lista. Antes de executar, o worker chama `lake.refresh()` para enxergar tabelas criadas por outros jobs. Cada instrucao e planejada e executada no DataFusion. `CREATE SCHEMA lake.x` cria um namespace Iceberg; `CREATE TABLE lake.x.t (...)` cria a tabela; `INSERT INTO ... VALUES/SELECT` faz append. |
| `noop`, `smoke` | Sucesso imediato. |
| outro | Erro permanente: `unknown job kind "..."`. |

A entrega e *at-least-once*: um job pode rodar mais de uma vez (por exemplo, apos um crash). Por isso, cada job precisa ser seguro para repetir (`IF NOT EXISTS`, cargas idempotentes). Transformacoes que nao cabem em SQL entram como um novo ramo do `match`.

### Desligamento

Quando chega SIGTERM ou SIGINT, o loop termina na proxima espera (fila vazia) ou logo depois de concluir o job atual. Um job em execucao nao e interrompido. Se ele passar dos 2 minutos de grace period, o container e morto e o reaper recupera o job.

### O catalogo `lake` e o cache de metadados

O `IcebergCatalogProvider` do crate `iceberg-datafusion` 0.10 lista namespaces e tabelas uma unica vez, quando e criado (ha um `TODO` sobre isso no proprio crate). Os dados nao ficam desatualizados, porque cada scan e cada insert recarregam o snapshot atual. A **lista** de tabelas, porem, fica. Por isso, os dois servicos o envolvem em um `LakeCatalogProvider` que reconstroi a listagem:

- **worker:** antes de cada job `sql`. O wrapper tambem traduz `CREATE SCHEMA` para a criacao de um namespace no Lakekeeper.
- **query:** quando a listagem tem mais de `LAKE_METADATA_CACHE_TTL_SECS` (30 s), **e** imediatamente quando uma consulta cita uma tabela desconhecida (refresh-on-miss, seguido de uma nova tentativa de planejamento).

---

## Etapa 9: `lake-query` (Flight SQL)

**Perfil:** `query`. So sobe com `make up-query` ou `--profile query`. **Espera:** `lakekeeper-init` concluido. Codigo em [lake-query/src/main.rs](../lake-query/src/main.rs).

- Limites: `mem_limit: 4g`, `cpus: 4`, pool do DataFusion de 2800 MB e spill em `/spill`.
- `SessionContext` com `information_schema` ligado e o catalogo Iceberg registrado como **`lake`**, atualizado conforme descrito na etapa anterior.
- Serve **Arrow Flight SQL** em `0.0.0.0:50051` (no host: `127.0.0.1:50051`).

### Autenticacao

Toda chamada passa por um interceptor que exige `LAKE_QUERY_TOKEN` (gerado pelo `make env`), em uma de duas formas:

- header `authorization: Bearer <token>` (ADBC: opcao `adbc.flight.sql.authorization_header`);
- usuario qualquer com **senha = token**. O handshake Basic do JDBC/ADBC e aceito e devolve o bearer para as chamadas seguintes.

A comparacao do token e feita em tempo constante. Sem token, ou com token errado, a resposta e `UNAUTHENTICATED`. Nao ha TLS: o endpoint continua restrito a `127.0.0.1`.

### Somente leitura

O SQL e planejado com `SQLOptions` que proibem DDL (`CREATE`/`DROP`), DML (`INSERT`) e statements como `SET`. A escrita no lake fica restrita aos jobs do worker.

Antes dessa correcao, a combinacao das duas chamadas Flight (`GetFlightInfo` e depois `DoGet`) executava um `CREATE TABLE` no planejamento e de novo na execucao, e qualquer processo local podia escrever no lake.

### Metodos implementados

| Metodo | Uso |
|---|---|
| `GetFlightInfo(CommandStatementQuery)` + `DoGet` | Executar uma consulta. O ticket carrega o proprio SQL, que e replanejado e executado em streaming. |
| `GetCatalogs`, `GetDbSchemas`, `GetTables` (com schema Arrow opcional), `GetTableTypes` | Navegacao de objetos em DBeaver/ADBC. Tabelas de metadados Iceberg (`t$snapshots`, `t$manifests`) nao aparecem na listagem, mas podem ser consultadas. |
| `GetSqlInfo` | Nome e versao do servidor e `read_only=true`. |
| `Handshake` | Basic -> bearer, como descrito acima. |

Prepared statements, chaves primarias e estrangeiras e tipos XDBC continuam respondendo `unimplemented`.

---

## Etapa extra: `pg-backup` e `lake-backup`

**Perfil:** `ops`. Nao sobem com `up`; rodam sob demanda com `make backup`, que executa os dois em sequencia.

- **`pg-backup`:** usa a imagem do Postgres e executa `pg_dump -Fc` (formato custom, restauravel com `pg_restore`) para `catalog` e `jobs`, gerando `./backups/<banco>-<timestamp UTC>.dump`.
- **`lake-backup`:** usa a imagem `rustfs/rc` com a **chave restrita ao bucket** (nunca a root) e executa `rc mirror lake/lake ./backups/lake-<timestamp UTC>/`. O resultado contem os Parquet (`data/`) e os metadados Iceberg (`metadata/`) de cada tabela.

Para restaurar, voce precisa do dump do `catalog`, da copia do bucket do **mesmo momento** e da mesma `LAKEKEEPER_ENCRYPTION_KEY`. Os arquivos sao gravados com dono `root`, porque os dois containers escrevem no bind mount como root. Nao ha agendamento nem copia para fora do host.

---

## Ciclo de vida de um job, com dados reais

Execucao feita com a pilha recem-subida e o worker ocioso, ainda com o hook `run_job` original (`SELECT 1`). Hoje, um job `demo` falharia com `unknown job kind`; use `noop` para reproduzir o mesmo fluxo.

**1. Insercao** (equivalente a `make psql` e depois o SQL abaixo):

```sql
insert into jobs.job_queue (kind, payload)
values ('demo', '{"origem":"claude","tabela":"sales"}')
returning id, kind, status, attempts, available_at;
```

```text
 id | kind | status | attempts |         available_at
----+------+--------+----------+-------------------------------
  1 | demo | queued |        0 | 2026-10-08 00:45:07.421141+00
```

**2. Estado da linha consultado a cada ~0,5 s:**

```text
20:45:07.524  queued attempts=0
20:45:08.119  queued attempts=0
20:45:08.714  queued attempts=0
20:45:09.306  queued attempts=0
20:45:09.901  done   attempts=1
```

O job esperou cerca de 2,2 s na fila. Esse tempo e o restante do intervalo de polling de 5 s, porque o worker estava dormindo. O estado `running` durou so 5 ms e nao apareceu na amostragem.

**3. Linha final:**

```text
 id | kind | payload                                 | status | attempts | claimed_at      | finished_at     | duracao         | last_error
----+------+-----------------------------------------+--------+----------+-----------------+-----------------+-----------------+-----------
  1 | demo | {"origem": "claude", "tabela": "sales"} | done   |        1 | 00:45:09.656402 | 00:45:09.661901 | 00:00:00.005499 |
```

**4. Logs do worker no mesmo intervalo:**

```json
{"timestamp":"2026-10-08T00:45:09.660254Z","level":"INFO","fields":{"message":"running job","kind":"demo","payload":"{\"origem\":\"claude\",\"tabela\":\"sales\"}"}}
{"timestamp":"2026-10-08T00:45:09.662779Z","level":"INFO","fields":{"message":"job done","id":1,"kind":"demo"}}
{"timestamp":"2026-10-08T00:45:09.663054Z","level":"INFO","fields":{"message":"polling jobs.job_queue: idle"}}
```

Maquina de estados de um job:

```text
           INSERT
             │
             ▼
         ┌────────┐  worker: UPDATE ... FOR UPDATE SKIP LOCKED
    ┌───▶│ queued │──────────────────────────────────────────┐
    │    └────────┘                                          ▼
    │                                                  ┌─────────┐  attempts += 1, claimed_at,
    │  erro com tentativas restantes                   │ running │  heartbeat_at a cada lease/3
    │  (available_at = now() + backoff*2^(n-1))        └────┬────┘
    ├───────────────────────────────────────────────────────┤
    │  reaper: sem heartbeat ha JOB_LEASE_SECS              │
    └───────────────────────────────────────────────────────┤
                                               run_job Ok ┌─┴──────────┐ erro permanente ou
                                                          ▼            ▼ tentativas esgotadas
                                                      ┌──────┐    ┌────────┐
                                                      │ done │    │ failed │  last_error
                                                      └──────┘    └────────┘
                                                (estados terminais: nada os move de volta)
```

### Exemplo real: pipeline raw -> curated

Dois jobs `sql` independentes. O segundo enxerga a tabela que o primeiro criou, porque o catalogo e recarregado antes de cada job:

```sql
insert into jobs.job_queue(kind, payload) values ('sql', '{"sql": [
  "CREATE SCHEMA IF NOT EXISTS lake.raw",
  "CREATE SCHEMA IF NOT EXISTS lake.curated",
  "CREATE TABLE IF NOT EXISTS lake.raw.vendas (id BIGINT, produto VARCHAR, valor DOUBLE, dia DATE)",
  "INSERT INTO lake.raw.vendas VALUES (1,''cafe'',12.5,DATE ''2026-10-01''), (2,''pao'',3.0,DATE ''2026-10-01''), ..."
]}');

insert into jobs.job_queue(kind, payload) values ('sql', '{"sql": [
  "CREATE TABLE IF NOT EXISTS lake.curated.vendas_por_produto (produto VARCHAR, qtd BIGINT, total DOUBLE)",
  "INSERT INTO lake.curated.vendas_por_produto SELECT produto, count(*), sum(valor) FROM lake.raw.vendas GROUP BY produto"
]}');
```

Log do worker:

```text
running job {id:7}  statement done {statement:0..2, rows:0}  statement done {statement:3, rows:1}  job done {id:7}
running job {id:8}  statement done {statement:0, rows:0}     statement done {statement:1, rows:1}  job done {id:8}
```

Consulta via Flight SQL (`SELECT * FROM lake.curated.vendas_por_produto ORDER BY produto`):

```text
{'produto': 'cafe',  'qtd': 2, 'total': 25.5}
{'produto': 'leite', 'qtd': 1, 'total': 6.2}
{'produto': 'pao',   'qtd': 2, 'total': 6.5}
```

No bucket, cada tabela vira `lake/<uuid-da-tabela>/data/*.parquet` mais `metadata/*.metadata.json`, manifests e snapshots `.avro`.

Para **agendar** jobs recorrentes, o pg_cron insere linhas na fila (horario em UTC):

```sql
select cron.schedule('nightly', '0 3 * * *',
  $$insert into jobs.job_queue(kind, payload) values ('sql',
    '{"sql": "INSERT INTO lake.curated.vendas_por_produto SELECT ..."}')$$);
```

---

## Fluxo completo de um evento: carga incremental de vendas

Esta secao acompanha um evento real, executado em 2026-10-08, do `INSERT` na fila ate o dado aparecer numa consulta SQL. Ele mostra o que cada componente faz em cada passo. Os horarios estao em UTC.

**Cenario:** a tabela `lake.raw.vendas` ja tinha 5 vendas (dias 01/10 e 02/10), gravadas pelo pipeline da secao anterior. Chega a carga do dia 08/10, com 4 vendas.

### Visao geral

```text
 operador / pg_cron          Postgres (jobs)            lake-worker                    Lakekeeper              RustFS (bucket lake)        lake-query
        │                          │                          │                            │                          │                        │
 P0     │                          │                          │                            │                          │  ◀── SELECT (antes) ── │
 P1     │── INSERT job_queue ─────▶│ status=queued            │                            │                          │                        │
 P2     │                          │◀── reaper + claim ───────│ (acorda do poll de 5 s)    │                          │                        │
        │                          │── id, kind, payload ────▶│ status=running             │                          │                        │
 P3     │                          │                          │── listar namespaces ──────▶│                          │                        │
        │                          │                          │   e tabelas (refresh)      │                          │                        │
 P4     │                          │                          │── load_table ─────────────▶│── metadata.json atual    │                        │
 P5     │                          │                          │── PUT data/*.parquet ─────────────────────────────────▶│                        │
        │                          │                          │── PUT manifest + snap ────────────────────────────────▶│                        │
 P6     │                          │                          │── commit (novo snapshot) ─▶│── grava 00002...json ───▶│                        │
 P7     │                          │◀── UPDATE done ──────────│ (fencing id+attempts)      │                          │                        │
 P8     │                          │                          │                            │◀── load_table ───────────────────────────────── │
        │                          │                          │                            │                          │◀── GET parquet ─────── │
        │                          │                          │                            │                          │  ── resultado ───────▶ │
```

### Linha do tempo observada

| Horario (UTC) | Passo | Evento | Fonte |
|---|---|---|---|
| 12:31:27.75 | P0 | Consulta "antes" no `lake-query`: 2 dias, 5 vendas | cliente Flight SQL |
| 12:31:27.877 | P1 | Job 14 inserido como `queued` (`available_at`) | `jobs.job_queue` |
| 12:31:29.701 | P2 | Worker `45cc0336be03` pega o job: `running`, `attempts=1` (`claimed_at`, `heartbeat_at`) | `jobs.job_queue` |
| 12:31:29.704 | P2 | Log `running job {id:14, kind:sql, attempt:1}` | log do worker |
| 12:31:29.736 | P6 | Snapshot `942438293836199047` registrado no Lakekeeper (`timestamp-ms`) | Lakekeeper |
| 12:31:29.747 | P6 | Log `statement done {statement:0, rows:1}` | log do worker |
| 12:31:29.748 | P7 | `status=done` (`finished_at`) e log `job done {id:14}` | `jobs.job_queue` e log |
| 12:31:30.4 | P8 | Consulta "depois": 3 dias, 9 vendas | cliente Flight SQL |

O job passou **1,8 s** na fila, porque o worker estava no meio do `sleep` de 5 s do polling. A execucao levou **46 ms**, do claim ao `done`. Entre o `INSERT` na fila e o dado visivel na consulta passaram cerca de 2,5 s.

### P0: consulta "antes" (`lake-query`)

```sql
SELECT dia, count(*) AS vendas, sum(valor) AS total FROM lake.raw.vendas GROUP BY dia ORDER BY dia
```

```text
{'dia': 2026-10-01, 'vendas': 2, 'total': 15.5}
{'dia': 2026-10-02, 'vendas': 3, 'total': 22.7}
```

O caminho dessa consulta e o mesmo descrito em P8.

### P1: o evento entra na fila (Postgres, banco `jobs`)

Um operador (via `make psql`) ou uma regra do `pg_cron` insere uma linha. Neste caso, um job `sql` com uma unica instrucao:

```sql
insert into jobs.job_queue(kind, payload) values ('sql', '{"sql":
  "INSERT INTO lake.raw.vendas VALUES (6,''cafe'',14.0,DATE ''2026-10-08''),(7,''cafe'',12.5,DATE ''2026-10-08''),(8,''pao'',4.0,DATE ''2026-10-08''),(9,''bolo'',22.9,DATE ''2026-10-08'')"}')
returning id, kind, status, attempts, max_attempts, available_at;
```

```text
 id | kind | status | attempts | max_attempts |         available_at
----+------+--------+----------+--------------+-------------------------------
 14 | sql  | queued |        0 |            3 | 2026-10-08 12:31:27.877722+00
```

O que o Postgres preenche sozinho, pelos defaults da tabela:

- `status='queued'`: o job esta pronto para ser pego;
- `available_at=now()`: pode ser pego imediatamente. Um valor futuro agendaria o job;
- `attempts=0`, `max_attempts=3`: ate 3 tentativas em caso de erro.

Nada avisa o worker. Ele descobre o job no proximo polling.

### P2: o worker pega o job (reaper + claim)

O worker estava ocioso, logando `polling jobs.job_queue: idle` a cada 5 s. Ao acordar, a funcao `poll_once` executa duas instrucoes no banco `jobs`:

1. **Reaper.** Procura jobs `running` sem heartbeat ha mais de 60 s, de algum worker que morreu. Nao havia nenhum.
2. **Claim atomico.** Seleciona o job `queued` mais antigo com `available_at <= now()`, trava a linha com `FOR UPDATE SKIP LOCKED` e a atualiza na mesma instrucao:

   ```text
   status       queued  -> running
   attempts     0       -> 1                    (tambem e o token de fencing)
   claimed_at           -> 12:31:29.701610
   heartbeat_at         -> 12:31:29.701610
   claimed_by           -> 45cc0336be03         (hostname do container)
   ```

Com varias replicas (`make scale WORKERS=3`), o `SKIP LOCKED` faz cada replica pular a linha ja travada por outra. Assim, o job 14 so pode ser pego por um worker.

A partir daqui, enquanto o job roda, o worker atualizaria `heartbeat_at` a cada 20 s (`JOB_LEASE_SECS/3`). Este job terminou em 46 ms, entao nenhum heartbeat extra foi necessario: `heartbeat_at` ficou igual a `claimed_at`.

### P3: o worker atualiza a visao do catalogo (Lakekeeper)

`run_job` ve `kind = "sql"`, le `payload.sql` (uma string, portanto uma instrucao) e chama `lake.refresh()`. O refresh reconstroi o `IcebergCatalogProvider` consultando o Lakekeeper pela API REST Iceberg:

- `GET /catalog/v1/{warehouse}/namespaces` -> `raw`, `curated`;
- `GET .../namespaces/{ns}/tables` para cada namespace;
- `GET .../tables/{t}` para cada tabela, carregando os metadados.

Toda chamada leva o header `X-Iceberg-Access-Delegation: client-managed`. O refresh garante que o worker enxergue qualquer tabela criada por outro job desde o ultimo refresh.

### P4: planejamento do `INSERT` (DataFusion + iceberg-datafusion)

`ctx.sql(...)` faz o parse e o planejamento:

1. Resolve `lake.raw.vendas`: catalogo `lake` (o `LakeCatalogProvider`), schema `raw` e tabela `vendas` (um `IcebergTableProvider`).
2. Converte o `VALUES` em um plano que produz 4 linhas no schema da tabela (`id BIGINT, produto VARCHAR, valor DOUBLE, dia DATE`).
3. Como e um `INSERT INTO` (append), chama `insert_into` do provider Iceberg. Ele faz um `load_table` **fresco** no Lakekeeper, para partir do snapshot atual. O provider so aceita append: overwrite e delete nao sao suportados no iceberg-rust 0.10.

A resposta do `load_table` traz o `metadata-location` atual e a configuracao de acesso ao storage. Para esta tabela, ela incluiu:

```text
s3.endpoint=http://rustfs:9000/   s3.path-style-access=true   s3.region=local-01
s3.remote-signing-enabled=true    s3.signer.uri=...           (campos de remote signing para clientes externos)
```

O worker usa endpoint e regiao e assina as requisicoes S3 com a chave restrita ao bucket que recebeu por variavel de ambiente (`LAKE_S3_ACCESS_KEY_ID`). O FileIO e o `OpenDalStorageFactory::S3`.

### P5: gravacao dos arquivos (RustFS)

`collect()` executa o plano. O writer Iceberg grava no bucket `lake`, sob o diretorio da tabela `lake/01a11913-7716-7590-894d-74167765a85b/`:

| Arquivo novo | Tamanho | O que e |
|---|---|---|
| `data/01a11b7f-4bb4-...-00000.parquet` | 1,67 KiB | as 4 linhas novas, em Parquet |
| `metadata/01a11b7f-4bc2-...-m0.avro` | 3,61 KiB | *manifest*: lista o arquivo Parquet novo, com contagem de linhas e estatisticas por coluna |
| `metadata/snap-942438293836199047-0-....avro` | 1,71 KiB | *manifest list* do novo snapshot: aponta para os manifests validos (o antigo e o novo) |

Os arquivos antigos (`data/01a11924-...parquet` com as 5 vendas originais, manifest e manifest list do snapshot anterior) **nao sao alterados**. O Iceberg so acrescenta arquivos.

### P6: commit do snapshot (Lakekeeper + RustFS)

Com os arquivos gravados, o worker envia um *commit* ao Lakekeeper (`POST .../tables/vendas`) com:

- **requisito:** o snapshot atual ainda e `2671366834670874304`. Se outro processo tivesse commitado antes, o commit seria recusado, o que da controle de concorrencia otimista;
- **atualizacao:** adicionar o snapshot `942438293836199047` e torna-lo o atual.

O Lakekeeper valida o requisito, grava um novo arquivo de metadados `metadata/00002-01a11b7f-...gz.metadata.json` (712 B) no RustFS, usando a credencial que guarda cifrada, e atualiza no banco `catalog` o ponteiro da tabela. **Esse ponteiro e o que torna o dado visivel.** Antes do commit, os arquivos de P5 existem no bucket, mas nenhum leitor os enxerga.

Estado da tabela depois do commit, como o Lakekeeper informa:

```text
metadata-location:   s3://lake/lake/01a11913-.../metadata/00002-01a11b7f-...gz.metadata.json
current-snapshot-id: 942438293836199047
snapshot 2671366834670874304  parent None                 append  added-records=5  total-records=5  total-data-files=1
snapshot 942438293836199047   parent 2671366834670874304  append  added-records=4  total-records=9  total-data-files=2
metadata-log: 2 versoes anteriores (00000 = tabela vazia, 00001 = carga inicial)
```

O DataFusion devolve 1 linha com a contagem inserida, que o worker registra como `statement done {statement:0, rows:1}`.

### P7: o worker registra o resultado (Postgres)

`run_job` retorna `Ok` e `finish_job` executa o `DONE_SQL` com fencing:

```sql
UPDATE jobs.job_queue SET status = 'done', finished_at = now(), last_error = NULL
 WHERE id = 14 AND attempts = 1 AND status = 'running';
```

A condicao `attempts = 1` garante que so o dono atual do job grava o resultado. Se o reaper tivesse devolvido o job para a fila e outro worker o tivesse pego (`attempts = 2`), este `UPDATE` nao encontraria a linha.

Linha final:

```text
id           | 14
kind         | sql
status       | done
attempts     | 1  (de max_attempts 3)
available_at | 12:31:27.877722
claimed_at   | 12:31:29.701610
heartbeat_at | 12:31:29.701610
claimed_by   | 45cc0336be03
finished_at  | 12:31:29.748152
last_error   | (vazio)
```

Logs do worker no mesmo intervalo:

```text
12:31:29.704 INFO running job     {id:14, kind:sql, attempt:1}
12:31:29.747 INFO statement done  {id:14, statement:0, rows:1}
12:31:29.748 INFO job done        {id:14, kind:sql}
```

Em seguida, o worker volta direto ao claim. Com a fila vazia, ele loga `idle` e dorme 5 s.

**Se algo tivesse dado errado:**

- **Erro no `INSERT`** (RustFS fora do ar, por exemplo): o job voltaria para `queued` com `available_at = now() + 30 s` e `last_error` preenchido, e seria tentado de novo ate 3 vezes.
- **Commit recusado por concorrencia:** mesmo caminho do erro acima. A nova tentativa parte do snapshot atualizado.
- **Worker morto entre P5 e P6:** os arquivos de P5 ficariam orfaos no bucket, invisiveis porque nao houve commit. Depois de 60 s sem heartbeat, o reaper devolveria o job para a fila e a nova tentativa gravaria arquivos novos. A limpeza dos orfaos e uma das manutencoes ainda pendentes.
- **Worker morto entre P6 e P7:** o dado ja estaria commitado, mas o job seria repetido, e as 4 vendas entrariam **duas vezes**. Por isso os jobs precisam ser idempotentes. Uma carga real deveria, por exemplo, gravar apenas os ids que ainda nao existem (`INSERT ... SELECT ... WHERE id NOT IN (...)`).

### P8: consulta "depois" (`lake-query`)

O cliente enviou a mesma consulta de P0, cerca de 0,7 s depois do `done`, sem reiniciar nenhum servico:

1. **Autenticacao:** o interceptor confere `authorization: Bearer <LAKE_QUERY_TOKEN>`.
2. **Catalogo:** `refresh_lake` so recarrega a listagem se ela tiver mais de 30 s. A tabela ja existia, entao nao e preciso recarregar a lista. Mesmo que fosse uma tabela nova, o refresh-on-miss a encontraria.
3. **Planejamento somente leitura:** `sql_with_options` com DDL, DML e statements proibidos. A consulta e um `SELECT`, entao passa.
4. **`GetFlightInfo`:** devolve o schema do resultado e um ticket com o SQL.
5. **`DoGet`:** replaneja e executa. O scan do `IcebergTableProvider` faz um `load_table` **fresco** no Lakekeeper e recebe o snapshot `942438293836199047`. Le a manifest list e os dois manifests, decide quais Parquet ler (os dois) e os le no RustFS. O `GROUP BY` roda no DataFusion e o resultado volta em streaming Arrow.

```text
{'dia': 2026-10-01, 'vendas': 2, 'total': 15.5}
{'dia': 2026-10-02, 'vendas': 3, 'total': 22.7}
{'dia': 2026-10-08, 'vendas': 4, 'total': 53.4}   <- carga do evento
```

O dado ficou visivel sem nenhum cache a invalidar, porque cada scan parte do snapshot atual.

### Historico de versoes da tabela (`vendas$snapshots`)

Cada commit cria um snapshot. A tabela de metadados mostra o historico:

```sql
SELECT * FROM lake.raw."vendas$snapshots"
```

```text
committed_at 2026-10-08 01:32:42.984  snapshot_id 2671366834670874304  parent_id None                 operation append  added-records 5  total-records 5
committed_at 2026-10-08 12:31:29.736  snapshot_id 942438293836199047   parent_id 2671366834670874304  operation append  added-records 4  total-records 9
```

Apenas `SELECT *` funciona nessa tabela. Ao pedir colunas especificas (`SELECT snapshot_id, operation ...`) ou `count(*)`, o DataFusion devolve erro interno, por causa de um bug do `iceberg-datafusion` 0.10.1 registrado nas [lacunas](#lacunas-e-riscos-observados). O problema apareceu justamente durante este evento.

### Como reproduzir

```bash
make psql
```

```sql
insert into jobs.job_queue(kind, payload) values ('sql',
  '{"sql": "INSERT INTO lake.raw.vendas VALUES (10, ''cha'', 7.0, DATE ''2026-10-09'')"}')
returning id;

select id, status, attempts, claimed_by, claimed_at, finished_at, last_error
  from jobs.job_queue order by id desc limit 1;
```

Em outro terminal, `make logs SERVICE=lake-worker` mostra `running job` -> `statement done` -> `job done`. Depois, consulte via Flight SQL (veja [Como verificar cada etapa](#como-verificar-cada-etapa)).

---

## Lacunas e riscos observados

Pontos encontrados ao ler o codigo e operar a pilha em 2026-10-07, com o que foi feito em cada um.

### Pendentes (dependem de decisao)

| # | Lacuna | Por que nao foi feito | Proximo passo sugerido |
|---|---|---|---|
| 9 | Lakekeeper `allow-all`, sem OpenID. | Exige um provedor de identidade (por exemplo, Keycloak ou o IdP corporativo) e um novo bootstrap do catalogo. Pela politica, mudar controles de acesso precisa da aprovacao da Seguranca da Informacao. | Definir o IdP com SI. Depois, configurar `LAKEKEEPER__OPENID_PROVIDER_URI` / `LAKEKEEPER__OPENID_AUDIENCE` e o fluxo client-credentials nos servicos Rust. |
| 10 (resto) | Sem compaction, expiracao de snapshots ou limpeza de orfaos; backup sem agendamento nem copia off-host. | O iceberg-rust 0.10 nao implementa essas operacoes. Destino e agenda de backup sao decisoes de infraestrutura. | Avaliar um job de manutencao com PyIceberg ou Spark, ou aguardar suporte no iceberg-rust. Agendar `make backup` e enviar a copia para fora do host. |
| 7 (resto) | Flight SQL sem TLS, sem usuarios individuais e sem prepared statements. | Aceitavel enquanto a porta 50051 estiver em localhost. | TLS e identidade por usuario entram junto com o IdP do item 9. |
| - | Tabelas de metadados Iceberg (`t$snapshots`, `t$manifests`) so funcionam com `SELECT *`. Com `count(*)` ou escolhendo colunas, o DataFusion da erro interno (`Physical input schema should be the same...`). | Bug do `iceberg-datafusion` 0.10.1: o `scan` dessas tabelas ignora a projecao (`_projection`). Encontrado em 2026-10-08. | Usar `SELECT *` e filtrar no cliente, ou envolver o provider aplicando a projecao no `lake-query`. Vale reportar ao projeto iceberg-rust. |
| - | `stop_grace_period` de 2 min versus jobs longos. | Um job com mais de 2 min e morto num deploy e repetido (at-least-once). | Garantir que os jobs sao idempotentes, ou aumentar o grace period para os kinds pesados. |

---

## Como verificar cada etapa

```bash
make ps                                   # estado de todos os servicos
make logs SERVICE=lake-worker             # logs ao vivo (Ctrl+C para sair)
docker compose logs storage-init          # saida dos one-shots
docker compose logs lakekeeper-init

curl -s http://127.0.0.1:9000/health                         # RustFS
curl -s http://127.0.0.1:8181/management/v1/info             # Lakekeeper (bootstrapped: true)
curl -s http://127.0.0.1:8181/management/v1/warehouse        # warehouse "lake"
make psql                                                    # banco jobs
make psql DB=catalog                                         # banco catalog
```

Flight SQL (ADBC, Python): `adbc_driver_flightsql.dbapi.connect("grpc://127.0.0.1:50051", db_kwargs={"adbc.flight.sql.authorization_header": "Bearer <LAKE_QUERY_TOKEN>"})`. Em ferramentas JDBC, use qualquer usuario e o token como senha.

Interfaces web (somente localhost):

- Console RustFS: <http://127.0.0.1:9001/rustfs/console/> (login com `RUSTFS_ROOT_USER` / `RUSTFS_ROOT_PASSWORD` do `.env`)
- UI do Lakekeeper: <http://127.0.0.1:8181/ui/>

Para validar tudo de forma isolada, sem tocar na pilha atual: `make smoke`. Ele usa outro projeto Compose e outras portas.

Para derrubar:

```bash
make down                        # para tudo, mantem dados
make down-volumes CONFIRM=yes    # para e APAGA bucket e bancos
```
