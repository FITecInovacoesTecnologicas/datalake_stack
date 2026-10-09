# Como criar um novo tipo de job no lake-worker

Este guia e para quem vai colocar logica de um projeto no `lake-worker`. O resto da pilha (RustFS, Lakekeeper, Postgres, `lake-query`) e agnostico e nao muda.

## Sumario

1. [Primeiro: precisa mesmo de codigo?](#1-primeiro-precisa-mesmo-de-codigo)
2. [Onde fica cada coisa](#2-onde-fica-cada-coisa)
3. [O contrato de um kind](#3-o-contrato-de-um-kind)
4. [Passo a passo](#4-passo-a-passo)
5. [Testar](#5-testar)
6. [Checklist de revisao](#6-checklist-de-revisao)
7. [Referencia: `ingest_http`](#7-referencia-ingest_http)
8. [Exemplo completo: dolar PTAX (BCB)](#8-exemplo-completo-dolar-ptax-bcb)

---

## 1. Primeiro: precisa mesmo de codigo?

| Necessidade | Use | Codigo Rust? |
|---|---|---|
| Transformar tabelas do lake (raw -> clean -> curated) com SQL | kind `sql` | Nao |
| Ler JSON de uma API HTTPS publica ou interna | kind `ingest_http` | Nao |
| Ler outra fonte (banco, arquivo, fila, API com autenticacao, paginacao) | kind novo | Sim |
| Logica que nao cabe em SQL do DataFusion | kind novo | Sim |

Os kinds `sql` e `ingest_http` sao configurados so pelo `payload`. Um projeto que cabe neles e apenas SQL versionado em `pipelines/` (veja o [exemplo](#8-exemplo-completo-dolar-ptax-bcb)).

## 2. Onde fica cada coisa

```text
lake-worker/src/
  main.rs           runtime da fila: claim, heartbeat, retry, reaper. Nao muda ao criar um kind.
  jobs/
    mod.rs          contrato (trait JobKind, JobContext) e registro (dispatch, KINDS)
    noop.rs         kind noop / smoke
    sql.rs          kind sql
    ingest_http.rs  kind ingest_http
  lake.rs           catalogo lake: TableName, create_table, append_batch, query_count
  records.rs        JSON -> Arrow RecordBatch (inferencia de tipos)
  http_source.rs    cliente HTTPS com allowlist de hosts e limite de tamanho
pipelines/          SQL versionado que enfileira e agenda jobs de cada projeto
```

Um kind novo e **um arquivo em `jobs/`** mais **tres linhas em `jobs/mod.rs`**.

## 3. O contrato de um kind

```rust
pub trait JobKind {
    const KIND: &'static str;          // valor de jobs.job_queue.kind
    type Payload: DeserializeOwned;    // formato de jobs.job_queue.payload
    async fn run(cx: &JobContext<'_>, payload: Self::Payload) -> Result<()>;
}
```

O runtime ja faz o parse do `payload` para o tipo `Payload`. Se o JSON nao bater, o job falha na hora com `invalid payload for kind "...": ...`, sem retry.

**O que o `run` recebe em `cx`:**

| Campo / metodo | Para que serve |
|---|---|
| `cx.id` | id do job na fila. Igual em todas as tentativas: use como chave de idempotencia. |
| `cx.attempt` | 1 na primeira execucao, 2 no primeiro retry, e assim por diante. |
| `cx.lake().await?` | catalogo `lake` atualizado (ve tabelas criadas por outros jobs). Da `create_table` e `table_schema`. |
| `cx.session` | sessao DataFusion com o catalogo registrado como `lake`. |
| `cx.sql("...").await?` | executa uma instrucao SQL e devolve o resultado (`Vec<RecordBatch>`). Para um `INSERT`, `lake::written_rows` da o numero de linhas gravadas. |
| `cx.shared.http` | cliente HTTPS com allowlist (`get_json(url)`). |

**O que o `run` devolve:**

| Retorno | Status na fila |
|---|---|
| `Ok(())` | `done` |
| `Err(permanent("..."))` | `failed` na hora. Use para entrada invalida que um retry nao conserta. |
| qualquer outro `Err` | `queued` de novo, com backoff exponencial, ate `max_attempts`. Depois, `failed`. |

A mensagem do erro vai para `jobs.job_queue.last_error`.

**Idempotencia e obrigatoria.** A entrega e *at-least-once*: um job pode rodar de novo apos um crash, um deploy (o grace period e de 2 min) ou um lease perdido. Estrategias que funcionam aqui:

- Gravar `cx.id` numa coluna (`_job_id`) e, antes de gravar, checar se ja existem linhas com esse id. Um append Iceberg e atomico: ou todas as linhas do job estao la, ou nenhuma. E o que o `ingest_http` faz.
- `CREATE ... IF NOT EXISTS`.
- `INSERT ... SELECT ... WHERE chave NOT IN (SELECT chave FROM destino)`.

O iceberg-rust 0.10 so faz **append**: nao ha `UPDATE`, `DELETE`, `MERGE` nem overwrite.

## 4. Passo a passo

Exemplo: um kind `ingest_exemplo` que le uma fonte e grava em `lake.raw.<tabela>`.

**1. Crie `lake-worker/src/jobs/ingest_exemplo.rs`:**

```rust
//! `ingest_exemplo`: <o que faz, em uma frase>.
//!
//! Payload:
//! ```json
//! {"table": "raw.exemplo", "parametro": "valor"}
//! ```

use anyhow::Result;
use serde::Deserialize;
use tracing::info;

use super::{permanent, JobContext, JobKind};
use crate::lake::TableName;

pub struct IngestExemplo;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)] // campo com erro de digitacao falha o job em vez de ser ignorado
pub struct Payload {
    table: String,
    parametro: String,
    #[serde(default)]
    opcional: Option<u32>,
}

impl JobKind for IngestExemplo {
    const KIND: &'static str = "ingest_exemplo";
    type Payload = Payload;

    async fn run(cx: &JobContext<'_>, payload: Payload) -> Result<()> {
        let table = TableName::parse(&payload.table).map_err(permanent)?;
        let lake = cx.lake().await?;
        // 1. idempotencia: este job (cx.id) ja gravou?
        // 2. buscar os dados
        // 3. converter (records::to_batch para JSON) e criar a tabela se faltar
        // 4. gravar com lake::append_batch (um commit atomico)
        info!(id = cx.id, table = %table, "ingest_exemplo done");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Teste o parse do payload e as funcoes puras do kind.
}
```

Use [jobs/ingest_http.rs](../lake-worker/src/jobs/ingest_http.rs) como referencia completa.

**2. Registre em [jobs/mod.rs](../lake-worker/src/jobs/mod.rs):**

```rust
mod ingest_exemplo;                                   // 1. modulo
pub use ingest_exemplo::IngestExemplo;

pub const KINDS: &[&str] = &[..., IngestExemplo::KIND];             // 2. lista (log de startup)

match kind {
    ...
    IngestExemplo::KIND => run::<IngestExemplo>(cx, payload).await, // 3. dispatch
```

**3. Se precisar de configuracao**, leia do ambiente uma vez, no startup: adicione um campo em `Shared` (`jobs/mod.rs`), a variavel no `docker-compose.yml` (servico `lake-worker`) e no `env.example`. **Segredos nunca vao no payload:** ele fica gravado em `jobs.job_queue` e aparece nos logs.

**4. Se precisar de dependencia nova**, adicione em [lake-worker/Cargo.toml](../lake-worker/Cargo.toml) e atualize o `Cargo.lock` (veja [Testar](#5-testar)). O build da imagem usa `--locked`.

**5. Versione o uso do kind** em `pipelines/<projeto>.sql` (enfileiramento e `cron.schedule`) e vincule ao item do Jira.

## 5. Testar

Nao e preciso Rust no host. Testes unitarios em um container (o cache de dependencias fica em volumes Docker):

```bash
docker run --rm -v "$PWD":/app -w /app \
  -v rdl-cargo-registry:/usr/local/cargo/registry -v rdl-target:/app/target \
  rust:1.99-bookworm cargo test -p lake-worker
```

Sem `--locked`, esse comando tambem atualiza o `Cargo.lock` quando uma dependencia muda. O arquivo sai com dono root: rode `sudo chown $USER Cargo.lock` depois.

Na pilha:

```bash
docker compose up -d --build lake-worker      # recompila e sobe o worker
make logs SERVICE=lake-worker                 # o log de startup lista os kinds registrados
make psql
```

```sql
INSERT INTO jobs.job_queue (kind, payload)
VALUES ('ingest_exemplo', '{"table": "raw.exemplo", "parametro": "x"}') RETURNING id;

SELECT id, status, attempts, last_error FROM jobs.job_queue ORDER BY id DESC LIMIT 5;
```

**Teste a idempotencia:** com o job `done`, devolva-o para a fila. Ele deve terminar `done` de novo, sem duplicar dados.

```sql
UPDATE jobs.job_queue SET status = 'queued', available_at = now() WHERE id = <id>;
```

## 6. Checklist de revisao

- [ ] `Payload` com `#[serde(deny_unknown_fields)]` e documentado no topo do arquivo.
- [ ] Rodar duas vezes o mesmo job nao duplica nem corrompe dados.
- [ ] Entrada invalida devolve `permanent(...)`; falha transitoria (rede, 5xx, Lakekeeper fora) devolve erro comum.
- [ ] Nomes de tabela passam por `TableName::parse`; nada do payload e concatenado em SQL sem validacao.
- [ ] Nenhum segredo no payload nem nos logs.
- [ ] Acesso externo passa pelo `HttpSource` (ou por uma allowlist equivalente).
- [ ] Testes unitarios do parse do payload e das funcoes puras.
- [ ] Job grande cabe em 2 min (grace period de deploy) ou e seguro de repetir.
- [ ] Kind registrado em `dispatch` e em `KINDS`.
- [ ] Uso versionado em `pipelines/` e vinculado ao Jira.

## 7. Referencia: `ingest_http`

Le JSON de uma API HTTPS e faz append numa tabela Iceberg da camada raw.

```json
{
  "url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados/ultimos/20?formato=json",
  "table": "raw.bcb_dolar",
  "records_path": ""
}
```

| Campo | Obrigatorio | Descricao |
|---|---|---|
| `url` | sim | `https://` num host de `INGEST_HTTP_ALLOWED_HOSTS`. |
| `table` | sim | `<namespace>.<tabela>` no catalogo `lake`, so `[a-z0-9_]`. |
| `records_path` | nao | JSON Pointer ate os registros, por exemplo `/data/items`. Vazio = a resposta inteira. O alvo deve ser uma lista de objetos ou um objeto (uma linha). |

**O que acontece:**

1. Se a tabela existe e ja tem linhas com `_job_id = <id do job>`, o job termina `done` sem buscar nada. E uma tentativa anterior que ja tinha gravado.
2. `GET` na URL. Ha um limite de tamanho (`INGEST_HTTP_MAX_BYTES`, 32 MiB) e um timeout (`INGEST_HTTP_TIMEOUT_SECS`, 60 s). Redirects so sao seguidos para hosts da allowlist.
3. Cada chave de primeiro nivel vira uma coluna, com nome normalizado (`Data Ref.` vira `data_ref_`). Os tipos sao inferidos: so booleanos viram `BOOLEAN`, so inteiros `BIGINT`, inteiros e decimais `DOUBLE`, e o resto `VARCHAR`. Objetos e listas aninhados viram texto JSON.
4. Na primeira carga, cria o namespace e a tabela com esses tipos, mais `_job_id BIGINT`, `_ingested_at TIMESTAMPTZ` e `_source_url VARCHAR`.
5. Faz um unico append (commit Iceberg atomico). Colunas da fonte que nao existem na tabela sao ignoradas, com um aviso no log. Colunas da tabela que faltam na fonte ficam `NULL`.

**Schema fixo:** para tipos exatos (por exemplo `DATE`), crie a tabela antes com um job `sql`, incluindo a coluna `_job_id BIGINT`. O `ingest_http` respeita a tabela existente e o DataFusion converte cada valor para o tipo dela.

**Erros:**

| Situacao | Resultado |
|---|---|
| URL invalida, `http://`, host fora da allowlist | `failed` na hora |
| HTTP 4xx (exceto 408 e 429), corpo acima do limite, `records_path` inexistente | `failed` na hora |
| Rede, timeout, HTTP 5xx, 408, 429, corpo que nao e JSON | retry com backoff |

**Seguranca.** O worker esta na mesma rede que Postgres, RustFS e Lakekeeper. Por isso so aceita `https://` e so para hosts listados em `INGEST_HTTP_ALLOWED_HOSTS`. Uma URL vinda do payload nao consegue chegar aos servicos internos. Com a allowlist vazia, nenhuma fonte externa e permitida. Proxy corporativo: defina `HTTPS_PROXY` no ambiente do `lake-worker`.

**Fora do escopo:** autenticacao (tokens, API keys), paginacao e corpo de requisicao (POST). Cada um desses pede um kind proprio ou uma extensao com os segredos lidos do ambiente, nunca do payload.

## 8. Exemplo completo: dolar PTAX (BCB)

O arquivo [pipelines/exemplo-ptax.sql](../pipelines/exemplo-ptax.sql) monta um pipeline com a API publica do Banco Central:

1. **raw:** um job `ingest_http` grava os ultimos 20 valores da serie em `lake.raw.bcb_dolar`, com as colunas `data` e `valor` como texto, mais os metadados.
2. **clean:** um job `sql` cria `lake.clean.dolar_ptax (dia DATE, cotacao DOUBLE)`. Ele converte os tipos, fica com o valor da carga mais recente de cada dia e so insere dias novos.
3. **agendamento:** dois `cron.schedule`, a carga as 22:00 UTC e a limpeza as 22:15 UTC, de segunda a sexta.

```bash
# 1. api.bcb.gov.br precisa estar em INGEST_HTTP_ALLOWED_HOSTS no .env
docker compose up -d lake-worker
# 2. enfileira, agenda e acompanha
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d jobs' < pipelines/exemplo-ptax.sql
make logs SERVICE=lake-worker
```

O log do worker mostra `created iceberg table`, `ingested records {rows:20}` e, no job seguinte, `statement done {statement:2, written:20}`. Rodar a limpeza de novo grava `written:0`. A consulta pelo `lake-query`:

```sql
SELECT * FROM lake.clean.dolar_ptax ORDER BY dia DESC;
```
