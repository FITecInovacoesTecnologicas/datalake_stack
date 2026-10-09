-- Pipeline de teste: cotacao do dolar (API publica do Banco Central) -> raw -> clean.
--
-- Fonte: SGS/BCB, serie 1 (dolar americano, venda, diario), sem autenticacao.
--   https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados/ultimos/20?formato=json
--   resposta: [{"data":"08/10/2026","valor":"5.0119"}, ...]
--
-- Pre-requisito: api.bcb.gov.br em INGEST_HTTP_ALLOWED_HOSTS (.env) e o worker recriado:
--   docker compose up -d lake-worker
--
-- Rodar no banco jobs:
--   docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d jobs' < pipelines/exemplo-ptax.sql
--
-- A fila nao tem dependencia entre jobs: com varias replicas, dois jobs enfileirados juntos
-- podem rodar em paralelo. Por isso o clean entra com available_at alguns minutos depois.

-- 1. raw: ultimos 20 dias uteis da API (a API aceita no maximo 20) em lake.raw.bcb_dolar (a tabela nasce na primeira carga).
--    Cada carga leva _job_id, _ingested_at e _source_url; o raw guarda todas as cargas.
INSERT INTO jobs.job_queue (kind, payload) VALUES ('ingest_http', '{
  "url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados/ultimos/20?formato=json",
  "table": "raw.bcb_dolar"
}');

-- 2. clean: um registro por dia, tipado, com o valor da carga mais recente.
--    Idempotente: so insere dias que ainda nao estao em lake.clean.dolar_ptax.
INSERT INTO jobs.job_queue (kind, payload, available_at) VALUES ('sql', '{"sql": [
  "CREATE SCHEMA IF NOT EXISTS lake.clean",
  "CREATE TABLE IF NOT EXISTS lake.clean.dolar_ptax (dia DATE, cotacao DOUBLE)",
  "INSERT INTO lake.clean.dolar_ptax SELECT dia, cotacao FROM (SELECT to_date(data, ''%d/%m/%Y'') AS dia, CAST(valor AS DOUBLE) AS cotacao, row_number() OVER (PARTITION BY data ORDER BY _ingested_at DESC) AS rn FROM lake.raw.bcb_dolar) s WHERE rn = 1 AND dia NOT IN (SELECT dia FROM lake.clean.dolar_ptax)"
]}', now() + interval '2 minutes');

-- 3. Agendamento (pg_cron, horario UTC): carga as 22:00 e limpeza as 22:15, dias uteis.
--    cron.schedule com o mesmo nome substitui o agendamento anterior.
SELECT cron.schedule('ptax-raw', '0 22 * * 1-5', $job$
  INSERT INTO jobs.job_queue (kind, payload) VALUES ('ingest_http',
    '{"url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados/ultimos/20?formato=json", "table": "raw.bcb_dolar"}')
$job$);
SELECT cron.schedule('ptax-clean', '15 22 * * 1-5', $job$
  INSERT INTO jobs.job_queue (kind, payload) VALUES ('sql', '{"sql": [
    "CREATE SCHEMA IF NOT EXISTS lake.clean",
    "CREATE TABLE IF NOT EXISTS lake.clean.dolar_ptax (dia DATE, cotacao DOUBLE)",
    "INSERT INTO lake.clean.dolar_ptax SELECT dia, cotacao FROM (SELECT to_date(data, ''%d/%m/%Y'') AS dia, CAST(valor AS DOUBLE) AS cotacao, row_number() OVER (PARTITION BY data ORDER BY _ingested_at DESC) AS rn FROM lake.raw.bcb_dolar) s WHERE rn = 1 AND dia NOT IN (SELECT dia FROM lake.clean.dolar_ptax)"
  ]}')
$job$);

-- Acompanhar:
--   SELECT id, kind, status, attempts, last_error FROM jobs.job_queue ORDER BY id DESC LIMIT 5;
-- Consultar (lake-query, Flight SQL):
--   SELECT * FROM lake.clean.dolar_ptax ORDER BY dia DESC;
-- Remover os agendamentos:
--   SELECT cron.unschedule('ptax-raw'); SELECT cron.unschedule('ptax-clean');
