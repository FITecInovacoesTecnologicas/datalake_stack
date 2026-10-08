#!/usr/bin/env bash
# check-assessment-test.sh — self-test for scripts/check-assessment.py.
# Builds a minimal conformant fixture set under tests/.tmp/, asserts every
# subcommand passes, then applies one mutation per rule family and asserts
# each is rejected with a VIOLATION line. Removes only its own scratch dir.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
CHECK="python3 scripts/check-assessment.py"
mkdir -p tests/.tmp
SCRATCH="$(mktemp -d tests/.tmp/XXXXXX)"
MUTATED="$(mktemp -d tests/.tmp/XXXXXX)"
trap 'rm -rf "$SCRATCH" "$MUTATED"; rmdir tests/.tmp 2>/dev/null || true' EXIT
mkdir -p "$SCRATCH/evidence"

cat > "$SCRATCH/evidence/compose-facts.json" <<'JSON'
{"services": ["alpha", "beta"], "missing_build_contexts": ["ctx-a", "ctx-b"], "misplaced_init_scripts": ["00-x.sh"]}
JSON

cat > "$SCRATCH/01-compose-review.md" <<'MD'
# Review

## Inventario de servicos

| Servico | Papel |
|---|---|
| alpha | a |
| beta | b |

## Premissas

- Premissa: enxuta | Veredito: atende | Justificativa: ok
- Premissa: pequena | Veredito: parcial | Justificativa: ok
- Premissa: rapida | Veredito: atende | Justificativa: ok
- Premissa: facil-deploy | Veredito: nao-atende | Justificativa: ok

## Achados

### F-1 Contextos ausentes
Severidade: P0
Evidencia: docker-compose.yml:1
Faltam ctx-a e ctx-b; 00-x.sh esta fora do lugar.

### F-2 Menor
Severidade: P2
Evidencia: env.example
Texto.
MD

cat > "$SCRATCH/02-datalake-fit.md" <<'MD'
# Fit

## Capacidades

| Capacidade | Status | Evidencia | Comentario |
|---|---|---|---|
| armazenamento-objetos | presente | docker-compose.yml | x |
| formato-tabela | presente | docker-compose.yml | x |
| catalogo | presente | docker-compose.yml | x |
| ingestao | ausente | n/a | x |
| processamento | placeholder | docker-compose.yml | x |
| consulta-sql | placeholder | docker-compose.yml | x |
| orquestracao | placeholder | docker-compose.yml | x |
| segredos | presente | env.example | x |
| controle-acesso | ausente | n/a | x |
| observabilidade | ausente | n/a | x |
| backup-recuperacao | placeholder | docker-compose.yml | x |
| qualidade-dados | ausente | n/a | x |
| governanca-dados | ausente | n/a | x |

## Veredito

Veredito geral: parcial
MD

cat > "$SCRATCH/evidence/claims-register.md" <<'MD'
# Claims

| ID | Afirmacao | Status | Fonte | Data |
|---|---|---|---|---|
| C-1 | a | verificado | https://example.org/one | 2026-10-07 |
| C-2 | b | reportado | n/a | 2026-10-07 |
| C-3 | c | refutado | https://example.org/three | 2026-10-07 |
| C-4 | d | reportado | https://example.org/four | 2026-10-07 |
| C-5 | e | reportado | n/a | 2026-10-07 |
| C-6 | f | reportado | n/a | 2026-10-07 |
| C-7 | g | reportado | n/a | 2026-10-07 |
| C-8 | h | reportado | n/a | 2026-10-07 |
| C-9 | i | reportado | n/a | 2026-10-07 |
| C-10 | j | reportado | n/a | 2026-10-07 |
| C-11 | k | reportado | n/a | 2026-10-07 |
| C-12 | l | reportado | n/a | 2026-10-07 |
| C-13 | m | reportado | n/a | 2026-10-07 |
| C-14 | n | reportado | n/a | 2026-10-07 |
MD

cat > "$SCRATCH/evidence/authoring-evidence.json" <<'JSON'
{"contract": "AuthoringEvidence/v1", "provider": "test", "request_id": "r1", "query": "q",
 "observed_at": "2026-10-07T00:00:00Z", "state": "ok",
 "sources": [{"url": "https://example.org/one", "title": "one", "retrieved_at": "2026-10-07T00:00:00Z",
              "content_digest": "0000000000000000000000000000000000000000000000000000000000000000"}],
 "claims": [{"text": "t", "source_refs": [0]}]}
JSON

cat > "$SCRATCH/03-recommendations.md" <<'MD'
# Recs

### R-1 Criar contextos
Prioridade: P0
Tipo: correcao
Aderencia: alta
Fonte: docker-compose.yml
Criar ctx-a, ctx-b e mover 00-x.sh.

### R-2 Ferramenta X
Prioridade: P2
Tipo: ferramenta
Aderencia: media
Fonte: https://example.org/one
Texto.

### R-3 Via claim
Prioridade: P1
Tipo: melhoria
Aderencia: baixa
Fonte: C-3
Texto.

## Decisoes em aberto

- D1 ...
- D2 ...
- D3 ...
MD

cat > "$SCRATCH/README.md" <<'MD'
# Resumo

Links: [review](01-compose-review.md) [fit](02-datalake-fit.md) [recs](03-recommendations.md) [claims](evidence/claims-register.md)

## Vereditos

- Premissa: enxuta | Veredito: atende | Justificativa: ok
- Premissa: pequena | Veredito: parcial | Justificativa: ok
- Premissa: rapida | Veredito: atende | Justificativa: ok
- Premissa: facil-deploy | Veredito: nao-atende | Justificativa: ok

Veredito geral: parcial

## Proximos passos

- R-1 primeiro.
MD

pass_count=0; fail_count=0
expect_pass() { # name, cmd...
  local name="$1"; shift
  if out="$("$@" 2>&1)"; then pass_count=$((pass_count+1)); else echo "FAIL (expected pass) $name"; echo "$out"; exit 1; fi
}
expect_fail() { # name, cmd...
  local name="$1"; shift
  if out="$("$@" 2>&1)"; then echo "FAIL (expected rejection) $name"; echo "$out"; exit 1; fi
  if ! grep -q '^VIOLATION:' <<<"$out"; then echo "FAIL (no VIOLATION line) $name"; echo "$out"; exit 1; fi
  fail_count=$((fail_count+1))
}

S="$SCRATCH"
expect_pass review  $CHECK review "$S/01-compose-review.md" --facts "$S/evidence/compose-facts.json"
expect_pass fit     $CHECK fit "$S/02-datalake-fit.md"
expect_pass claims  $CHECK claims "$S/evidence/claims-register.md" --evidence "$S/evidence/authoring-evidence.json"
expect_pass recs    $CHECK recs "$S/03-recommendations.md" --facts "$S/evidence/compose-facts.json" --claims "$S/evidence/claims-register.md"
expect_pass summary $CHECK summary "$S/README.md" --review "$S/01-compose-review.md" --fit "$S/02-datalake-fit.md" --recs "$S/03-recommendations.md"
expect_pass all     $CHECK all "$S"

# Mutations: each copies the fixture, breaks one rule, expects rejection.
# The copy lives outside $S: copying a directory into itself fails.
M="$MUTATED"; cp -R "$S/." "$M"
grep -v 'Premissa: rapida' "$S/01-compose-review.md" > "$M/01-compose-review.md"
expect_fail "review: missing premise" $CHECK review "$M/01-compose-review.md" --facts "$S/evidence/compose-facts.json"

grep -v '^| catalogo ' "$S/02-datalake-fit.md" > "$M/02-datalake-fit.md"
expect_fail "fit: missing capability row" $CHECK fit "$M/02-datalake-fit.md"

sed 's/ctx-b e mover/e mover/' "$S/03-recommendations.md" > "$M/03-recommendations.md"
expect_fail "recs: P0 gap not covered" $CHECK recs "$M/03-recommendations.md" --facts "$S/evidence/compose-facts.json" --claims "$S/evidence/claims-register.md"

sed 's#| C-2 | b | reportado | n/a |#| C-2 | b | verificado | n/a |#' "$S/evidence/claims-register.md" > "$M/evidence/claims-register.md"
expect_fail "claims: verificado without URL" $CHECK claims "$M/evidence/claims-register.md" --evidence "$S/evidence/authoring-evidence.json"

sed 's/Premissa: enxuta | Veredito: atende/Premissa: enxuta | Veredito: parcial/' "$S/README.md" > "$M/README.md"
expect_fail "summary: verdict mismatch" $CHECK summary "$M/README.md" --review "$S/01-compose-review.md" --fit "$S/02-datalake-fit.md" --recs "$S/03-recommendations.md"

{ cat "$S/03-recommendations.md"; printf '\n### R-4 Fonte desconhecida\nPrioridade: P1\nTipo: melhoria\nAderencia: alta\nFonte: https://example.org/not-in-register\nTexto.\n'; } > "$M/03-recommendations.md"
expect_fail "recs: URL not in register" $CHECK recs "$M/03-recommendations.md" --facts "$S/evidence/compose-facts.json" --claims "$S/evidence/claims-register.md"

echo "CHECK_ASSESSMENT_SELFTEST=OK passes=$pass_count rejections=$fail_count"
