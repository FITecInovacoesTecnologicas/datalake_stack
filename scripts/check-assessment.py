#!/usr/bin/env python3
"""check-assessment.py — enforce the docs/assessment document contract.

Contract: tasks/.plans/datalake-stack-assessment/document-contract.md
Subcommands: review, fit, recs, claims, summary, all.
Exit 0 and print CHECK_<KIND>=OK on success; print VIOLATION lines and exit 1
on failure; exit 2 on usage errors or unreadable input. Python stdlib only.
"""
import argparse
import json
import os
import re
import sys

CAPABILITIES = [
    "armazenamento-objetos", "formato-tabela", "catalogo", "ingestao", "processamento",
    "consulta-sql", "orquestracao", "segredos", "controle-acesso", "observabilidade",
    "backup-recuperacao", "qualidade-dados", "governanca-dados",
]
CLAIM_IDS = [f"C-{i}" for i in range(1, 15)]
PREMISES = ["enxuta", "pequena", "rapida", "facil-deploy"]
PREMISE_VERDICTS = {"atende", "parcial", "nao-atende"}
CAP_STATUS = {"presente", "placeholder", "ausente"}
GENERAL_VERDICTS = {"completo", "parcial", "insuficiente"}
PRIORITIES = {"P0", "P1", "P2"}
TYPES = {"correcao", "melhoria", "ferramenta"}
FIT = {"alta", "media", "baixa"}
CLAIM_STATUS = {"verificado", "reportado", "refutado"}
URL_RE = re.compile(r"https?://\S+")


class Usage(Exception):
    pass


def read_text(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError as exc:
        raise Usage(f"cannot read {path}: {exc}")


def read_json(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError) as exc:
        raise Usage(f"cannot read JSON {path}: {exc}")


def section(text, heading):
    """Body of a `## heading` section up to the next h1/h2 heading, or None."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.strip() == heading:
            body = []
            for nxt in lines[i + 1:]:
                if re.match(r"^#{1,2} ", nxt):
                    break
                body.append(nxt)
            return "\n".join(body)
    return None


def h3_sections(text, id_prefix):
    """[(number, title, body)] for `### <PREFIX>-<n> <title>` sections in document order."""
    found = []
    lines = text.splitlines()
    pat = re.compile(rf"^### ({id_prefix}-(\d+))\s+(.*)$")
    i = 0
    while i < len(lines):
        m = pat.match(lines[i])
        if m:
            body = []
            j = i + 1
            while j < len(lines) and not re.match(r"^#{1,3} ", lines[j]):
                body.append(lines[j])
                j += 1
            found.append((int(m.group(2)), m.group(3), "\n".join([lines[i]] + body)))
            i = j
        else:
            i += 1
    return found


def table_rows(body):
    rows = []
    for line in (body or "").splitlines():
        if not line.strip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if all(re.fullmatch(r":?-{2,}:?", c or "--") for c in cells):
            continue
        rows.append(cells)
    return rows


def strip_ref(ref):
    ref = re.sub(r"#.*$", "", ref)
    ref = re.sub(r":\d+$", "", ref)
    return ref


def path_exists(ref):
    return os.path.exists(strip_ref(ref))


def field(body, name):
    m = re.search(rf"^{name}: (.+?)\s*$", body, re.M)
    return m.group(1).strip() if m else None


def premise_lines(body):
    """{key: verdict} from `- Premissa: k | Veredito: v | Justificativa: ...` lines, plus raw count."""
    result, count = {}, 0
    for line in (body or "").splitlines():
        if line.startswith("- Premissa:"):
            count += 1
            m = re.match(r"^- Premissa: (\S+) \| Veredito: (\S+) \| Justificativa: (.+)$", line)
            if m:
                result.setdefault(m.group(1), []).append(m.group(2))
    return result, count


def gap_items(facts):
    return list(facts.get("missing_build_contexts", [])) + list(facts.get("misplaced_init_scripts", []))


# --- checks -----------------------------------------------------------------

def check_review(doc, facts_path):
    v = []
    text = read_text(doc)
    facts = read_json(facts_path)
    inv = section(text, "## Inventario de servicos")
    if inv is None:
        v.append("missing heading '## Inventario de servicos'")
    else:
        first = {r[0] for r in table_rows(inv) if r}
        for svc in facts.get("services", []):
            if svc not in first:
                v.append(f"inventory table lacks service '{svc}'")
    prem = section(text, "## Premissas")
    if prem is None:
        v.append("missing heading '## Premissas'")
    else:
        parsed, count = premise_lines(prem)
        if count != 4:
            v.append(f"'## Premissas' must contain exactly four '- Premissa:' lines (found {count})")
        for key in PREMISES:
            vals = parsed.get(key, [])
            if len(vals) != 1:
                v.append(f"premise '{key}' must appear exactly once with the exact line format (found {len(vals)})")
            elif vals[0] not in PREMISE_VERDICTS:
                v.append(f"premise '{key}' has invalid verdict '{vals[0]}'")
        for key in parsed:
            if key not in PREMISES:
                v.append(f"unknown premise key '{key}'")
    findings = h3_sections(text, "F")
    if not findings:
        v.append("no '### F-<n>' finding section")
    for idx, (num, title, body) in enumerate(findings, 1):
        if num != idx:
            v.append(f"finding F-{num} is out of order (expected F-{idx})")
        sev = field(body, "Severidade")
        if sev not in PRIORITIES:
            v.append(f"F-{num}: missing or invalid 'Severidade:' (got {sev!r})")
        ev = field(body, "Evidencia")
        if not ev:
            v.append(f"F-{num}: missing 'Evidencia:' line")
        elif not path_exists(ev):
            v.append(f"F-{num}: evidence path does not exist: {ev}")
    p0_text = "\n".join(body for _, _, body in findings if field(body, "Severidade") == "P0")
    for item in gap_items(facts):
        if item not in p0_text:
            v.append(f"gap '{item}' from compose-facts is not named in any P0 finding")
    return v


def check_fit(doc):
    v = []
    text = read_text(doc)
    cap = section(text, "## Capacidades")
    if cap is None:
        v.append("missing heading '## Capacidades'")
    else:
        rows = table_rows(cap)
        for key in CAPABILITIES:
            matches = [r for r in rows if r and r[0] == key]
            if len(matches) != 1:
                v.append(f"capability '{key}' must have exactly one row (found {len(matches)})")
                continue
            row = matches[0]
            if len(row) < 3:
                v.append(f"capability '{key}': row needs status and evidence cells")
                continue
            status, evidence = row[1], row[2]
            if status not in CAP_STATUS:
                v.append(f"capability '{key}': invalid status '{status}'")
            if evidence == "n/a":
                if status != "ausente":
                    v.append(f"capability '{key}': 'n/a' evidence is only allowed when status is 'ausente'")
            elif not path_exists(evidence):
                v.append(f"capability '{key}': evidence path does not exist: {evidence}")
    ver = section(text, "## Veredito")
    if ver is None:
        v.append("missing heading '## Veredito'")
    else:
        g = general_verdict(ver)
        if g not in GENERAL_VERDICTS:
            v.append(f"'Veredito geral:' missing or invalid (got {g!r})")
    return v


def general_verdict(body):
    m = re.search(r"^Veredito geral: (\S+)\s*$", body or "", re.M)
    return m.group(1) if m else None


def register_index(register_path):
    text = read_text(register_path)
    urls = set(URL_RE.findall(text))
    ids = set(re.findall(r"^\|\s*(C-\d+)\s*\|", text, re.M))
    return urls, ids


def check_recs(doc, facts_path, claims_path):
    v = []
    text = read_text(doc)
    facts = read_json(facts_path)
    urls, ids = register_index(claims_path)
    recs = h3_sections(text, "R")
    if not recs:
        v.append("no '### R-<n>' recommendation section")
    types_seen = set()
    for idx, (num, title, body) in enumerate(recs, 1):
        if num != idx:
            v.append(f"recommendation R-{num} is out of order (expected R-{idx})")
        pr = field(body, "Prioridade")
        if pr not in PRIORITIES:
            v.append(f"R-{num}: missing or invalid 'Prioridade:' (got {pr!r})")
        tp = field(body, "Tipo")
        if tp not in TYPES:
            v.append(f"R-{num}: missing or invalid 'Tipo:' (got {tp!r})")
        else:
            types_seen.add(tp)
        ad = field(body, "Aderencia")
        if ad not in FIT:
            v.append(f"R-{num}: missing or invalid 'Aderencia:' (got {ad!r})")
        src = field(body, "Fonte")
        if not src:
            v.append(f"R-{num}: missing 'Fonte:' line")
        elif src.startswith("http://") or src.startswith("https://"):
            if src not in urls:
                v.append(f"R-{num}: source URL is not in the claims register: {src}")
        elif re.fullmatch(r"C-\d+", src):
            if src not in ids:
                v.append(f"R-{num}: claim {src} is not in the claims register")
        elif not path_exists(src):
            v.append(f"R-{num}: source path does not exist: {src}")
    p0_text = "\n".join(body for _, _, body in recs if field(body, "Prioridade") == "P0")
    for item in gap_items(facts):
        if item not in p0_text:
            v.append(f"gap '{item}' from compose-facts is not named in any P0 recommendation")
    if "ferramenta" not in types_seen:
        v.append("at least one recommendation with 'Tipo: ferramenta' is required")
    if "correcao" not in types_seen:
        v.append("at least one recommendation with 'Tipo: correcao' is required")
    dec = section(text, "## Decisoes em aberto")
    if dec is None:
        v.append("missing heading '## Decisoes em aberto'")
    else:
        for d in ("D1", "D2", "D3"):
            if not re.search(rf"\b{d}\b", dec):
                v.append(f"'## Decisoes em aberto' does not mention {d}")
    return v


def check_claims(register, evidence):
    v = []
    text = read_text(register)
    rows = re.findall(r"^\|\s*(C-\d+)\s*\|\s*(.*?)\s*\|\s*(\S+)\s*\|\s*(\S+)\s*\|\s*(\d{4}-\d{2}-\d{2})\s*\|", text, re.M)
    seen = {}
    for cid, _subject, status, source, _date in rows:
        seen.setdefault(cid, []).append((status, source))
    for cid in CLAIM_IDS:
        entries = seen.get(cid, [])
        if len(entries) != 1:
            v.append(f"claim {cid} must have exactly one well-formed row (found {len(entries)})")
            continue
        status, source = entries[0]
        if status not in CLAIM_STATUS:
            v.append(f"claim {cid}: invalid status '{status}'")
        is_url = source.startswith("http://") or source.startswith("https://")
        if status in ("verificado", "refutado") and not is_url:
            v.append(f"claim {cid}: status '{status}' requires an http(s) source (got '{source}')")
        if status == "reportado" and not (is_url or source == "n/a"):
            v.append(f"claim {cid}: 'reportado' source must be a URL or n/a (got '{source}')")
    for cid in seen:
        if cid not in CLAIM_IDS:
            v.append(f"unexpected claim id {cid}")
    ev = read_json(evidence)
    if not isinstance(ev, dict) or ev.get("contract") != "AuthoringEvidence/v1":
        v.append("evidence: contract must be 'AuthoringEvidence/v1'")
        return v
    for key in ("contract", "provider", "request_id", "query", "observed_at", "state", "sources", "claims"):
        if key not in ev:
            v.append(f"evidence: missing key '{key}'")
    sources = ev.get("sources") if isinstance(ev.get("sources"), list) else []
    for i, s in enumerate(sources):
        for key in ("url", "title", "retrieved_at", "content_digest"):
            if not isinstance(s, dict) or key not in s:
                v.append(f"evidence: source[{i}] missing '{key}'")
        if isinstance(s, dict) and not re.fullmatch(r"[0-9a-f]{64}", str(s.get("content_digest", ""))):
            v.append(f"evidence: source[{i}] content_digest must be 64 hex characters")
    for i, c in enumerate(ev.get("claims") if isinstance(ev.get("claims"), list) else []):
        if not isinstance(c, dict) or "text" not in c or not isinstance(c.get("source_refs"), list):
            v.append(f"evidence: claim[{i}] needs 'text' and 'source_refs'")
            continue
        for ref in c["source_refs"]:
            if not isinstance(ref, int) or ref < 0 or ref >= len(sources):
                v.append(f"evidence: claim[{i}] source_ref {ref!r} out of range")
    return v


def check_summary(readme, review, fit, recs):
    v = []
    text = read_text(readme)
    base = os.path.dirname(os.path.abspath(readme))
    links = set(re.findall(r"\]\(([^)\s]+)\)", text))
    for target in ("01-compose-review.md", "02-datalake-fit.md", "03-recommendations.md", "evidence/claims-register.md"):
        if target not in links:
            v.append(f"README lacks a relative link to {target}")
        elif not os.path.exists(os.path.join(base, target)):
            v.append(f"README link target does not exist: {target}")
    ver = section(text, "## Vereditos")
    rev_text = read_text(review)
    rev_prem, _ = premise_lines(section(rev_text, "## Premissas") or "")
    fit_general = general_verdict(section(read_text(fit), "## Veredito") or "")
    if ver is None:
        v.append("missing heading '## Vereditos'")
    else:
        sum_prem, _ = premise_lines(ver)
        for key in PREMISES:
            expected = (rev_prem.get(key) or [None])[0]
            got = (sum_prem.get(key) or [None])[0]
            if got is None:
                v.append(f"README '## Vereditos' lacks the premise line for '{key}'")
            elif got != expected:
                v.append(f"README verdict for '{key}' is '{got}' but 01 says '{expected}'")
        got_general = general_verdict(ver)
        if got_general != fit_general:
            v.append(f"README 'Veredito geral' is {got_general!r} but 02 says {fit_general!r}")
    nxt = section(text, "## Proximos passos")
    p0 = [f"R-{num}" for num, _, body in h3_sections(read_text(recs), "R") if field(body, "Prioridade") == "P0"]
    if nxt is None:
        v.append("missing heading '## Proximos passos'")
    else:
        for rid in p0:
            if not re.search(rf"\b{rid}\b", nxt):
                v.append(f"'## Proximos passos' does not mention P0 recommendation {rid}")
    return v


def run_all(directory):
    d = directory
    results = [
        ("CLAIMS", lambda: check_claims(f"{d}/evidence/claims-register.md", f"{d}/evidence/authoring-evidence.json")),
        ("REVIEW", lambda: check_review(f"{d}/01-compose-review.md", f"{d}/evidence/compose-facts.json")),
        ("FIT", lambda: check_fit(f"{d}/02-datalake-fit.md")),
        ("RECS", lambda: check_recs(f"{d}/03-recommendations.md", f"{d}/evidence/compose-facts.json", f"{d}/evidence/claims-register.md")),
        ("SUMMARY", lambda: check_summary(f"{d}/README.md", f"{d}/01-compose-review.md", f"{d}/02-datalake-fit.md", f"{d}/03-recommendations.md")),
    ]
    violations = []
    for kind, fn in results:
        found = fn()
        violations.extend(f"[{kind.lower()}] {x}" for x in found)
        if not found:
            print(f"CHECK_{kind}=OK")
    return violations


def main(argv):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd")
    s = sub.add_parser("review"); s.add_argument("doc"); s.add_argument("--facts", required=True)
    s = sub.add_parser("fit"); s.add_argument("doc")
    s = sub.add_parser("recs"); s.add_argument("doc"); s.add_argument("--facts", required=True); s.add_argument("--claims", required=True)
    s = sub.add_parser("claims"); s.add_argument("register"); s.add_argument("--evidence", required=True)
    s = sub.add_parser("summary"); s.add_argument("readme"); s.add_argument("--review", required=True); s.add_argument("--fit", required=True); s.add_argument("--recs", required=True)
    s = sub.add_parser("all"); s.add_argument("directory", nargs="?", default="docs/assessment")
    try:
        args = p.parse_args(argv)
    except SystemExit:
        return 2
    if not args.cmd:
        p.print_usage()
        return 2
    try:
        if args.cmd == "review":
            kind, v = "REVIEW", check_review(args.doc, args.facts)
        elif args.cmd == "fit":
            kind, v = "FIT", check_fit(args.doc)
        elif args.cmd == "recs":
            kind, v = "RECS", check_recs(args.doc, args.facts, args.claims)
        elif args.cmd == "claims":
            kind, v = "CLAIMS", check_claims(args.register, args.evidence)
        elif args.cmd == "summary":
            kind, v = "SUMMARY", check_summary(args.readme, args.review, args.fit, args.recs)
        else:
            kind, v = "ALL", run_all(args.directory.rstrip("/"))
    except Usage as exc:
        print(f"usage error: {exc}", file=sys.stderr)
        return 2
    if v:
        for item in v:
            print(f"VIOLATION: {item}")
        return 1
    print(f"CHECK_{kind}=OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
