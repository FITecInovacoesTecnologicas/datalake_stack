#!/usr/bin/env bash
# check-compose.sh — render docker-compose.yml offline and emit a deterministic fact sheet.
#
# Usage: bash scripts/check-compose.sh [--out <file>|-]
#   default --out docs/assessment/evidence/compose-facts.json ; "-" writes to stdout.
#
# Only `docker compose config` is used: nothing is pulled, built or started.
# Every `${NAME:?...}` variable the compose requires is supplied inline: non-empty
# values come from env.example (versions, names), empty ones get the placeholder "x".
# No .env file is read or written. The JSON has no timestamps or absolute paths.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
OUT="docs/assessment/evidence/compose-facts.json"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) [[ $# -ge 2 ]] || { echo "--out requires a value" >&2; exit 2; }; OUT="$2"; shift 2 ;;
    -h|--help) sed -n '2,10p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

required="$(grep -oE '\$\{[A-Z0-9_]+:\?' docker-compose.yml | sed -E 's/^\$\{//; s/:\?$//' | sort -u)"
env_args=()
for name in $required; do
  value="$(grep -E "^${name}=" env.example | head -1 | cut -d= -f2- || true)"
  [[ -n "$value" ]] || value="x"
  env_args+=("${name}=${value}")
done

config_valid=true
if ! rendered="$(env "${env_args[@]}" docker compose --profile query --profile ops config --format json 2>/dev/null)"; then
  config_valid=false
  rendered='{}'
fi

RENDERED="$rendered" python3 - "$config_valid" "$required" "$OUT" <<'PY'
import hashlib, json, os, re, sys, glob

config_valid = sys.argv[1] == "true"
required_env = sorted(v for v in sys.argv[2].split() if v)
out = sys.argv[3]
rendered = json.loads(os.environ.get("RENDERED") or "{}")
root = os.getcwd()

def rel(path):
    if not path:
        return path
    if os.path.isabs(path):
        try:
            path = os.path.relpath(path, root)
        except ValueError:
            return path
    return path if path.startswith(".") else "./" + path if not path.startswith("/") else path

services = rendered.get("services", {}) or {}
profiles, images, build_contexts, bind_mounts = set(), set(), [], []
host_ports, healthchecks, restart_policies, resource_limits, one_shot = [], [], {}, [], []
mounts_initdb = False
for name in sorted(services):
    svc = services[name] or {}
    profiles.update(svc.get("profiles", []) or [])
    if svc.get("image"):
        images.add(svc["image"])
    build = svc.get("build")
    if isinstance(build, dict) and build.get("context"):
        ctx = rel(build["context"])
        build_contexts.append({"service": name, "context": ctx, "exists": os.path.isdir(ctx)})
    elif isinstance(build, str):
        ctx = rel(build)
        build_contexts.append({"service": name, "context": ctx, "exists": os.path.isdir(ctx)})
    for vol in svc.get("volumes", []) or []:
        if isinstance(vol, dict) and vol.get("type") == "bind":
            src = rel(vol.get("source", ""))
            bind_mounts.append({"service": name, "source": src, "exists": os.path.exists(src)})
            if vol.get("target") == "/docker-entrypoint-initdb.d":
                mounts_initdb = True
    for port in svc.get("ports", []) or []:
        if isinstance(port, dict):
            host_ports.append(f"{port.get('host_ip', '0.0.0.0')}:{port.get('published', '')}->{port.get('target', '')}")
    if svc.get("healthcheck"):
        healthchecks.append(name)
    restart = svc.get("restart")
    restart_policies[name] = restart if restart else "none"
    if restart == "no":
        one_shot.append(name)
    limits = ((svc.get("deploy") or {}).get("resources") or {}).get("limits") or {}
    if svc.get("mem_limit") or svc.get("cpus") or limits.get("memory") or limits.get("cpus"):
        resource_limits.append(name)

missing_build = sorted(os.path.basename(b["context"].rstrip("/")) for b in build_contexts if not b["exists"])
misplaced = sorted(os.path.basename(p) for p in glob.glob("[0-9][0-9]-*.sh")) if mounts_initdb else []
env_keys = sorted(set(re.findall(r"^([A-Z0-9_]+)=", open("env.example", encoding="utf-8").read(), re.M)))
facts = {
    "compose_digest": hashlib.sha256(open("docker-compose.yml", "rb").read()).hexdigest(),
    "config_valid": config_valid,
    "services": sorted(services),
    "profiles": sorted(profiles),
    "images": sorted(images),
    "build_contexts": build_contexts,
    "missing_build_contexts": missing_build,
    "bind_mounts": bind_mounts,
    "misplaced_init_scripts": misplaced,
    "required_env": required_env,
    "env_example_keys": env_keys,
    "missing_from_env_example": sorted(set(required_env) - set(env_keys)),
    "host_ports": sorted(host_ports),
    "healthchecks": sorted(healthchecks),
    "restart_policies": restart_policies,
    "resource_limits": sorted(resource_limits),
    "one_shot_services": sorted(one_shot),
}
text = json.dumps(facts, indent=2, sort_keys=True) + "\n"
if out == "-":
    sys.stdout.write(text)
else:
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(f"wrote {out}")
PY
