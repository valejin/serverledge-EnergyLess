#!/usr/bin/env bash
# Tuning preliminare per EnergyLessPolicy (punto 18, suggerimento di Cecilia
# dell'11/11): prima degli esperimenti si eseguono le funzioni del workload,
# così Prometheus ha i tempi reali di esecuzione e di init, e lo SpeedUp
# DICHIARATO della variante viene registrato uguale a quello MISURATO.
#
# Cosa fa:
#   1. registra funzione base, variante e workflow (test-energyless-variants.sh);
#   2. invoca il workflow N volte con MinAccuracy alta (gira la funzione base)
#      e N volte con MinAccuracy bassa (la policy sceglie la variante);
#   3. aspetta che Prometheus abbia le misure di entrambe (con etichetta node,
#      cioè arrivate dal Pushgateway);
#   4. stampa tempi medi di esecuzione e init per funzione e nodo, lo speedup
#      misurato (base/variante) e le righe suggerite per il file YAML della
#      policy (default.exec.time, default.init.time);
#   5. registra di nuovo la variante con lo speedup misurato (salvo --no-apply);
#   6. salva le misure per funzione e tipo di nodo (edge/cloud) in un file YAML
#      (default examples/energyless/tuning-energyless.yaml, salvo --no-save),
#      che la policy legge all'avvio con la chiave tuning.file del suo file
#      YAML: così il tuning non si perde riavviando Prometheus o i nodi.
#
# Con --cloud-port esegue il tuning anche sul nodo cloud: invoca direttamente
# le singole funzioni (base e variante) sul nodo cloud, così Prometheus ha
# anche i tempi e la probabilità di cold start della regione cloud (il nodo
# edge li legge con registry.remote.area = area del cloud).
#
# Prerequisiti: etcd, Pushgateway, Prometheus che legge il Pushgateway, nodo
# avviato con metrics.enabled e metrics.prometheus.pushgateway.host.
#
# Uso: tune-energyless.sh [--host H] [--port 1323] [--prom http://localhost:9090]
#                         [--runs 3] [--low-accuracy 0.5] [--timeout 180]
#                         [--base euler_standard] [--variant euler_light]
#                         [--workflow euler_variants_test] [--no-apply]
#                         [--cloud-port 1328] [--cloud-host H] [--cloud-area cloud_region_a]
#                         [--save FILE] [--no-save]

set -euo pipefail

HOST="${SERVERLEDGE_HOST:-localhost}"
PORT="${SERVERLEDGE_PORT:-1323}"
PROM="${PROMETHEUS_URL:-http://localhost:9090}"
RUNS=3
LOW_ACCURACY=0.5
TIMEOUT=180
BASE_FN="euler_standard"
VARIANT_FN="euler_light"
WORKFLOW_NAME="${SERVERLEDGE_TEST_WORKFLOW:-euler_variants_test}"
APPLY=1
CLOUD_HOST=""
CLOUD_PORT=""
CLOUD_AREA="cloud_region_a"
SAVE_FILE="examples/energyless/tuning-energyless.yaml"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --prom) PROM="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --low-accuracy) LOW_ACCURACY="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    --base) BASE_FN="$2"; shift 2 ;;
    --variant) VARIANT_FN="$2"; shift 2 ;;
    --workflow) WORKFLOW_NAME="$2"; shift 2 ;;
    --no-apply) APPLY=0; shift ;;
    --cloud-host) CLOUD_HOST="$2"; shift 2 ;;
    --cloud-port) CLOUD_PORT="$2"; shift 2 ;;
    --cloud-area) CLOUD_AREA="$2"; shift 2 ;;
    --save) SAVE_FILE="$2"; shift 2 ;;
    --no-save) SAVE_FILE=""; shift ;;
    -h|--help) sed -n 2,38p "$0"; exit 0 ;;
    *) printf 'Argomento sconosciuto: %s\n' "$1" >&2; exit 2 ;;
  esac
done

for cmd in curl python3; do
  command -v "$cmd" >/dev/null 2>&1 || { printf '%s è necessario\n' "$cmd" >&2; exit 1; }
done

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
URL="http://${HOST}:${PORT}"

curl -fsS "${URL}/status" >/dev/null 2>&1 || { printf 'Nessuna risposta da %s/status\n' "$URL" >&2; exit 1; }
curl -fsS "${PROM}/-/ready" >/dev/null 2>&1 || { printf 'Prometheus non risponde su %s\n' "$PROM" >&2; exit 1; }
CLOUD_URL=""
if [[ -n "$CLOUD_PORT" ]]; then
  CLOUD_URL="http://${CLOUD_HOST:-$HOST}:${CLOUD_PORT}"
  curl -fsS "${CLOUD_URL}/status" >/dev/null 2>&1 || { printf 'Nessuna risposta dal nodo cloud %s/status\n' "$CLOUD_URL" >&2; exit 1; }
fi

# 1. Registrazione (speedup provvisorio: verrà sostituito da quello misurato).
"$HERE/test-energyless-variants.sh" --host "$HOST" --port "$PORT" --no-invoke

invoke() { # $1 = MinAccuracy
  curl -sS -o /dev/null -w '%{http_code}' -X POST "${URL}/workflow/invoke/${WORKFLOW_NAME}" \
    -H 'Content-Type: application/json' \
    -d "{\"Params\":{},\"QoS\":{\"MinAccuracy\":$1},\"CanDoOffloading\":true,\"Async\":false}"
}

# 2. Esecuzioni di tuning.
printf '\n%s invocazioni con MinAccuracy 0.95 (funzione base)...\n' "$RUNS"
for ((i = 1; i <= RUNS; i++)); do printf '  #%d HTTP %s\n' "$i" "$(invoke 0.95)"; done
printf '%s invocazioni con MinAccuracy %s (variante)...\n' "$RUNS" "$LOW_ACCURACY"
for ((i = 1; i <= RUNS; i++)); do printf '  #%d HTTP %s\n' "$i" "$(invoke "$LOW_ACCURACY")"; done

# 2b. Esecuzioni di tuning sul nodo cloud: singole funzioni, senza offloading
# (le funzioni sono già registrate su etcd, condiviso con il nodo cloud).
invoke_fn() { # $1 = nome funzione
  curl -sS -o /dev/null -w '%{http_code}' -X POST "${CLOUD_URL}/invoke/$1" \
    -H 'Content-Type: application/json' \
    -d '{"Params":{},"CanDoOffloading":false,"Async":false}'
}
if [[ -n "$CLOUD_URL" ]]; then
  for fn in "$BASE_FN" "$VARIANT_FN"; do
    printf '%s invocazioni di %s sul nodo cloud %s...\n' "$RUNS" "$fn" "$CLOUD_URL"
    for ((i = 1; i <= RUNS; i++)); do printf '  #%d HTTP %s\n' "$i" "$(invoke_fn "$fn")"; done
  done
fi

# 3-4. Attesa delle misure e calcolo.
printf '\nAttendo che Prometheus abbia le misure di %s e %s (max %s s)...\n' "$BASE_FN" "$VARIANT_FN" "$TIMEOUT"
WANT_CLOUD=""
[[ -n "$CLOUD_URL" ]] && WANT_CLOUD="$CLOUD_AREA"
RESULT="$(python3 -I - "$PROM" "$BASE_FN" "$VARIANT_FN" "$TIMEOUT" "$WANT_CLOUD" "$SAVE_FILE" <<'PY'
import json, sys, time, urllib.parse, urllib.request
prom, base, variant, timeout = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
cloud_area = sys.argv[5] if len(sys.argv) > 5 else ""
save_file = sys.argv[6] if len(sys.argv) > 6 else ""
cloud_prefix = "(%s)" % cloud_area if cloud_area else None

def query(q):
    url = prom + "/api/v1/query?" + urllib.parse.urlencode({"query": q})
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.load(r)
    out = {}
    for item in data.get("data", {}).get("result", []):
        m = item["metric"]
        if "node" in m and "function" in m:
            out.setdefault(m["function"], {})[m["node"]] = float(item["value"][1])
    return out

def avg(metric):
    return query('%s_sum{node!=""} / %s_count{node!=""}' % (metric, metric))

def has_cloud(exec_t, fn):
    return any(nd.startswith(cloud_prefix) for nd in exec_t.get(fn, {}))

deadline = time.time() + timeout
while True:
    exec_t = avg("execution_time")
    ok = base in exec_t and variant in exec_t
    if ok and cloud_prefix:
        ok = has_cloud(exec_t, base) and has_cloud(exec_t, variant)
    if ok:
        break
    if time.time() > deadline:
        print("Misure non trovate in tempo. Funzioni presenti: %s" % sorted(exec_t), file=sys.stderr)
        print("Controlla Pushgateway/Prometheus (target UP, honor_labels) e rilancia con --timeout più alto.", file=sys.stderr)
        sys.exit(1)
    time.sleep(5)

init_t = avg("init_time")
cold = query('cold_starts_count{node!=""} / completed_count{node!=""}')
print("\n%-16s %-6s %-14s %-12s %-8s %s" % ("funzione", "tipo", "nodo", "esecuzione s", "init s", "P(cold)"), file=sys.stderr)
for fn in (base, variant):
    for nd, t in sorted(exec_t[fn].items()):
        it = init_t.get(fn, {}).get(nd)
        pc = cold.get(fn, {}).get(nd)
        kind = "cloud" if cloud_prefix and nd.startswith(cloud_prefix) else "edge"
        print("%-16s %-6s %-14s %-12.3f %-8s %s" % (fn, kind, nd[-12:], t,
              "%.3f" % it if it is not None else "n/d", "%.2f" % pc if pc is not None else "n/d"), file=sys.stderr)

ratios = [exec_t[base][nd] / exec_t[variant][nd] for nd in exec_t[base] if exec_t[variant].get(nd, 0) > 0]
if not ratios:
    print("Nessun nodo con misure di entrambe le funzioni: speedup non calcolabile.", file=sys.stderr)
    sys.exit(1)
speedup = sum(ratios) / len(ratios)
base_avg = sum(exec_t[base].values()) / len(exec_t[base])
inits = list(init_t.get(base, {}).values())
print("\nspeedup misurato %s/%s: %.1f (media su %d nodo/i)" % (base, variant, speedup, len(ratios)), file=sys.stderr)
print("\nRighe suggerite per il file YAML della policy (ripiego se mancano misure):", file=sys.stderr)
print("default.exec.time: %.2f" % base_avg, file=sys.stderr)
if inits:
    print("default.init.time: %.2f" % (sum(inits) / len(inits)), file=sys.stderr)

if save_file:
    def kind_of(nd):
        return "cloud" if cloud_prefix and nd.startswith(cloud_prefix) else "edge"
    def mean(vals):
        return sum(vals) / len(vals) if vals else None
    lines = ["# Misure del tuning di EnergyLessPolicy, scritte da scripts/tune-energyless.sh.",
             "# Lette dalla policy con la chiave tuning.file nel suo file YAML: usate quando",
             "# manca la metrica live da Prometheus, prima dei default (punto 20).",
             "# Edge: media sui nodi edge misurati; cloud: media sui nodi della regione cloud.",
             'measured: "%s"' % time.strftime("%Y-%m-%d %H:%M"),
             "functions:"]
    for fn in (base, variant):
        lines.append("  %s:" % fn)
        for kind in ("edge", "cloud"):
            nodes = [nd for nd in exec_t.get(fn, {}) if kind_of(nd) == kind]
            if not nodes:
                continue
            fields = ["exec.time: %.4f" % mean([exec_t[fn][nd] for nd in nodes])]
            it = mean([init_t[fn][nd] for nd in nodes if nd in init_t.get(fn, {})])
            if it is not None:
                fields.append("init.time: %.4f" % it)
            pc = mean([cold[fn][nd] for nd in nodes if nd in cold.get(fn, {})])
            if pc is not None:
                fields.append("cold.start.prob: %.4f" % min(max(pc, 0.0), 1.0))
            lines.append("    %s: {%s}" % (kind, ", ".join(fields)))
    with open(save_file, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print("\nMisure salvate in %s" % save_file, file=sys.stderr)
print("%.1f" % speedup)
PY
)"

# 5. Registrazione della variante con lo speedup misurato.
if [[ "$APPLY" -eq 1 ]]; then
  printf '\nRegistro di nuovo %s con speedup dichiarato = %s (misurato)...\n' "$VARIANT_FN" "$RESULT"
  "$HERE/test-energyless-variants.sh" --host "$HOST" --port "$PORT" --no-invoke --speedup "$RESULT"
  printf '\nFatto. Usa --speedup %s anche negli altri script che registrano %s.\n' "$RESULT" "$VARIANT_FN"
else
  printf '\nSpeedup misurato: %s (non applicato, --no-apply).\n' "$RESULT"
fi
if [[ -n "$SAVE_FILE" ]]; then
  printf 'Per usarle anche dopo un riavvio, nel file YAML della policy: tuning.file: "%s"\n' "$SAVE_FILE"
fi
printf 'Attendi ~20 s (rilettura delle metriche da parte del nodo) prima degli esperimenti.\n'