#!/usr/bin/env bash
# Test della correzione del punto 17 di EnergyLessPolicy (11/11): quando il
# piano non riesce a rispettare la deadline residua a metà catena, TUTTI i task
# devono comunque entrare nel piano (prima la visita si fermava e i task
# successivi restavano fuori, eseguiti in locale senza essere valutati).
#
# Basta un solo nodo: il bug è nella costruzione del piano, non nell'offloading.
#
# Cosa fa:
#   1. registra funzioni e workflow (test-energyless-variants.sh --no-invoke);
#   2. opzionale: N invocazioni di riscaldamento SENZA deadline, così Prometheus
#      ha i tempi reali di esecuzione e init (tuning preliminare suggerito da
#      Cecilia);
#   3. invoca il workflow con ciascuna MaxRespT indicata e, per ogni richiesta,
#      stampa dove è girato ogni task e se la richiesta è COMPLETATA (tempo di
#      risposta <= MaxRespT) o FALLITA, con il conteggio finale.
#
# Cosa controllare nel log del nodo (vedi le istruzioni in chat):
#   - "il piano prevede di sforare la deadline al task ..."  (una volta per richiesta)
#   - "piano per la richiesta ... (3 task, ...)"           (tutti i task nel piano)
#
# Uso: test-energyless-deadline.sh [--host H] [--port 1323]
#                                  [--max-resp-t "8"] [--min-accuracy 0.95]
#                                  [--warmup 0] [--workflow euler_variants_test]

set -euo pipefail

HOST="${SERVERLEDGE_HOST:-localhost}"
PORT="${SERVERLEDGE_PORT:-1323}"
WORKFLOW_NAME="${SERVERLEDGE_TEST_WORKFLOW:-euler_variants_test}"
MAX_RESP_TS="8"
MIN_ACCURACY="0.95"
WARMUP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --max-resp-t) MAX_RESP_TS="$2"; shift 2 ;;
    --min-accuracy) MIN_ACCURACY="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --workflow) WORKFLOW_NAME="$2"; shift 2 ;;
    -h|--help) sed -n 2,25p "$0"; exit 0 ;;
    *) printf 'Argomento sconosciuto: %s\n' "$1" >&2; exit 2 ;;
  esac
done

for cmd in curl python3; do
  command -v "$cmd" >/dev/null 2>&1 || { printf '%s è necessario\n' "$cmd" >&2; exit 1; }
done

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
URL="http://${HOST}:${PORT}"

if ! curl -fsS "${URL}/status" >/dev/null 2>&1; then
  printf 'Nessuna risposta da %s/status: il nodo è acceso?\n' "$URL" >&2
  exit 1
fi

"$HERE/test-energyless-variants.sh" --host "$HOST" --port "$PORT" --no-invoke

OUT="$(mktemp "${TMPDIR:-/tmp}/deadline-resp.XXXXXX")"
trap 'rm -f "$OUT"' EXIT

invoke() { # $1 = corpo QoS JSON
  curl -sS -o "$OUT" -w '%{http_code}' -X POST "${URL}/workflow/invoke/${WORKFLOW_NAME}" \
    -H 'Content-Type: application/json' \
    -d "{\"Params\":{},\"QoS\":$1,\"CanDoOffloading\":true,\"Async\":false}"
}

if [[ "$WARMUP" -gt 0 ]]; then
  printf '\nRiscaldamento: %s invocazioni senza deadline...\n' "$WARMUP"
  for ((i = 1; i <= WARMUP; i++)); do
    code="$(invoke "{\"MinAccuracy\":${MIN_ACCURACY}}")"
    printf '  #%d HTTP %s\n' "$i" "$code"
  done
  printf 'Attendi che Prometheus aggiorni le metriche (intervallo di scrape) prima di proseguire.\n'
  read -r -p 'Premi Invio per continuare... ' _
fi

completed=0
failed=0
for maxt in $MAX_RESP_TS; do
  printf '\n=== Invocazione con MaxRespT = %s s, MinAccuracy = %s ===\n' "$maxt" "$MIN_ACCURACY"
  code="$(invoke "{\"MaxRespT\":${maxt},\"MinAccuracy\":${MIN_ACCURACY}}")"
  printf 'HTTP %s\n' "$code"
  result="$(python3 -I - "$OUT" "$maxt" <<'PY'
import json, sys
raw = open(sys.argv[1], encoding="utf-8").read()
max_t = float(sys.argv[2])
try:
    resp = json.loads(raw)
except ValueError:
    print(raw[:500], file=sys.stderr); print("ERRORE"); sys.exit(0)
reports = resp.get("Reports") or {}
if not reports:
    print(raw[:500], file=sys.stderr); print("ERRORE"); sys.exit(0)
print("%-16s %-12s %-10s %s" % ("task", "nodo", "variante", "durata s"), file=sys.stderr)
for name in sorted(reports):
    r = reports[name]
    print("%-16s %-12s %-10s %.3f" % (
        name, (r.get("ExecutionNode") or "?")[-8:],
        "sì" if not r.get("isDefault", True) else "no", r.get("Duration", 0.0)), file=sys.stderr)
rt = resp.get("ResponseTime", 0.0)
esito = "COMPLETATA" if rt <= max_t else "FALLITA"
print("tempo di risposta %.3f s, deadline %.3f s -> %s" % (rt, max_t, esito), file=sys.stderr)
print(esito)
PY
)"
  case "$result" in
    COMPLETATA) completed=$((completed + 1)) ;;
    FALLITA) failed=$((failed + 1)) ;;
  esac
done

printf '\nRichieste completate entro la deadline: %d, fallite: %d\n' "$completed" "$failed"
printf 'Nel log del nodo cerca: "prevede di sforare la deadline" e "piano per la richiesta".\n'