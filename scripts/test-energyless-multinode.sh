#!/usr/bin/env bash
# Test di EnergyLessPolicy con DUE nodi nella stessa area: verifica che i task
# possano essere offloadati al vicino e che la variante scelta venga eseguita
# anche sul nodo remoto (OffloadingPlan.Variants / ResumeWorkflow).
#
# Prerequisiti (vedi le istruzioni in chat):
#   - etcd attivo;
#   - nodo A (origine, porta 1323) e nodo B (vicino, porta 1324, udp 9877),
#     stessa area, entrambi con la build che contiene l'esecuzione delle varianti;
#   - A configurato perché il vicino risulti più conveniente (potenze più alte
#     su A, pesi sull'energia), altrimenti la policy tiene tutto in locale.
#
# Cosa fa: registra funzioni e workflow tramite il nodo A (etcd è condiviso, quindi
# li vede anche B), invoca il workflow per ogni soglia e stampa, per ogni task,
# su quale nodo è girato e se ha eseguito la variante.
#
# Uso: scripts/test-energyless-multinode.sh [--host H] [--port-a 1323] [--port-b 1324]
#                                           [--thresholds "0.95 0.50"] [--wait 30]

set -euo pipefail

HOST="${SERVERLEDGE_HOST:-localhost}"
PORT_A="${SERVERLEDGE_PORT:-1323}"
PORT_B="${SERVERLEDGE_PORT_B:-1324}"
WORKFLOW_NAME="${SERVERLEDGE_TEST_WORKFLOW:-euler_variants_test}"
THRESHOLDS="${SERVERLEDGE_TEST_THRESHOLDS:-0.95 0.50}"
WAIT_SEC="${SERVERLEDGE_TEST_WAIT:-30}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --port-a) PORT_A="$2"; shift 2 ;;
    --port-b) PORT_B="$2"; shift 2 ;;
    --thresholds) THRESHOLDS="$2"; shift 2 ;;
    --wait) WAIT_SEC="$2"; shift 2 ;;
    -h|--help) sed -n 2,17p "$0"; exit 0 ;;
    *) printf 'Argomento sconosciuto: %s\n' "$1" >&2; exit 2 ;;
  esac
done

for cmd in curl python3; do
  command -v "$cmd" >/dev/null 2>&1 || { printf '%s è necessario\n' "$cmd" >&2; exit 1; }
done

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# I due nodi devono rispondere.
for p in "$PORT_A" "$PORT_B"; do
  if ! curl -fsS "http://${HOST}:${p}/status" >/dev/null 2>&1; then
    printf 'Nessuna risposta da http://%s:%s/status: il nodo è acceso?\n' "$HOST" "$p" >&2
    exit 1
  fi
done

# Registrazione di funzioni e workflow tramite A (nessuna invocazione).
"$HERE/test-energyless-variants.sh" --host "$HOST" --port "$PORT_A" --no-invoke

# I vicini si scoprono con il monitoraggio periodico (~20 s): senza questa attesa
# il nodo A potrebbe non vedere ancora B e tenere tutto in locale.
printf '\nAttendo %s s che A scopra B (monitoraggio periodico)...\n' "$WAIT_SEC"
sleep "$WAIT_SEC"

OUT="$(mktemp "${TMPDIR:-/tmp}/multinode-resp.XXXXXX")"
trap 'rm -f "$OUT"' EXIT

for thr in $THRESHOLDS; do
  printf '\n=== Invocazione su A con MinAccuracy = %s ===\n' "$thr"
  code="$(curl -sS -o "$OUT" -w '%{http_code}' -X POST \
    "http://${HOST}:${PORT_A}/workflow/invoke/${WORKFLOW_NAME}" \
    -H 'Content-Type: application/json' \
    -d "{\"Params\":{},\"QoS\":{\"MinAccuracy\":${thr}},\"CanDoOffloading\":true,\"Async\":false}")"
  printf 'HTTP %s\n' "$code"
  python3 -I - "$OUT" <<'PY'
import json, sys
raw = open(sys.argv[1], encoding="utf-8").read()
try:
    resp = json.loads(raw)
except ValueError:
    print(raw[:500]); sys.exit(0)
reports = resp.get("Reports") or {}
if not reports:
    print(raw[:500]); sys.exit(0)
print("%-16s %-12s %-10s %-10s %s" % ("task", "nodo", "variante", "durata s", "offload latency s"))
nodes = set()
for name in sorted(reports):
    r = reports[name]
    node = (r.get("ExecutionNode") or "?")[-8:]
    nodes.add(node)
    print("%-16s %-12s %-10s %-10.3f %.3f" % (
        name, node, "sì" if not r.get("isDefault", True) else "no",
        r.get("Duration", 0.0), r.get("OffloadLatency", 0.0)))
print("nodi distinti usati: %d | tempo totale %.2f s" % (len(nodes), resp.get("ResponseTime", 0.0)))
PY
done

printf '\nControlla anche i log: su A le righe "scelta la variante"; su B "Resuming workflow" e "eseguo la variante".\n'