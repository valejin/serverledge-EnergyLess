#!/usr/bin/env bash
# Test di EnergyLessPolicy con le varianti di funzione (euler_standard / euler_light).
#
# Cosa fa, in ordine:
#   1. registra euler_standard e la sua variante euler_light (con Signature,
#      necessaria per usarle in un workflow);
#   2. registra un workflow a catena di 3 task euler_standard
#      (nome: euler_variants_test, definizione incorporata in questo file);
#   3. invoca il workflow una volta per ogni soglia di accuratezza
#      (QoS.MinAccuracy) e stampa la risposta.
#
# Prerequisiti:
#   - nodo Serverledge in esecuzione con "workflow.offloading.policy: energyless"
#     e "metrics.enabled: true";
#   - RequestQoS.MinAccuracy presente nel codice compilato (altrimenti la soglia
#     viene ignorata e vale sempre il default 0.95);
#   - immagine my_python310 disponibile (usare --build-images la prima volta);
#   - serverledge-cli compilato (default: bin/serverledge-cli).
#
# Da eseguire dalla radice del repo o da qualunque cartella: lo script si
# posiziona da solo nella radice (la cartella sopra "scripts/").
#
# Risultato atteso con Utility(euler_light) = 0.8 (task che usano la variante):
#   soglia 0.95 -> nessuno | 0.80 -> Euler1 | 0.64 -> Euler1, Euler2 | 0.50 -> tutti e 3
# La scelta si legge nel log del SERVER, righe "EnergyLess: per il task ... scelta la
# variante euler_light ...", seguite da "task ...: eseguo la variante euler_light al
# posto di euler_standard". Nella risposta HTTP i task eseguiti con la variante hanno
# "isDefault":false e "step size h : 0.001" nel risultato (la base dà 1e-05).

set -euo pipefail

HOST="${SERVERLEDGE_HOST:-localhost}"
PORT="${SERVERLEDGE_PORT:-1323}"
CLI="${SERVERLEDGE_CLI:-bin/serverledge-cli}"
WORKFLOW_NAME="${SERVERLEDGE_TEST_WORKFLOW:-euler_variants_test}"
THRESHOLDS="${SERVERLEDGE_TEST_THRESHOLDS:-0.95 0.80 0.64 0.50}"
UTILITY="${SERVERLEDGE_TEST_UTILITY:-0.8}"
SPEEDUP="${SERVERLEDGE_TEST_SPEEDUP:-2.0}"
BUILD_IMAGES=false
INVOKE=true

usage() {
  cat <<EOF
Usage: $0 [--host HOST] [--port PORT] [--cli PATH] [--build-images] [--no-invoke]
          [--thresholds "0.95 0.80 0.64 0.50"] [--utility 0.8] [--speedup 2.0]

  --build-images   builda prima l'immagine my_python310
  --no-invoke      registra funzioni e workflow ma non lo invoca
  --thresholds     soglie MinAccuracy da provare, separate da spazio
  --utility        Utility dichiarata per euler_light (default 0.8)
  --speedup        SpeedUp dichiarato per euler_light (default 2.0)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --cli) CLI="$2"; shift 2 ;;
    --thresholds) THRESHOLDS="$2"; shift 2 ;;
    --utility) UTILITY="$2"; shift 2 ;;
    --speedup) SPEEDUP="$2"; shift 2 ;;
    --build-images) BUILD_IMAGES=true; shift ;;
    --no-invoke) INVOKE=false; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage; printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done

# Si lavora dalla radice del repo (la cartella sopra "scripts/"), così i percorsi
# relativi (bin/, examples/, images/) funzionano da qualunque posizione.
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."

if [[ ! -x "$CLI" ]]; then
  printf 'Serverledge CLI not found or not executable: %s\n' "$CLI" >&2
  exit 1
fi
if [[ "$INVOKE" == true ]] && ! command -v curl >/dev/null 2>&1; then
  printf 'curl è necessario per invocare il workflow con la soglia (usare --no-invoke per saltare)\n' >&2
  exit 1
fi

run() {
  printf '\n+'
  printf ' %q' "$@"
  printf '\n'
  "$@"
}

WORKFLOW_JSON="$(mktemp "${TMPDIR:-/tmp}/euler-variants-workflow.XXXXXX")"
trap 'rm -f "$WORKFLOW_JSON"' EXIT

cat > "$WORKFLOW_JSON" <<'JSON'
{
  "Comment": "Catena di 3 task euler_standard per testare la selezione delle varianti di EnergyLessPolicy",
  "StartAt": "Euler1",
  "States": {
    "Euler1": { "Type": "Task", "Resource": "euler_standard", "Next": "Euler2" },
    "Euler2": { "Type": "Task", "Resource": "euler_standard", "Next": "Euler3" },
    "Euler3": { "Type": "Task", "Resource": "euler_standard", "End": true }
  }
}
JSON

if [[ "$BUILD_IMAGES" == true ]]; then
  run docker build -t ceciliacal/my_python310 images/my_python310/
fi

# --- 1. Funzione base e variante, ENTRAMBE con la stessa Signature (output result:Text)
run "$CLI" --host "$HOST" --port "$PORT" create --update \
  -f euler_standard \
  --memory 100 \
  --src examples/function_variant/euler_standard.py \
  --runtime my_python310 \
  --handler "euler_standard.handler" \
  --output "result:Text"

run "$CLI" --host "$HOST" --port "$PORT" create --update \
  -f euler_standard \
  --variant euler_light \
  --memory 100 \
  --src examples/function_variant/euler_light.py \
  --runtime my_python310 \
  --handler "euler_light.handler" \
  --speedup "$SPEEDUP" \
  --utility "$UTILITY" \
  --output "result:Text"

# --- 2. Workflow
run "$CLI" --host "$HOST" --port "$PORT" create-workflow --update \
  -s "$WORKFLOW_JSON" \
  -f "$WORKFLOW_NAME"

# --- 3. Invocazioni, una per soglia
if [[ "$INVOKE" == true ]]; then
  for thr in $THRESHOLDS; do
    printf '\n=== Invocazione con MinAccuracy = %s ===\n' "$thr"
    curl -sS -X POST "http://${HOST}:${PORT}/workflow/invoke/${WORKFLOW_NAME}" \
      -H 'Content-Type: application/json' \
      -d "{\"Params\":{},\"QoS\":{\"MinAccuracy\":${thr}},\"CanDoOffloading\":true,\"Async\":false}" \
      -w '\nHTTP %{http_code}\n'
  done
  printf '\nFatto. Nel log del server: righe "scelta la variante" e "eseguo la variante"; nella risposta: isDefault e step size h.\n'
fi