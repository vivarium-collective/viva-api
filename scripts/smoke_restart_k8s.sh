#!/usr/bin/env bash
# The --restart-command for `atlantis smoke run --tier 3` (Tier R) on a Kubernetes deployment.
#
#   uv run atlantis smoke run --only restart --url http://localhost:1111 \
#     --restart-command "scripts/smoke_restart_k8s.sh sms-api-stanford-test 1111"
#
# Tier R's contract: the command must return only once the API answers again AT THE SAME URL.
# Through the ALB / SSM tunnel a rollout is enough. Through `kubectl port-forward` the forward
# is bound to the OLD pod and dies with it, so this script re-establishes it too.
#
# Usage: smoke_restart_k8s.sh <namespace> [local-port]   (needs KUBECONFIG / AWS_PROFILE set)
set -euo pipefail

NS="${1:?usage: $0 <namespace> [local-port]}"
PORT="${2:-}"

# The pods that are serving NOW. `rollout status` can return before they are gone (seen live:
# 3.6 s), and "restarted" has to mean the old process is no longer answering.
OLD_PODS=$(kubectl get pods -n "$NS" -l app=api -o jsonpath='{.items[*].metadata.name}')

kubectl rollout restart deployment/api -n "$NS" >/dev/null
kubectl rollout status deployment/api -n "$NS" --timeout=300s >/dev/null
for pod in $OLD_PODS; do
  kubectl wait --for=delete "pod/$pod" -n "$NS" --timeout=180s >/dev/null 2>&1 || true
done

if [[ -n "$PORT" ]]; then
  pkill -f "port-forward -n $NS deployment/api $PORT:8000" 2>/dev/null || true
  nohup kubectl port-forward -n "$NS" deployment/api "$PORT:8000" >/dev/null 2>&1 &
  for _ in $(seq 1 30); do
    curl -fsS -m 3 "http://localhost:$PORT/version" >/dev/null 2>&1 && exit 0
    sleep 2
  done
  echo "API did not answer on localhost:$PORT after the restart" >&2
  exit 1
fi
