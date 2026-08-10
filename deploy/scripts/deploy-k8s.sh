#!/bin/sh
set -eu
if [ "$#" -ne 1 ] || { [ "$1" != local ] && [ "$1" != production ]; }; then
  echo "usage: deploy-k8s.sh local|production" >&2
  exit 64
fi
overlay=$1
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
for tool in kubectl kubeconform; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing deployment prerequisite: $tool" >&2; exit 127; }
done
if [ "$overlay" = production ] && [ "${PROMETHEUS_ADAPTER_CUSTOM_METRICS_READY:-}" != true ]; then
  echo "production requires Prometheus Adapter and custom.metrics.k8s.io (set PROMETHEUS_ADAPTER_CUSTOM_METRICS_READY=true after verification)" >&2
  exit 1
fi
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-deploy-apply.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
"$root/deploy/scripts/render-kustomize.sh" "$overlay" >"$scratch/all.yaml"
readiness=local
[ "$overlay" = production ] && readiness=deploy
"$root/deploy/scripts/validate-render.sh" "$overlay" "$readiness" "$scratch/all.yaml"
kubeconform -strict -summary "$scratch/all.yaml"
(cd "$root" && GOCACHE="${TMPDIR:-/tmp}/gpt-live-task23-go-cache" go run ./deploy/validate --overlay "$overlay" --readiness "$readiness" --split job "$scratch/all.yaml") >"$scratch/migrate.yaml"
(cd "$root" && GOCACHE="${TMPDIR:-/tmp}/gpt-live-task23-go-cache" go run ./deploy/validate --overlay "$overlay" --readiness "$readiness" --split workloads "$scratch/all.yaml") >"$scratch/workloads.yaml"
# Namespace ownership is explicit and precedes every namespaced preflight.
kubectl apply -f "$root/deploy/k8s/base/namespace.yaml"

kubectl get secret live-secrets -n gpt-live >/dev/null 2>&1 || { echo "preflight failed: external Secret gpt-live/live-secrets is missing" >&2; exit 1; }
required_keys="DATABASE_URL S2S_API_KEY DEEPSEEK_API_KEY LIVE_ADMIN_BEARER_TOKEN LIVE_PRIVACY_HMAC_KEY LIVE_RETENTION_HMAC_KEY LIVE_RECONNECT_HMAC_KEY LIVE_TOOL_ENCRYPTION_KEY"
[ "$overlay" = local ] && required_keys="$required_keys LIVE_STATIC_BEARER_TOKEN"
for key in $required_keys; do
  present=$(kubectl get secret live-secrets -n gpt-live -o "go-template={{if index .data \"$key\"}}present{{end}}")
  [ "$present" = present ] || { echo "preflight failed: live-secrets key $key is missing" >&2; exit 1; }
done
kubectl get secret live-synthetic-identity -n gpt-live >/dev/null 2>&1 || { echo "preflight failed: renewable Secret gpt-live/live-synthetic-identity is missing" >&2; exit 1; }
synthetic_identity_present=$(kubectl get secret live-synthetic-identity -n gpt-live -o 'go-template={{if and (eq (len .data) 1) (index .data "token")}}present{{end}}')
[ "$synthetic_identity_present" = present ] || { echo "preflight failed: live-synthetic-identity must contain exactly the nonempty data.token key" >&2; exit 1; }
if [ "$overlay" = production ]; then
  kubectl get secret "$LIVE_PRODUCTION_TLS_SECRET" -n gpt-live >/dev/null 2>&1 || { echo "preflight failed: production TLS Secret is missing" >&2; exit 1; }
  for key in tls.crt tls.key; do
    present=$(kubectl get secret "$LIVE_PRODUCTION_TLS_SECRET" -n gpt-live -o "go-template={{if index .data \"$key\"}}present{{end}}")
    [ "$present" = present ] || { echo "preflight failed: production TLS Secret key $key is missing" >&2; exit 1; }
  done
fi

# Enforce isolation before migration starts: default deny plus only the exact
# DNS/PostgreSQL migration allow policy.
kubectl apply -f "$root/deploy/k8s/base/default-deny.yaml"
kubectl apply -f "$root/deploy/k8s/base/migrate-network-policy.yaml"
# Jobs have immutable pod templates. Remove only the prior, completed migration
# Job so every deployment executes the currently rendered migration image.
kubectl delete job live-migrate -n gpt-live --ignore-not-found --wait=true
kubectl apply -f "$scratch/migrate.yaml"
kubectl wait --for=condition=complete --timeout=10m job/live-migrate -n gpt-live
kubectl apply -f "$scratch/workloads.yaml"
kubectl rollout status deployment/live-synthetic-canary -n gpt-live --timeout=5m
