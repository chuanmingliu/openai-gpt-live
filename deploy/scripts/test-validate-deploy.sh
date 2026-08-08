#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
cache=${TMPDIR:-/tmp}/gpt-live-task23-go-cache
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-deploy-test.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
(cd "$root" && GOCACHE="$cache" go build -o "$scratch/deploy-validate" ./deploy/validate)
validator=$scratch/deploy-validate

"$root/deploy/scripts/render-kustomize.sh" local >"$scratch/local.yaml"
LIVE_PRODUCTION_RENDER_MODE=fixture "$root/deploy/scripts/render-kustomize.sh" production >"$scratch/production.yaml"
"$root/deploy/scripts/validate-render.sh" local local "$scratch/local.yaml"
"$root/deploy/scripts/validate-render.sh" production fixture "$scratch/production.yaml"

# All public deployment entrypoints must resolve the repository independently
# of the caller's working directory.
(cd "$scratch" && "$root/deploy/scripts/render-kustomize.sh" local >"$scratch/from-temp-local.yaml")
(cd "$scratch" && LIVE_PRODUCTION_RENDER_MODE=fixture "$root/deploy/scripts/render-kustomize.sh" production >"$scratch/from-temp-production.yaml")
(cd "$scratch" && "$root/deploy/scripts/validate-render.sh" local local "$scratch/from-temp-local.yaml")
(cd "$scratch" && "$root/deploy/scripts/validate-render.sh" production fixture "$scratch/from-temp-production.yaml")

# Each mutation changes one named workload/container invariant. Acceptance of
# any mutated render is a policy-test failure.
for mutation in \
  live-gateway:root live-worker:root live-migrate:root \
  live-gateway:writable live-worker:writable live-migrate:writable \
  live-gateway:privilege live-worker:privilege live-migrate:privilege \
  live-gateway:capabilities live-worker:capabilities live-migrate:capabilities \
  live-gateway:seccomp live-worker:seccomp live-migrate:seccomp \
  live-gateway:automount live-worker:automount live-migrate:automount \
  live-gateway:readiness live-worker:liveness live-gateway:startup \
  live-gateway:grace live-worker:grace live-migrate:grace \
  live-gateway:prestop live-worker:prestop \
  live-gateway:readiness-path live-worker:readiness-port \
  live-gateway:prestop-noop live-worker:prestop-binary \
  live-gateway:prestop-port live-worker:prestop-wait \
  live-gateway:prestop-auth live-worker:prestop-auth \
  live-gateway:config-hash live-worker:config-hash \
  live-gateway:pod-label live-worker:pod-label live-migrate:pod-label \
  live-gateway:image live-worker:image live-migrate:image \
  live-gateway:image-scheme live-worker:image-tag live-migrate:image-space \
  live-gateway:image-256 live-worker:image-256 live-migrate:image-256 \
  live-gateway:resources live-worker:resources live-migrate:resources \
  live-worker:init-writable \
  live-gateway:static-secret-prod live-worker:static-secret-prod \
  live-gateway:static-value-prod live-worker:static-value-prod \
  live-gateway:duplicate-inline-admin live-worker:duplicate-inline-admin \
  live-gateway:inline-admin live-worker:unexpected-secret \
  service:selector service:port service:target-port service:protocol service:extra-port \
  pdb-gateway:selector pdb-gateway:minimum pdb-gateway:extra pdb-worker:selector pdb-worker:minimum pdb-worker:extra \
  hpa-gateway:target hpa-gateway:replicas hpa-gateway:behavior hpa-gateway:metric-name hpa-gateway:metric-value hpa-gateway:metric-type hpa-gateway:extra-metric \
  hpa-worker:target hpa-worker:replicas hpa-worker:behavior hpa-worker:metric-name hpa-worker:metric-value hpa-worker:metric-type hpa-worker:extra-metric \
  ingress:backend ingress:backend-port ingress:rule-host ingress:tls-host ingress:tls-secret ingress:path ingress:class ingress:annotation ingress:extra-path \
  live-config:missing live-config:extra live-config:content deploy-metadata:trusted deploy-metadata:extra \
  live-gateway:metadata-label live-gateway:selector live-gateway:config-volume live-gateway:config-subpath live-gateway:config-mount live-gateway:config-key live-gateway:config-path live-gateway:extra-volume live-gateway:args live-gateway:container-port live-gateway:container-port-name live-gateway:container-port-protocol \
  live-worker:metadata-label live-worker:selector live-worker:config-volume live-worker:config-subpath live-worker:config-mount live-worker:config-key live-worker:config-path live-worker:extra-volume live-worker:args live-migrate:args \
  namespace:name default-deny:selector \
  gateway-ingress:selector gateway-ingress:source-namespace gateway-ingress:source-pod gateway-ingress:port gateway-ingress:extra \
  migrate-egress:selector migrate-egress:source migrate-egress:widen migrate-egress:cross-namespace migrate-egress:port migrate-egress:extra \
  application-egress:selector application-egress:widen application-egress:cross-namespace application-egress:oidc-port application-egress:extra application-egress:proxy-port \
  identity:duplicate-safe identity:duplicate-namespace identity:duplicate-configmap identity:duplicate-service \
  identity:duplicate-deployment identity:duplicate-job identity:duplicate-pdb identity:duplicate-hpa \
  identity:duplicate-ingress identity:duplicate-policy
do
  "$validator" --overlay production --readiness fixture --mutate "$mutation" "$scratch/production.yaml" >"$scratch/mutated.yaml"
  if "$validator" --overlay production --readiness fixture "$scratch/mutated.yaml" >/dev/null 2>&1; then
    echo "validator accepted mutation $mutation" >&2
    exit 1
  fi
done

for mutation in \
  live-gateway:static-secret live-worker:static-secret \
  service:selector service:port service:target-port service:protocol service:extra-port \
  pdb-gateway:selector pdb-gateway:minimum pdb-gateway:extra pdb-worker:selector pdb-worker:minimum pdb-worker:extra \
  hpa-gateway:target hpa-gateway:replicas hpa-gateway:behavior hpa-gateway:metric-name hpa-gateway:metric-value hpa-gateway:metric-type hpa-gateway:extra-metric \
  hpa-worker:target hpa-worker:replicas hpa-worker:behavior hpa-worker:metric-name hpa-worker:metric-value hpa-worker:metric-type hpa-worker:extra-metric \
  live-config:missing live-config:extra live-config:content deploy-metadata:trusted deploy-metadata:extra \
  live-gateway:metadata-label live-gateway:selector live-gateway:config-volume live-gateway:config-subpath live-gateway:config-mount live-gateway:config-key live-gateway:config-path live-gateway:extra-volume live-gateway:args live-gateway:container-port live-gateway:container-port-name live-gateway:container-port-protocol \
  live-worker:metadata-label live-worker:selector live-worker:config-volume live-worker:config-subpath live-worker:config-mount live-worker:config-key live-worker:config-path live-worker:extra-volume live-worker:args live-migrate:args \
  gateway-ingress:selector gateway-ingress:source-namespace gateway-ingress:source-pod gateway-ingress:port gateway-ingress:extra \
  application-egress:selector application-egress:widen application-egress:cross-namespace application-egress:oidc-port application-egress:extra \
  migrate-egress:widen migrate-egress:cross-namespace
do
  "$validator" --overlay local --readiness local --mutate "$mutation" "$scratch/local.yaml" >"$scratch/mutated-local.yaml"
  if "$validator" --overlay local --readiness local "$scratch/mutated-local.yaml" >/dev/null 2>&1; then
    echo "validator accepted local mutation $mutation" >&2
    exit 1
  fi
done

# The exact 255-byte distribution repository-name boundary remains valid in a
# rendered workload, while the 256-byte cases above fail.
"$validator" --overlay production --readiness fixture --mutate live-gateway:image-255 "$scratch/production.yaml" >"$scratch/image-255.yaml"
"$validator" --overlay production --readiness fixture "$scratch/image-255.yaml"

# The parser-backed split stage independently rejects shadow duplicates even if
# invoked outside the apply pipeline.
"$validator" --overlay production --readiness fixture --mutate identity:duplicate-safe "$scratch/production.yaml" >"$scratch/duplicate.yaml"
if "$validator" --overlay production --readiness fixture --split workloads "$scratch/duplicate.yaml" >/dev/null 2>&1; then
  echo "splitter accepted a duplicate Kubernetes object identity" >&2
  exit 1
fi

"$validator" --overlay production --readiness fixture --split job "$scratch/production.yaml" >"$scratch/split-job.yaml"
"$validator" --overlay production --readiness fixture --split workloads "$scratch/production.yaml" >"$scratch/split-workloads.yaml"
[ "$(rg -c '^kind: Job$' "$scratch/split-job.yaml")" -eq 1 ] || { echo "parser-backed job split is not exact" >&2; exit 1; }
workload_job_count=$(rg -c '^kind: Job$' "$scratch/split-workloads.yaml" || true)
[ "${workload_job_count:-0}" -eq 0 ] || { echo "parser-backed workload split retained Job" >&2; exit 1; }

# Unrendered production templates and repeated/dummy digests can never pass.
kubectl kustomize "$root/deploy/k8s/overlays/production" >"$scratch/unrendered.yaml"
if "$root/deploy/scripts/validate-render.sh" production deploy "$scratch/unrendered.yaml" >/dev/null 2>&1; then
  echo "validator accepted unrendered production inputs" >&2
  exit 1
fi
