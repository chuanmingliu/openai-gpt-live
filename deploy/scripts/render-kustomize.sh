#!/bin/sh
set -eu
if [ "$#" -ne 1 ] || { [ "$1" != local ] && [ "$1" != production ]; }; then
  echo "usage: render-kustomize.sh local|production" >&2
  exit 64
fi
overlay=$1
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
for tool in go kubectl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing deployment prerequisite: $tool" >&2; exit 127; }
done
if [ "$overlay" = local ]; then
  kubectl kustomize "$root/deploy/k8s/overlays/local" | sed 's|PRODUCTION_DEPLOYMENT_REGION|local|g'
  exit
fi

readiness=${LIVE_PRODUCTION_RENDER_MODE:-deploy}
if [ "$readiness" = fixture ]; then
  # Reserved values are allowed only in an explicitly labeled, never-applied
  # fixture render used by static/schema tests.
  LIVE_PRODUCTION_HOST=gpt-live.fixture.invalid
  LIVE_PRODUCTION_TLS_SECRET=fixture-tls
  LIVE_PRODUCTION_TRUSTED_PROXY_CIDR=198.51.100.0/25
  LIVE_PRODUCTION_EGRESS_PROXY_CIDR=203.0.113.0/25
  LIVE_PRODUCTION_VOICE_URL=wss://voice.fixture.invalid/v1/realtime
  LIVE_PRODUCTION_OIDC_ISSUER=https://oidc.fixture.invalid
  LIVE_DEPLOYMENT_REGION=us-east-1
  LIVE_PRODUCTION_GATEWAY_IMAGE=registry.fixture.invalid/gpt-live/gateway@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3
  LIVE_PRODUCTION_WORKER_IMAGE=registry.fixture.invalid/gpt-live/worker@sha256:49e907719fa53f1b00e43d8721620f42979c31117ed46daa3fadf320aef94b4e
  LIVE_PRODUCTION_MIGRATE_IMAGE=registry.fixture.invalid/gpt-live/migrate@sha256:1488a529509b2d9d0135713e75afdd3b3730b2884add211c6267980c87275078
  export LIVE_PRODUCTION_HOST LIVE_PRODUCTION_TLS_SECRET LIVE_PRODUCTION_TRUSTED_PROXY_CIDR LIVE_PRODUCTION_EGRESS_PROXY_CIDR LIVE_PRODUCTION_VOICE_URL LIVE_PRODUCTION_OIDC_ISSUER LIVE_DEPLOYMENT_REGION LIVE_PRODUCTION_GATEWAY_IMAGE LIVE_PRODUCTION_WORKER_IMAGE LIVE_PRODUCTION_MIGRATE_IMAGE
elif [ "$readiness" != deploy ]; then
  echo "LIVE_PRODUCTION_RENDER_MODE must be deploy or fixture" >&2
  exit 64
fi

cache=${TMPDIR:-/tmp}/gpt-live-task23-go-cache
(cd "$root" && GOCACHE="$cache" go run ./deploy/validate --check-inputs --readiness "$readiness")
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-production-render.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

substitute() {
  sed \
    -e "s|LIVE_PRODUCTION_HOST|$LIVE_PRODUCTION_HOST|g" \
    -e "s|LIVE_PRODUCTION_TLS_SECRET|$LIVE_PRODUCTION_TLS_SECRET|g" \
    -e "s|LIVE_PRODUCTION_TRUSTED_PROXY_CIDR|$LIVE_PRODUCTION_TRUSTED_PROXY_CIDR|g" \
    -e "s|LIVE_PRODUCTION_EGRESS_PROXY_CIDR|$LIVE_PRODUCTION_EGRESS_PROXY_CIDR|g" \
    -e "s|LIVE_PRODUCTION_VOICE_URL|$LIVE_PRODUCTION_VOICE_URL|g" \
    -e "s|LIVE_PRODUCTION_OIDC_ISSUER|$LIVE_PRODUCTION_OIDC_ISSUER|g" \
    -e "s|PRODUCTION_DEPLOYMENT_REGION|$LIVE_DEPLOYMENT_REGION|g" \
    -e "s|LIVE_PRODUCTION_GATEWAY_IMAGE|$LIVE_PRODUCTION_GATEWAY_IMAGE|g" \
    -e "s|LIVE_PRODUCTION_WORKER_IMAGE|$LIVE_PRODUCTION_WORKER_IMAGE|g" \
    -e "s|LIVE_PRODUCTION_MIGRATE_IMAGE|$LIVE_PRODUCTION_MIGRATE_IMAGE|g"
}

substitute <"$root/deploy/config/production.yaml" >"$scratch/config.yaml"
(cd "$root" && GOCACHE="$cache" go run ./cmd/live-worker check-config --config "$scratch/config.yaml")
config_hash=$(cd "$root" && GOCACHE="$cache" go run ./cmd/live-worker config-hash --config "$scratch/config.yaml")
kubectl kustomize "$root/deploy/k8s/overlays/production" >"$scratch/raw.yaml"
substitute <"$scratch/raw.yaml" | sed \
  -e "s|PRODUCTION_CONFIG_HASH|$config_hash|g" \
  -e "s|PRODUCTION_RENDER_READINESS|$readiness|g"
