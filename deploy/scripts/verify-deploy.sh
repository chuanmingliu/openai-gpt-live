#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
for tool in docker kubectl kubeconform kind; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing deployment prerequisite: $tool" >&2; exit 127; }
done

"$root/deploy/scripts/verify-deploy-static.sh"

for target in gateway worker migrate load; do
  docker build --target "$target" -t "gpt-live-$target:verify" "$root"
done

for target in gateway worker migrate load; do
  user=$(docker image inspect "gpt-live-$target:verify" --format '{{.Config.User}}')
  [ "$user" = 65532:65532 ] || { echo "$target image user is not numeric 65532:65532" >&2; exit 1; }
done
docker run --rm --read-only --cap-drop=ALL --security-opt=no-new-privileges gpt-live-gateway:verify check-config --config /etc/live/config.yaml
docker run --rm --read-only --cap-drop=ALL --security-opt=no-new-privileges gpt-live-worker:verify check-config --config /etc/live/config.yaml

scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-deploy-full.XXXXXX")
cluster=gpt-live-deploy-verify
cleanup() {
  kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup EXIT HUP INT TERM
if docker run --rm --read-only --cap-drop=ALL --security-opt=no-new-privileges gpt-live-load:verify >"$scratch/load.out" 2>&1; then
  echo "live-load unexpectedly accepted absent explicit arguments" >&2
  exit 1
else
  code=$?
  [ "$code" -eq 64 ] || { echo "live-load returned $code, expected configuration exit 64" >&2; exit 1; }
fi
grep -qx 'load configuration failed' "$scratch/load.out" || { echo "live-load content-free configuration message drifted" >&2; exit 1; }
"$root/deploy/scripts/render-kustomize.sh" local >"$scratch/local.yaml"
"$root/deploy/scripts/render-kustomize.sh" production >"$scratch/production.yaml"
"$root/deploy/scripts/validate-render.sh" local local "$scratch/local.yaml"
"$root/deploy/scripts/validate-render.sh" production deploy "$scratch/production.yaml"
kubeconform -strict -summary -kubernetes-version 1.35.0 "$scratch/local.yaml" "$scratch/production.yaml"
kind create cluster --name "$cluster" --wait 120s
kubectl apply -f "$root/deploy/k8s/base/namespace.yaml"
kubectl apply --server-side --dry-run=server -f "$scratch/local.yaml" >/dev/null
kubectl apply --server-side --dry-run=server -f "$scratch/production.yaml" >/dev/null

echo "full deployment verification passed"
