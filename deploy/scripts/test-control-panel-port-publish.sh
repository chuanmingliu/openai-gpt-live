#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
source_dir="$root/deploy/tests/control-panel-port-listener"
name="gpt-live-control-panel-port-$$"
image="gpt-live-control-panel-port:$$"

command -v docker >/dev/null 2>&1 || { echo "control-panel port publish test skipped: docker unavailable"; exit 0; }
docker info >/dev/null 2>&1 || { echo "control-panel port publish test skipped: docker daemon unavailable"; exit 0; }
command -v go >/dev/null 2>&1 || { echo "go is required for the control-panel port publish test" >&2; exit 127; }
[ -f "$source_dir/main.go" ] && [ -f "$source_dir/Dockerfile" ] || { echo "control-panel listener source is missing" >&2; exit 1; }
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-control-panel-port.XXXXXX")

cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker image rm -f "$image" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup EXIT HUP INT TERM

docker_arch=$(docker version --format '{{.Server.Arch}}')
case "$docker_arch" in
  amd64|arm64) go_arch=$docker_arch ;;
  x86_64) go_arch=amd64 ;;
  aarch64) go_arch=arm64 ;;
  *) echo "unsupported Docker architecture for control-panel port test" >&2; exit 1 ;;
esac
CGO_ENABLED=0 GOOS=linux GOARCH="$go_arch" go build -trimpath -ldflags='-s -w' -o "$scratch/listener" ./deploy/tests/control-panel-port-listener
docker build --pull=false -f "$source_dir/Dockerfile" -t "$image" "$scratch" >/dev/null

# This is deliberately provider-free and disposable. It proves the engine path
# used by the control-panel diagnostics: a container-wide listener remains
# reachable through a host-loopback-only publication.
docker run -d --rm --name "$name" -p 127.0.0.1::9090 "$image" >/dev/null

published=$(docker port "$name" 9090/tcp | sed -n '1p')
case "$published" in
  127.0.0.1:*) ;;
  *) echo "control-panel port publish test received unexpected publication" >&2; exit 1 ;;
esac

for attempt in 1 2 3 4 5 6 7 8 9 10; do
  if curl --fail --silent --output /dev/null --connect-timeout 0.2 --max-time 1 "http://$published/"; then
    echo "control-panel port publish test passed"
    exit 0
  fi
  sleep 0.1
done

echo "control-panel port publish listener was unreachable" >&2
exit 1
