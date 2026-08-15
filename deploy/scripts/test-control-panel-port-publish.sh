#!/bin/sh
set -eu

image=${CONTROL_PANEL_PORT_TEST_IMAGE:-busybox:1.36}
name="gpt-live-control-panel-port-$$"

command -v docker >/dev/null 2>&1 || { echo "control-panel port publish test skipped: docker unavailable"; exit 0; }
docker info >/dev/null 2>&1 || { echo "control-panel port publish test skipped: docker daemon unavailable"; exit 0; }
# A runnable daemon must execute the regression rather than silently pass on a
# cold image cache. Pulling this fixed public test image never contacts an
# application provider or an operator's service.
docker image inspect "$image" >/dev/null 2>&1 || docker pull "$image" >/dev/null

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT HUP INT TERM

# This is deliberately provider-free and disposable. It proves the engine path
# used by the control-panel diagnostics: a container-wide listener remains
# reachable through a host-loopback-only publication.
docker run -d --rm --name "$name" -p 127.0.0.1::9090 "$image" \
  sh -c 'exec busybox httpd -f -p 0.0.0.0:9090' >/dev/null

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
