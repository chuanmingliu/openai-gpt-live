# Voice provider outage

Confirm provider/region/config-hash scope from readiness, degraded-session events, connect failures, and latency. Do not send traffic directly to the provider or weaken authentication, origin, or egress controls.

1. Halt rollout and disable handoffs if durable presentation is affected.
2. Drain unhealthy gateway instances so new sessions choose healthy capacity; existing bounded reconnect may recover only within configured retry limits.
3. Check the reviewed egress proxy, DNS/TLS, provider quota/status, and circuit saturation without printing credentials or media.
4. If switching providers/config, create a new immutable hash and restart from shadow/internal rollout. Never mutate the active hash in place.
5. Require readiness, completed synthetic sessions, zero stale/leaked output, and stable SLO/error rate before restoring traffic.

If output ownership is uncertain, fence and close the session. Do not replay uncommitted audio.
