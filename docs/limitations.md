# Limitations

- The cascaded simulator is not a native full-duplex voice model and cannot provide GPT-Live's model-native mid-generation audio safety.
- The local simulator's implicit-VAD flow can emit audio before `response.created`; the contract suite treats that ordering as an upstream external blocker rather than claiming compatibility.
- This is an explicit OpenAI Realtime subset plus documented `live.*` extensions, not a drop-in implementation of every upstream event or modality.
- Native Kubernetes NetworkPolicy cannot authorize external destinations by DNS name. Production provider traffic must traverse the reviewed egress proxy.
- Exactly-once external side effects are not assumed. Durable idempotency, approvals, lease fencing, and reconciliation provide at-most-once framework effects within their documented boundaries.
- Load latency thresholds are release claims only on the pinned self-hosted runner profile. Short capacity/soak runs are smoke evidence only.
- Live-provider tests are opt-in and require explicit credentials. A skipped or absent provider suite is never promoted to release evidence.
