# Queue overload

Any bounded queue overflow is a release halt. Identify queue, component, instance, provider, region, and config hash. Compare depth, bytes, oldest age, block duration, admissions, provider latency, outbox lag, CPU, heap, and goroutines.

1. Stop rollout and shed new sessions through normal admission; never enlarge a queue live or remove its bound.
2. Disable handoffs first if background work or presentation contributes.
3. Drain the affected hash and add known-good capacity within existing admission and database limits.
4. Resolve the downstream bottleneck: slow clients, speech provider, reasoner, database/outbox, or worker capacity.
5. Require zero overflow, recovered queue age/depth, synthetic isolation, and two clean five-minute SLO windows before resuming at the prior canary stage.

Changing queue limits requires reviewed configuration, a new immutable hash, load evidence, and rollout from shadow mode.
