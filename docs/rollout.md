# Rollout

Every rollout is selected by immutable image digest and immutable configuration hash. Never mix an unreviewed hash into an existing canary cohort.

Before unsigned release evidence is collected, the protected candidate-canary
edge deploys the exact proposed digests/config, observes them for at least 30
minutes, evaluates the fixed halt policy, and rolls them back. Its verified
content-free platform receipt is part of the signed evidence bundle. This
pre-release proof does not replace the progressive cohort windows below.

1. Enable implemented shadow mode. It performs durable acceptance and policy/accounting paths while suppressing provider and client handoff presentation.
2. Internal cohort: observe for 30 minutes.
3. 1% canary: observe for 30 minutes.
4. 5% canary: observe for 30 minutes.
5. 25% canary: observe for 60 minutes.
6. 100% only after every prior window is clean.

At each stage compare the new immutable config hash against the last known-good hash by instance, provider, and region. Confirm synthetic isolation, readiness, admissions, audio p95/p99, handoff acknowledgements, presentation delay, queue depth/age/overflow, outbox lag, expired leases, error rate, goroutine/heap trend, and drain behavior.

Halt rollout on any stale output, cross-session leakage, two consecutive five-minute SLO breaches, or error-rate regression above one percentage point. Also halt if the synthetic canary stops publishing; absence is not success.

Rollback by disabling handoffs first, then deploy the last known-good authenticated gateway image. Keep workers available long enough to reconcile already durable work unless safety requires a full stop. Drain the rejected gateway hash, verify readiness and synthetic isolation on the restored hash, and preserve content-free release/incident evidence. Never expose speech-to-speech directly.
