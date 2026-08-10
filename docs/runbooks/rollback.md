# Rollback

Trigger on any stale output, cross-session leakage, two consecutive five-minute SLO breaches, error-rate regression above one percentage point, missing synthetic canary, or a security/safety uncertainty.

1. Disable handoffs first with the authenticated runtime feature control and record the committed feature version.
2. Stop rollout routing to the rejected immutable config hash.
3. Deploy the last known-good authenticated gateway image by digest and its matching config hash. Never expose speech-to-speech directly.
4. Drain rejected gateway pods; let workers reconcile durable work only while lease fencing and safety remain trusted.
5. Verify readiness, synthetic isolation, zero stale output/overflow, database cursors/outbox, error rate, and SLO windows.
6. Preserve content-free metrics, image/config digests, evidence hashes, and operator actions. Do not capture customer media or content.

Restart rollout at shadow mode only after root cause, fix, full release evidence, independent human privacy attestation, and incident review.
