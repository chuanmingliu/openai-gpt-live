# Database outage

Page when readiness fails, outbox lag rises, leases expire, or admission cannot persist. Confirm scope by region, instance, and config hash; do not log the connection value or query/customer content.

1. Freeze rollout and disable new handoffs through the authenticated runtime feature endpoint.
2. Keep the gateway authenticated; do not bypass durability or route clients to providers.
3. Verify PostgreSQL health, saturation, replication, storage, and network path. Prefer failover to the tested replica/restore procedure over ad hoc schema changes.
4. After recovery, run schema check, verify outbox and delivery cursors advance, allow expired leases to be reacquired under a newer fence, and watch duplicate suppression.
5. Re-enable handoffs only after readiness, synthetic isolation, lag, and error rate are stable for a full observation window.

If recovery exceeds the incident objective, follow rollback. Preserve content-free timestamps, config/image digests, alert series, and release evidence hashes.
