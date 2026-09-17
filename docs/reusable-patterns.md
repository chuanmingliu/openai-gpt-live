# Reusable Patterns from GPT Live Go

> A distillation of the three ideas in this repository that are portable on their own —
> separable from the ~80k lines of framework that defend them.
>
> Everything below is quoted from the actual source, lightly trimmed (elisions marked "...")
> and re-fenced with tildes for readability. Source: *github.com/chuanmingliu/openai-gpt-live*,
> Go 1.26, PostgreSQL 16.

---

## Why these three

The codebase is a production-grade realtime-voice + durable-reasoning framework. Most of its
80k lines are *defense*: bounded queues, redacting telemetry, admission quotas, release gates.
The *ideas* underneath are three, and each is small enough to copy into a different system
without the surrounding machinery:

| # | Pattern | Answers | Mechanism |
|---|---------|---------|-----------|
| 1 | Epoch fence | "May this event reach the user *right now*?" | A monotonic counter + one equality check, in a single serialized goroutine |
| 2 | Lease CAS | "Who owns this job, and is their view still current?" | A SQL WHERE clause carrying every ownership guard |
| 3 | Two-key kill switch | "How do I emergency-stop a feature without a redeploy, *and* without runtime state silently overriding reviewed config?" | A static config flag AND a versioned DB flag that must both be true |

They share one philosophy: **a single serialized authority plus a cheap equality/CAS check
beats optimistic locking with reconciliation.**

---

## Pattern 1 — The Epoch Fence

**Context.** A realtime voice session has one provider socket producing events (audio deltas,
transcripts, *response.created*) and one client consuming them. When the user barges in, or
sends *response.cancel*, any *in-flight* provider output must be dropped — even though it
arrives later on the wire. The problem is "late-event suppression," and the classic mistakes
are sequence-number bookkeeping, per-event dedup, or cancel-then-reconcile loops.

**The reduction.** None of that. A single atomic counter is the authority. Barge-in/cancel
don't cancel anything; they *advance the fence*. Every inbound provider event carries an
*Epoch*; it is admitted only if it matches the fence *and* the active response, evaluated in
one goroutine.

### The fence (the whole type)

~~~go
// internal/session/epoch.go
type EpochFence struct { value atomic.Uint64 }

func NewEpochFence(initial identity.ResponseEpoch) *EpochFence {
    fence := &EpochFence{}
    fence.value.Store(uint64(initial))
    return fence
}
func (f *EpochFence) Current() identity.ResponseEpoch { return identity.ResponseEpoch(f.value.Load()) }
func (f *EpochFence) Advance() identity.ResponseEpoch { return identity.ResponseEpoch(f.value.Add(1)) }
func (f *EpochFence) Accept(epoch identity.ResponseEpoch) bool { return f != nil && f.Current() == epoch }
~~~

### The admission predicate

~~~go
// internal/session/reactor.go — currentResponseEvent
// An event is "current" only if ALL FOUR match. Streamed output is dropped otherwise.
func (r *Reactor) currentResponseEvent(event realtime.Event) bool {
    return event.Epoch != 0 &&
        event.Epoch == r.fence.Current() &&
        event.Epoch == r.activeResponseEpoch &&
        event.ResponseID != "" &&
        event.ResponseID == r.activeResponseID
}
~~~

### Cancel advances the fence, then flushes, then commands

~~~go
// applyResponseCancel (trimmed)
epoch := r.fence.Advance()               // 1. advance BEFORE anything else can arrive
r.activeResponseEpoch = epoch
command := realtime.Command{Kind: realtime.CommandCancelResponse, Epoch: epoch, ...}
r.pendingCancelSequence = command.Sequence
r.state.Response = ResponseCancelRequested
effects := []Effect{{FlushBefore: epoch}, {VoiceCommand: &command}}
~~~

The *FlushBefore: epoch* effect tells the egress writer: "emit queued audio *up to* this
boundary, then stop." So buffered audio completes cleanly, but nothing from the old epoch
leaks past it.

### Barge-in is the same move, triggered by speech

~~~go
// applySpeechStarted (trimmed) — user starts talking over the model
epoch := r.fence.Advance()               // barge-in == advance the fence
r.state.UserSpeaking = true
effects := []Effect{{FlushBefore: epoch}}
if r.state.Response == ResponsePending || r.state.Response == ResponseStreaming {
    r.activeResponseEpoch = epoch
    cancel := realtime.Command{Kind: realtime.CommandCancelResponse, Epoch: epoch, ...}
    r.pendingCancelSequence = cancel.Sequence
    r.state.Response = ResponseCancelRequested
    effects = append(effects, Effect{VoiceCommand: &cancel})
}
~~~

### A stale *response.created* is silently dropped

~~~go
// applyExplicitResponseCreated (trimmed)
if event.Epoch != r.fence.Current() { return nil, nil }   // late response created is discarded
if r.state.Response != ResponsePending { return nil, fmt.Errorf(...) }
...
r.state.Response = ResponseStreaming
r.activeResponseID = event.ResponseID
r.activeResponseEpoch = event.Epoch
~~~

### The one hard case is *repaired*, never weakened

The simulator has an implicit-VAD bug: it emits *response.created* with no preceding create
command, i.e. an event with no epoch binding. The reactor does **not** trust it — it
*re-fences* it by advancing the epoch and sending a bind command back to the provider:

~~~go
// applyImplicitResponseCreated (trimmed)
epoch := r.fence.Advance()
r.state.Response = ResponseStreaming
r.activeResponseID = event.ResponseID
r.activeResponseEpoch = epoch
bind := realtime.Command{Kind: realtime.CommandBindResponseEpoch, Epoch: epoch, ResponseID: event.ResponseID}
~~~

### How to reuse it

1. Give every producer event a monotonically increasing *epoch* tag (or let the consumer
   assign one at admission).
2. Serialize state mutation through one goroutine (channel-in / effects-out).
3. Replace "cancel" with "advance the epoch." Drop any event whose epoch != current.
4. If a producer can emit unbounded events, repair by *binding* the straggler to a fresh
   epoch — never by weakening the equality check.

This pattern is why the realtime path needs no distributed consensus and no idempotent-dedup
keyed by sequence: the equality check *is* the dedup.

---

## Pattern 2 — Lease CAS (fenced durable jobs)

**Context.** A pool of workers must execute durable background jobs (here: DeepSeek
reasoning). Requirements: at-least-once execution, no two workers presenting the same
result, a crashed worker's late write must be impossible, and *steer*/cancel must beat a
racing generation. The classic answer is a message broker or a distributed lock. Here it is
one PostgreSQL table and a disciplined WHERE clause.

### Claim: lock + skip + compare-and-swap

~~~sql
-- LeaseNext: pick the highest-priority eligible row and atomically claim it
SELECT h.tenant_id, h.conversation_id, h.id, h.state, h.attempt, h.lease_version
FROM handoffs h
WHERE h.cancel_requested_at IS NULL AND h.deadline > $1
  AND (
    (h.state IN ('pending','running') AND h.lease_owner IS NULL AND h.ready_at <= $1) OR
    (h.state IN ('leased','running')  AND h.lease_owner IS NOT NULL AND h.lease_expires_at <= $1) OR
    (h.state = 'needs_input' AND h.input_revision > h.consumed_input_revision
       AND (h.lease_owner IS NULL OR h.lease_expires_at <= $1))
  )
ORDER BY h.priority DESC, h.ready_at, h.created_at, ...
FOR UPDATE OF h SKIP LOCKED LIMIT 1
~~~

*SKIP LOCKED* means concurrent workers claim *distinct* rows — no contention, no retry
storm. Then the claim is a compare-and-swap that **increments both *attempt* and
*lease_version***:

~~~sql
UPDATE handoffs SET state=$4, attempt=$5, lease_version=$6, lease_owner=$7,
    lease_expires_at=$8, ...
WHERE tenant_id=$2 AND conversation_id=$3 AND id=$9
~~~

If *RowsAffected != 1*, the caller returns *ErrLeaseLost*.

### Heartbeat: renew *and* receive steering in one round-trip

The same heartbeat that renews the lease also reports cancellation and steering, guarded by
**five simultaneous conditions**:

~~~sql
UPDATE handoffs SET lease_expires_at = GREATEST(lease_expires_at, $1), updated_at = $2
WHERE tenant_id=$3 AND conversation_id=$4 AND id=$5
  AND lease_owner=$6 AND lease_version=$7 AND attempt=$8
  AND state IN ('leased','running') AND lease_expires_at > $2
RETURNING cancel_requested_at IS NOT NULL,
          objective, objective_revision, input_revision, consumed_input_revision, input
~~~

Zero rows → *ErrLeaseLost* (a newer owner took over, or the lease expired). The
*RETURNING* carries *objective_revision* / *input_revision* so a *steer* shows up as a
revision change the worker must honor. Every terminal write (*Complete*, *Fail*,
*MarkCancelled*, *AppendProgress*, *MarkNeedsInput*) repeats the **same guard set** plus
*objective_revision = $N* — so a steered job's stale generation physically cannot commit.

### Retry: bounded full jitter, deadline-aware, Retry-After-honoring

~~~go
// internal/worker/retry.go — RetryPolicy.Next (trimmed)
if retryCount >= p.MaxRetries { return time.Time{}, false, nil }
bound := exponentialDuration(p.Base, retryCount)   // base << retryCount
if bound > p.Maximum { bound = p.Maximum }
delay := jitter.Duration(bound)                     // full jitter in [0, bound]
if retryAfter > delay { delay = retryAfter }        // honor Retry-After only if longer
ready := now.Add(delay)
if ready.Before(now) || !ready.Before(deadline) { return time.Time{}, false, nil }
~~~

And *ScheduleRetry* *releases the lease* (*lease_owner=NULL, lease_expires_at=NULL*) with a
future *ready_at* — a retry is a fresh claim, never a sleep-and-hold.

### How to reuse it

1. Give each job three fencing columns: *lease_owner*, *lease_version*, *attempt*, plus a
   monotonic *objective_revision* (or *input_revision*) if jobs can be steered.
2. Claim with *FOR UPDATE SKIP LOCKED LIMIT 1* and a CAS UPDATE that increments the
   version. Treat *RowsAffected == 0* as "lost."
3. Put every ownership guard in every mutation's WHERE — owner, version, attempt,
   unexpired lease, no cancel, exact revision.
4. Retry by *releasing* the lease, not by holding it. Honor *Retry-After* only inside the
   job deadline.

The whole "at-least-once without double-presentation" property falls out of PostgreSQL
MVCC + one disciplined WHERE. No broker, no lock service, no consensus.

---

## Pattern 3 — The Two-Key Kill Switch

**Context.** You want to ship a feature (here: durable handoffs) but be able to disable it
instantly in production — *and* you want a paper trail, *and* you don't want a runtime flag
to silently override a reviewed, source-controlled decision. One flag does not satisfy all
three; two flags, each with a different authority, do.

### Key 1 — static capability (source-reviewed config)

~~~yaml
# deploy/config/production.yaml (template)
features:
  handoffs: true            # reviewed, source-controlled, part of the config hash
  side_effecting_tools: false
~~~

This gate is compiled into the process's view of itself and — critically — **feeds the config
hash**, so enabling it is a reviewed change that fails drift validation if only one copy is
edited. Local mode *refuses* to run with *handoffs: true* (it lacks the production durable
coordinator), so you cannot accidentally test production-only behavior in dev.

### Key 2 — runtime authority (PostgreSQL, versioned, audited)

The DB holds a singleton authority row and a flags table, both versioned, plus an
**append-only audit table**:

~~~sql
-- migrations/000004_runtime_flags.up.sql
CREATE TABLE runtime_feature_authority (
    singleton boolean PRIMARY KEY DEFAULT true,
    version   bigint  NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT runtime_feature_authority_singleton_check CHECK (singleton),
    CONSTRAINT runtime_feature_authority_version_check CHECK (version > 0)
);
CREATE TABLE runtime_feature_flags (
    name    text PRIMARY KEY,       -- CHECK (name IN ('handoffs_enabled','shadow_mode'))
    enabled boolean NOT NULL,
    version bigint  NOT NULL,
    ...
);
CREATE TABLE runtime_feature_audit (
    version bigint PRIMARY KEY, name text, enabled boolean,
    actor text, reason text, changed_at timestamptz, ...
);
-- append-only enforcement: a trigger raises on UPDATE/DELETE/TRUNCATE
INSERT INTO runtime_feature_authority (singleton, version) VALUES (true, 1);
INSERT INTO runtime_feature_flags (name, enabled, version) VALUES
    ('handoffs_enabled', true, 1), ('shadow_mode', false, 1);
~~~

### Mutation: one serialized writer, one version bump, one audit row

~~~go
// internal/feature/postgres.go — Set (trimmed)
// The singleton row lock is THE writer serialization point.
tx, _ := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
tx.QueryRow(ctx, "SELECT version FROM runtime_feature_authority WHERE singleton=true FOR UPDATE").Scan(&previous)
next := previous + 1
tx.Exec(ctx, "UPDATE runtime_feature_authority SET version=$1 WHERE singleton=true", next)
tx.Exec(ctx, "UPDATE runtime_feature_flags SET enabled=$1, version=$2 WHERE name=$3", enabled, next, name)
tx.Exec(ctx, "INSERT INTO runtime_feature_audit(version,name,enabled,actor,reason) VALUES($1,$2,$3,$4,$5)",
        next, name, enabled, actor, reason)
tx.Exec(ctx, "SELECT pg_notify($1,$2)", featureNotificationChannel, payload)
~~~

Readers never mutate; they load an **immutable snapshot** via an atomic pointer and apply
only a *strictly newer, complete, valid* view:

~~~go
// internal/feature/gates.go — Gates.Apply (trimmed)
if !snapshot.Valid() { return false }                  // must carry BOTH values + version>0
for {
    current := g.state.Load()
    if current != nil && snapshot.Version <= current.version { return false }  // strictly newer only
    next := &gateState{version: snapshot.Version, values: cloneValues(snapshot.Values)}
    if g.state.CompareAndSwap(current, next) { g.healthy.Store(true); return true }
}
~~~

*LISTEN/NOTIFY* (*pg_notify*) is only a wake-up hint; the gate also polls every
*runtime_refresh_interval* and loads once at startup, so missed notifications self-heal.

### The enforcement point: BOTH must be true

Handoff tool injection requires *features.handoffs: true* **and** the runtime
*handoffs_enabled* flag. Disabling either one — redeploying with the flag off, or flipping
the DB flag via the audited admin API — stops the feature. The two authorities are
*independent*, so neither can silently override the other:

- Runtime flag off → feature off **without a redeploy** (emergency kill switch).
- Config flag off → feature off **regardless of DB state** (reviewed, source-controlled floor).

### How to reuse it

1. Ship features behind a static config flag that participates in your config hash (so
   enabling is a reviewed, drift-checked change).
2. Mirror each flag into a DB table with a **singleton version row + append-only audit**.
3. Serialize writes on the singleton row lock (*SELECT ... FOR UPDATE*), bump the version,
   write the audit row, *NOTIFY* — atomically, in one transaction.
4. Readers consume immutable, versioned snapshots and reject non-monotonic versions.
5. Gate the feature on the logical AND of both keys.

This is the correct answer to "emergency stop without redeploy, *and* without runtime state
silently overriding reviewed config."

---

## What to skip

Deliberately **not** portable, and not recommended to copy wholesale:

- The 12-gate release flow with detached Ed25519 signatures and "content-free evidence" —
  defensible for a *regulated* deployment, friction for an adopter.
- The config-hash fail-closed immutability — powerful, but it punishes manual intervention
  and trains operators to fear the system.
- The empty tool registry and the "no browser auth" stance — these are *product* scoping
  decisions, not architecture.

The durable value of this repository is these three patterns plus its audit trail; the
framework is the (impressive) scaffolding that proves them.
