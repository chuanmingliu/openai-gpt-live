package admission

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type reconcileTestDB struct {
	mu        sync.Mutex
	rows      map[leaseKey]bool
	outage    bool
	attempts  int
	active    int
	maxActive int
}

func (d *reconcileTestDB) Ping(context.Context) error { return nil }
func (d *reconcileTestDB) BeginTx(context.Context) (leaseTx, error) {
	return nil, errors.New("unexpected transaction")
}
func (d *reconcileTestDB) Exec(ctx context.Context, sql string, args ...any) error {
	if !strings.HasPrefix(strings.TrimSpace(sql), "DELETE FROM session_admission_leases WHERE instance_id") {
		return errors.New("unexpected statement")
	}
	key := leaseKey{instanceID: args[0].(string), sessionID: args[1].(string)}
	d.mu.Lock()
	d.attempts++
	d.active++
	if d.active > d.maxActive {
		d.maxActive = d.active
	}
	d.mu.Unlock()
	select {
	case <-ctx.Done():
		d.mu.Lock()
		d.active--
		d.mu.Unlock()
		return ctx.Err()
	case <-time.After(time.Millisecond):
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active--
	if d.outage {
		return errors.New("injected cleanup outage")
	}
	delete(d.rows, key)
	return nil
}
func (d *reconcileTestDB) QueryRow(_ context.Context, _ string, args ...any) scanRow {
	key := leaseKey{instanceID: args[0].(string), sessionID: args[1].(string)}
	d.mu.Lock()
	count := 0
	if d.rows[key] {
		count = 1
	}
	d.mu.Unlock()
	return intRow(count)
}

type intRow int

func (r intRow) Scan(dest ...any) error {
	*(dest[0].(*int)) = int(r)
	return nil
}

func pendingReconciliations(p *Postgres) int {
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	return len(p.pending)
}

type blockingReconcileDB struct {
	*reconcileTestDB
	entered chan struct{}
}

type acquireRaceDB struct {
	*reconcileTestDB
	commitStarted chan struct{}
	startOnce     sync.Once
	apply         bool
}

func (d *acquireRaceDB) BeginTx(context.Context) (leaseTx, error) {
	return &acquireRaceTx{parent: d}, nil
}

type acquireRaceTx struct {
	parent *acquireRaceDB
	key    leaseKey
}

func (t *acquireRaceTx) Exec(_ context.Context, sql string, args ...any) error {
	if strings.HasPrefix(strings.TrimSpace(sql), "INSERT INTO session_admission_leases") {
		t.key = leaseKey{instanceID: args[0].(string), sessionID: args[1].(string)}
	}
	return nil
}
func (*acquireRaceTx) QueryRow(_ context.Context, sql string, _ ...any) scanRow {
	if strings.Contains(sql, "clock_timestamp()") {
		return timeRow{value: time.Now()}
	}
	return countsRow{}
}
func (t *acquireRaceTx) Commit(ctx context.Context) error {
	t.parent.startOnce.Do(func() { close(t.parent.commitStarted) })
	<-ctx.Done()
	if t.parent.apply {
		t.parent.mu.Lock()
		t.parent.rows[t.key] = true
		t.parent.mu.Unlock()
	}
	return errors.New("injected commit acknowledgement loss")
}
func (*acquireRaceTx) Rollback(context.Context) error { return nil }

type timeRow struct{ value time.Time }

func (r timeRow) Scan(dest ...any) error {
	*(dest[0].(*time.Time)) = r.value
	return nil
}

type countsRow struct{}

func (countsRow) Scan(dest ...any) error {
	for _, value := range dest {
		*(value.(*int)) = 0
	}
	return nil
}

type heartbeatTestDB struct {
	mu      sync.Mutex
	results []scanRow
	deletes int
}

func (*heartbeatTestDB) Ping(context.Context) error { return nil }
func (*heartbeatTestDB) BeginTx(context.Context) (leaseTx, error) {
	return nil, errors.New("unexpected transaction")
}
func (d *heartbeatTestDB) Exec(_ context.Context, sql string, _ ...any) error {
	if !strings.HasPrefix(strings.TrimSpace(sql), "DELETE FROM session_admission_leases") {
		return errors.New("unexpected statement")
	}
	d.mu.Lock()
	d.deletes++
	d.mu.Unlock()
	return nil
}
func (d *heartbeatTestDB) QueryRow(context.Context, string, ...any) scanRow {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.results) == 0 {
		return errorRow{err: errors.New("heartbeat outage")}
	}
	result := d.results[0]
	d.results = d.results[1:]
	return result
}

type errorRow struct{ err error }

func (r errorRow) Scan(...any) error { return r.err }

type heartbeatTimeRow struct{ expiresAt, databaseNow time.Time }

func (r heartbeatTimeRow) Scan(dest ...any) error {
	*(dest[0].(*time.Time)) = r.expiresAt
	*(dest[1].(*time.Time)) = r.databaseNow
	return nil
}

type delayedHeartbeatTimeRow struct {
	heartbeatTimeRow
	delay time.Duration
}

func (r delayedHeartbeatTimeRow) Scan(dest ...any) error {
	time.Sleep(r.delay)
	return r.heartbeatTimeRow.Scan(dest...)
}

func TestPostgresMonitoredHeartbeatFailsClosedAtConfirmedExpiryAndRecoversBriefOutage(t *testing.T) {
	t.Run("uncertainty cancels by the last confirmed deadline", func(t *testing.T) {
		db := &heartbeatTestDB{}
		store, err := newPostgres(db, PostgresConfig{InstanceID: "store", LeaseDuration: 120 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond, OperationTimeout: 10 * time.Millisecond})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		started := time.Now()
		lease := store.maintain("lease", started.Add(120*time.Millisecond), started, started)
		defer lease.Release()
		require.Eventually(t, func() bool { return errors.Is(store.Ready(t.Context()), ErrUnavailable) }, 80*time.Millisecond, 5*time.Millisecond)
		select {
		case <-lease.Lost:
		case <-time.After(150 * time.Millisecond):
			t.Fatal("lease loss was not reported before the confirmed database lease could expire")
		}
	})

	t.Run("one heartbeat outage is tolerated when authority recovers", func(t *testing.T) {
		now := time.Now()
		db := &heartbeatTestDB{results: []scanRow{
			errorRow{err: errors.New("brief outage")},
			heartbeatTimeRow{expiresAt: now.Add(240 * time.Millisecond), databaseNow: now.Add(120 * time.Millisecond)},
			heartbeatTimeRow{expiresAt: now.Add(270 * time.Millisecond), databaseNow: now.Add(150 * time.Millisecond)},
		}}
		store, err := newPostgres(db, PostgresConfig{InstanceID: "store", LeaseDuration: 120 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond, OperationTimeout: 10 * time.Millisecond})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		lease := store.maintain("lease", now.Add(120*time.Millisecond), now, now)
		defer lease.Release()
		require.Eventually(t, func() bool { return errors.Is(store.Ready(t.Context()), ErrUnavailable) }, 80*time.Millisecond, 5*time.Millisecond)
		require.Eventually(t, func() bool { return store.Ready(t.Context()) == nil }, 80*time.Millisecond, 5*time.Millisecond)
		select {
		case <-lease.Lost:
			t.Fatal("brief recovered outage must not revoke the lease")
		case <-time.After(20 * time.Millisecond):
		}
	})

	t.Run("zero updated rows is immediate definitive loss", func(t *testing.T) {
		db := &heartbeatTestDB{results: []scanRow{errorRow{err: pgx.ErrNoRows}}}
		store, err := newPostgres(db, PostgresConfig{InstanceID: "store", LeaseDuration: 120 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond, OperationTimeout: 10 * time.Millisecond})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		started := time.Now()
		lease := store.maintain("lease", started.Add(120*time.Millisecond), started, started)
		defer lease.Release()
		select {
		case <-lease.Lost:
		case <-time.After(70 * time.Millisecond):
			t.Fatal("zero-row heartbeat did not revoke the lease immediately")
		}
	})

	t.Run("a delayed success cannot extend authority during the following outage", func(t *testing.T) {
		started := time.Now()
		databaseNow := started.Add(100 * time.Millisecond)
		db := &heartbeatTestDB{results: []scanRow{
			delayedHeartbeatTimeRow{
				heartbeatTimeRow: heartbeatTimeRow{expiresAt: databaseNow.Add(400 * time.Millisecond), databaseNow: databaseNow},
				delay:            150 * time.Millisecond,
			},
		}}
		store, err := newPostgres(db, PostgresConfig{
			InstanceID: "store", LeaseDuration: 400 * time.Millisecond, HeartbeatInterval: 100 * time.Millisecond,
			OperationTimeout: 250 * time.Millisecond,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		lease := store.maintain("lease", started.Add(400*time.Millisecond), started, started)
		defer lease.Release()
		select {
		case <-lease.Lost:
			assert.Less(t, time.Since(started), 480*time.Millisecond,
				"response latency must not be added to the last database-confirmed authority window")
		case <-time.After(480 * time.Millisecond):
			t.Fatal("delayed heartbeat response extended local authority past the conservative deadline")
		}
	})
}

func (d *blockingReconcileDB) Exec(ctx context.Context, sql string, args ...any) error {
	select {
	case d.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestPostgresReconcilerIsBoundedDeduplicatedAndCancellable(t *testing.T) {
	shared := leaseKey{instanceID: "store", sessionID: "shared"}
	foreign := leaseKey{instanceID: "other", sessionID: "shared"}
	db := &reconcileTestDB{rows: map[leaseKey]bool{shared: true, foreign: true}, outage: true}
	store, err := newPostgres(db, PostgresConfig{
		InstanceID: "store", LeaseDuration: 200 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond,
		OperationTimeout: 40 * time.Millisecond, ReconcileInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	deadline := time.Now().Add(240 * time.Millisecond)

	var retained atomic.Int32
	var group sync.WaitGroup
	for range 200 {
		group.Add(1)
		go func() {
			defer group.Done()
			if store.reserveReconciliation(4) {
				retained.Add(1)
				store.retainAmbiguous(shared, deadline)
			}
		}()
	}
	group.Wait()
	require.Positive(t, retained.Load())
	require.Equal(t, 1, pendingReconciliations(store), "duplicate exact keys must share one retained entry")

	for _, sessionID := range []string{"two", "three", "four"} {
		key := leaseKey{instanceID: "store", sessionID: sessionID}
		db.mu.Lock()
		db.rows[key] = true
		db.mu.Unlock()
		require.True(t, store.reserveReconciliation(4))
		store.retainAmbiguous(key, deadline)
	}
	require.Equal(t, 4, pendingReconciliations(store))
	assert.False(t, store.reserveReconciliation(4), "saturated ownership must fail closed before another ambiguous commit")
	assert.ErrorIs(t, store.Ready(t.Context()), ErrUnavailable)

	time.Sleep(125 * time.Millisecond)
	db.mu.Lock()
	attemptsDuringOutage := db.attempts
	maxActive := db.maxActive
	db.outage = false
	db.mu.Unlock()
	assert.LessOrEqual(t, attemptsDuringOutage, 8, "one scheduler must globally rate-limit cleanup queries")
	assert.Equal(t, 1, maxActive, "cleanup attempts must never run concurrently")
	store.wakeReconciler()
	require.Eventually(t, func() bool { return pendingReconciliations(store) == 0 }, time.Second, 5*time.Millisecond)
	require.NoError(t, store.Ready(t.Context()))
	db.mu.Lock()
	assert.True(t, db.rows[foreign], "exact cleanup must preserve a foreign instance's lease")
	db.mu.Unlock()

	require.NoError(t, store.Close())
	select {
	case <-store.reconcileDone:
	default:
		t.Fatal("Close returned before the reconciler stopped")
	}
	assert.ErrorIs(t, store.Ready(t.Context()), ErrUnavailable)
	assert.False(t, store.reserveReconciliation(4))
}

func TestPostgresReconcilerCapsUnappliedAmbiguityFloodAndExpiresOutageState(t *testing.T) {
	db := &reconcileTestDB{rows: make(map[leaseKey]bool), outage: true}
	store, err := newPostgres(db, PostgresConfig{
		InstanceID: "store", LeaseDuration: 80 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond,
		OperationTimeout: 20 * time.Millisecond, ReconcileInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	deadline := time.Now().Add(100 * time.Millisecond)
	for i := range 3 {
		require.True(t, store.reserveReconciliation(3))
		store.retainAmbiguous(leaseKey{instanceID: "store", sessionID: string(rune('a' + i))}, deadline)
	}
	for range 100 {
		assert.False(t, store.reserveReconciliation(3))
	}
	require.Equal(t, 3, pendingReconciliations(store))
	assert.ErrorIs(t, store.Ready(t.Context()), ErrUnavailable)
	require.Eventually(t, func() bool { return pendingReconciliations(store) == 0 }, 300*time.Millisecond, 5*time.Millisecond,
		"unconfirmed entries must be forgotten no later than lease expiry plus the operation bound")
	require.NoError(t, store.Ready(t.Context()))
}

func TestPostgresCloseReturnsBoundedErrorForBlockedReconciliation(t *testing.T) {
	db := &blockingReconcileDB{
		reconcileTestDB: &reconcileTestDB{rows: make(map[leaseKey]bool)},
		entered:         make(chan struct{}, 1),
	}
	store, err := newPostgres(db, PostgresConfig{
		InstanceID: "store", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		OperationTimeout: 40 * time.Millisecond, ReconcileInterval: 10 * time.Millisecond, ShutdownTimeout: 40 * time.Millisecond,
	})
	require.NoError(t, err)
	require.True(t, store.reserveReconciliation(1))
	store.retainAmbiguous(leaseKey{instanceID: "store", sessionID: "blocked"}, time.Now().Add(time.Second))
	select {
	case <-db.entered:
	case <-time.After(time.Second):
		t.Fatal("reconciler did not begin cleanup")
	}
	started := time.Now()
	closeErr := store.Close()
	assert.Less(t, time.Since(started), 150*time.Millisecond)
	assert.ErrorIs(t, closeErr, ErrShutdownIncomplete)
	assert.ErrorIs(t, store.Ready(t.Context()), ErrUnavailable)
}

func TestPostgresCloseCleansPendingAppliedAmbiguity(t *testing.T) {
	owned := leaseKey{instanceID: "store", sessionID: "owned"}
	foreign := leaseKey{instanceID: "other", sessionID: "owned"}
	db := &reconcileTestDB{rows: map[leaseKey]bool{owned: true, foreign: true}, outage: true}
	store, err := newPostgres(db, PostgresConfig{
		InstanceID: "store", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		OperationTimeout: 20 * time.Millisecond, ReconcileInterval: 10 * time.Millisecond, ShutdownTimeout: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	require.True(t, store.reserveReconciliation(1))
	store.retainAmbiguous(owned, time.Now().Add(time.Second))
	require.Eventually(t, func() bool {
		db.mu.Lock()
		defer db.mu.Unlock()
		return db.attempts > 0
	}, time.Second, time.Millisecond)
	db.mu.Lock()
	db.outage = false
	db.mu.Unlock()
	require.NoError(t, store.Close())
	db.mu.Lock()
	assert.False(t, db.rows[owned])
	assert.True(t, db.rows[foreign])
	attemptsAfterClose := db.attempts
	db.mu.Unlock()
	time.Sleep(30 * time.Millisecond)
	db.mu.Lock()
	assert.Equal(t, attemptsAfterClose, db.attempts, "the joined worker must not query after Close returns")
	db.mu.Unlock()
}

func TestPostgresCloseJoinsAmbiguousCommitOutcome(t *testing.T) {
	for _, test := range []struct {
		name  string
		apply bool
	}{
		{name: "applied acknowledgement loss", apply: true},
		{name: "unapplied ambiguity", apply: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			foreign := leaseKey{instanceID: "other", sessionID: "preserved"}
			db := &acquireRaceDB{
				reconcileTestDB: &reconcileTestDB{rows: map[leaseKey]bool{foreign: true}},
				commitStarted:   make(chan struct{}),
				apply:           test.apply,
			}
			store, err := newPostgres(db, PostgresConfig{
				InstanceID: "store", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
				OperationTimeout: 100 * time.Millisecond, ReconcileInterval: 10 * time.Millisecond, ShutdownTimeout: 100 * time.Millisecond,
			})
			require.NoError(t, err)
			acquireResult := make(chan error, 1)
			go func() {
				_, acquireErr := store.Acquire(context.Background(), testSubject(), Limits{Global: 1, PerTenant: 1, PerUser: 1})
				acquireResult <- acquireErr
			}()
			select {
			case <-db.commitStarted:
			case <-time.After(time.Second):
				t.Fatal("Acquire did not reach Commit")
			}
			require.NoError(t, store.Close())
			assert.ErrorIs(t, <-acquireResult, ErrUnavailable)
			db.mu.Lock()
			for key := range db.rows {
				assert.NotEqual(t, "store", key.instanceID, "late exact ambiguity must be cleaned before Close returns")
			}
			assert.True(t, db.rows[foreign])
			db.mu.Unlock()
			assert.Zero(t, pendingReconciliations(store))
		})
	}
}

func TestPostgresCloseOutageIsBoundedAndFailClosed(t *testing.T) {
	owned := leaseKey{instanceID: "store", sessionID: "owned"}
	foreign := leaseKey{instanceID: "other", sessionID: "owned"}
	db := &reconcileTestDB{rows: map[leaseKey]bool{owned: true, foreign: true}, outage: true}
	store, err := newPostgres(db, PostgresConfig{
		InstanceID: "store", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		OperationTimeout: 20 * time.Millisecond, ReconcileInterval: 10 * time.Millisecond, ShutdownTimeout: 30 * time.Millisecond,
	})
	require.NoError(t, err)
	require.True(t, store.reserveReconciliation(1))
	store.retainAmbiguous(owned, time.Now().Add(time.Second))
	started := time.Now()
	closeErr := store.Close()
	assert.ErrorIs(t, closeErr, ErrShutdownIncomplete)
	assert.Less(t, time.Since(started), 100*time.Millisecond)
	assert.ErrorIs(t, store.Ready(t.Context()), ErrUnavailable)
	assert.ErrorIs(t, store.Close(), ErrShutdownIncomplete)
	_, acquireErr := store.Acquire(t.Context(), testSubject(), Limits{Global: 1, PerTenant: 1, PerUser: 1})
	assert.ErrorIs(t, acquireErr, ErrUnavailable)
	db.mu.Lock()
	assert.True(t, db.rows[owned], "failed final cleanup must explicitly fall back to lease expiry")
	assert.True(t, db.rows[foreign], "exact cleanup must never touch a foreign owner")
	db.mu.Unlock()
}

func TestPostgresConcurrentCloseIsIdempotentAndDoesNotLeakWorkers(t *testing.T) {
	baseline := runtime.NumGoroutine()
	for range 20 {
		db := &reconcileTestDB{rows: make(map[leaseKey]bool)}
		store, err := newPostgres(db, PostgresConfig{
			InstanceID: "store", LeaseDuration: 100 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond,
			OperationTimeout: 20 * time.Millisecond, ReconcileInterval: 10 * time.Millisecond, ShutdownTimeout: 20 * time.Millisecond,
		})
		require.NoError(t, err)
		var group sync.WaitGroup
		errorsSeen := make(chan error, 32)
		for range 32 {
			group.Add(1)
			go func() {
				defer group.Done()
				errorsSeen <- store.Close()
			}()
		}
		group.Wait()
		close(errorsSeen)
		for closeErr := range errorsSeen {
			require.NoError(t, closeErr)
		}
		_, acquireErr := store.Acquire(t.Context(), testSubject(), Limits{Global: 1, PerTenant: 1, PerUser: 1})
		assert.ErrorIs(t, acquireErr, ErrUnavailable)
	}
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline+4 }, time.Second, 10*time.Millisecond)
}

func testSubject() session.Subject {
	return session.Subject{TenantID: "tenant", UserID: "user"}
}
