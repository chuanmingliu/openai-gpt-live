//go:build integration

package admission

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/auth"
	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/chuanmingliu/openai-gpt-live/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresAtomicCrossInstanceQuotaReleaseAndStaleRecovery(t *testing.T) {
	pool := testkit.Postgres(t)
	_, err := pool.Exec(t.Context(), `CREATE TABLE session_admission_leases(instance_id text NOT NULL,session_id text NOT NULL,tenant_id text NOT NULL,user_id text NOT NULL,expires_at timestamptz NOT NULL,heartbeat_at timestamptz NOT NULL,created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(instance_id,session_id),CHECK(heartbeat_at>=created_at AND expires_at>=heartbeat_at))`)
	require.NoError(t, err)
	one, err := NewPostgres(pool, PostgresConfig{InstanceID: "one", LeaseDuration: 200 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond, OperationTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, one.Close()) })
	two, err := NewPostgres(pool, PostgresConfig{InstanceID: "two", LeaseDuration: 200 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond, OperationTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, two.Close()) })
	subject := session.Subject{TenantID: "tenant", UserID: "user"}
	limits := Limits{Global: 1, PerTenant: 10, PerUser: 10}
	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)
	releases := make(chan ReleaseFunc, 2)
	for _, store := range []*Postgres{one, two} {
		go func(store *Postgres) {
			defer wg.Done()
			release, err := store.Acquire(t.Context(), subject, limits)
			results <- err
			if release != nil {
				releases <- release
			}
		}(store)
	}
	wg.Wait()
	close(results)
	var admitted, limited int
	for err := range results {
		if err == nil {
			admitted++
		} else if assert.ErrorIs(t, err, ErrLimited) {
			limited++
		}
	}
	assert.Equal(t, 1, admitted)
	assert.Equal(t, 1, limited)
	release := <-releases
	var releaseGroup sync.WaitGroup
	releaseGroup.Add(8)
	for range 8 {
		go func() { defer releaseGroup.Done(); release() }()
	}
	releaseGroup.Wait()
	next, err := two.Acquire(t.Context(), subject, limits)
	require.NoError(t, err)
	next()
	_, err = pool.Exec(t.Context(), `INSERT INTO session_admission_leases(instance_id,session_id,tenant_id,user_id,expires_at,heartbeat_at,created_at) VALUES('crashed','stale','tenant','user',transaction_timestamp()-interval '1 second',transaction_timestamp()-interval '2 seconds',transaction_timestamp()-interval '3 seconds')`)
	require.NoError(t, err)
	next, err = one.Acquire(t.Context(), subject, limits)
	require.NoError(t, err)
	next()
}

func TestPostgresTenantUserHeartbeatAndExactOwnership(t *testing.T) {
	pool := testkit.Postgres(t)
	_, err := pool.Exec(t.Context(), `CREATE TABLE session_admission_leases(instance_id text NOT NULL,session_id text NOT NULL,tenant_id text NOT NULL,user_id text NOT NULL,expires_at timestamptz NOT NULL,heartbeat_at timestamptz NOT NULL,created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(instance_id,session_id),CHECK(heartbeat_at>=created_at AND expires_at>=heartbeat_at))`)
	require.NoError(t, err)
	one, err := NewPostgres(pool, PostgresConfig{InstanceID: "one", LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 60 * time.Millisecond, OperationTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, one.Close()) })
	two, err := NewPostgres(pool, PostgresConfig{InstanceID: "two", LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 60 * time.Millisecond, OperationTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, two.Close()) })
	first, err := one.Acquire(t.Context(), session.Subject{TenantID: "tenant-a", UserID: "user-a"}, Limits{Global: 10, PerTenant: 1, PerUser: 10})
	require.NoError(t, err)
	defer first()
	_, err = two.Acquire(t.Context(), session.Subject{TenantID: "tenant-a", UserID: "user-b"}, Limits{Global: 10, PerTenant: 1, PerUser: 10})
	assert.ErrorIs(t, err, ErrLimited)
	otherTenant, err := two.Acquire(t.Context(), session.Subject{TenantID: "tenant-b", UserID: "user-a"}, Limits{Global: 10, PerTenant: 1, PerUser: 10})
	require.NoError(t, err)
	otherTenant()
	first()
	owned, err := one.Acquire(t.Context(), session.Subject{TenantID: "tenant-a", UserID: "user-a"}, Limits{Global: 10, PerTenant: 10, PerUser: 1})
	require.NoError(t, err)
	otherUser, err := two.Acquire(t.Context(), session.Subject{TenantID: "tenant-a", UserID: "user-b"}, Limits{Global: 10, PerTenant: 10, PerUser: 1})
	require.NoError(t, err)
	otherUser()
	_, err = two.Acquire(t.Context(), session.Subject{TenantID: "tenant-a", UserID: "user-a"}, Limits{Global: 10, PerTenant: 10, PerUser: 1})
	assert.ErrorIs(t, err, ErrLimited)
	var sessionID string
	var before time.Time
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT session_id,expires_at FROM session_admission_leases WHERE instance_id='one'`).Scan(&sessionID, &before))
	time.Sleep(90 * time.Millisecond)
	var after time.Time
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT expires_at FROM session_admission_leases WHERE instance_id='one' AND session_id=$1`, sessionID).Scan(&after))
	assert.True(t, after.After(before), "heartbeat must extend the owned lease")
	_, err = pool.Exec(t.Context(), `INSERT INTO session_admission_leases(instance_id,session_id,tenant_id,user_id,expires_at,heartbeat_at) VALUES('other',$1,'tenant-x','user-x',transaction_timestamp()+interval '1 minute',transaction_timestamp())`, sessionID)
	require.NoError(t, err)
	owned()
	var remaining int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM session_admission_leases WHERE instance_id='other' AND session_id=$1`, sessionID).Scan(&remaining))
	assert.Equal(t, 1, remaining, "release must not delete another instance's lease")
}

type commitFaultDB struct {
	leaseDB
	applied            bool
	cleanupFailures    atomic.Int32
	cleanupUnavailable atomic.Bool
	cleanupAttempts    atomic.Int32
}

type readinessFaultDB struct {
	leaseDB
	unavailable atomic.Bool
}

func (d *readinessFaultDB) Ping(ctx context.Context) error {
	if d.unavailable.Load() {
		return errors.New("injected readiness outage")
	}
	return d.leaseDB.Ping(ctx)
}

func (d *commitFaultDB) BeginTx(ctx context.Context) (leaseTx, error) {
	tx, err := d.leaseDB.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	return &commitFaultTx{leaseTx: tx, parent: d}, nil
}
func (d *commitFaultDB) Exec(ctx context.Context, sql string, args ...any) error {
	if strings.HasPrefix(strings.TrimSpace(sql), "DELETE FROM session_admission_leases WHERE instance_id") {
		d.cleanupAttempts.Add(1)
		if d.cleanupUnavailable.Load() || d.cleanupFailures.Add(-1) >= 0 {
			return errors.New("injected cleanup outage")
		}
	}
	return d.leaseDB.Exec(ctx, sql, args...)
}

type commitFaultTx struct {
	leaseTx
	parent *commitFaultDB
}

func (t *commitFaultTx) Commit(ctx context.Context) error {
	if t.parent.applied {
		if err := t.leaseTx.Commit(ctx); err != nil {
			return err
		}
	}
	return errors.New("injected commit acknowledgement loss")
}

func TestPostgresAmbiguousCommitReconcilesExactLease(t *testing.T) {
	for _, test := range []struct {
		name            string
		applied         bool
		cleanupFailures int32
	}{{"applied acknowledgement lost", true, 0}, {"unapplied", false, 0}, {"retained reconciliation", true, 2}} {
		t.Run(test.name, func(t *testing.T) {
			pool := testkit.Postgres(t)
			_, err := pool.Exec(t.Context(), `CREATE TABLE session_admission_leases(instance_id text NOT NULL,session_id text NOT NULL,tenant_id text NOT NULL,user_id text NOT NULL,expires_at timestamptz NOT NULL,heartbeat_at timestamptz NOT NULL,created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(instance_id,session_id),CHECK(heartbeat_at>=created_at AND expires_at>=heartbeat_at))`)
			require.NoError(t, err)
			_, err = pool.Exec(t.Context(), `INSERT INTO session_admission_leases(instance_id,session_id,tenant_id,user_id,expires_at,heartbeat_at) VALUES('other','preserved','tenant','user',clock_timestamp()+interval '1 minute',clock_timestamp())`)
			require.NoError(t, err)
			fault := &commitFaultDB{leaseDB: poolLeaseDB{pool}, applied: test.applied}
			fault.cleanupFailures.Store(test.cleanupFailures)
			store, err := newPostgres(fault, PostgresConfig{InstanceID: "faulted", LeaseDuration: 200 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond, OperationTimeout: time.Second, ReconcileInterval: 20 * time.Millisecond})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			release, err := store.Acquire(t.Context(), session.Subject{TenantID: "tenant", UserID: "user"}, Limits{Global: 10, PerTenant: 10, PerUser: 10})
			assert.ErrorIs(t, err, ErrUnavailable)
			assert.Nil(t, release)
			require.Eventually(t, func() bool {
				var count int
				return pool.QueryRow(t.Context(), `SELECT count(*) FROM session_admission_leases WHERE instance_id='faulted'`).Scan(&count) == nil && count == 0
			}, time.Second, 10*time.Millisecond)
			var preserved int
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM session_admission_leases WHERE instance_id='other' AND session_id='preserved'`).Scan(&preserved))
			assert.Equal(t, 1, preserved)
		})
	}
}

func TestPostgresAmbiguityFloodUsesBoundedOwnershipAndOneQueryStream(t *testing.T) {
	pool := testkit.Postgres(t)
	_, err := pool.Exec(t.Context(), `CREATE TABLE session_admission_leases(instance_id text NOT NULL,session_id text NOT NULL,tenant_id text NOT NULL,user_id text NOT NULL,expires_at timestamptz NOT NULL,heartbeat_at timestamptz NOT NULL,created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(instance_id,session_id),CHECK(heartbeat_at>=created_at AND expires_at>=heartbeat_at))`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO session_admission_leases(instance_id,session_id,tenant_id,user_id,expires_at,heartbeat_at) VALUES('other','preserved','tenant','user',clock_timestamp()+interval '1 minute',clock_timestamp())`)
	require.NoError(t, err)
	fault := &commitFaultDB{leaseDB: poolLeaseDB{pool}, applied: false}
	fault.cleanupUnavailable.Store(true)
	store, err := newPostgres(fault, PostgresConfig{InstanceID: "faulted", LeaseDuration: 400 * time.Millisecond, HeartbeatInterval: 100 * time.Millisecond, OperationTimeout: time.Second, ReconcileInterval: 20 * time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	limits := Limits{Global: 4, PerTenant: 4, PerUser: 4}
	var group sync.WaitGroup
	for range 64 {
		group.Add(1)
		go func() {
			defer group.Done()
			release, acquireErr := store.Acquire(t.Context(), session.Subject{TenantID: "tenant", UserID: "user"}, limits)
			assert.ErrorIs(t, acquireErr, ErrUnavailable)
			assert.Nil(t, release)
		}()
	}
	group.Wait()
	retained := pendingReconciliations(store)
	require.Positive(t, retained)
	require.LessOrEqual(t, retained, 4, "the unresolved set must never exceed the global lease maximum")
	assert.ErrorIs(t, store.Ready(t.Context()), ErrUnavailable)
	fault.cleanupAttempts.Store(0)
	time.Sleep(125 * time.Millisecond)
	assert.LessOrEqual(t, fault.cleanupAttempts.Load(), int32(8), "cleanup retries must use one globally rate-limited query stream")

	fault.cleanupUnavailable.Store(false)
	store.wakeReconciler()
	require.Eventually(t, func() bool { return pendingReconciliations(store) == 0 }, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, store.Ready(t.Context()))
	var preserved int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM session_admission_leases WHERE instance_id='other' AND session_id='preserved'`).Scan(&preserved))
	assert.Equal(t, 1, preserved)
}

func TestPostgresCloseFinalDrainCleansAppliedAmbiguity(t *testing.T) {
	pool := testkit.Postgres(t)
	_, err := pool.Exec(t.Context(), `CREATE TABLE session_admission_leases(instance_id text NOT NULL,session_id text NOT NULL,tenant_id text NOT NULL,user_id text NOT NULL,expires_at timestamptz NOT NULL,heartbeat_at timestamptz NOT NULL,created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(instance_id,session_id),CHECK(heartbeat_at>=created_at AND expires_at>=heartbeat_at))`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `INSERT INTO session_admission_leases(instance_id,session_id,tenant_id,user_id,expires_at,heartbeat_at) VALUES('other','preserved','tenant','user',clock_timestamp()+interval '1 minute',clock_timestamp())`)
	require.NoError(t, err)
	fault := &commitFaultDB{leaseDB: poolLeaseDB{pool}, applied: true}
	fault.cleanupUnavailable.Store(true)
	store, err := newPostgres(fault, PostgresConfig{
		InstanceID: "faulted", LeaseDuration: 400 * time.Millisecond, HeartbeatInterval: 100 * time.Millisecond,
		OperationTimeout: time.Second, ReconcileInterval: 20 * time.Millisecond, ShutdownTimeout: time.Second,
	})
	require.NoError(t, err)
	release, err := store.Acquire(t.Context(), session.Subject{TenantID: "tenant", UserID: "user"}, Limits{Global: 2, PerTenant: 2, PerUser: 2})
	assert.ErrorIs(t, err, ErrUnavailable)
	assert.Nil(t, release)
	var owned int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM session_admission_leases WHERE instance_id='faulted'`).Scan(&owned))
	require.Equal(t, 1, owned)

	fault.cleanupUnavailable.Store(false)
	require.NoError(t, store.Close())
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM session_admission_leases WHERE instance_id='faulted'`).Scan(&owned))
	assert.Zero(t, owned)
	var preserved int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM session_admission_leases WHERE instance_id='other' AND session_id='preserved'`).Scan(&preserved))
	assert.Equal(t, 1, preserved)
}

func TestControllerPostgresReadinessOutageAndRecovery(t *testing.T) {
	pool := testkit.Postgres(t)
	_, err := pool.Exec(t.Context(), `CREATE TABLE session_admission_leases(instance_id text NOT NULL,session_id text NOT NULL,tenant_id text NOT NULL,user_id text NOT NULL,expires_at timestamptz NOT NULL,heartbeat_at timestamptz NOT NULL,created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(instance_id,session_id),CHECK(heartbeat_at>=created_at AND expires_at>=heartbeat_at))`)
	require.NoError(t, err)
	fault := &readinessFaultDB{leaseDB: poolLeaseDB{pool}}
	store, err := newPostgres(fault, PostgresConfig{InstanceID: "readiness", LeaseDuration: 200 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond, OperationTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	static := auth.NewStatic([]byte("development-token"), auth.Principal{TenantID: "tenant", UserID: "user", Scopes: map[string]struct{}{"realtime": {}}})
	controller, err := New(static, Config{RequireTLS: true, AllowedOrigins: []string{"https://app.example"}, GlobalRate: 100, GlobalBurst: 10, IPRate: 100, IPBurst: 10, IPKeyTTL: time.Minute, MaxIPKeys: 10, RequiredScopes: []string{"realtime"}, Limits: Limits{Global: 10, PerTenant: 10, PerUser: 10}, RequireStore: true}, store)
	require.NoError(t, err)
	request := func() *http.Request {
		r := validRequest()
		r.Header.Set("Authorization", "Bearer development-token")
		return r
	}
	_, release, err := controller.Admit(t.Context(), request())
	require.NoError(t, err)
	release()
	fault.unavailable.Store(true)
	_, _, err = controller.Admit(t.Context(), request())
	assert.ErrorIs(t, err, ErrUnavailable)
	fault.unavailable.Store(false)
	_, release, err = controller.Admit(t.Context(), request())
	require.NoError(t, err)
	release()
}
