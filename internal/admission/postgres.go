package admission

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/session"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresConfig struct {
	InstanceID string
	LeaseDuration, HeartbeatInterval, OperationTimeout, ReconcileInterval,
	ShutdownTimeout time.Duration
}

var ErrShutdownIncomplete = errors.New("admission store shutdown could not confirm exact lease cleanup")

type postgresState uint8

const (
	postgresOpen postgresState = iota
	postgresClosing
	postgresClosed
)

type Postgres struct {
	pool leaseDB
	cfg  PostgresConfig

	reconcileCtx    context.Context
	reconcileCancel context.CancelFunc
	reconcileWake   chan struct{}
	reconcileDone   chan struct{}
	reconcileMu     sync.Mutex
	pending         map[leaseKey]reconcileEntry
	uncertain       map[leaseKey]time.Time
	pendingCap      int
	reservations    int
	state           postgresState
	drainRequested  bool
	drainDeadline   time.Time
	closeErr        error

	acquireCtx    context.Context
	acquireCancel context.CancelFunc
	acquisitions  sync.WaitGroup
}

type leaseKey struct {
	instanceID string
	sessionID  string
}

type reconcileEntry struct {
	deadline time.Time
	next     time.Time
	backoff  time.Duration
}

type scanRow interface{ Scan(...any) error }
type leaseDB interface {
	Ping(context.Context) error
	BeginTx(context.Context) (leaseTx, error)
	Exec(context.Context, string, ...any) error
	QueryRow(context.Context, string, ...any) scanRow
}
type leaseTx interface {
	Exec(context.Context, string, ...any) error
	QueryRow(context.Context, string, ...any) scanRow
	Commit(context.Context) error
	Rollback(context.Context) error
}
type poolLeaseDB struct{ pool *pgxpool.Pool }

func (p poolLeaseDB) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }
func (p poolLeaseDB) BeginTx(ctx context.Context) (leaseTx, error) {
	// The transaction-wide advisory lock supplies serialization. Read committed
	// ensures a waiter takes its quota snapshot after the preceding lock owner
	// commits rather than retaining a snapshot from before the lock wait.
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	return pgxLeaseTx{tx}, nil
}
func (p poolLeaseDB) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := p.pool.Exec(ctx, sql, args...)
	return err
}
func (p poolLeaseDB) QueryRow(ctx context.Context, sql string, args ...any) scanRow {
	return p.pool.QueryRow(ctx, sql, args...)
}

type pgxLeaseTx struct{ pgx.Tx }

func (t pgxLeaseTx) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := t.Tx.Exec(ctx, sql, args...)
	return err
}
func (t pgxLeaseTx) QueryRow(ctx context.Context, sql string, args ...any) scanRow {
	return t.Tx.QueryRow(ctx, sql, args...)
}

func NewPostgres(pool *pgxpool.Pool, cfg PostgresConfig) (*Postgres, error) {
	if pool == nil {
		return nil, ErrInvalidConfig
	}
	return newPostgres(poolLeaseDB{pool}, cfg)
}
func newPostgres(pool leaseDB, cfg PostgresConfig) (*Postgres, error) {
	if pool == nil || !validID(cfg.InstanceID) || cfg.LeaseDuration < 30*time.Millisecond || cfg.LeaseDuration > 24*time.Hour || cfg.HeartbeatInterval < 10*time.Millisecond || cfg.HeartbeatInterval*2 >= cfg.LeaseDuration {
		return nil, ErrInvalidConfig
	}
	if cfg.OperationTimeout == 0 {
		cfg.OperationTimeout = 3 * time.Second
	}
	if cfg.OperationTimeout < time.Millisecond || cfg.OperationTimeout > time.Minute {
		return nil, ErrInvalidConfig
	}
	if cfg.ReconcileInterval == 0 {
		cfg.ReconcileInterval = cfg.HeartbeatInterval
	}
	if cfg.ReconcileInterval < 10*time.Millisecond || cfg.ReconcileInterval > time.Minute {
		return nil, ErrInvalidConfig
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = cfg.OperationTimeout
	}
	if cfg.ShutdownTimeout < time.Millisecond || cfg.ShutdownTimeout > 5*time.Minute {
		return nil, ErrInvalidConfig
	}
	reconcileCtx, reconcileCancel := context.WithCancel(context.Background())
	acquireCtx, acquireCancel := context.WithCancel(context.Background())
	p := &Postgres{
		pool:            pool,
		cfg:             cfg,
		reconcileCtx:    reconcileCtx,
		reconcileCancel: reconcileCancel,
		reconcileWake:   make(chan struct{}, 1),
		reconcileDone:   make(chan struct{}),
		pending:         make(map[leaseKey]reconcileEntry),
		uncertain:       make(map[leaseKey]time.Time),
		acquireCtx:      acquireCtx,
		acquireCancel:   acquireCancel,
	}
	go p.reconcileLoop()
	return p, nil
}

func RandomInstanceID() (string, error) {
	var value [24]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "gateway-" + base64.RawURLEncoding.EncodeToString(value[:]), nil
}
func validID(value string) bool {
	if value == "" || len(value) > 256 || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func (p *Postgres) Ready(ctx context.Context) error {
	if p == nil || p.pool == nil {
		return ErrUnavailable
	}
	p.reconcileMu.Lock()
	unavailable := p.state != postgresOpen || len(p.pending) > 0 || len(p.uncertain) > 0 || (p.pendingCap > 0 && len(p.pending)+p.reservations >= p.pendingCap)
	p.reconcileMu.Unlock()
	if unavailable {
		return ErrUnavailable
	}
	check, cancel := context.WithTimeout(ctx, p.cfg.OperationTimeout)
	defer cancel()
	return p.pool.Ping(check)
}

func (p *Postgres) Acquire(ctx context.Context, subject session.Subject, limits Limits) (ReleaseFunc, error) {
	lease, err := p.acquire(ctx, subject, limits)
	return lease.Release, err
}

func (p *Postgres) AcquireMonitored(ctx context.Context, subject session.Subject, limits Limits) (MonitoredLease, error) {
	return p.acquire(ctx, subject, limits)
}

func (p *Postgres) acquire(ctx context.Context, subject session.Subject, limits Limits) (MonitoredLease, error) {
	if p == nil || p.pool == nil || !validID(subject.TenantID) || !validID(subject.UserID) || limits.Global <= 0 || limits.PerTenant <= 0 || limits.PerUser <= 0 {
		return MonitoredLease{}, ErrUnavailable
	}
	if !p.beginAcquire() {
		return MonitoredLease{}, ErrUnavailable
	}
	defer p.acquisitions.Done()
	operationStarted := time.Now()
	opCtx, cancel := context.WithTimeout(ctx, p.cfg.OperationTimeout)
	stopClosing := context.AfterFunc(p.acquireCtx, cancel)
	defer stopClosing()
	defer cancel()
	sessionID, err := RandomInstanceID()
	if err != nil {
		return MonitoredLease{}, ErrUnavailable
	}
	tx, err := p.pool.BeginTx(opCtx)
	if err != nil {
		return MonitoredLease{}, ErrUnavailable
	}
	committed := false
	defer func() {
		if !committed {
			rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), p.cfg.OperationTimeout)
			defer rollbackCancel()
			_ = tx.Rollback(rollbackCtx)
		}
	}()
	if err = tx.Exec(opCtx, `SELECT pg_advisory_xact_lock(716773219507)`); err != nil {
		return MonitoredLease{}, ErrUnavailable
	}
	var databaseNow time.Time
	if err = tx.QueryRow(opCtx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return MonitoredLease{}, ErrUnavailable
	}
	if err = tx.Exec(opCtx, `DELETE FROM session_admission_leases WHERE expires_at <= $1`, databaseNow); err != nil {
		return MonitoredLease{}, ErrUnavailable
	}
	var global, tenant, user int
	err = tx.QueryRow(opCtx, `SELECT count(*), count(*) FILTER (WHERE tenant_id=$1), count(*) FILTER (WHERE tenant_id=$1 AND user_id=$2) FROM session_admission_leases WHERE expires_at > $3`, subject.TenantID, subject.UserID, databaseNow).Scan(&global, &tenant, &user)
	if err != nil {
		return MonitoredLease{}, ErrUnavailable
	}
	if global >= limits.Global || tenant >= limits.PerTenant || user >= limits.PerUser {
		return MonitoredLease{}, ErrLimited
	}
	if !p.reserveReconciliation(limits.Global) {
		return MonitoredLease{}, ErrUnavailable
	}
	reserved := true
	defer func() {
		if reserved {
			p.releaseReservation()
		}
	}()
	err = tx.Exec(opCtx, `INSERT INTO session_admission_leases(instance_id,session_id,tenant_id,user_id,expires_at,heartbeat_at) VALUES($1,$2,$3,$4,$5,$6)`, p.cfg.InstanceID, sessionID, subject.TenantID, subject.UserID, databaseNow.Add(p.cfg.LeaseDuration), databaseNow)
	if err != nil {
		return MonitoredLease{}, ErrUnavailable
	}
	if err = tx.Commit(opCtx); err != nil {
		p.retainAmbiguous(leaseKey{instanceID: p.cfg.InstanceID, sessionID: sessionID}, databaseNow.Add(p.cfg.LeaseDuration+p.cfg.OperationTimeout))
		reserved = false
		return MonitoredLease{}, ErrUnavailable
	}
	committed = true
	p.releaseReservation()
	reserved = false
	return p.maintain(sessionID, databaseNow.Add(p.cfg.LeaseDuration), databaseNow, operationStarted), nil
}

func (p *Postgres) beginAcquire() bool {
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	if p.state != postgresOpen || len(p.pending) > 0 || len(p.uncertain) > 0 {
		return false
	}
	p.acquisitions.Add(1)
	return true
}

func (p *Postgres) maintain(sessionID string, confirmedExpiry, databaseNow, operationStarted time.Time) MonitoredLease {
	key := leaseKey{instanceID: p.cfg.InstanceID, sessionID: sessionID}
	stop := make(chan struct{})
	done := make(chan struct{})
	lost := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		ticker := time.NewTicker(p.cfg.HeartbeatInterval)
		defer ticker.Stop()
		remaining := confirmedExpiry.Sub(databaseNow) - p.cfg.HeartbeatInterval
		if remaining < 0 {
			remaining = 0
		}
		lossDeadline := operationStarted.Add(remaining)
		untilLoss := time.Until(lossDeadline)
		if untilLoss < 0 {
			untilLoss = 0
		}
		lossTimer := time.NewTimer(untilLoss)
		defer lossTimer.Stop()
		resetLossTimer := func() {
			if !lossTimer.Stop() {
				select {
				case <-lossTimer.C:
				default:
				}
			}
			untilLoss := time.Until(lossDeadline)
			if untilLoss < 0 {
				untilLoss = 0
			}
			lossTimer.Reset(untilLoss)
		}
		for {
			select {
			case <-stop:
				return
			case <-lossTimer.C:
				close(lost)
				return
			case <-ticker.C:
				heartbeatStarted := time.Now()
				queryDeadline := heartbeatStarted.Add(p.cfg.OperationTimeout)
				if lossDeadline.Before(queryDeadline) {
					queryDeadline = lossDeadline
				}
				ctx, cancel := context.WithDeadline(context.Background(), queryDeadline)
				var nextExpiry, databaseNow time.Time
				err := p.pool.QueryRow(ctx, `WITH lease_clock AS (SELECT clock_timestamp() AS at), updated AS (
					UPDATE session_admission_leases SET heartbeat_at=lease_clock.at, expires_at=lease_clock.at+$3::interval
					FROM lease_clock WHERE instance_id=$1 AND session_id=$2 AND expires_at>lease_clock.at
					RETURNING expires_at
				) SELECT updated.expires_at, lease_clock.at FROM updated CROSS JOIN lease_clock`,
					p.cfg.InstanceID, sessionID, p.cfg.LeaseDuration.String()).Scan(&nextExpiry, &databaseNow)
				cancel()
				if err == nil {
					confirmedExpiry = nextExpiry
					remaining := nextExpiry.Sub(databaseNow) - p.cfg.HeartbeatInterval
					if remaining < 0 {
						remaining = 0
					}
					lossDeadline = heartbeatStarted.Add(remaining)
					if !time.Now().Before(lossDeadline) {
						p.markHeartbeatUncertain(key, confirmedExpiry)
						close(lost)
						return
					}
					p.clearHeartbeatUncertainty(key)
					resetLossTimer()
					continue
				}
				if errors.Is(err, pgx.ErrNoRows) {
					p.clearHeartbeatUncertainty(key)
					close(lost)
					return
				}
				p.markHeartbeatUncertain(key, confirmedExpiry)
			}
		}
	}()
	release := func() {
		once.Do(func() {
			close(stop)
			<-done
			ctx, cancel := context.WithTimeout(context.Background(), p.cfg.OperationTimeout)
			defer cancel()
			_ = p.pool.Exec(ctx, `DELETE FROM session_admission_leases WHERE instance_id=$1 AND session_id=$2`, p.cfg.InstanceID, sessionID)
			p.clearHeartbeatUncertainty(key)
		})
	}
	return MonitoredLease{Release: release, Lost: lost}
}

func (p *Postgres) markHeartbeatUncertain(key leaseKey, confirmedExpiry time.Time) {
	p.reconcileMu.Lock()
	p.uncertain[key] = confirmedExpiry
	p.reconcileMu.Unlock()
}

func (p *Postgres) clearHeartbeatUncertainty(key leaseKey) {
	p.reconcileMu.Lock()
	delete(p.uncertain, key)
	p.reconcileMu.Unlock()
}

func (p *Postgres) reserveReconciliation(globalLimit int) bool {
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	if p.state == postgresClosed {
		return false
	}
	owned := len(p.pending) + p.reservations
	if p.pendingCap == 0 {
		p.pendingCap = globalLimit
	} else if globalLimit < p.pendingCap {
		if owned >= globalLimit {
			return false
		}
		p.pendingCap = globalLimit
	}
	if owned >= p.pendingCap {
		return false
	}
	p.reservations++
	return true
}

func (p *Postgres) releaseReservation() {
	p.reconcileMu.Lock()
	if p.reservations > 0 {
		p.reservations--
	}
	p.reconcileMu.Unlock()
	p.wakeReconciler()
}

func (p *Postgres) retainAmbiguous(key leaseKey, deadline time.Time) {
	p.reconcileMu.Lock()
	if p.reservations > 0 {
		p.reservations--
	}
	if existing, ok := p.pending[key]; ok {
		if deadline.Before(existing.deadline) {
			existing.deadline = deadline
		}
		p.pending[key] = existing
	} else if p.state != postgresClosed && len(p.pending) < p.pendingCap {
		p.pending[key] = reconcileEntry{deadline: deadline, next: time.Now(), backoff: p.cfg.ReconcileInterval}
	}
	p.reconcileMu.Unlock()
	p.wakeReconciler()
}

func (p *Postgres) wakeReconciler() {
	select {
	case p.reconcileWake <- struct{}{}:
	default:
	}
}

func (p *Postgres) reconcileLoop() {
	defer close(p.reconcileDone)
	var nextAttempt time.Time
	for {
		if p.finalDrainRequested() {
			p.finalDrain()
			return
		}
		key, due, ok := p.nextReconciliation(nextAttempt)
		if !ok {
			select {
			case <-p.reconcileCtx.Done():
				return
			case <-p.reconcileWake:
				continue
			}
		}
		wait := time.Until(due)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-p.reconcileCtx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-p.reconcileWake:
				if !timer.Stop() {
					<-timer.C
				}
				continue
			case <-timer.C:
			}
		}
		deadline, ok := p.reconciliationDeadline(key, time.Now())
		if !ok {
			continue
		}
		clean := p.cleanupExact(key, deadline)
		attemptedAt := time.Now()
		nextAttempt = attemptedAt.Add(p.cfg.ReconcileInterval)
		p.finishReconciliation(key, clean, attemptedAt)
	}
}

func (p *Postgres) finalDrainRequested() bool {
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	return p.drainRequested
}

func (p *Postgres) finalDrain() {
	p.reconcileMu.Lock()
	keys := make([]leaseKey, 0, len(p.pending))
	for key := range p.pending {
		keys = append(keys, key)
	}
	deadline := p.drainDeadline
	p.reconcileMu.Unlock()

	for i, key := range keys {
		remaining := time.Until(deadline)
		keysLeft := len(keys) - i
		keyDeadline := time.Now()
		if remaining > 0 {
			keyDeadline = keyDeadline.Add(remaining / time.Duration(keysLeft))
		}
		ctx, cancel := context.WithDeadline(context.Background(), keyDeadline)
		clean := p.cleanupExactContext(ctx, key)
		cancel()
		if clean {
			p.reconcileMu.Lock()
			delete(p.pending, key)
			p.reconcileMu.Unlock()
		}
	}

	p.reconcileMu.Lock()
	if len(p.pending) > 0 || p.reservations > 0 {
		p.closeErr = ErrShutdownIncomplete
	}
	p.state = postgresClosed
	p.reconcileCancel()
	p.reconcileMu.Unlock()
}

func (p *Postgres) nextReconciliation(nextAttempt time.Time) (leaseKey, time.Time, bool) {
	now := time.Now()
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	var selected leaseKey
	var due time.Time
	for key, entry := range p.pending {
		if p.state == postgresOpen && !now.Before(entry.deadline) {
			delete(p.pending, key)
			continue
		}
		candidate := entry.next
		if candidate.Before(nextAttempt) {
			candidate = nextAttempt
		}
		if p.state == postgresOpen && entry.deadline.Before(candidate) {
			candidate = entry.deadline
		} else if p.state == postgresClosing && candidate.Before(now) {
			candidate = now
		}
		if due.IsZero() || candidate.Before(due) {
			selected, due = key, candidate
		}
	}
	return selected, due, !due.IsZero()
}

func (p *Postgres) reconciliationDeadline(key leaseKey, now time.Time) (time.Time, bool) {
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	entry, ok := p.pending[key]
	if !ok {
		return time.Time{}, false
	}
	if !now.Before(entry.deadline) {
		if p.state == postgresOpen {
			delete(p.pending, key)
			return time.Time{}, false
		}
		return now.Add(p.cfg.OperationTimeout), true
	}
	return entry.deadline, true
}

func (p *Postgres) finishReconciliation(key leaseKey, clean bool, attemptedAt time.Time) {
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	entry, ok := p.pending[key]
	if !ok {
		return
	}
	if clean || (p.state == postgresOpen && !attemptedAt.Before(entry.deadline)) {
		delete(p.pending, key)
		return
	}
	entry.next = attemptedAt.Add(entry.backoff)
	entry.backoff *= 2
	maxBackoff := p.cfg.LeaseDuration / 2
	if maxBackoff < p.cfg.ReconcileInterval {
		maxBackoff = p.cfg.ReconcileInterval
	}
	if maxBackoff > time.Minute {
		maxBackoff = time.Minute
	}
	if entry.backoff > maxBackoff {
		entry.backoff = maxBackoff
	}
	p.pending[key] = entry
}

func (p *Postgres) cleanupExact(key leaseKey, deadline time.Time) bool {
	opDeadline := time.Now().Add(p.cfg.OperationTimeout)
	if deadline.Before(opDeadline) {
		opDeadline = deadline
	}
	ctx, cancel := context.WithDeadline(p.reconcileCtx, opDeadline)
	defer cancel()
	return p.cleanupExactContext(ctx, key)
}

func (p *Postgres) cleanupExactContext(ctx context.Context, key leaseKey) bool {
	if err := p.pool.Exec(ctx, `DELETE FROM session_admission_leases WHERE instance_id=$1 AND session_id=$2`, key.instanceID, key.sessionID); err != nil {
		return false
	}
	var count int
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM session_admission_leases WHERE instance_id=$1 AND session_id=$2`, key.instanceID, key.sessionID).Scan(&count); err != nil {
		return false
	}
	return count == 0
}

// Close rejects new acquisitions, joins commit outcomes already in flight,
// and asks the sole reconciler to perform a bounded final exact-key drain.
// Active successful lease owners remain responsible for their own rows.
func (p *Postgres) Close() error {
	if p == nil {
		return nil
	}
	p.reconcileMu.Lock()
	if p.state != postgresOpen {
		done := p.reconcileDone
		p.reconcileMu.Unlock()
		<-done
		p.reconcileMu.Lock()
		err := p.closeErr
		p.reconcileMu.Unlock()
		return err
	}
	p.state = postgresClosing
	p.acquireCancel()
	p.reconcileMu.Unlock()

	// No Add can race this Wait: beginAcquire performs Add while holding the
	// same lock used above to move the store out of postgresOpen.
	p.acquisitions.Wait()
	p.reconcileMu.Lock()
	p.drainRequested = true
	p.drainDeadline = time.Now().Add(p.cfg.ShutdownTimeout)
	done := p.reconcileDone
	p.reconcileMu.Unlock()
	p.wakeReconciler()
	<-done
	p.reconcileMu.Lock()
	err := p.closeErr
	p.reconcileMu.Unlock()
	return err
}
