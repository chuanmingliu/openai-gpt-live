// Package migrations embeds and applies the database schema.
package migrations

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratedatabase "github.com/golang-migrate/migrate/v4/database"
	pgx5migrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const latestVersion uint = 1

const (
	migrationCancelTimeout     = 2 * time.Second
	migrationLockTimeout       = 5 * time.Second
	migrationClientLockTimeout = 10 * time.Second
	migrationStatementTimeout  = 2 * time.Minute
)

var migrationRunnerSequence atomic.Uint64

//go:embed *.sql
var files embed.FS

type runner struct {
	migration       *migrate.Migrate
	driver          *cancellablePGXDriver
	cancelConfig    *pgx.ConnConfig
	applicationName string
	backendPID      uint32
}

// cancellablePGXDriver implements migrate's database.Driver contract with pgx
// contexts. The upstream pgx migrate driver uses context.Background during
// initialization, locking, and execution, so it cannot honor this package's
// context-aware API when another migrator holds the advisory lock.
type cancellablePGXDriver struct {
	connection *pgx.Conn
	schema     string
	table      string
	qualified  string
	lockID     string
	ctx        context.Context
	locked     atomic.Bool
}

func Up(ctx context.Context, pool *pgxpool.Pool) error {
	return apply(ctx, pool, func(m *migrate.Migrate) error { return m.Up() })
}

func Down(ctx context.Context, pool *pgxpool.Pool) error {
	return apply(ctx, pool, func(m *migrate.Migrate) error { return m.Down() })
}

func Steps(ctx context.Context, pool *pgxpool.Pool, steps int) error {
	if steps == 0 {
		return ctx.Err()
	}
	return apply(ctx, pool, func(m *migrate.Migrate) error { return m.Steps(steps) })
}

func Check(ctx context.Context, pool *pgxpool.Pool) error {
	version, dirty, err := Version(ctx, pool)
	if err != nil {
		return fmt.Errorf("check schema version: %w", err)
	}
	if dirty {
		return errors.New("schema migration is dirty")
	}
	if version != latestVersion {
		return fmt.Errorf("schema version is behind: got %d, want %d", version, latestVersion)
	}
	return nil
}

func Version(ctx context.Context, pool *pgxpool.Pool) (uint, bool, error) {
	if err := validate(ctx, pool); err != nil {
		return 0, false, err
	}
	r, err := newRunner(ctx, pool)
	if err != nil {
		return 0, false, err
	}
	defer r.close()
	var databaseVersion int
	var dirty bool
	err = r.execute(ctx, func(*migrate.Migrate) error {
		var versionErr error
		databaseVersion, dirty, versionErr = r.driver.Version()
		return versionErr
	})
	if err != nil {
		return 0, false, err
	}
	if databaseVersion == migratedatabase.NilVersion {
		// Preserve dirty=true for a cancelled first down migration. migrate's
		// public Version method maps all NilVersion rows to dirty=false.
		return 0, dirty, nil
	}
	if databaseVersion < 0 {
		return 0, false, nil
	}
	return uint(databaseVersion), dirty, nil
}

func apply(ctx context.Context, pool *pgxpool.Pool, operation func(*migrate.Migrate) error) error {
	if err := validate(ctx, pool); err != nil {
		return err
	}
	r, err := newRunner(ctx, pool)
	if err != nil {
		return err
	}
	defer r.close()
	if err := r.execute(ctx, operation); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("apply embedded migration: %w", err)
	}
	return nil
}

func validate(ctx context.Context, pool *pgxpool.Pool) error {
	if ctx == nil {
		return errors.New("migration context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if pool == nil {
		return errors.New("migration pool is required")
	}
	return nil
}

func newRunner(ctx context.Context, pool *pgxpool.Pool) (*runner, error) {
	source, err := iofs.New(files, ".")
	if err != nil {
		return nil, fmt.Errorf("open embedded migrations: %w", err)
	}
	applicationName := fmt.Sprintf("live-migrate-%d-%d", os.Getpid(), migrationRunnerSequence.Add(1))
	config := pool.Config().ConnConfig.Copy()
	config.RuntimeParams["application_name"] = applicationName
	config.RuntimeParams["lock_timeout"] = fmt.Sprintf("%dms", migrationLockTimeout.Milliseconds())
	config.RuntimeParams["statement_timeout"] = fmt.Sprintf("%dms", migrationStatementTimeout.Milliseconds())
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		_ = source.Close()
		return nil, errors.New("open migration database")
	}
	var databaseName, schemaName string
	var backendPID uint32
	if err := connection.QueryRow(ctx, `SELECT current_database(), current_schema(), pg_backend_pid()`).
		Scan(&databaseName, &schemaName, &backendPID); err != nil {
		_ = source.Close()
		_ = connection.Close(context.Background())
		return nil, errors.New("identify migration database backend")
	}
	lockID, err := migratedatabase.GenerateAdvisoryLockId(databaseName, schemaName, pgx5migrate.DefaultMigrationsTable)
	if err != nil {
		_ = source.Close()
		_ = connection.Close(context.Background())
		return nil, errors.New("generate migration advisory lock")
	}
	driver := &cancellablePGXDriver{
		connection: connection,
		schema:     schemaName,
		table:      pgx5migrate.DefaultMigrationsTable,
		qualified:  pgx.Identifier{schemaName, pgx5migrate.DefaultMigrationsTable}.Sanitize(),
		lockID:     lockID,
		ctx:        ctx,
	}
	if err := driver.ensureVersionTable(ctx); err != nil {
		_ = source.Close()
		_ = driver.Close()
		return nil, err
	}
	m, err := migrate.NewWithInstance("iofs", source, "pgx5-cancellable", driver)
	if err != nil {
		_ = source.Close()
		_ = driver.Close()
		return nil, fmt.Errorf("create migration runner: %w", err)
	}
	m.LockTimeout = migrationClientLockTimeout
	cancelConfig := pool.Config().ConnConfig.Copy()
	cancelConfig.RuntimeParams["application_name"] = applicationName + "-cancel"
	return &runner{
		migration: m, driver: driver, cancelConfig: cancelConfig,
		applicationName: applicationName, backendPID: backendPID,
	}, nil
}

func (r *runner) execute(ctx context.Context, operation func(*migrate.Migrate) error) error {
	r.driver.ctx = ctx
	result := make(chan error, 1)
	go func() { result <- operation(r.migration) }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		select {
		case r.migration.GracefulStop <- true:
		default:
		}
		_ = r.cancelBackend()
		// The driver uses ctx for every database operation and the dedicated
		// backend has been terminated as a safety net. Wait for the operation
		// goroutine so Close never races it or leaves an orphan behind.
		<-result
		return ctx.Err()
	}
}

func (r *runner) cancelBackend() error {
	ctx, cancel := context.WithTimeout(context.Background(), migrationCancelTimeout)
	defer cancel()
	connection, err := pgx.ConnectConfig(ctx, r.cancelConfig)
	if err != nil {
		return errors.New("connect migration cancellation backend")
	}
	defer connection.Close(context.Background())
	var terminated bool
	err = connection.QueryRow(ctx, `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE pid = $1 AND application_name = $2 AND pid <> pg_backend_pid()`,
		r.backendPID, r.applicationName).Scan(&terminated)
	if err != nil {
		return errors.New("cancel migration backend")
	}
	if !terminated {
		return errors.New("migration backend was not found")
	}
	return nil
}

func (r *runner) close() {
	_, _ = r.migration.Close()
}

func (d *cancellablePGXDriver) Open(string) (migratedatabase.Driver, error) {
	return nil, errors.New("embedded migration driver cannot be opened by URL")
}

func (d *cancellablePGXDriver) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), migrationCancelTimeout)
	defer cancel()
	return d.connection.Close(ctx)
}

func (d *cancellablePGXDriver) Lock() error {
	if !d.locked.CompareAndSwap(false, true) {
		return migratedatabase.ErrLocked
	}
	if _, err := d.connection.Exec(d.ctx, `SELECT pg_advisory_lock($1)`, d.lockID); err != nil {
		d.locked.Store(false)
		return err
	}
	return nil
}

func (d *cancellablePGXDriver) Unlock() error {
	if !d.locked.CompareAndSwap(true, false) {
		return migratedatabase.ErrNotLocked
	}
	ctx := d.ctx
	if ctx == nil || ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), migrationCancelTimeout)
		defer cancel()
	}
	if _, err := d.connection.Exec(ctx, `SELECT pg_advisory_unlock($1)`, d.lockID); err != nil {
		d.locked.Store(true)
		return err
	}
	return nil
}

func (d *cancellablePGXDriver) Run(migration io.Reader) error {
	statement, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	if len(statement) == 0 {
		return nil
	}
	tx, err := d.connection.Begin(d.ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(d.ctx, string(statement)); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	return tx.Commit(d.ctx)
}

func (d *cancellablePGXDriver) SetVersion(version int, dirty bool) error {
	tx, err := d.connection.Begin(d.ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(d.ctx, `TRUNCATE `+d.qualified); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	if version >= 0 || (version == migratedatabase.NilVersion && dirty) {
		if _, err := tx.Exec(d.ctx, `INSERT INTO `+d.qualified+` (version, dirty) VALUES ($1, $2)`, version, dirty); err != nil {
			_ = tx.Rollback(context.Background())
			return err
		}
	}
	return tx.Commit(d.ctx)
}

func (d *cancellablePGXDriver) Version() (int, bool, error) {
	var version int
	var dirty bool
	err := d.connection.QueryRow(d.ctx, `SELECT version, dirty FROM `+d.qualified+` LIMIT 1`).Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return migratedatabase.NilVersion, false, nil
	}
	return version, dirty, err
}

func (d *cancellablePGXDriver) Drop() error {
	return errors.New("drop-all is not supported by the embedded migration driver")
}

func (d *cancellablePGXDriver) ensureVersionTable(ctx context.Context) (err error) {
	d.ctx = ctx
	if err := d.Lock(); err != nil {
		return fmt.Errorf("lock migration version table: %w", err)
	}
	defer func() { err = errors.Join(err, d.Unlock()) }()
	_, err = d.connection.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+d.qualified+` (
		version BIGINT NOT NULL PRIMARY KEY,
		dirty BOOLEAN NOT NULL
	)`)
	return err
}
