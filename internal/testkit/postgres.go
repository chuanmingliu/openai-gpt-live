package testkit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	postgresStartupTimeout = 90 * time.Second
	postgresConnectTimeout = 15 * time.Second
)

// Postgres returns a ready, isolated PostgreSQL pool and registers cleanup.
func Postgres(t testing.TB) *pgxpool.Pool {
	t.Helper()
	if databaseURL := os.Getenv("TEST_DATABASE_URL"); databaseURL != "" {
		return suppliedPostgres(t, databaseURL)
	}
	return containerPostgres(t)
}

func suppliedPostgres(t testing.TB, databaseURL string) *pgxpool.Pool {
	t.Helper()
	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal("configured TEST_DATABASE_URL is invalid")
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), postgresConnectTimeout)
	defer cancel()
	admin, err := pgxpool.NewWithConfig(connectCtx, adminConfig)
	if err != nil {
		t.Fatal("configured TEST_DATABASE_URL is unreachable")
	}
	if err := admin.Ping(connectCtx); err != nil {
		admin.Close()
		t.Fatal("configured TEST_DATABASE_URL is unreachable")
	}

	schema := "task9_" + randomSuffix(t)
	if _, err := admin.Exec(connectCtx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("cannot create isolated schema in configured TEST_DATABASE_URL")
	}
	var (
		pool        *pgxpool.Pool
		cleanupOnce sync.Once
	)
	t.Cleanup(func() {
		cleanupOnce.Do(func() {
			if pool != nil {
				pool.Close()
			}
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), postgresConnectTimeout)
			defer cleanupCancel()
			_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
			admin.Close()
		})
	})
	config := adminConfig.Copy()
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err = pgxpool.NewWithConfig(connectCtx, config)
	if err != nil {
		t.Fatal("cannot connect to isolated TEST_DATABASE_URL schema")
	}
	if err := pool.Ping(connectCtx); err != nil {
		t.Fatal("cannot connect to isolated TEST_DATABASE_URL schema")
	}
	return pool
}

func containerPostgres(t testing.TB) *pgxpool.Pool {
	t.Helper()
	startupCtx, cancel := context.WithTimeout(context.Background(), postgresStartupTimeout)
	defer cancel()
	container, err := tcpostgres.Run(startupCtx, "postgres:16-alpine",
		tcpostgres.WithDatabase("task9_"+randomSuffix(t)),
		tcpostgres.WithUsername("task9"),
		tcpostgres.WithPassword("task9"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(postgresStartupTimeout),
		),
	)
	if err != nil {
		t.Fatal("PostgreSQL 16 test container could not start")
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), postgresConnectTimeout)
		defer cleanupCancel()
		_ = container.Terminate(cleanupCtx)
	})
	databaseURL, err := container.ConnectionString(startupCtx, "sslmode=disable")
	if err != nil {
		t.Fatal("PostgreSQL 16 test container address is unavailable")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal("PostgreSQL 16 test container address is invalid")
	}
	connectCtx, connectCancel := context.WithTimeout(context.Background(), postgresConnectTimeout)
	defer connectCancel()
	pool, err := pgxpool.NewWithConfig(connectCtx, config)
	if err != nil {
		t.Fatal("PostgreSQL 16 test container is unreachable")
	}
	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		t.Fatal("PostgreSQL 16 test container is unreachable")
	}
	t.Cleanup(pool.Close)
	return pool
}

func randomSuffix(t testing.TB) string {
	t.Helper()
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatal("cannot generate PostgreSQL test isolation identifier")
	}
	return hex.EncodeToString(value[:])
}
