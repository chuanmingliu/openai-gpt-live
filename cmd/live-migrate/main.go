package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	"github.com/chuanmingliu/openai-gpt-live/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var errInvalidDatabaseURL = errors.New("database URL is invalid")

type options struct {
	databaseURLEnv string
	direction      string
	steps          int
	check          bool
}

type migrationSession interface {
	Up(context.Context) error
	Down(context.Context) error
	Steps(context.Context, int) error
	Check(context.Context) error
	Version(context.Context) (uint, bool, error)
	Close()
}

type pgxMigrationSession struct {
	pool *pgxpool.Pool
}

type migrationSessionFactory func(context.Context, string) (migrationSession, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "migration failed:", err)
		os.Exit(1)
	}
}

func run(
	ctx context.Context,
	args []string,
	lookupEnv func(string) (string, bool),
	stdout io.Writer,
	stderr io.Writer,
) error {
	return runWithSessionFactory(ctx, args, lookupEnv, stdout, stderr, openMigrationSession)
}

func runWithSessionFactory(
	ctx context.Context,
	args []string,
	lookupEnv func(string) (string, bool),
	stdout io.Writer,
	stderr io.Writer,
	factory migrationSessionFactory,
) error {
	parsed, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}
	databaseURL, ok := lookupEnv(parsed.databaseURLEnv)
	if !ok || databaseURL == "" {
		return fmt.Errorf("database URL environment variable %s is not set", parsed.databaseURLEnv)
	}
	session, err := factory(ctx, databaseURL)
	if err != nil {
		if errors.Is(err, errInvalidDatabaseURL) {
			return fmt.Errorf("database URL environment variable %s is invalid", parsed.databaseURLEnv)
		}
		return safeDatabaseError(databaseURL, err)
	}
	defer session.Close()

	if parsed.check {
		if err := session.Check(ctx); err != nil {
			return safeDatabaseError(databaseURL, fmt.Errorf("schema check failed: %w", err))
		}
		version, _, err := session.Version(ctx)
		if err != nil {
			return safeDatabaseError(databaseURL, fmt.Errorf("read schema version: %w", err))
		}
		_, err = fmt.Fprintf(stdout, "schema check passed at version %d\n", version)
		return err
	}

	if parsed.steps > 0 {
		steps := parsed.steps
		if parsed.direction == "down" {
			steps = -steps
		}
		err = session.Steps(ctx, steps)
	} else if parsed.direction == "up" {
		err = session.Up(ctx)
	} else {
		err = session.Down(ctx)
	}
	if err != nil {
		return safeDatabaseError(databaseURL, fmt.Errorf("migrate %s: %w", parsed.direction, err))
	}
	version, dirty, err := session.Version(ctx)
	if err != nil {
		return safeDatabaseError(databaseURL, fmt.Errorf("read schema version: %w", err))
	}
	if dirty {
		return errors.New("migration completed with a dirty schema")
	}
	_, err = fmt.Fprintf(stdout, "schema migration %s complete at version %d\n", parsed.direction, version)
	return err
}

func openMigrationSession(ctx context.Context, databaseURL string) (migrationSession, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errInvalidDatabaseURL
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.New("database connection failed")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("database connection failed")
	}
	return &pgxMigrationSession{pool: pool}, nil
}

func (s *pgxMigrationSession) Up(ctx context.Context) error {
	return migrations.Up(ctx, s.pool)
}

func (s *pgxMigrationSession) Down(ctx context.Context) error {
	return migrations.Down(ctx, s.pool)
}

func (s *pgxMigrationSession) Steps(ctx context.Context, steps int) error {
	return migrations.Steps(ctx, s.pool, steps)
}

func (s *pgxMigrationSession) Check(ctx context.Context) error {
	return migrations.Check(ctx, s.pool)
}

func (s *pgxMigrationSession) Version(ctx context.Context) (uint, bool, error) {
	return migrations.Version(ctx, s.pool)
}

func (s *pgxMigrationSession) Close() {
	s.pool.Close()
}

func safeDatabaseError(databaseURL string, err error) error {
	message := strings.ReplaceAll(err.Error(), databaseURL, "[redacted]")
	if parsed, parseErr := url.Parse(databaseURL); parseErr == nil && parsed.User != nil {
		if password, ok := parsed.User.Password(); ok && password != "" {
			message = strings.ReplaceAll(message, password, "[redacted]")
		}
	}
	return errors.New(message)
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	var parsed options
	flags := flag.NewFlagSet("live-migrate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&parsed.databaseURLEnv, "database-url-env", "DATABASE_URL", "environment variable containing the database URL")
	flags.StringVar(&parsed.direction, "direction", "up", "migration direction: up or down")
	flags.IntVar(&parsed.steps, "steps", 0, "number of versions to migrate; zero migrates all")
	flags.BoolVar(&parsed.check, "check", false, "verify the schema is at the latest clean version")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	directionSet := false
	stepsSet := false
	flags.Visit(func(current *flag.Flag) {
		switch current.Name {
		case "direction":
			directionSet = true
		case "steps":
			stepsSet = true
		}
	})
	if flags.NArg() != 0 {
		return options{}, errors.New("positional arguments are not accepted")
	}
	if !environmentName.MatchString(parsed.databaseURLEnv) {
		return options{}, errors.New("database URL environment variable name is invalid")
	}
	if parsed.direction != "up" && parsed.direction != "down" {
		return options{}, errors.New("direction must be up or down")
	}
	if parsed.steps < 0 {
		return options{}, errors.New("steps must be nonnegative; use direction=down")
	}
	if parsed.check && (stepsSet || directionSet) {
		return options{}, errors.New("check cannot be combined with direction or steps")
	}
	return parsed, nil
}
