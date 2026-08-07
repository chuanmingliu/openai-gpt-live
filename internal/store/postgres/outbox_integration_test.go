//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixedJitter time.Duration

func (j fixedJitter) Duration(max time.Duration) time.Duration {
	if time.Duration(j) > max {
		return max
	}
	return time.Duration(j)
}

func publisherConfig() OutboxPublisherConfig {
	return OutboxPublisherConfig{BatchSize: 8, PollInterval: time.Second, MaxJitter: time.Second}
}

func createOutboxFixture(t *testing.T) (*Repository, handoff.Scope, handoff.CreateRecord) {
	t.Helper()
	repo, pool, _ := repositoryHarness(t)
	scope := testScope("tenant-a", "conversation-1")
	insertConversation(t, pool, scope)
	record := prepareCreate(t, repo, scope, "outbox")
	_, _, err := repo.CreateHandoff(t.Context(), record)
	require.NoError(t, err)
	return repo, scope, record
}

func TestOutboxPublisherPublishesMetadataAndMarksAtomically(t *testing.T) {
	repo, scope, record := createOutboxFixture(t)
	listener, err := repo.pool.Acquire(t.Context())
	require.NoError(t, err)
	defer listener.Release()
	_, err = listener.Exec(t.Context(), `LISTEN gpt_live_handoff_events`)
	require.NoError(t, err)
	publisher, err := NewOutboxPublisher(repo.pool, repo.clock, fixedJitter(0), publisherConfig())
	require.NoError(t, err)
	published, err := publisher.publishBatch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, published)

	for i := 0; i < 2; i++ {
		notification, err := listener.Conn().WaitForNotification(t.Context())
		require.NoError(t, err)
		assert.Equal(t, notificationChannel, notification.Channel)
		assert.NotContains(t, notification.Payload, record.Request.Objective)
		assert.NotContains(t, notification.Payload, record.Request.UserRequest)
		var fields map[string]any
		require.NoError(t, json.Unmarshal([]byte(notification.Payload), &fields))
		assert.Equal(t, scope.TenantID, fields["tenant_id"])
		assert.Equal(t, string(scope.ConversationID), fields["conversation_id"])
	}
	var unpublished int64
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&unpublished))
	assert.Zero(t, unpublished)
}

func TestOutboxPublishersUseSkipLockedWithoutDoubleSuccess(t *testing.T) {
	repo, _, _ := createOutboxFixture(t)
	config := publisherConfig()
	config.BatchSize = 1
	first, err := NewOutboxPublisher(repo.pool, repo.clock, fixedJitter(0), config)
	require.NoError(t, err)
	second, err := NewOutboxPublisher(repo.pool, repo.clock, fixedJitter(0), config)
	require.NoError(t, err)
	claimed := make(chan struct{})
	release := make(chan struct{})
	first.hook = func(ctx context.Context, stage string) error {
		if stage == "claimed" {
			close(claimed)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	firstResult := make(chan error, 1)
	go func() { _, err := first.publishBatch(t.Context()); firstResult <- err }()
	<-claimed
	published, err := second.publishBatch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, published)
	close(release)
	require.NoError(t, <-firstResult)
	var publishedRows, totalAttempts int64
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT count(*) FILTER (WHERE published_at IS NOT NULL),sum(attempts) FROM outbox`).
		Scan(&publishedRows, &totalAttempts))
	assert.Equal(t, int64(2), publishedRows)
	assert.Equal(t, int64(2), totalAttempts)
}

func TestOutboxPublisherCancellationRollsBackClaim(t *testing.T) {
	repo, _, _ := createOutboxFixture(t)
	publisher, err := NewOutboxPublisher(repo.pool, repo.clock, fixedJitter(0), publisherConfig())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	publisher.hook = func(_ context.Context, stage string) error {
		if stage == "claimed" {
			cancel()
			return context.Canceled
		}
		return nil
	}
	_, err = publisher.publishBatch(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	var published, attempts int64
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT count(*) FILTER (WHERE published_at IS NOT NULL),sum(attempts) FROM outbox`).
		Scan(&published, &attempts))
	assert.Zero(t, published)
	assert.Zero(t, attempts)
}

func TestOutboxPublisherRunCancelsWhileIdleWithoutSleep(t *testing.T) {
	repo, _, _ := createOutboxFixture(t)
	publisher, err := NewOutboxPublisher(repo.pool, repo.clock, fixedJitter(0), publisherConfig())
	require.NoError(t, err)
	notified := make(chan struct{}, 2)
	publisher.hook = func(_ context.Context, stage string) error {
		if stage == "notified" {
			notified <- struct{}{}
		}
		return nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- publisher.Run(ctx) }()
	<-notified
	<-notified
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestOutboxPublisherRecordsMalformedPayloadWithoutLeakingIt(t *testing.T) {
	repo, _, record := createOutboxFixture(t)
	var seq int64
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT seq FROM handoff_events WHERE event_id=$1`, record.EventID).Scan(&seq))
	_, err := repo.pool.Exec(t.Context(), `INSERT INTO outbox(topic,aggregate_id,event_seq,payload,created_at)
		VALUES ('unknown.topic',$1,$2,'{"tenant_id":"secret-value"}'::json,$3)`, record.HandoffID, seq, repositoryTestNow)
	require.NoError(t, err)
	publisher, err := NewOutboxPublisher(repo.pool, repo.clock, fixedJitter(0), publisherConfig())
	require.NoError(t, err)
	published, err := publisher.publishBatch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, published)
	var attempts int
	var lastError string
	var publishedAt *time.Time
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT attempts,last_error,published_at FROM outbox WHERE topic='unknown.topic'`).
		Scan(&attempts, &lastError, &publishedAt))
	assert.Equal(t, 1, attempts)
	assert.Equal(t, "invalid outbox payload", lastError)
	assert.NotContains(t, lastError, "secret-value")
	assert.Nil(t, publishedAt)
}

func TestOutboxPublisherPoisonRowsDoNotStarveValidOrNewRows(t *testing.T) {
	repo, scope, record := createOutboxFixture(t)
	var seq int64
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT seq FROM handoff_events WHERE event_id=$1`, record.EventID).Scan(&seq))
	for i, topic := range []string{"poison.a", "poison.b"} {
		_, err := repo.pool.Exec(t.Context(), `INSERT INTO outbox(topic,aggregate_id,event_seq,payload,created_at)
			VALUES ($1,$2,$3,$4::json,$5)`, topic, record.HandoffID, seq,
			`{"tenant_id":"secret-poison-`+string(rune('a'+i))+`"}`, repositoryTestNow.Add(-time.Minute))
		require.NoError(t, err)
	}
	config := publisherConfig()
	config.BatchSize = 1
	publisher, err := NewOutboxPublisher(repo.pool, repo.clock, fixedJitter(0), config)
	require.NoError(t, err)
	for _, want := range []int{0, 0, 1, 1, 0} {
		got, err := publisher.publishBatch(t.Context())
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	newRecord := prepareCreate(t, repo, scope, "outbox-new")
	_, _, err = repo.CreateHandoff(t.Context(), newRecord)
	require.NoError(t, err)
	got, err := publisher.publishBatch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, got, "new zero-attempt work must pass retried poison rows")
	rows, err := repo.pool.Query(t.Context(), `SELECT attempts,last_error FROM outbox WHERE topic LIKE 'poison.%' ORDER BY topic`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var attempts int
		var lastError string
		require.NoError(t, rows.Scan(&attempts, &lastError))
		assert.Positive(t, attempts)
		assert.Equal(t, "invalid outbox payload", lastError)
		assert.NotContains(t, lastError, "secret-poison")
	}
	require.NoError(t, rows.Err())
}

func TestOutboxMalformedAttemptCounterSaturates(t *testing.T) {
	repo, _, record := createOutboxFixture(t)
	var seq int64
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT seq FROM handoff_events WHERE event_id=$1`, record.EventID).Scan(&seq))
	_, err := repo.pool.Exec(t.Context(), `INSERT INTO outbox(topic,aggregate_id,event_seq,payload,created_at,attempts)
		VALUES ('poison.max',$1,$2,'{}'::json,$3,2147483647)`, record.HandoffID, seq, repositoryTestNow.Add(-time.Minute))
	require.NoError(t, err)
	publisher, err := NewOutboxPublisher(repo.pool, repo.clock, fixedJitter(0), publisherConfig())
	require.NoError(t, err)
	_, err = publisher.publishBatch(t.Context())
	require.NoError(t, err)
	var attempts int
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT attempts FROM outbox WHERE topic='poison.max'`).Scan(&attempts))
	assert.Equal(t, 2147483647, attempts)
}

func TestOutboxSuccessfulAttemptCounterSaturates(t *testing.T) {
	repo, _, _ := createOutboxFixture(t)
	_, err := repo.pool.Exec(t.Context(), `UPDATE outbox SET attempts=2147483647`)
	require.NoError(t, err)
	publisher, err := NewOutboxPublisher(repo.pool, repo.clock, fixedJitter(0), publisherConfig())
	require.NoError(t, err)
	published, err := publisher.publishBatch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, published)
	var minAttempts, maxAttempts int
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT min(attempts),max(attempts) FROM outbox`).Scan(&minAttempts, &maxAttempts))
	assert.Equal(t, 2147483647, minAttempts)
	assert.Equal(t, 2147483647, maxAttempts)
}

func TestTypedPayloadDecoderRejectsDuplicatesTrailingAndNonObjects(t *testing.T) {
	validDispatch := []byte(`{"tenant_id":"tenant","conversation_id":"conversation","handoff_id":"handoff","event_seq":1}`)
	validWakeup := []byte(`{"tenant_id":"tenant","conversation_id":"conversation","highest_seq":1}`)
	assert.True(t, validOutboxPayload("handoff.dispatch", validDispatch))
	_, ok := parseWakeup(validWakeup)
	assert.True(t, ok)
	for name, raw := range map[string][]byte{
		"duplicate tenant":   []byte(`{"tenant_id":"first","tenant_id":"second","conversation_id":"conversation","highest_seq":1}`),
		"duplicate sequence": []byte(`{"tenant_id":"tenant","conversation_id":"conversation","highest_seq":1,"highest_seq":2}`),
		"trailing":           []byte(`{"tenant_id":"tenant","conversation_id":"conversation","highest_seq":1} true`),
		"array root":         []byte(`[]`),
	} {
		t.Run(name, func(t *testing.T) { _, ok := parseWakeup(raw); assert.False(t, ok) })
	}
	for name, raw := range map[string][]byte{
		"duplicate tenant":   []byte(`{"tenant_id":"first","tenant_id":"second","conversation_id":"conversation","handoff_id":"handoff","event_seq":1}`),
		"duplicate sequence": []byte(`{"tenant_id":"tenant","conversation_id":"conversation","handoff_id":"handoff","event_seq":1,"event_seq":2}`),
		"trailing":           []byte(`{"tenant_id":"tenant","conversation_id":"conversation","handoff_id":"handoff","event_seq":1} true`),
		"scalar root":        []byte(`true`),
	} {
		t.Run("dispatch "+name, func(t *testing.T) { assert.False(t, validOutboxPayload("handoff.dispatch", raw)) })
	}
}

type rawJitter time.Duration

func (j rawJitter) Duration(time.Duration) time.Duration { return time.Duration(j) }

func TestRetryDurationBoundsRejectOverflowAndClampRuntimeJitter(t *testing.T) {
	repo, _, _ := createOutboxFixture(t)
	maxDuration := time.Duration(1<<63 - 1)
	safePublisher := publisherConfig()
	safePublisher.PollInterval, safePublisher.MaxJitter = 1, maxDuration-1
	_, err := NewOutboxPublisher(repo.pool, repo.clock, rawJitter(0), safePublisher)
	require.NoError(t, err)
	overflowPublisher := safePublisher
	overflowPublisher.PollInterval = 2
	_, err = NewOutboxPublisher(repo.pool, repo.clock, rawJitter(0), overflowPublisher)
	assert.Error(t, err)
	safeListener := listenerConfig()
	safeListener.ReconnectDelay, safeListener.MaxJitter = 1, maxDuration-1
	_, err = NewListener(repo.pool, repo.clock, rawJitter(0), safeListener)
	require.NoError(t, err)
	overflowListener := safeListener
	overflowListener.ReconnectDelay = 2
	_, err = NewListener(repo.pool, repo.clock, rawJitter(0), overflowListener)
	assert.Error(t, err)
	assert.Zero(t, jitterDuration(rawJitter(-1), time.Second))
	assert.Equal(t, time.Second, jitterDuration(rawJitter(2*time.Second), time.Second))
}

func listenerConfig() ListenerConfig {
	return ListenerConfig{ReconnectDelay: time.Second, MaxJitter: time.Second}
}

func TestListenerEmitsTypedWakeupsAndIgnoresMalformedPayload(t *testing.T) {
	repo, scope, _ := createOutboxFixture(t)
	listener, err := NewListener(repo.pool, repo.clock, fixedJitter(0), listenerConfig())
	require.NoError(t, err)
	connected := make(chan struct{}, 1)
	processed := make(chan struct{}, 4)
	listener.onConnected = func(uint32) { connected <- struct{}{} }
	listener.onNotification = func() { processed <- struct{}{} }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	wakeups := make(chan handoff.EventWakeup, 4)
	go func() { done <- listener.Run(ctx, wakeups) }()
	<-connected
	for _, payload := range []string{
		`{"tenant_id":"tenant-a"}`,
		`{"tenant_id":"tenant-a","conversation_id":"conversation-1","handoff_id":"wrong","event_seq":1}`,
		`{"tenant_id":"tenant-a","conversation_id":"conversation-1","highest_seq":42}`,
	} {
		_, err := repo.pool.Exec(t.Context(), `SELECT pg_notify($1,$2)`, notificationChannel, payload)
		require.NoError(t, err)
		<-processed
	}
	wakeup := <-wakeups
	assert.Equal(t, handoff.EventWakeup{TenantID: scope.TenantID, ConversationID: scope.ConversationID, HighestSeq: 42}, wakeup)
	assert.Empty(t, wakeups)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestListenerBackpressureIsBoundedAndDuplicatesAreHints(t *testing.T) {
	repo, _, _ := createOutboxFixture(t)
	listener, err := NewListener(repo.pool, repo.clock, fixedJitter(0), listenerConfig())
	require.NoError(t, err)
	connected := make(chan struct{}, 1)
	processed := make(chan struct{}, 4)
	listener.onConnected = func(uint32) { connected <- struct{}{} }
	listener.onNotification = func() { processed <- struct{}{} }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	wakeups := make(chan handoff.EventWakeup, 1)
	go func() { done <- listener.Run(ctx, wakeups) }()
	<-connected
	payload := `{"tenant_id":"tenant-a","conversation_id":"conversation-1","highest_seq":9}`
	for i := 0; i < 3; i++ {
		_, err := repo.pool.Exec(t.Context(), `SELECT pg_notify($1,$2)`, notificationChannel, payload)
		require.NoError(t, err)
		<-processed
	}
	assert.Len(t, wakeups, 1)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestListenerReconnectsWithManualClockWithoutSleep(t *testing.T) {
	repo, _, _ := createOutboxFixture(t)
	manual := clock.NewManual(repositoryTestNow)
	listener, err := NewListener(repo.pool, manual, fixedJitter(0), listenerConfig())
	require.NoError(t, err)
	connected := make(chan uint32, 2)
	waiting := make(chan struct{}, 1)
	listener.onConnected = func(pid uint32) { connected <- pid }
	listener.beforeReconnect = func() { waiting <- struct{}{} }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- listener.Run(ctx, make(chan handoff.EventWakeup, 1)) }()
	firstPID := <-connected
	var terminated bool
	require.NoError(t, repo.pool.QueryRow(t.Context(), `SELECT pg_terminate_backend($1)`, firstPID).Scan(&terminated))
	assert.True(t, terminated)
	<-waiting
	manual.Advance(time.Second)
	secondPID := <-connected
	assert.NotEqual(t, firstPID, secondPID)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestPublisherAndListenerConstructorsRejectInvalidDependenciesAndBounds(t *testing.T) {
	repo, _, _ := createOutboxFixture(t)
	_, err := NewOutboxPublisher(nil, repo.clock, fixedJitter(0), publisherConfig())
	assert.Error(t, err)
	_, err = NewOutboxPublisher(repo.pool, nil, fixedJitter(0), publisherConfig())
	assert.Error(t, err)
	badPublisher := publisherConfig()
	badPublisher.BatchSize = 0
	_, err = NewOutboxPublisher(repo.pool, repo.clock, fixedJitter(0), badPublisher)
	assert.Error(t, err)
	_, err = NewListener(repo.pool, repo.clock, nil, listenerConfig())
	assert.Error(t, err)
	badListener := listenerConfig()
	badListener.ReconnectDelay = 0
	_, err = NewListener(repo.pool, repo.clock, fixedJitter(0), badListener)
	assert.Error(t, err)
}
