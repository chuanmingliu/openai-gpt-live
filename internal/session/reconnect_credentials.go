package session

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/clock"
	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/identity"
)

const MaximumReconnectCredentialBytes = 2_048

var (
	ErrInvalidReconnectCredential = errors.New("invalid reconnect credential")
	ErrExpiredReconnectCredential = errors.New("expired reconnect credential")
)

type ReconnectClaims struct {
	Scope        handoff.Scope
	SessionID    identity.SessionID
	Incarnation  identity.Incarnation
	Cursor       int64
	Model        string
	ExpiresAt    time.Time
	CredentialID string
}

type IssuedReconnectCredential struct {
	Credential string
	Claims     ReconnectClaims
}

type ReconnectCredentials interface {
	Issue(ReconnectClaims) (IssuedReconnectCredential, error)
	Verify(string) (ReconnectClaims, error)
}

type reconnectCredentials struct {
	key   []byte
	clock clock.Clock
}

type reconnectCredentialPayload struct {
	TenantID      string                  `json:"tenant_id"`
	UserID        string                  `json:"user_id"`
	Conversation  identity.ConversationID `json:"conversation_id"`
	SessionID     identity.SessionID      `json:"session_id"`
	Incarnation   identity.Incarnation    `json:"incarnation"`
	Cursor        int64                   `json:"cursor"`
	Model         string                  `json:"model"`
	ExpiresUnixMS int64                   `json:"expires_unix_ms"`
	CredentialID  string                  `json:"credential_id"`
}

func NewReconnectCredentials(key []byte, source clock.Clock) (ReconnectCredentials, error) {
	if len(key) < 32 || len(key) > 4_096 || isNilInterface(source) {
		return nil, ErrInvalidManagerConfig
	}
	return &reconnectCredentials{key: append([]byte(nil), key...), clock: source}, nil
}

func (c *reconnectCredentials) Issue(claims ReconnectClaims) (IssuedReconnectCredential, error) {
	if c == nil || len(c.key) < 32 {
		return IssuedReconnectCredential{}, ErrInvalidReconnectCredential
	}
	if claims.CredentialID == "" {
		var nonce [18]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return IssuedReconnectCredential{}, ErrInvalidReconnectCredential
		}
		claims.CredentialID = base64.RawURLEncoding.EncodeToString(nonce[:])
	}
	claims.ExpiresAt = time.UnixMilli(claims.ExpiresAt.UTC().UnixMilli()).UTC()
	if err := validateReconnectClaims(claims, c.clock.Now(), false); err != nil {
		return IssuedReconnectCredential{}, err
	}
	payload, err := json.Marshal(payloadFromReconnectClaims(claims))
	if err != nil {
		return IssuedReconnectCredential{}, ErrInvalidReconnectCredential
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	unsigned := "lrt1." + encoded
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write([]byte(unsigned))
	credential := unsigned + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if len(credential) > MaximumReconnectCredentialBytes {
		return IssuedReconnectCredential{}, ErrInvalidReconnectCredential
	}
	return IssuedReconnectCredential{Credential: credential, Claims: claims}, nil
}

func (c *reconnectCredentials) Verify(credential string) (ReconnectClaims, error) {
	if c == nil || len(credential) == 0 || len(credential) > MaximumReconnectCredentialBytes || strings.TrimSpace(credential) != credential {
		return ReconnectClaims{}, ErrInvalidReconnectCredential
	}
	parts := strings.Split(credential, ".")
	if len(parts) != 3 || parts[0] != "lrt1" {
		return ReconnectClaims{}, ErrInvalidReconnectCredential
	}
	provided, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(provided) != sha256.Size {
		return ReconnectClaims{}, ErrInvalidReconnectCredential
	}
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return ReconnectClaims{}, ErrInvalidReconnectCredential
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(payload) == 0 || len(payload) > 1_024 {
		return ReconnectClaims{}, ErrInvalidReconnectCredential
	}
	var decoded reconnectCredentialPayload
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil || decoder.Decode(new(any)) != io.EOF {
		return ReconnectClaims{}, ErrInvalidReconnectCredential
	}
	claims := decoded.claims()
	if err := validateReconnectClaims(claims, c.clock.Now(), true); err != nil {
		return ReconnectClaims{}, err
	}
	return claims, nil
}

func payloadFromReconnectClaims(claims ReconnectClaims) reconnectCredentialPayload {
	return reconnectCredentialPayload{TenantID: claims.Scope.TenantID, UserID: claims.Scope.UserID, Conversation: claims.Scope.ConversationID,
		SessionID: claims.SessionID, Incarnation: claims.Incarnation, Cursor: claims.Cursor, Model: claims.Model,
		ExpiresUnixMS: claims.ExpiresAt.UTC().UnixMilli(), CredentialID: claims.CredentialID}
}

func (p reconnectCredentialPayload) claims() ReconnectClaims {
	return ReconnectClaims{Scope: handoff.Scope{TenantID: p.TenantID, UserID: p.UserID, ConversationID: p.Conversation},
		SessionID: p.SessionID, Incarnation: p.Incarnation, Cursor: p.Cursor, Model: p.Model,
		ExpiresAt: time.UnixMilli(p.ExpiresUnixMS).UTC(), CredentialID: p.CredentialID}
}

func validateReconnectClaims(claims ReconnectClaims, now time.Time, checkingExpiry bool) error {
	for _, value := range []string{claims.Scope.TenantID, claims.Scope.UserID, string(claims.Scope.ConversationID), string(claims.SessionID), claims.Model, claims.CredentialID} {
		if !boundedReconnectValue(value) {
			return ErrInvalidReconnectCredential
		}
	}
	if claims.Incarnation == 0 || claims.Cursor < 0 || claims.ExpiresAt.IsZero() || claims.ExpiresAt.Year() < 1970 || claims.ExpiresAt.Year() > 9999 {
		return ErrInvalidReconnectCredential
	}
	if checkingExpiry && !now.Before(claims.ExpiresAt) {
		return ErrExpiredReconnectCredential
	}
	if !checkingExpiry && !claims.ExpiresAt.After(now) {
		return ErrInvalidReconnectCredential
	}
	return nil
}

func boundedReconnectValue(value string) bool {
	if strings.TrimSpace(value) == "" || len(value) > 256 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

type ReconnectOwnerState string

const (
	ReconnectOwnerEstablishing ReconnectOwnerState = "establishing"
	ReconnectOwnerActive       ReconnectOwnerState = "active"
	ReconnectOwnerTombstoned   ReconnectOwnerState = "tombstoned"
	ReconnectOwnerClaiming     ReconnectOwnerState = "claiming"
)

type ReconnectOwner struct {
	Scope            handoff.Scope
	SessionID        identity.SessionID
	Incarnation      identity.Incarnation
	Cursor           int64
	Model            string
	ResponseEpoch    identity.ResponseEpoch
	CredentialID     string
	OwnerID          string
	State            ReconnectOwnerState
	TurnBoundary     bool
	ExpiresAt        time.Time
	SubjectExpiresAt time.Time
	// Previous* is a single bounded overlap slot. It is replaced only after the
	// successor credential has been durably installed; callers serialize wire
	// acknowledgement before performing another rotation.
	PreviousCredentialID string
	PreviousIncarnation  identity.Incarnation
	PreviousCursor       int64
	PreviousExpiresAt    time.Time
	PreviousAcceptUntil  time.Time
	OwnerLeaseExpiresAt  time.Time
	Ephemeral            bool
}

func (o ReconnectOwner) Claims() ReconnectClaims {
	return ReconnectClaims{Scope: o.Scope, SessionID: o.SessionID, Incarnation: o.Incarnation, Cursor: o.Cursor,
		Model: o.Model, ExpiresAt: o.ExpiresAt, CredentialID: o.CredentialID}
}

type ReconnectOwnerKey struct {
	SessionID    identity.SessionID
	Incarnation  identity.Incarnation
	OwnerID      string
	CredentialID string
}

func (o ReconnectOwner) Key() ReconnectOwnerKey {
	return ReconnectOwnerKey{SessionID: o.SessionID, Incarnation: o.Incarnation, OwnerID: o.OwnerID, CredentialID: o.CredentialID}
}

type ReconnectOwnerLease struct {
	DatabaseNow time.Time
	ExpiresAt   time.Time
}

type ReconnectOwnerStore interface {
	Create(context.Context, ReconnectOwner, time.Duration) (ReconnectOwnerLease, error)
	Claim(context.Context, ReconnectClaims, ReconnectOwner, time.Duration) (ReconnectOwner, ReconnectOwnerLease, error)
	AbortClaim(context.Context, ReconnectOwnerKey, ReconnectOwner) error
	Publish(context.Context, ReconnectOwnerKey, identity.ResponseEpoch, bool, time.Duration) (ReconnectOwnerLease, error)
	Heartbeat(context.Context, ReconnectOwnerKey, identity.ResponseEpoch, bool, time.Duration) (ReconnectOwnerLease, error)
	Rotate(context.Context, ReconnectOwnerKey, ReconnectClaims, ReconnectClaims, time.Time) error
	Tombstone(context.Context, identity.SessionID, identity.Incarnation, string, identity.ResponseEpoch, bool) error
	Delete(context.Context, identity.SessionID, identity.Incarnation, string) error
	CleanupExpired(context.Context, int) (int, error)
}

type MemoryReconnectOwners struct {
	mu     sync.Mutex
	clock  clock.Clock
	owners map[identity.SessionID]ReconnectOwner
}

func NewMemoryReconnectOwners(source clock.Clock) *MemoryReconnectOwners {
	return &MemoryReconnectOwners{clock: source, owners: make(map[identity.SessionID]ReconnectOwner)}
}

func (s *MemoryReconnectOwners) Create(ctx context.Context, owner ReconnectOwner, leaseDuration time.Duration) (ReconnectOwnerLease, error) {
	if err := ctx.Err(); err != nil {
		return ReconnectOwnerLease{}, err
	}
	if s == nil || !validReconnectOwner(owner) || owner.State != ReconnectOwnerEstablishing || leaseDuration <= 0 {
		return ReconnectOwnerLease{}, ErrStaleReconnect
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.owners[owner.SessionID]; exists {
		return ReconnectOwnerLease{}, ErrStaleReconnect
	}
	now := s.clock.Now().UTC()
	owner.OwnerLeaseExpiresAt = now.Add(leaseDuration)
	s.owners[owner.SessionID] = owner
	return ReconnectOwnerLease{DatabaseNow: now, ExpiresAt: owner.OwnerLeaseExpiresAt}, nil
}

func (s *MemoryReconnectOwners) Claim(ctx context.Context, claims ReconnectClaims, next ReconnectOwner, leaseDuration time.Duration) (ReconnectOwner, ReconnectOwnerLease, error) {
	if err := ctx.Err(); err != nil {
		return ReconnectOwner{}, ReconnectOwnerLease{}, err
	}
	if s == nil || !validReconnectOwner(next) || next.State != ReconnectOwnerClaiming || leaseDuration <= 0 {
		return ReconnectOwner{}, ReconnectOwnerLease{}, ErrStaleReconnect
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.owners[claims.SessionID]
	now := s.clock.Now().UTC()
	currentToken := current.Claims() == claims && now.Before(current.ExpiresAt)
	previousToken := reconnectPreviousClaims(current) == claims && now.Before(current.PreviousExpiresAt) && now.Before(current.PreviousAcceptUntil)
	if !exists || (!currentToken && !previousToken) || !current.TurnBoundary ||
		next.Scope != current.Scope || next.SessionID != current.SessionID || uint64(claims.Incarnation) == ^uint64(0) || next.Incarnation != claims.Incarnation+1 ||
		next.Cursor != claims.Cursor || next.Model != current.Model ||
		((current.State == ReconnectOwnerClaiming || current.State == ReconnectOwnerActive) && previousToken &&
			next.Incarnation == current.Incarnation && now.Before(current.OwnerLeaseExpiresAt)) ||
		(current.State != ReconnectOwnerTombstoned && current.OwnerID != next.OwnerID && now.Before(current.OwnerLeaseExpiresAt)) {
		return ReconnectOwner{}, ReconnectOwnerLease{}, ErrStaleReconnect
	}
	restaging := (current.State == ReconnectOwnerClaiming || current.State == ReconnectOwnerActive) &&
		previousToken && next.Incarnation == current.Incarnation
	minimumEpoch := current.ResponseEpoch
	if uint64(minimumEpoch) == ^uint64(0) {
		return ReconnectOwner{}, ReconnectOwnerLease{}, ErrStaleReconnect
	}
	minimumEpoch++
	if next.ResponseEpoch < minimumEpoch {
		next.ResponseEpoch = minimumEpoch
	}
	if restaging {
		previous := current
		current.OwnerID = next.OwnerID
		current.State = ReconnectOwnerClaiming
		current.TurnBoundary = true
		current.ResponseEpoch = next.ResponseEpoch
		current.OwnerLeaseExpiresAt = now.Add(leaseDuration)
		s.owners[current.SessionID] = current
		return previous, ReconnectOwnerLease{DatabaseNow: now, ExpiresAt: current.OwnerLeaseExpiresAt}, nil
	}
	next.TurnBoundary = true
	next.PreviousCredentialID = claims.CredentialID
	next.PreviousIncarnation = claims.Incarnation
	next.PreviousCursor = claims.Cursor
	next.PreviousExpiresAt = claims.ExpiresAt
	if next.PreviousAcceptUntil.IsZero() {
		next.PreviousAcceptUntil = earlierTime(claims.ExpiresAt, now.Add(leaseDuration))
	}
	if !next.PreviousAcceptUntil.After(now) || next.PreviousAcceptUntil.After(claims.ExpiresAt) {
		return ReconnectOwner{}, ReconnectOwnerLease{}, ErrStaleReconnect
	}
	next.OwnerLeaseExpiresAt = now.Add(leaseDuration)
	s.owners[next.SessionID] = next
	return current, ReconnectOwnerLease{DatabaseNow: now, ExpiresAt: next.OwnerLeaseExpiresAt}, nil
}

func (s *MemoryReconnectOwners) AbortClaim(ctx context.Context, failed ReconnectOwnerKey, previous ReconnectOwner) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, exists := s.owners[failed.SessionID]
	validIncarnationTransition := (failed.Incarnation > previous.Incarnation && failed.Incarnation-previous.Incarnation == 1) ||
		((previous.State == ReconnectOwnerClaiming || previous.State == ReconnectOwnerActive) && previous.Incarnation == failed.Incarnation)
	if !exists || owner.Key() != failed || owner.State != ReconnectOwnerClaiming || previous.SessionID != failed.SessionID ||
		!validIncarnationTransition || !validStoredReconnectOwner(previous) {
		return ErrStaleReconnect
	}
	s.owners[failed.SessionID] = previous
	return nil
}

func (s *MemoryReconnectOwners) Publish(ctx context.Context, key ReconnectOwnerKey, epoch identity.ResponseEpoch, boundary bool, leaseDuration time.Duration) (ReconnectOwnerLease, error) {
	if err := ctx.Err(); err != nil {
		return ReconnectOwnerLease{}, err
	}
	if leaseDuration <= 0 {
		return ReconnectOwnerLease{}, ErrStaleReconnect
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, exists := s.owners[key.SessionID]
	now := s.clock.Now().UTC()
	if !exists || owner.Key() != key || !now.Before(owner.OwnerLeaseExpiresAt) ||
		(owner.State != ReconnectOwnerEstablishing && owner.State != ReconnectOwnerClaiming && owner.State != ReconnectOwnerActive) ||
		(owner.State == ReconnectOwnerActive && (epoch != owner.ResponseEpoch || boundary != owner.TurnBoundary)) {
		return ReconnectOwnerLease{}, ErrStaleReconnect
	}
	owner.State = ReconnectOwnerActive
	owner.TurnBoundary = boundary
	owner.ResponseEpoch = epoch
	owner.OwnerLeaseExpiresAt = now.Add(leaseDuration)
	s.owners[key.SessionID] = owner
	return ReconnectOwnerLease{DatabaseNow: now, ExpiresAt: owner.OwnerLeaseExpiresAt}, nil
}

func (s *MemoryReconnectOwners) Heartbeat(ctx context.Context, key ReconnectOwnerKey, epoch identity.ResponseEpoch, boundary bool, leaseDuration time.Duration) (ReconnectOwnerLease, error) {
	if err := ctx.Err(); err != nil {
		return ReconnectOwnerLease{}, err
	}
	if leaseDuration <= 0 {
		return ReconnectOwnerLease{}, ErrStaleReconnect
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, exists := s.owners[key.SessionID]
	now := s.clock.Now().UTC()
	if !exists || owner.Key() != key || owner.State != ReconnectOwnerActive || !now.Before(owner.OwnerLeaseExpiresAt) {
		return ReconnectOwnerLease{}, ErrStaleReconnect
	}
	owner.ResponseEpoch, owner.TurnBoundary = epoch, boundary
	owner.OwnerLeaseExpiresAt = now.Add(leaseDuration)
	s.owners[key.SessionID] = owner
	return ReconnectOwnerLease{DatabaseNow: now, ExpiresAt: owner.OwnerLeaseExpiresAt}, nil
}

func (s *MemoryReconnectOwners) Rotate(ctx context.Context, key ReconnectOwnerKey, current, next ReconnectClaims, overlapUntil time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, exists := s.owners[key.SessionID]
	now := s.clock.Now().UTC()
	if !exists || owner.Key() != key || owner.State != ReconnectOwnerActive || !now.Before(owner.OwnerLeaseExpiresAt) ||
		owner.Claims() != current || validateReconnectClaims(next, now, false) != nil || next.Scope != current.Scope ||
		next.SessionID != current.SessionID || next.Incarnation != current.Incarnation || next.Model != current.Model ||
		next.Cursor < current.Cursor || !overlapUntil.After(now) || overlapUntil.After(current.ExpiresAt) {
		return ErrStaleReconnect
	}
	owner.PreviousCredentialID, owner.PreviousIncarnation, owner.PreviousCursor = current.CredentialID, current.Incarnation, current.Cursor
	owner.PreviousExpiresAt, owner.PreviousAcceptUntil = current.ExpiresAt, overlapUntil.UTC()
	owner.CredentialID, owner.Cursor, owner.ExpiresAt = next.CredentialID, next.Cursor, next.ExpiresAt
	s.owners[key.SessionID] = owner
	return nil
}

func (s *MemoryReconnectOwners) Tombstone(ctx context.Context, sessionID identity.SessionID, incarnation identity.Incarnation, ownerID string, epoch identity.ResponseEpoch, boundary bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, exists := s.owners[sessionID]
	if !exists || owner.Incarnation != incarnation || owner.OwnerID != ownerID || owner.State != ReconnectOwnerActive {
		return ErrStaleReconnect
	}
	owner.State = ReconnectOwnerTombstoned
	owner.ResponseEpoch = epoch
	owner.TurnBoundary = boundary
	s.owners[sessionID] = owner
	return nil
}

func (s *MemoryReconnectOwners) Delete(ctx context.Context, sessionID identity.SessionID, incarnation identity.Incarnation, ownerID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, exists := s.owners[sessionID]
	if !exists {
		return nil
	}
	if owner.Incarnation != incarnation || owner.OwnerID != ownerID {
		return ErrStaleReconnect
	}
	delete(s.owners, sessionID)
	return nil
}

func (s *MemoryReconnectOwners) CleanupExpired(ctx context.Context, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s == nil || limit <= 0 || limit > 10_000 {
		return 0, ErrStaleReconnect
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now().UTC()
	cleaned := 0
	for sessionID, owner := range s.owners {
		if cleaned == limit {
			break
		}
		previousExpired := owner.PreviousCredentialID == "" ||
			(!now.Before(owner.PreviousExpiresAt) && !now.Before(owner.PreviousAcceptUntil))
		if !now.Before(owner.OwnerLeaseExpiresAt) && !now.Before(owner.ExpiresAt) && previousExpired {
			delete(s.owners, sessionID)
			cleaned++
		}
	}
	return cleaned, nil
}

func (s *MemoryReconnectOwners) Read(sessionID identity.SessionID) (ReconnectOwner, bool) {
	if s == nil {
		return ReconnectOwner{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, ok := s.owners[sessionID]
	return owner, ok
}

func validReconnectOwner(owner ReconnectOwner) bool {
	if owner.Claims().CredentialID == "" || validateReconnectClaims(owner.Claims(), time.Time{}, false) != nil || !boundedReconnectValue(owner.OwnerID) {
		return false
	}
	if !validPreviousReconnectOwner(owner) {
		return false
	}
	return owner.State == ReconnectOwnerEstablishing || owner.State == ReconnectOwnerActive || owner.State == ReconnectOwnerTombstoned || owner.State == ReconnectOwnerClaiming
}

func validStoredReconnectOwner(owner ReconnectOwner) bool {
	return validReconnectOwner(owner) && !owner.OwnerLeaseExpiresAt.IsZero()
}

func validPreviousReconnectOwner(owner ReconnectOwner) bool {
	empty := owner.PreviousCredentialID == "" && owner.PreviousIncarnation == 0 && owner.PreviousCursor == 0 && owner.PreviousExpiresAt.IsZero() && owner.PreviousAcceptUntil.IsZero()
	if empty {
		return true
	}
	return boundedReconnectValue(owner.PreviousCredentialID) && owner.PreviousIncarnation > 0 && owner.PreviousCursor >= 0 && !owner.PreviousExpiresAt.IsZero() &&
		!owner.PreviousAcceptUntil.IsZero() && !owner.PreviousAcceptUntil.After(owner.PreviousExpiresAt)
}

func reconnectPreviousClaims(owner ReconnectOwner) ReconnectClaims {
	return ReconnectClaims{Scope: owner.Scope, SessionID: owner.SessionID, Incarnation: owner.PreviousIncarnation, Cursor: owner.PreviousCursor,
		Model: owner.Model, ExpiresAt: owner.PreviousExpiresAt, CredentialID: owner.PreviousCredentialID}
}

func earlierTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}
