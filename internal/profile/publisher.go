package profile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"reflect"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
)

const (
	builtinProfileID         ID = "builtin"
	templateMediaTypePlain      = "text/plain"
	templateMediaTypeUTF8       = "text/plain; charset=utf-8"
	fixtureMediaTypeJSON        = "application/json"
	fixtureMediaTypeJSONUTF8    = "application/json; charset=utf-8"
	maxFixtureJSONDepth         = 16
)

// AdapterCatalog validates profile restrictions against the deployed,
// server-owned adapter registry and exposes that registry's immutable version.
type AdapterCatalog interface {
	ValidateBindings(context.Context, string, []AdapterBinding) error
	Version() string
}

// PerResponseInstructionCatalog is the optional capability required by a
// profile that publishes turn_instruction.j2.
type PerResponseInstructionCatalog interface {
	SupportsPerResponseInstructions(context.Context, string) (bool, error)
}

// KnowledgeReferenceValidator verifies exact tenant-owned immutable knowledge
// references. A nil validator intentionally accepts only an empty reference list.
type KnowledgeReferenceValidator interface {
	ValidateReferences(context.Context, string, []KnowledgeRef) error
}

// PublishCommand identifies one exact draft generation to validate or publish.
type PublishCommand struct {
	TenantID           string
	ProfileID          ID
	ExpectedGeneration int64
	Actor              string
	Activate           bool
}

// PreviewResult contains only owned synthetic fixture output and immutable
// integrity identifiers. It never exposes draft source implicitly.
type PreviewResult struct {
	Outputs      map[string]string
	BundleHash   string
	SnapshotHash string
}

// PublisherClock supplies deterministic publication audit timestamps.
type PublisherClock interface {
	Now() time.Time
}

type realPublicationClock struct{}

func (realPublicationClock) Now() time.Time { return time.Now() }

// PublisherOption configures deterministic, non-authoritative publisher
// dependencies. Publication identity remains supplied by the command/store.
type PublisherOption func(*Publisher)

// WithPublisherClock installs the clock used for publication/revocation audit
// timestamps. A nil or typed-nil clock is ignored.
func WithPublisherClock(clock PublisherClock) PublisherOption {
	return func(publisher *Publisher) {
		if publisher != nil && !isNilProfileDependency(clock) {
			publisher.clock = clock
		}
	}
}

// Publisher owns the one validation pipeline used by validate, preview, and
// immutable publication.
type Publisher struct {
	Store     Store
	Engine    prompttmpl.Engine
	Catalog   AdapterCatalog
	Knowledge KnowledgeReferenceValidator

	clock PublisherClock
}

// NewPublisher constructs a publisher while preserving the four-dependency
// composition shape used by later setup/runtime slices.
func NewPublisher(store Store, engine prompttmpl.Engine, catalog AdapterCatalog, knowledge KnowledgeReferenceValidator, options ...PublisherOption) *Publisher {
	publisher := &Publisher{Store: store, Engine: engine, Catalog: catalog, Knowledge: knowledge, clock: realPublicationClock{}}
	for _, option := range options {
		if option != nil {
			option(publisher)
		}
	}
	return publisher
}

// Validate runs the complete bounded publication pipeline and returns no
// rendered fixture content.
func (publisher *Publisher) Validate(ctx context.Context, command PublishCommand) error {
	_, err := publisher.prepare(ctx, command)
	return err
}

// Preview runs the exact publication validation path without Store mutation.
func (publisher *Publisher) Preview(ctx context.Context, command PublishCommand) (PreviewResult, error) {
	prepared, err := publisher.prepare(ctx, command)
	if err != nil {
		return PreviewResult{}, err
	}
	return PreviewResult{
		Outputs: cloneFixtureOutputs(prepared.outputs), BundleHash: prepared.snapshot.BundleHash,
		SnapshotHash: prepared.snapshot.SnapshotHash,
	}, nil
}

// Publish validates the exact generation completely before asking Store to
// atomically persist the already-built immutable record.
func (publisher *Publisher) Publish(ctx context.Context, command PublishCommand) (Published, error) {
	prepared, err := publisher.prepare(ctx, command)
	if err != nil {
		return Published{}, err
	}
	if err := contextBoundaryError(ctx); err != nil {
		return Published{}, err
	}
	record := PublishRecord{
		TenantID: command.TenantID, ProfileID: command.ProfileID,
		ExpectedGeneration: command.ExpectedGeneration, DraftGeneration: prepared.generation,
		Manifest: prepared.manifest.Clone(), Snapshot: prepared.snapshot.Clone(),
		EngineVersion: prepared.snapshot.EngineVersion, Actor: command.Actor,
		Activate: command.Activate, PublishedAt: publisher.now(),
	}
	if record.Validate() != nil || !validPublisherAuditTime(record.PublishedAt) {
		return Published{}, ErrInvalidProfile
	}
	published, err := publisher.Store.Publish(ctx, record)
	if err != nil {
		return Published{}, normalizeStoreBoundaryError(ctx, err)
	}
	return published, nil
}

// Activate delegates one exact future-session active-version swap to Store.
func (publisher *Publisher) Activate(ctx context.Context, tenantID string, ref VersionRef, actor string) error {
	if err := validatePublisherMutation(publisher, ctx, tenantID, ref, actor); err != nil {
		return err
	}
	if err := publisher.Store.Activate(ctx, tenantID, ref.ID, ref.Version, actor); err != nil {
		return normalizeStoreBoundaryError(ctx, err)
	}
	return nil
}

// Revoke delegates one exact version revocation using the publisher audit clock.
func (publisher *Publisher) Revoke(ctx context.Context, tenantID string, ref VersionRef, actor string) error {
	if err := validatePublisherMutation(publisher, ctx, tenantID, ref, actor); err != nil {
		return err
	}
	revokedAt := publisher.now()
	if !validPublisherAuditTime(revokedAt) {
		return ErrInvalidProfile
	}
	if err := publisher.Store.Revoke(ctx, tenantID, ref.ID, ref.Version, actor, revokedAt); err != nil {
		return normalizeStoreBoundaryError(ctx, err)
	}
	return nil
}

func validatePublisherMutation(publisher *Publisher, ctx context.Context, tenantID string, ref VersionRef, actor string) error {
	if err := validateProfileBoundaryContext(ctx); err != nil {
		return err
	}
	if publisher == nil || isNilProfileDependency(publisher.Store) {
		return ErrProfileIncompatible
	}
	if !validIdentifier(tenantID) || ref.Validate() != nil || ref.ID == builtinProfileID || !validIdentifier(actor) {
		return ErrInvalidProfile
	}
	return nil
}

type preparedPublication struct {
	manifest   Manifest
	snapshot   Snapshot
	outputs    map[string]string
	generation int64
}

func (publisher *Publisher) prepare(ctx context.Context, command PublishCommand) (preparedPublication, error) {
	if err := validateProfileBoundaryContext(ctx); err != nil {
		return preparedPublication{}, err
	}
	if command.ProfileID == builtinProfileID || !validIdentifier(command.TenantID) || command.ProfileID.Validate() != nil || command.ExpectedGeneration <= 0 || !validIdentifier(command.Actor) {
		return preparedPublication{}, ErrInvalidProfile
	}
	if publisher == nil || isNilProfileDependency(publisher.Store) || isNilProfileDependency(publisher.Engine) || isNilProfileDependency(publisher.Catalog) {
		return preparedPublication{}, ErrProfileIncompatible
	}

	engineVersion := publisher.Engine.Version()
	catalogVersion := publisher.Catalog.Version()
	if !validShortText(engineVersion) || !validShortText(catalogVersion) {
		return preparedPublication{}, ErrProfileIncompatible
	}

	draft, ok, err := publisher.Store.LoadDraft(ctx, command.TenantID, command.ProfileID)
	if err != nil {
		return preparedPublication{}, normalizeStoreBoundaryError(ctx, err)
	}
	if !ok || draft.Generation != command.ExpectedGeneration {
		return preparedPublication{}, ErrStaleDraft
	}
	if draft.TenantID != command.TenantID || draft.ID != command.ProfileID || draft.Validate() != nil {
		return preparedPublication{}, ErrInvalidProfile
	}
	manifest := draft.Manifest.Clone()
	bundle, fixtureSources, err := loadManifestBundle(ctx, publisher.Store, command.TenantID, manifest)
	if err != nil {
		return preparedPublication{}, err
	}

	compiled, err := publisher.Engine.Compile(ctx, cloneRenderBundle(bundle), prompttmpl.DefaultLimits())
	if err != nil {
		return preparedPublication{}, normalizeCompileBoundaryError(ctx, err)
	}
	if isNilProfileDependency(compiled) {
		return preparedPublication{}, ErrProfileIncompatible
	}
	engineVersionAfter := publisher.Engine.Version()
	if !validShortText(engineVersionAfter) || engineVersionAfter != engineVersion || compiled.EngineVersion() != engineVersion {
		return preparedPublication{}, ErrProfileIncompatible
	}

	outputs, err := validateAndRenderFixtures(ctx, manifest, bundle, fixtureSources, compiled)
	if err != nil {
		return preparedPublication{}, err
	}

	bindings := cloneBindings(manifest.Bindings)
	if err := publisher.Catalog.ValidateBindings(ctx, command.TenantID, bindings); err != nil {
		return preparedPublication{}, normalizeIncompatibleBoundaryError(ctx, err)
	}
	if err := contextBoundaryError(ctx); err != nil {
		return preparedPublication{}, err
	}
	if _, hasTurn := manifest.Bundle.Templates["turn_instruction.j2"]; hasTurn {
		capability, ok := publisher.Catalog.(PerResponseInstructionCatalog)
		if !ok || isNilProfileDependency(capability) {
			return preparedPublication{}, ErrProfileIncompatible
		}
		allowed, capabilityErr := capability.SupportsPerResponseInstructions(ctx, command.TenantID)
		if capabilityErr != nil {
			return preparedPublication{}, normalizeIncompatibleBoundaryError(ctx, capabilityErr)
		}
		if !allowed {
			return preparedPublication{}, ErrProfileIncompatible
		}
	}
	catalogVersionAfter := publisher.Catalog.Version()
	if !validShortText(catalogVersionAfter) || catalogVersionAfter != catalogVersion {
		return preparedPublication{}, ErrProfileIncompatible
	}

	knowledge := append([]KnowledgeRef(nil), manifest.Knowledge...)
	if isNilProfileDependency(publisher.Knowledge) {
		if len(knowledge) != 0 {
			return preparedPublication{}, ErrProfileIncompatible
		}
	} else if err := publisher.Knowledge.ValidateReferences(ctx, command.TenantID, knowledge); err != nil {
		return preparedPublication{}, normalizeIncompatibleBoundaryError(ctx, err)
	}
	if err := contextBoundaryError(ctx); err != nil {
		return preparedPublication{}, err
	}

	if _, err := manifest.Hash(); err != nil {
		return preparedPublication{}, ErrInvalidProfile
	}
	bundleHash, err := manifest.Bundle.Hash()
	if err != nil {
		return preparedPublication{}, ErrInvalidProfile
	}
	version, err := publisher.Store.NextVersion(ctx, command.TenantID, command.ProfileID)
	if err != nil {
		return preparedPublication{}, normalizeStoreBoundaryError(ctx, err)
	}
	if err := contextBoundaryError(ctx); err != nil {
		return preparedPublication{}, err
	}
	if version == 0 || version > uint64(math.MaxInt64) {
		return preparedPublication{}, ErrPublicationConflict
	}
	snapshot := Snapshot{
		TenantID: command.TenantID, Ref: VersionRef{ID: command.ProfileID, Version: version},
		EngineVersion: engineVersion, BundleHash: bundleHash,
		System: manifest.Bundle.Templates["system.j2"], Opening: manifest.Bundle.Templates["opening_message.j2"],
		Turn: manifest.Bundle.Templates["turn_instruction.j2"], Recipes: cloneRecipes(manifest.Recipes),
		Bindings: cloneBindings(manifest.Bindings), Knowledge: append([]KnowledgeRef(nil), manifest.Knowledge...),
		AdapterRegistryVersion: catalogVersion,
	}
	snapshotHash, err := snapshot.Hash()
	if err != nil {
		return preparedPublication{}, ErrInvalidProfile
	}
	snapshot.SnapshotHash = snapshotHash
	if snapshot.Validate() != nil || manifest.ValidateSnapshot(snapshot) != nil {
		return preparedPublication{}, ErrInvalidProfile
	}
	return preparedPublication{manifest: manifest.Clone(), snapshot: snapshot.Clone(), outputs: cloneFixtureOutputs(outputs), generation: draft.Generation}, nil
}

func (publisher *Publisher) now() time.Time {
	if publisher == nil || isNilProfileDependency(publisher.clock) {
		return realPublicationClock{}.Now().UTC().Truncate(time.Microsecond)
	}
	return publisher.clock.Now().UTC().Truncate(time.Microsecond)
}

func validPublisherAuditTime(value time.Time) bool {
	return !value.IsZero() && value.Year() >= 1 && value.Year() <= 9999
}

func loadManifestBundle(ctx context.Context, store Store, tenantID string, manifest Manifest) (prompttmpl.Bundle, map[string][]byte, error) {
	if manifest.Validate() != nil {
		return prompttmpl.Bundle{}, nil, ErrInvalidProfile
	}
	templates := make(map[string]string, len(manifest.Bundle.Templates))
	fixtures := make(map[string][]byte, len(manifest.Bundle.Fixtures))
	for _, name := range sortedTemplateRefNames(manifest.Bundle.Templates) {
		ref := manifest.Bundle.Templates[name]
		if !reviewedTemplateMediaType(ref.MediaType) {
			return prompttmpl.Bundle{}, nil, ErrInvalidProfile
		}
		content, err := loadExactProfileBlob(ctx, store, tenantID, ref)
		if err != nil {
			return prompttmpl.Bundle{}, nil, err
		}
		if !utf8.Valid(content) {
			return prompttmpl.Bundle{}, nil, ErrInvalidProfile
		}
		templates[name] = string(content)
	}
	for _, name := range sortedTemplateRefNames(manifest.Bundle.Fixtures) {
		ref := manifest.Bundle.Fixtures[name]
		if !reviewedFixtureMediaType(ref.MediaType) {
			return prompttmpl.Bundle{}, nil, ErrInvalidProfile
		}
		content, err := loadExactProfileBlob(ctx, store, tenantID, ref)
		if err != nil {
			return prompttmpl.Bundle{}, nil, err
		}
		if !utf8.Valid(content) {
			return prompttmpl.Bundle{}, nil, ErrInvalidProfile
		}
		fixtures[name] = append([]byte(nil), content...)
	}
	if err := prompttmpl.ValidateBundleSources(templates); err != nil {
		return prompttmpl.Bundle{}, nil, prompttmpl.ErrTemplateInvalid
	}
	return prompttmpl.Bundle{Templates: templates, Schema: clonePromptSchema(manifest.Bundle.Schema)}, fixtures, nil
}

func loadExactProfileBlob(ctx context.Context, store Store, tenantID string, ref TemplateRef) ([]byte, error) {
	blob, ok, err := store.LoadBlob(ctx, tenantID, ref.Hash)
	if err != nil {
		return nil, normalizeStoreBoundaryError(ctx, err)
	}
	if !ok {
		return nil, ErrInvalidProfile
	}
	if err := contextBoundaryError(ctx); err != nil {
		return nil, err
	}
	canonical, err := blob.Canonical()
	if err != nil || blob.Hash != ref.Hash || blob.MediaType != ref.MediaType || blob.ContentBytes != ref.Bytes || canonical.Hash != ref.Hash || canonical.MediaType != ref.MediaType || canonical.ContentBytes != ref.Bytes {
		return nil, ErrInvalidProfile
	}
	return append([]byte(nil), canonical.Content...), nil
}

func reviewedTemplateMediaType(mediaType string) bool {
	return mediaType == templateMediaTypePlain || mediaType == templateMediaTypeUTF8
}

func reviewedFixtureMediaType(mediaType string) bool {
	return mediaType == fixtureMediaTypeJSON || mediaType == fixtureMediaTypeJSONUTF8
}

func sortedTemplateRefNames(refs map[string]TemplateRef) []string {
	names := make([]string, 0, len(refs))
	for name := range refs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type syntheticFixture struct {
	template string
	context  prompttmpl.Context
	expected string
}

func validateAndRenderFixtures(ctx context.Context, manifest Manifest, bundle prompttmpl.Bundle, sources map[string][]byte, compiled prompttmpl.CompiledBundle) (map[string]string, error) {
	if len(sources) == 0 {
		return nil, prompttmpl.ErrTemplateInvalid
	}
	outputs := make(map[string]string, len(sources))
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fixture, err := decodeSyntheticFixture(sources[name])
		if err != nil {
			return nil, err
		}
		if _, ok := manifest.Bundle.Templates[fixture.template]; !ok {
			return nil, prompttmpl.ErrTemplateInvalid
		}
		ownedContext, err := prompttmpl.ValidateContext(bundle.Schema, fixture.context, fixture.template)
		if err != nil {
			return nil, prompttmpl.ErrContextInvalid
		}
		output, err := compiled.Render(ctx, fixture.template, ownedContext)
		if err != nil {
			return nil, normalizeRenderBoundaryError(ctx, err)
		}
		if !utf8.ValidString(output) {
			return nil, prompttmpl.ErrTemplateInvalid
		}
		if len(output) > prompttmpl.MaxOutputBytes {
			return nil, prompttmpl.ErrRenderLimit
		}
		if output != fixture.expected {
			return nil, prompttmpl.ErrTemplateInvalid
		}
		outputs[name] = output
	}
	return outputs, nil
}

func decodeSyntheticFixture(raw []byte) (syntheticFixture, error) {
	if len(raw) == 0 || len(raw) > prompttmpl.MaxTemplateBytes || !utf8.Valid(raw) || !validJSONSurrogates(raw) || !json.Valid(raw) {
		return syntheticFixture{}, prompttmpl.ErrTemplateInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeStrictFixtureValue(decoder, 0, new(int))
	if err != nil {
		return syntheticFixture{}, prompttmpl.ErrTemplateInvalid
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return syntheticFixture{}, prompttmpl.ErrTemplateInvalid
	}
	object, ok := value.(map[string]any)
	if !ok || len(object) != 3 {
		return syntheticFixture{}, prompttmpl.ErrTemplateInvalid
	}
	template, templateOK := object["template"].(string)
	expected, expectedOK := object["expected"].(string)
	contextValue, contextPresent := object["context"]
	if !templateOK || !expectedOK || !contextPresent || ValidateTemplateName(template) != nil {
		return syntheticFixture{}, prompttmpl.ErrTemplateInvalid
	}
	contextObject, contextOK := contextValue.(map[string]any)
	if !contextOK {
		return syntheticFixture{}, prompttmpl.ErrContextInvalid
	}
	if len(expected) > prompttmpl.MaxOutputBytes || !utf8.ValidString(expected) {
		return syntheticFixture{}, prompttmpl.ErrRenderLimit
	}
	return syntheticFixture{template: template, context: prompttmpl.Context(contextObject), expected: expected}, nil
}

func decodeStrictFixtureValue(decoder *json.Decoder, depth int, count *int) (any, error) {
	if depth > maxFixtureJSONDepth || count == nil || *count >= prompttmpl.MaxCollectionItems {
		return nil, prompttmpl.ErrTemplateInvalid
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '{':
			object := make(map[string]any)
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, prompttmpl.ErrTemplateInvalid
				}
				if _, duplicate := object[key]; duplicate {
					return nil, prompttmpl.ErrTemplateInvalid
				}
				*count++
				if *count > prompttmpl.MaxCollectionItems {
					return nil, prompttmpl.ErrTemplateInvalid
				}
				value, err := decodeStrictFixtureValue(decoder, depth+1, count)
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return nil, prompttmpl.ErrTemplateInvalid
			}
			return object, nil
		case '[':
			array := make([]any, 0)
			for decoder.More() {
				*count++
				if *count > prompttmpl.MaxCollectionItems {
					return nil, prompttmpl.ErrTemplateInvalid
				}
				value, err := decodeStrictFixtureValue(decoder, depth+1, count)
				if err != nil {
					return nil, err
				}
				array = append(array, value)
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return nil, prompttmpl.ErrTemplateInvalid
			}
			return array, nil
		default:
			return nil, prompttmpl.ErrTemplateInvalid
		}
	case string, bool, nil, json.Number:
		return token, nil
	default:
		return nil, prompttmpl.ErrTemplateInvalid
	}
}

func clonePromptSchema(schema prompttmpl.ContextSchema) prompttmpl.ContextSchema {
	clone := prompttmpl.ContextSchema{Fields: make(map[string]prompttmpl.Field, len(schema.Fields))}
	for name, field := range schema.Fields {
		field.Templates = append([]string(nil), field.Templates...)
		clone.Fields[name] = field
	}
	return clone
}

func cloneRenderBundle(bundle prompttmpl.Bundle) prompttmpl.Bundle {
	clone := prompttmpl.Bundle{Templates: make(map[string]string, len(bundle.Templates)), Schema: clonePromptSchema(bundle.Schema)}
	for name, source := range bundle.Templates {
		clone.Templates[name] = source
	}
	return clone
}

func cloneFixtureOutputs(outputs map[string]string) map[string]string {
	clone := make(map[string]string, len(outputs))
	for name, output := range outputs {
		clone[name] = output
	}
	return clone
}

func validateProfileBoundaryContext(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidProfile
	}
	return ctx.Err()
}

func contextBoundaryError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func normalizeStoreBoundaryError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if contextErr := contextBoundaryError(ctx); contextErr != nil {
		return contextErr
	}
	for _, stable := range []error{ErrInvalidProfile, ErrStaleDraft, ErrPublicationConflict, ErrProfileIncompatible, ErrProfileRevoked, ErrStoreUnavailable} {
		if errors.Is(err, stable) {
			return stable
		}
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return ErrStoreUnavailable
}

func normalizeCompileBoundaryError(ctx context.Context, err error) error {
	if contextErr := contextBoundaryError(ctx); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, prompttmpl.ErrRenderLimit) {
		return prompttmpl.ErrRenderLimit
	}
	return prompttmpl.ErrTemplateInvalid
}

func normalizeRenderBoundaryError(ctx context.Context, err error) error {
	if contextErr := contextBoundaryError(ctx); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, prompttmpl.ErrRenderLimit) {
		return prompttmpl.ErrRenderLimit
	}
	return prompttmpl.ErrTemplateInvalid
}

func normalizeIncompatibleBoundaryError(ctx context.Context, err error) error {
	if contextErr := contextBoundaryError(ctx); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return ErrProfileIncompatible
}

func isNilProfileDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
