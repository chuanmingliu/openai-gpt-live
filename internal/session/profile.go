package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/chuanmingliu/openai-gpt-live/internal/handoff"
	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
)

func (runtime *ProfileRuntime) HasTurnTemplate() bool {
	if runtime == nil {
		return false
	}
	_, ok := runtime.Profile.Bundle.Templates["turn_instruction.j2"]
	return ok
}

// HandoffProfileSnapshot pins one deterministic server-owned recipe from the
// session's immutable profile. Model arguments cannot alter recipe, registry,
// engine, or knowledge authority.
func (runtime *ProfileRuntime) HandoffProfileSnapshot(transcript string) (handoff.ProfileSnapshot, bool) {
	if runtime == nil || len(runtime.Profile.Snapshot.Recipes) == 0 {
		return handoff.ProfileSnapshot{}, false
	}
	selected := runtime.Profile.Snapshot.Recipes[0]
	for _, candidate := range runtime.Profile.Snapshot.Recipes[1:] {
		if candidate.ID < selected.ID {
			selected = candidate
		}
	}
	recipeHash, err := selected.Hash()
	if err != nil {
		return handoff.ProfileSnapshot{}, false
	}
	evidence, err := json.Marshal(struct {
		Transcript string `json:"transcript"`
	}{Transcript: transcript})
	if err != nil {
		return handoff.ProfileSnapshot{}, false
	}
	knowledge := make([]handoff.KnowledgeVersion, len(runtime.Profile.Snapshot.Knowledge))
	for index, reference := range runtime.Profile.Snapshot.Knowledge {
		knowledge[index] = handoff.KnowledgeVersion{ID: strings.Clone(reference.ID), Version: reference.Version, Hash: strings.Clone(reference.Hash)}
	}
	return handoff.ProfileSnapshot{
		ProfileID: string(runtime.Profile.Snapshot.Ref.ID), ProfileVersion: runtime.Profile.Snapshot.Ref.Version,
		SnapshotHash: strings.Clone(runtime.Profile.Snapshot.SnapshotHash), EngineVersion: strings.Clone(runtime.Profile.Snapshot.EngineVersion),
		RecipeID: strings.Clone(selected.ID), RecipeVersion: selected.Version, RecipeHash: recipeHash,
		RegistryVersion: strings.Clone(runtime.Profile.Snapshot.AdapterRegistryVersion), Knowledge: knowledge,
		Evidence: evidence,
	}, true
}

func disableAutomaticResponse(raw json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return append(json.RawMessage(nil), raw...), nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(trimmed, &object) != nil || object == nil {
		return nil, profile.ErrProfileIncompatible
	}
	object["create_response"] = json.RawMessage("false")
	encoded, err := json.Marshal(object)
	if err != nil {
		return nil, profile.ErrProfileIncompatible
	}
	return encoded, nil
}

// ProfileRuntime owns the exact compiled bundle and the render results that
// are fixed for one immutable realtime profile snapshot. Prompt source never
// crosses the client or telemetry boundaries.
type ProfileRuntime struct {
	Profile        profile.Resolved
	Compiled       prompttmpl.CompiledBundle
	SessionContext prompttmpl.Context
	System         string
	Opening        string
}

func prepareProfileRuntime(ctx context.Context, selected profile.Resolved, engine prompttmpl.Engine) (*ProfileRuntime, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	selected = selected.Clone()
	if selected.Validate() != nil || isNilInterface(engine) || engine.Version() != selected.Snapshot.EngineVersion {
		return nil, profile.ErrProfileIncompatible
	}
	compiled, err := engine.Compile(ctx, selected.Bundle, prompttmpl.DefaultLimits())
	if err != nil || isNilInterface(compiled) || compiled.EngineVersion() != selected.Snapshot.EngineVersion || engine.Version() != selected.Snapshot.EngineVersion {
		return nil, profileRuntimeError(ctx, err)
	}
	sessionContext := prompttmpl.Context{}
	systemContext, err := prompttmpl.ValidateContext(selected.Bundle.Schema, sessionContext, "system.j2")
	if err != nil {
		return nil, profile.ErrProfileIncompatible
	}
	system, err := compiled.Render(ctx, "system.j2", systemContext)
	if err != nil || strings.TrimSpace(system) == "" {
		return nil, profileRuntimeError(ctx, err)
	}

	opening := ""
	if _, ok := selected.Bundle.Templates["opening_message.j2"]; ok {
		openingContext, contextErr := prompttmpl.ValidateContext(selected.Bundle.Schema, sessionContext, "opening_message.j2")
		if contextErr != nil {
			return nil, profile.ErrProfileIncompatible
		}
		opening, err = compiled.Render(ctx, "opening_message.j2", openingContext)
		if err != nil {
			return nil, profileRuntimeError(ctx, err)
		}
	}
	return &ProfileRuntime{
		Profile: selected, Compiled: compiled, SessionContext: sessionContext,
		System: strings.Clone(system), Opening: strings.Clone(opening),
	}, nil
}

func profileRuntimeError(ctx context.Context, err error) error {
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return profile.ErrProfileIncompatible
}
