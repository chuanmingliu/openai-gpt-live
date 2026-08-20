package session

import (
	"context"
	"errors"
	"strings"

	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
)

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
