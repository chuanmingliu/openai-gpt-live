package profile

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
	"github.com/stretchr/testify/require"
)

const builtinSystemPrompt = "You are a realtime spoken assistant. Reply in the user's language using at most three short sentences unless the user explicitly asks for more detail. Use natural speech without markdown, headings, lists, or narrated reasoning. Keep background work silent and mention it only after a result is available. Only use the server-owned reserved background-task tools: start_background_task, steer_background_task, and cancel_background_task. Never invent task IDs, task results, or capabilities."

func TestBuiltInProfileRequiresNoStoreAndHasStableLiteralHash(t *testing.T) {
	first, second := Builtin(), Builtin()
	require.NoError(t, first.Snapshot.Validate())
	require.Equal(t, VersionRef{ID: "builtin", Version: 1}, first.Snapshot.Ref)
	require.Equal(t, "builtin", first.Snapshot.TenantID)
	require.Equal(t, prompttmpl.NewMiniJinja().Version(), first.Snapshot.EngineVersion)
	require.Equal(t, first.Snapshot.SnapshotHash, second.Snapshot.SnapshotHash)

	// This literal is intentionally updated only when the reviewed built-in
	// assets or canonical snapshot contract changes.
	const stableSnapshotHash = "e31054fe0e94fab962253ac0a86d26ecdd05893d72520a9446befa9be7cfc568"
	require.Equal(t, stableSnapshotHash, first.Snapshot.SnapshotHash)
}

func TestBuiltInProfileRendersExactStaticAssetWithEmptyTypedContext(t *testing.T) {
	builtin := Builtin()
	require.Equal(t, map[string]string{"system.j2": builtinSystemPrompt}, builtin.Bundle.Templates)
	require.Empty(t, builtin.Bundle.Schema.Fields)
	compiled, err := prompttmpl.NewMiniJinja().Compile(t.Context(), builtin.Bundle, prompttmpl.DefaultLimits())
	require.NoError(t, err)
	output, err := compiled.Render(t.Context(), "system.j2", prompttmpl.Context{})
	require.NoError(t, err)
	require.Equal(t, builtinSystemPrompt, output)
}

func TestBuiltInProfileHasNoOptionalTemplatesOrCustomComponents(t *testing.T) {
	builtin := Builtin()
	require.NotEqual(t, TemplateRef{}, builtin.Snapshot.System)
	require.Equal(t, TemplateRef{}, builtin.Snapshot.Opening)
	require.Equal(t, TemplateRef{}, builtin.Snapshot.Turn)
	require.Empty(t, builtin.Snapshot.Recipes)
	require.Empty(t, builtin.Snapshot.Bindings)
	require.Empty(t, builtin.Snapshot.Knowledge)
	require.NotEmpty(t, builtin.Snapshot.AdapterRegistryVersion)
	_, hasOpening := builtin.Bundle.Templates["opening_message.j2"]
	require.False(t, hasOpening)
	_, hasTurn := builtin.Bundle.Templates["turn_instruction.j2"]
	require.False(t, hasTurn)
}

func TestBuiltInProfileReturnsDeepOwnedClones(t *testing.T) {
	first := Builtin()
	first.Bundle.Templates["system.j2"] = "mutated"
	first.Bundle.Schema.Fields["secret"] = prompttmpl.Field{Type: prompttmpl.TypeString}
	first.Snapshot.System.Hash = strings.Repeat("f", 64)
	first.Snapshot.Bindings = append(first.Snapshot.Bindings, AdapterBinding{AdapterID: "mutated"})

	second := Builtin()
	require.Equal(t, builtinSystemPrompt, second.Bundle.Templates["system.j2"])
	require.Empty(t, second.Bundle.Schema.Fields)
	require.NotEqual(t, strings.Repeat("f", 64), second.Snapshot.System.Hash)
	require.Empty(t, second.Snapshot.Bindings)
}

func TestResolveBuiltInWorksWithoutStoreForEveryPurposeAndChecksEngine(t *testing.T) {
	for _, purpose := range []ResolvePurpose{ResolveNewSession, ResolveReconnect, ResolveAcceptedWork} {
		engine := &publisherEngine{delegate: prompttmpl.NewMiniJinja()}
		resolved, err := NewResolver(nil, engine).Resolve(t.Context(), testTenant, VersionRef{ID: "builtin", Version: 1}, purpose)
		require.NoError(t, err)
		require.Equal(t, Builtin().Snapshot.SnapshotHash, resolved.Snapshot.SnapshotHash)
		require.Equal(t, 1, engine.calls())
	}

	engine := &publisherEngine{delegate: prompttmpl.NewMiniJinja(), versions: []string{"other-engine"}}
	_, err := NewResolver(nil, engine).Resolve(t.Context(), testTenant, VersionRef{ID: "builtin", Version: 1}, ResolveNewSession)
	require.ErrorIs(t, err, ErrProfileIncompatible)
}

func TestResolveBuiltInRejectsInvalidVersionPurposeContextAndTenant(t *testing.T) {
	resolver := NewResolver(nil, prompttmpl.NewMiniJinja())
	_, err := resolver.Resolve(t.Context(), testTenant, VersionRef{ID: "builtin", Version: 2}, ResolveNewSession)
	require.ErrorIs(t, err, ErrProfileIncompatible)
	_, err = resolver.Resolve(t.Context(), testTenant, VersionRef{ID: "builtin", Version: 1}, ResolvePurpose("invalid"))
	require.ErrorIs(t, err, ErrInvalidProfile)
	_, err = resolver.Resolve(nil, testTenant, VersionRef{ID: "builtin", Version: 1}, ResolveNewSession)
	require.ErrorIs(t, err, ErrInvalidProfile)
	_, err = resolver.Resolve(t.Context(), "bad\ntenant", VersionRef{ID: "builtin", Version: 1}, ResolveNewSession)
	require.ErrorIs(t, err, ErrInvalidProfile)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = resolver.Resolve(ctx, testTenant, VersionRef{ID: "builtin", Version: 1}, ResolveNewSession)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBuiltInProfileDoesNotDependOnEnvironmentOrWorkingDirectory(t *testing.T) {
	before := Builtin()
	t.Setenv("VERTICAL_PROFILE_PROMPT", "malicious prompt override")
	original, err := os.Getwd()
	require.NoError(t, err)
	temp := t.TempDir()
	require.NoError(t, os.Chdir(temp))
	t.Cleanup(func() { _ = os.Chdir(original) })
	after := Builtin()
	require.Equal(t, before, after)
	require.NotContains(t, after.Bundle.Templates["system.j2"], "malicious")
}
