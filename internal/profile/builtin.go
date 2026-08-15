package profile

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"strings"

	"github.com/chuanmingliu/openai-gpt-live/internal/prompttmpl"
)

const (
	builtinTenantID               = "builtin"
	builtinVersion         uint64 = 1
	builtinRegistryVersion        = "builtin-v1"
)

//go:embed builtin/system.j2
var builtinSystemSource string

//go:embed builtin/context.schema.json
var builtinSchemaSource []byte

//go:embed builtin/fixture.json
var builtinFixtureSource []byte

var embeddedBuiltin = mustBuildBuiltin()

// Builtin returns the tenant-neutral, checked-in voice-only profile. Every call
// returns owned maps and slices and performs no filesystem, environment,
// database, network, or clock access.
func Builtin() Resolved {
	return embeddedBuiltin.Clone()
}

func mustBuildBuiltin() Resolved {
	schema := decodeBuiltinSchema(builtinSchemaSource)
	logicalSystem := builtinSystemSource
	if logicalSystem == "" || strings.Contains(logicalSystem, "\r") {
		panic("profile: invalid built-in system asset")
	}
	templateBlob, err := (Blob{MediaType: templateMediaTypeUTF8, Content: []byte(logicalSystem)}).Canonical()
	if err != nil {
		panic("profile: invalid built-in system asset")
	}
	fixtureBlob, err := (Blob{MediaType: fixtureMediaTypeJSON, Content: builtinFixtureSource}).Canonical()
	if err != nil {
		panic("profile: invalid built-in fixture asset")
	}
	manifest := Manifest{Bundle: PromptBundle{
		Templates: map[string]TemplateRef{"system.j2": refForBuiltinBlob(templateBlob)},
		Schema:    schema,
		Fixtures:  map[string]TemplateRef{"fixture.json": refForBuiltinBlob(fixtureBlob)},
	}}
	if manifest.Validate() != nil {
		panic("profile: invalid built-in manifest")
	}
	bundle := prompttmpl.Bundle{Templates: map[string]string{"system.j2": logicalSystem}, Schema: clonePromptSchema(schema)}
	engine := prompttmpl.NewMiniJinja()
	compiled, err := engine.Compile(context.Background(), cloneRenderBundle(bundle), prompttmpl.DefaultLimits())
	if err != nil || isNilProfileDependency(compiled) || compiled.EngineVersion() != engine.Version() {
		panic("profile: incompatible built-in template")
	}
	outputs, err := validateAndRenderFixtures(context.Background(), manifest, bundle, map[string][]byte{"fixture.json": append([]byte(nil), builtinFixtureSource...)}, compiled)
	if err != nil || outputs["fixture.json"] != logicalSystem {
		panic("profile: invalid built-in fixture")
	}
	bundleHash, err := manifest.Bundle.Hash()
	if err != nil {
		panic("profile: invalid built-in bundle hash")
	}
	snapshot := Snapshot{
		TenantID: builtinTenantID, Ref: VersionRef{ID: builtinProfileID, Version: builtinVersion},
		EngineVersion: engine.Version(), BundleHash: bundleHash, System: manifest.Bundle.Templates["system.j2"],
		AdapterRegistryVersion: builtinRegistryVersion,
	}
	snapshotHash, err := snapshot.Hash()
	if err != nil {
		panic("profile: invalid built-in snapshot hash")
	}
	snapshot.SnapshotHash = snapshotHash
	if snapshot.Validate() != nil || manifest.ValidateSnapshot(snapshot) != nil {
		panic("profile: inconsistent built-in snapshot")
	}
	return Resolved{Snapshot: snapshot.Clone(), Bundle: cloneRenderBundle(bundle)}
}

func decodeBuiltinSchema(raw []byte) prompttmpl.ContextSchema {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var schema prompttmpl.ContextSchema
	if err := decoder.Decode(&schema); err != nil || decoder.Decode(&struct{}{}) != io.EOF || schema.Fields == nil {
		panic("profile: invalid built-in context schema")
	}
	return clonePromptSchema(schema)
}

func refForBuiltinBlob(blob Blob) TemplateRef {
	return TemplateRef{Hash: blob.Hash, MediaType: blob.MediaType, Bytes: blob.ContentBytes}
}
