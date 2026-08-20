package adaptercatalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/chuanmingliu/openai-gpt-live/internal/worker"
)

type Catalog struct {
	registryVersion string
	manifestByID    map[string]Manifest
	hash            string
}

func New(registryVersion string, manifests []Manifest, knownSecretRefs []string) (*Catalog, error) {
	if !validID(registryVersion) || len(manifests) > 256 {
		return nil, ErrInvalidManifest
	}
	known := make(map[string]struct{}, len(knownSecretRefs))
	for _, ref := range knownSecretRefs {
		if !validID(ref) {
			return nil, ErrInvalidManifest
		}
		known[ref] = struct{}{}
	}
	owned := &Catalog{registryVersion: registryVersion, manifestByID: make(map[string]Manifest, len(manifests))}
	for _, manifest := range manifests {
		if manifest.RegistryVersion != registryVersion {
			return nil, ErrInvalidManifest
		}
		if err := validateManifest(manifest, known, true); err != nil {
			return nil, err
		}
		if _, duplicate := owned.manifestByID[manifest.AdapterID]; duplicate {
			return nil, ErrInvalidManifest
		}
		owned.manifestByID[manifest.AdapterID] = manifest.clone()
	}
	ordered := sortedManifestClones(manifests)
	wires := make([]manifestWire, len(ordered))
	for index, manifest := range ordered {
		wires[index] = manifestToWire(manifest)
	}
	canonical, err := json.Marshal(struct {
		Version   string         `json:"version"`
		Manifests []manifestWire `json:"manifests"`
	}{registryVersion, wires})
	if err != nil {
		return nil, ErrInvalidManifest
	}
	digest := sha256.Sum256(canonical)
	owned.hash = hex.EncodeToString(digest[:])
	return owned, nil
}

func (catalog *Catalog) RegistryVersion() string {
	if catalog == nil {
		return ""
	}
	return catalog.registryVersion
}

func (catalog *Catalog) Hash() string {
	if catalog == nil {
		return ""
	}
	return catalog.hash
}

func (catalog *Catalog) Manifests() []Manifest {
	if catalog == nil {
		return nil
	}
	values := make([]Manifest, 0, len(catalog.manifestByID))
	for _, manifest := range catalog.manifestByID {
		values = append(values, manifest.clone())
	}
	return sortedManifestClones(values)
}

func (catalog *Catalog) Manifest(adapterID string) (Manifest, bool) {
	if catalog == nil {
		return Manifest{}, false
	}
	manifest, ok := catalog.manifestByID[adapterID]
	return manifest.clone(), ok
}

type Installed struct {
	AdapterID      string
	AdapterVersion string
	DestinationID  string
	TransportClass string
	Executor       worker.ToolExecutor
	Projector      worker.ToolResultProjector
	StartedPhase   string
	CompletedPhase string
}

func CompileRegistry(catalog *Catalog, bindings map[string]Binding, installed []Installed) (worker.ToolRegistry, error) {
	if catalog == nil {
		return nil, ErrInvalidManifest
	}
	installedByID := make(map[string]Installed, len(installed))
	for _, value := range installed {
		if value.Executor == nil || value.AdapterID == "" {
			return nil, ErrInstalledMismatch
		}
		if _, duplicate := installedByID[value.AdapterID]; duplicate {
			return nil, ErrInstalledMismatch
		}
		installedByID[value.AdapterID] = value
	}
	ids := make([]string, 0, len(bindings))
	for id := range bindings {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	registered := make([]worker.RegisteredTool, 0, len(ids))
	for _, id := range ids {
		manifest, ok := catalog.manifestByID[id]
		if !ok {
			return nil, ErrInstalledMismatch
		}
		binding, err := ValidateBinding(manifest, bindings[id])
		if err != nil {
			return nil, err
		}
		if !binding.Enabled {
			continue
		}
		native, ok := installedByID[id]
		if !ok || native.AdapterVersion != manifest.AdapterVersion || native.DestinationID != manifest.DestinationID || native.TransportClass != manifest.TransportClass || native.Executor == nil {
			return nil, ErrInstalledMismatch
		}
		registered = append(registered, worker.RegisteredTool{
			AdapterID: manifest.AdapterID, AdapterVersion: manifest.AdapterVersion,
			RegistryVersion: manifest.RegistryVersion, DestinationID: manifest.DestinationID,
			TransportClass: manifest.TransportClass, Approval: manifest.Approval,
			Definition: manifest.Tool.Clone(), ReadOnly: manifest.ReadOnly,
			SideEffecting: manifest.SideEffecting, ParallelSafe: manifest.ParallelSafe,
			StableIdempotency: manifest.StableIdempotency, Timeout: binding.Timeout,
			MaxConcurrency: binding.MaxConcurrency, MaxArgumentBytes: manifest.MaxArgumentBytes,
			MaxResultBytes: binding.MaxResultBytes, Executor: native.Executor, Projector: native.Projector,
			StartedPhase: native.StartedPhase, CompletedPhase: native.CompletedPhase,
		})
	}
	for id := range installedByID {
		if _, reviewed := catalog.manifestByID[id]; !reviewed {
			return nil, ErrInstalledMismatch
		}
	}
	return worker.NewToolRegistry(map[string][]worker.RegisteredTool{catalog.registryVersion: registered})
}
