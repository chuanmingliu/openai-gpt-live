package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
)

const (
	maximumProfileJSONBytes     = 1 << 20
	maximumProfileTemplateBytes = 64 << 10
	maximumProfileListLimit     = 100
	defaultProfileListLimit     = 32
)

var errProfileBadRequest = errors.New("profile request is invalid")

// coalesceProfileBodyError preserves the oversize class so routes can answer
// 413 while every other malformed body stays a generic 400.
func coalesceProfileBodyError(err error) error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return err
	}
	return errProfileBadRequest
}

// profileTemplateInfo is the safe logical-file projection. Blob hashes are
// integrity identifiers, never authority; source is fetched separately.
type profileTemplateInfo struct {
	Path  string `json:"path"`
	Hash  string `json:"hash"`
	Bytes int    `json:"bytes"`
}

type profileDraftResponse struct {
	ID          string                `json:"id"`
	Generation  int64                 `json:"generation"`
	Description string                `json:"description"`
	Templates   []profileTemplateInfo `json:"templates"`
	Fixtures    []profileTemplateInfo `json:"fixtures"`
	UpdatedAt   string                `json:"updated_at"`
}

type publishedProfileSummary struct {
	ID            string `json:"id"`
	Version       uint64 `json:"version"`
	Active        bool   `json:"active"`
	Revoked       bool   `json:"revoked"`
	PublishedAt   string `json:"published_at"`
	SnapshotHash  string `json:"snapshot_hash"`
	EngineVersion string `json:"engine_version"`
}

type publishedProfileList struct {
	Profiles []publishedProfileSummary `json:"profiles"`
	Cursor   string                    `json:"cursor,omitempty"`
}

type profilePreviewResponse struct {
	BundleHash   string            `json:"bundle_hash"`
	SnapshotHash string            `json:"snapshot_hash"`
	Outputs      map[string]string `json:"outputs"`
}

type profilePublishResponse struct {
	ID           string `json:"id"`
	Version      uint64 `json:"version"`
	SnapshotHash string `json:"snapshot_hash"`
	Active       bool   `json:"active"`
}

type profileErrorBody struct {
	Error profileErrorCode `json:"error"`
}

type profileErrorCode struct {
	Code string `json:"code"`
}

func draftResponse(draft profile.Draft) profileDraftResponse {
	response := profileDraftResponse{
		ID:          string(draft.ID),
		Generation:  draft.Generation,
		Description: draft.Manifest.Description,
		Templates:   sortedTemplateInfos(draft.Manifest.Bundle.Templates),
		Fixtures:    sortedTemplateInfos(draft.Manifest.Bundle.Fixtures),
		UpdatedAt:   draft.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
	return response
}

func sortedTemplateInfos(refs map[string]profile.TemplateRef) []profileTemplateInfo {
	infos := make([]profileTemplateInfo, 0, len(refs))
	for path, ref := range refs {
		infos = append(infos, profileTemplateInfo{Path: path, Hash: ref.Hash, Bytes: ref.Bytes})
	}
	sort.Slice(infos, func(left, right int) bool { return infos[left].Path < infos[right].Path })
	return infos
}

func publishedSummary(summary profile.PublishedSummary) publishedProfileSummary {
	return publishedProfileSummary{
		ID:            string(summary.Ref.ID),
		Version:       summary.Ref.Version,
		Active:        summary.Active,
		Revoked:       summary.RevokedAt != nil,
		PublishedAt:   summary.PublishedAt.UTC().Format(time.RFC3339Nano),
		SnapshotHash:  summary.SnapshotHash,
		EngineVersion: summary.EngineVersion,
	}
}

func writeProfileJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeProfileErrorCode(writer http.ResponseWriter, status int, code string) {
	writeProfileJSON(writer, status, profileErrorBody{Error: profileErrorCode{Code: code}})
}

// writeProfileDomainError maps only stable domain classes to statuses and
// never serializes dependency error text.
func writeProfileDomainError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, profile.ErrStaleDraft):
		writeProfileErrorCode(writer, http.StatusConflict, "stale_draft")
	case errors.Is(err, profile.ErrPublicationConflict):
		writeProfileErrorCode(writer, http.StatusConflict, "publication_conflict")
	case errors.Is(err, profile.ErrProfileRevoked):
		writeProfileErrorCode(writer, http.StatusConflict, "profile_revoked")
	case errors.Is(err, profile.ErrInvalidProfile):
		writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "profile_invalid")
	case errors.Is(err, profile.ErrProfileIncompatible):
		writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "profile_incompatible")
	default:
		writeProfileErrorCode(writer, http.StatusServiceUnavailable, "store_unavailable")
	}
}

// decodeProfileObject decodes one strict JSON object whose keys come from the
// allowed set exactly once, with no trailing content. String values return as
// strings; the two numeric keys return as canonical decimal strings; boolean
// keys return "true"/"false".
func decodeProfileObject(writer http.ResponseWriter, request *http.Request, allowed map[string]byte) (map[string]string, error) {
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maximumProfileJSONBytes))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return nil, coalesceProfileBodyError(err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return nil, errProfileBadRequest
	}
	values := make(map[string]string, len(allowed))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, coalesceProfileBodyError(err)
		}
		key, ok := token.(string)
		if !ok {
			return nil, errProfileBadRequest
		}
		kind, allowedKey := allowed[key]
		if !allowedKey {
			return nil, errProfileBadRequest
		}
		if _, duplicate := values[key]; duplicate {
			return nil, errProfileBadRequest
		}
		valueToken, err := decoder.Token()
		if err != nil {
			return nil, coalesceProfileBodyError(err)
		}
		switch kind {
		case 's':
			value, ok := valueToken.(string)
			if !ok {
				return nil, errProfileBadRequest
			}
			values[key] = value
		case 'n':
			number, ok := valueToken.(json.Number)
			if !ok {
				return nil, errProfileBadRequest
			}
			values[key] = number.String()
		case 'b':
			value, ok := valueToken.(bool)
			if !ok {
				return nil, errProfileBadRequest
			}
			values[key] = strconv.FormatBool(value)
		default:
			return nil, errProfileBadRequest
		}
	}
	token, err = decoder.Token()
	if err != nil {
		return nil, errProfileBadRequest
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '}' {
		return nil, errProfileBadRequest
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errProfileBadRequest
	}
	return values, nil
}

func parseProfileGeneration(value string) (int64, error) {
	generation, err := strconv.ParseInt(value, 10, 64)
	if err != nil || generation <= 0 || strconv.FormatInt(generation, 10) != value {
		return 0, errProfileBadRequest
	}
	return generation, nil
}

func parseProfileVersion(value string) (uint64, error) {
	version, err := strconv.ParseUint(value, 10, 64)
	if err != nil || version == 0 || strconv.FormatUint(version, 10) != value {
		return 0, errProfileBadRequest
	}
	return version, nil
}

// parseIfMatchGeneration parses `If-Match: "<generation>"`. A missing header
// is reported separately so the route can answer 428.
func parseIfMatchGeneration(request *http.Request) (int64, bool, error) {
	values := request.Header.Values("If-Match")
	if len(values) == 0 {
		return 0, false, nil
	}
	if len(values) != 1 {
		return 0, true, errProfileBadRequest
	}
	raw := strings.TrimSpace(values[0])
	if len(raw) < 3 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return 0, true, errProfileBadRequest
	}
	generation, err := parseProfileGeneration(raw[1 : len(raw)-1])
	if err != nil {
		return 0, true, err
	}
	return generation, true, nil
}

func draftETag(generation int64) string {
	return `"` + strconv.FormatInt(generation, 10) + `"`
}

// validProfilePathID mirrors the gateway's selector rules: valid UTF-8 with no
// spaces or control characters, bounded by the profile identifier domain.
func validProfilePathID(id string) bool {
	if id == "" || id == "builtin" || !utf8.ValidString(id) || profile.ID(id).Validate() != nil {
		return false
	}
	for _, value := range id {
		if unicode.IsSpace(value) || unicode.IsControl(value) || value == '/' || value == '%' {
			return false
		}
	}
	return true
}
