package ws

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"unicode"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/profile"
)

var (
	errProfileQuery       = errors.New("profile_query_invalid")
	errProfileForbidden   = errors.New("profile_forbidden")
	errProfileUnavailable = errors.New("profile_unavailable")
)

func resolveProfile(ctx context.Context, rawQuery, tenantID string, enabled bool, resolver profile.Resolver) (profile.Resolved, error) {
	ref, custom, err := parseProfileQuery(rawQuery)
	if err != nil {
		return profile.Resolved{}, errProfileQuery
	}
	if !custom || ref.ID == "builtin" {
		if custom && ref.Version != 1 {
			return profile.Resolved{}, errProfileQuery
		}
		return profile.Builtin(), nil
	}
	if !enabled {
		return profile.Resolved{}, errProfileForbidden
	}
	if nilProfileResolver(resolver) {
		return profile.Resolved{}, errProfileUnavailable
	}
	resolved, resolveErr := resolver.Resolve(nonNilProfileContext(ctx), tenantID, ref, profile.ResolveNewSession)
	if resolveErr != nil {
		if errors.Is(resolveErr, profile.ErrProfileRevoked) {
			return profile.Resolved{}, errProfileForbidden
		}
		return profile.Resolved{}, errProfileUnavailable
	}
	resolved = resolved.Clone()
	if resolved.Snapshot.TenantID != tenantID || resolved.Snapshot.Ref != ref || resolved.Snapshot.Validate() != nil {
		return profile.Resolved{}, errProfileUnavailable
	}
	return resolved, nil
}

func parseProfileQuery(rawQuery string) (profile.VersionRef, bool, error) {
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return profile.VersionRef{}, false, errProfileQuery
	}
	for key, values := range query {
		switch key {
		case "model", "reconnect", "profile", "profile_version":
		default:
			return profile.VersionRef{}, false, errProfileQuery
		}
		if len(values) != 1 {
			return profile.VersionRef{}, false, errProfileQuery
		}
	}
	ids, hasID := query["profile"]
	versions, hasVersion := query["profile_version"]
	if hasID != hasVersion {
		return profile.VersionRef{}, false, errProfileQuery
	}
	if !hasID {
		return profile.VersionRef{}, false, nil
	}
	version, err := strconv.ParseUint(versions[0], 10, 64)
	if err != nil || strconv.FormatUint(version, 10) != versions[0] {
		return profile.VersionRef{}, false, errProfileQuery
	}
	ref := profile.VersionRef{ID: profile.ID(ids[0]), Version: version}
	if ref.Validate() != nil || !validProfileSelectorID(ids[0]) {
		return profile.VersionRef{}, false, errProfileQuery
	}
	return ref, true, nil
}

func validProfileSelectorID(id string) bool {
	if !utf8.ValidString(id) {
		return false
	}
	for _, value := range id {
		if unicode.IsSpace(value) || unicode.IsControl(value) {
			return false
		}
	}
	return true
}

func nilProfileResolver(resolver profile.Resolver) bool {
	if resolver == nil {
		return true
	}
	value := reflect.ValueOf(resolver)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func nonNilProfileContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func writeProfileError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errProfileQuery):
		w.WriteHeader(http.StatusBadRequest)
	case errors.Is(err, errProfileForbidden):
		w.WriteHeader(http.StatusForbidden)
	default:
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}
