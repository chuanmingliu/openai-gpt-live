package main

import (
	"errors"
	"mime"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/knowledge"
)

const (
	// maximumKnowledgeRequestBytes leaves room for multipart framing above the
	// document ceiling the ingestor enforces.
	maximumKnowledgeRequestBytes = knowledge.MaxUploadBytes + (64 << 10)
	maximumKnowledgeListLimit    = 100
)

// knowledgeVersionSummary is the safe projection of one published version.
// Excerpt text is deliberately absent: listing is an operator overview, and
// material is read only through a preview the operator just uploaded.
type knowledgeVersionSummary struct {
	Version      uint64 `json:"version"`
	Hash         string `json:"hash"`
	Title        string `json:"title"`
	ExcerptCount int    `json:"excerpt_count"`
	PublishedAt  string `json:"published_at"`
	Tombstoned   bool   `json:"tombstoned"`
}

type knowledgeListResponse struct {
	Versions []knowledgeVersionSummary `json:"versions"`
}

type knowledgeExcerptView struct {
	Ordinal int    `json:"ordinal"`
	Text    string `json:"text"`
	Section string `json:"section"`
}

type knowledgePreviewResponse struct {
	PackID    string                 `json:"pack_id"`
	Hash      string                 `json:"hash"`
	Title     string                 `json:"title"`
	MediaType string                 `json:"media_type"`
	Excerpts  []knowledgeExcerptView `json:"excerpts"`
}

type knowledgePublishedResponse struct {
	PackID  string `json:"pack_id"`
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

// serveKnowledge routes the loopback knowledge endpoints. The whole surface is
// absent unless the demo was started with knowledge storage, so a misconfigured
// deployment cannot advertise material it could never resolve.
func (handler *demoHandler) serveKnowledge(writer http.ResponseWriter, request *http.Request, segments []string) {
	service := handler.profiles
	if service == nil || service.knowledge == nil {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if len(segments) < 2 {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	packID := segments[1]
	if !validKnowledgePackID(packID) {
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	switch {
	case len(segments) == 2 && request.Method == http.MethodGet:
		handler.serveKnowledgeList(writer, request, packID)
	case len(segments) == 2 && request.Method == http.MethodPost:
		handler.serveKnowledgePreview(writer, request, packID)
	case len(segments) == 3 && segments[2] == "publish" && request.Method == http.MethodPost:
		handler.serveKnowledgePublish(writer, request, packID)
	case len(segments) == 4 && segments[3] == "tombstone" && request.Method == http.MethodPost:
		handler.serveKnowledgeTombstone(writer, request, packID, segments[2])
	case len(segments) == 2 || (len(segments) == 3 && segments[2] == "publish") ||
		(len(segments) == 4 && segments[3] == "tombstone"):
		writer.WriteHeader(http.StatusMethodNotAllowed)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func (handler *demoHandler) serveKnowledgeList(writer http.ResponseWriter, request *http.Request, packID string) {
	service := handler.profiles
	versions, err := service.knowledge.List(request.Context(), service.tenantID, packID, maximumKnowledgeListLimit)
	if err != nil {
		writeKnowledgeError(writer, err)
		return
	}
	summaries := make([]knowledgeVersionSummary, 0, len(versions))
	for _, version := range versions {
		summaries = append(summaries, knowledgeVersionSummary{
			Version:      version.Ref.Version,
			Hash:         version.Ref.Hash,
			Title:        version.Title,
			ExcerptCount: len(version.Excerpts),
			PublishedAt:  version.PublishedAt.UTC().Format(time.RFC3339Nano),
			Tombstoned:   !version.Bindable(),
		})
	}
	writeProfileJSON(writer, http.StatusOK, knowledgeListResponse{Versions: summaries})
}

func (handler *demoHandler) serveKnowledgePreview(writer http.ResponseWriter, request *http.Request, packID string) {
	document, _, err := handler.readKnowledgeUpload(writer, request, packID)
	if err != nil {
		writeKnowledgeUploadError(writer, err)
		return
	}
	preview, err := knowledge.NewIngestor().Preview(request.Context(), document)
	if err != nil {
		writeKnowledgeError(writer, err)
		return
	}
	views := make([]knowledgeExcerptView, 0, len(preview.Excerpts))
	for _, excerpt := range preview.Excerpts {
		views = append(views, knowledgeExcerptView{
			Ordinal: excerpt.Ordinal,
			Text:    excerpt.Text,
			Section: excerpt.Citation.Section,
		})
	}
	writeProfileJSON(writer, http.StatusOK, knowledgePreviewResponse{
		PackID:    packID,
		Hash:      preview.Hash,
		Title:     preview.Title,
		MediaType: preview.MediaType,
		Excerpts:  views,
	})
}

func (handler *demoHandler) serveKnowledgePublish(writer http.ResponseWriter, request *http.Request, packID string) {
	service := handler.profiles
	document, previewedHash, err := handler.readKnowledgeUpload(writer, request, packID)
	if err != nil {
		writeKnowledgeUploadError(writer, err)
		return
	}
	// The ingestor re-derives excerpts from the uploaded bytes and refuses
	// unless they hash to what the operator previewed, so the browser cannot
	// publish content nobody reviewed.
	record, err := knowledge.NewIngestor().PublishRecord(request.Context(), document, previewedHash)
	if err != nil {
		writeKnowledgeError(writer, err)
		return
	}
	published, err := service.knowledge.Publish(request.Context(), record)
	if err != nil {
		writeKnowledgeError(writer, err)
		return
	}
	writeProfileJSON(writer, http.StatusCreated, knowledgePublishedResponse{
		PackID:  packID,
		Version: published.Ref.Version,
		Hash:    published.Ref.Hash,
	})
}

func (handler *demoHandler) serveKnowledgeTombstone(writer http.ResponseWriter, request *http.Request, packID, rawVersion string) {
	service := handler.profiles
	version, err := parseProfileVersion(rawVersion)
	if err != nil {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	versions, err := service.knowledge.List(request.Context(), service.tenantID, packID, maximumKnowledgeListLimit)
	if err != nil {
		writeKnowledgeError(writer, err)
		return
	}
	// The browser names a version number only; the hash comes from the store,
	// so a caller cannot tombstone by asserting content it never saw.
	for _, candidate := range versions {
		if candidate.Ref.Version != version {
			continue
		}
		if err := service.knowledge.Tombstone(request.Context(), service.tenantID,
			candidate.Ref, service.actor, service.now()); err != nil {
			writeKnowledgeError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	writer.WriteHeader(http.StatusNotFound)
}

// readKnowledgeUpload parses one bounded multipart document. It returns the
// document and the operator's previously previewed hash, if any. No filename or
// upload byte is retained beyond the returned value.
func (handler *demoHandler) readKnowledgeUpload(writer http.ResponseWriter, request *http.Request, packID string) (knowledge.Document, string, error) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" {
		return knowledge.Document{}, "", errKnowledgeUnsupportedMedia
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maximumKnowledgeRequestBytes)
	if err := request.ParseMultipartForm(1 << 20); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return knowledge.Document{}, "", errKnowledgeTooLarge
		}
		return knowledge.Document{}, "", errKnowledgeBadRequest
	}
	defer func() { _ = request.MultipartForm.RemoveAll() }()

	files := request.MultipartForm.File["document"]
	if len(files) != 1 || len(request.MultipartForm.File) != 1 {
		return knowledge.Document{}, "", errKnowledgeBadRequest
	}
	hash := ""
	if values := request.MultipartForm.Value["hash"]; len(values) == 1 {
		hash = values[0]
	} else if len(values) > 1 {
		return knowledge.Document{}, "", errKnowledgeBadRequest
	}
	if len(request.MultipartForm.Value) > 1 {
		return knowledge.Document{}, "", errKnowledgeBadRequest
	}

	content, err := readKnowledgeFile(files[0])
	if err != nil {
		return knowledge.Document{}, "", err
	}
	service := handler.profiles
	return knowledge.Document{
		TenantID: service.tenantID,
		PackID:   packID,
		Filename: files[0].Filename,
		Content:  content,
		Actor:    service.actor,
		At:       service.now(),
	}, hash, nil
}

func readKnowledgeFile(header *multipart.FileHeader) ([]byte, error) {
	if header.Size > knowledge.MaxUploadBytes {
		return nil, errKnowledgeTooLarge
	}
	file, err := header.Open()
	if err != nil {
		return nil, errKnowledgeBadRequest
	}
	defer func() { _ = file.Close() }()
	content := make([]byte, header.Size)
	read := 0
	for read < len(content) {
		n, err := file.Read(content[read:])
		read += n
		if err != nil {
			break
		}
	}
	return content[:read], nil
}

var (
	errKnowledgeBadRequest       = errors.New("knowledge request is invalid")
	errKnowledgeTooLarge         = errors.New("knowledge upload is too large")
	errKnowledgeUnsupportedMedia = errors.New("knowledge upload media type is unsupported")
)

func writeKnowledgeUploadError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errKnowledgeTooLarge):
		writer.WriteHeader(http.StatusRequestEntityTooLarge)
	case errors.Is(err, errKnowledgeUnsupportedMedia):
		writer.WriteHeader(http.StatusUnsupportedMediaType)
	default:
		writer.WriteHeader(http.StatusBadRequest)
	}
}

// writeKnowledgeError maps only stable domain classes. No filename, excerpt, or
// dependency error text is ever serialized.
func writeKnowledgeError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, knowledge.ErrUnsupportedContent):
		writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "unsupported_content")
	case errors.Is(err, knowledge.ErrContentLimit):
		writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "content_limit")
	case errors.Is(err, knowledge.ErrInvalidKnowledge):
		writeProfileErrorCode(writer, http.StatusUnprocessableEntity, "knowledge_invalid")
	case errors.Is(err, knowledge.ErrKnowledgeNotFound):
		writeProfileErrorCode(writer, http.StatusNotFound, "knowledge_not_found")
	case errors.Is(err, knowledge.ErrKnowledgeTombstoned):
		writeProfileErrorCode(writer, http.StatusConflict, "knowledge_tombstoned")
	default:
		writeProfileErrorCode(writer, http.StatusServiceUnavailable, "store_unavailable")
	}
}

// validKnowledgePackID reuses the profile identifier domain, which already
// refuses the reserved builtin name, path separators, and control characters.
func validKnowledgePackID(packID string) bool { return validProfilePathID(packID) }
