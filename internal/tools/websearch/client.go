// Package websearch provides the fixed, bounded DuckDuckGo HTML search tool.
package websearch

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"golang.org/x/net/html"
)

const (
	searchEndpoint           = "https://html.duckduckgo.com/html/"
	searchHost               = "html.duckduckgo.com"
	searchPort               = "443"
	maximumCompressedBytes   = 512 << 10
	maximumDecompressedBytes = 1 << 20
	maximumCandidates        = 20
	maximumResults           = 10
	maximumQueryBytes        = 512
	maximumTitleRunes        = 256
	maximumSnippetRunes      = 1024
	maximumURLBytes          = 2048
)

// ErrProviderChallenge classifies a content-free refusal by the fixed search
// provider. Callers may expose only this stable class, never the response body.
var ErrProviderChallenge = errors.New("web search provider challenge")

// ErrNetworkPolicy classifies a content-free rejection when local DNS or
// proxying resolves the fixed provider to an address that is not public.
var ErrNetworkPolicy = errors.New("web search network policy")

// Resolver is the narrow DNS boundary used by the fixed search transport.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type dialContextFunc func(context.Context, string, string) (net.Conn, error)

// Request combines private query text with the trusted, persisted policy.
type Request struct {
	Query  string
	Policy toolpolicy.WebSearchPolicy
}

// Result is worker-private until Project removes its query and snippet fields.
type Result struct {
	Title   string `json:"title"`
	Snippet string `json:"snippet"`
	URL     string `json:"url"`
}

// Executor owns a dedicated transport. It never accepts a destination, header,
// command, package, environment, or credential from tool arguments.
type Executor struct {
	client      *http.Client
	dialContext dialContextFunc
}

// New builds an executor that resolves the fixed DuckDuckGo hostname itself.
func New(resolver Resolver) (*Executor, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return newExecutor(resolver, dialer.DialContext)
}

func newExecutor(resolver Resolver, dial dialContextFunc) (*Executor, error) {
	if nilInterface(resolver) || dial == nil {
		return nil, errors.New("web search resolver configuration is invalid")
	}
	secureDial := func(ctx context.Context, network, address string) (net.Conn, error) {
		if ctx == nil {
			return nil, errors.New("web search connection context is invalid")
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil || host != searchHost || port != searchPort {
			return nil, errors.New("web search destination is invalid")
		}
		addresses, err := resolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("web search resolution canceled: %w", ctx.Err())
			}
			return nil, errors.New("web search resolution failed")
		}
		if len(addresses) == 0 {
			return nil, errors.New("web search resolution failed")
		}
		for _, address := range addresses {
			if !publicAddress(address) {
				return nil, ErrNetworkPolicy
			}
		}
		for _, address := range addresses {
			connection, dialErr := dial(ctx, network, net.JoinHostPort(address.String(), port))
			if dialErr == nil {
				return connection, nil
			}
			if ctx.Err() != nil {
				return nil, fmt.Errorf("web search connection canceled: %w", ctx.Err())
			}
		}
		return nil, errors.New("web search connection failed")
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           secureDial,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          8,
		MaxIdleConnsPerHost:   8,
	}
	executor := &Executor{dialContext: secureDial}
	executor.client = &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return executor, nil
}

// Search issues one fixed POST and returns only bounded parsed results.
func (e *Executor) Search(ctx context.Context, request Request) ([]Result, error) {
	if e == nil || e.client == nil || ctx == nil {
		return nil, errors.New("web search executor is unavailable")
	}
	query, err := normalizeQuery(request.Query)
	if err != nil {
		return nil, err
	}
	request.Query = query
	if err := validateSearchRequest(request); err != nil {
		return nil, err
	}
	requestContext, cancel := context.WithTimeout(ctx, time.Duration(request.Policy.TimeoutMS)*time.Millisecond)
	defer cancel()
	form := url.Values{
		"q":  {request.Query},
		"kl": {request.Policy.Region},
		"kp": {safeSearchParameter(request.Policy.SafeSearch)},
	}
	httpRequest, err := http.NewRequestWithContext(requestContext, http.MethodPost, searchEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, errors.New("web search request could not be built")
	}
	httpRequest.Header.Set("Accept", "text/html")
	httpRequest.Header.Set("Accept-Encoding", "gzip")
	httpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := e.client.Do(httpRequest)
	if err != nil {
		if requestContext.Err() != nil {
			return nil, fmt.Errorf("web search request canceled: %w", requestContext.Err())
		}
		if errors.Is(err, ErrNetworkPolicy) {
			return nil, ErrNetworkPolicy
		}
		return nil, errors.New("web search request failed")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusAccepted {
		return nil, ErrProviderChallenge
	}
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("web search response status is invalid")
	}
	mediaType, parameters, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "text/html") {
		return nil, errors.New("web search response type is invalid")
	}
	if charset := parameters["charset"]; charset != "" && !strings.EqualFold(charset, "utf-8") {
		return nil, errors.New("web search response charset is invalid")
	}
	document, err := boundedResponseBody(response.Body, response.Header.Get("Content-Encoding"))
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(document) {
		return nil, errors.New("web search response encoding is invalid")
	}
	root, err := html.Parse(bytes.NewReader(document))
	if err != nil {
		return nil, errors.New("web search response HTML is invalid")
	}
	return parseResults(root, request.Policy.MaxResults), nil
}

func validateSearchRequest(request Request) error {
	query, err := normalizeQuery(request.Query)
	if err != nil || query != request.Query {
		return errors.New("web search query is invalid")
	}
	canonical, err := toolpolicy.Canonicalize(toolpolicy.Policy{WebSearch: request.Policy}, toolpolicy.DefaultCatalog())
	if err != nil || canonical.WebSearch != request.Policy || !request.Policy.Enabled {
		return errors.New("web search policy is invalid")
	}
	return nil
}

func normalizeQuery(query string) (string, error) {
	if !utf8.ValidString(query) {
		return "", errors.New("web search query is invalid")
	}
	for _, character := range query {
		if unicode.IsControl(character) {
			return "", errors.New("web search query is invalid")
		}
	}
	query = strings.TrimSpace(query)
	if len(query) < 1 || len(query) > maximumQueryBytes {
		return "", errors.New("web search query is invalid")
	}
	return query, nil
}

func safeSearchParameter(value toolpolicy.SafeSearch) string {
	if value == toolpolicy.SafeSearchStrict {
		return "1"
	}
	return "-1"
}

func boundedResponseBody(body io.Reader, contentEncoding string) ([]byte, error) {
	compressed, err := readBounded(body, maximumCompressedBytes)
	if err != nil {
		return nil, errors.New("web search compressed response exceeds bounds")
	}
	var decoded io.Reader = bytes.NewReader(compressed)
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "", "identity":
	case "gzip":
		reader, gzipErr := gzip.NewReader(bytes.NewReader(compressed))
		if gzipErr != nil {
			return nil, errors.New("web search compressed response is invalid")
		}
		defer reader.Close()
		decoded = reader
	default:
		return nil, errors.New("web search response encoding is unsupported")
	}
	document, err := readBounded(decoded, maximumDecompressedBytes)
	if err != nil {
		return nil, errors.New("web search decompressed response exceeds bounds")
	}
	return document, nil
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errors.New("bounded read failed")
	}
	return data, nil
}

func parseResults(root *html.Node, limit int) []Result {
	if limit < 1 || limit > maximumResults {
		return nil
	}
	candidates := make([]*html.Node, 0, maximumCandidates)
	collectCandidates(root, &candidates)
	results := make([]Result, 0, min(limit, len(candidates)))
	for _, candidate := range candidates {
		anchor := findClass(candidate, "result__a")
		snippetNode := findClass(candidate, "result__snippet")
		if anchor == nil || snippetNode == nil {
			continue
		}
		title := normalizedText(anchor)
		snippet := normalizedText(snippetNode)
		resultURL, ok := resultURL(attribute(anchor, "href"))
		if !ok || !validField(title, 1, maximumTitleRunes) || !validField(snippet, 1, maximumSnippetRunes) {
			continue
		}
		results = append(results, Result{Title: title, Snippet: snippet, URL: resultURL})
		if len(results) == limit {
			break
		}
	}
	return results
}

func collectCandidates(node *html.Node, candidates *[]*html.Node) {
	if node == nil || len(*candidates) >= maximumCandidates {
		return
	}
	if node.Type == html.ElementNode && hasClass(node, "result") {
		*candidates = append(*candidates, node)
		return
	}
	for child := node.FirstChild; child != nil && len(*candidates) < maximumCandidates; child = child.NextSibling {
		collectCandidates(child, candidates)
	}
}

func findClass(node *html.Node, class string) *html.Node {
	if node == nil {
		return nil
	}
	if node.Type == html.ElementNode && hasClass(node, class) {
		return node
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if found := findClass(child, class); found != nil {
			return found
		}
	}
	return nil
}

func hasClass(node *html.Node, class string) bool {
	return containsField(attribute(node, "class"), class)
}

func containsField(value, expected string) bool {
	for _, field := range strings.Fields(value) {
		if field == expected {
			return true
		}
	}
	return false
}

func attribute(node *html.Node, key string) string {
	for _, attribute := range node.Attr {
		if attribute.Key == key {
			return attribute.Val
		}
	}
	return ""
}

func normalizedText(node *html.Node) string {
	var text strings.Builder
	var walk func(*html.Node)
	walk = func(current *html.Node) {
		if current.Type == html.TextNode {
			text.WriteString(current.Data)
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(node)
	return strings.Join(strings.Fields(text.String()), " ")
}

func validField(value string, minimum, maximum int) bool {
	if !utf8.ValidString(value) {
		return false
	}
	count := 0
	for _, char := range value {
		if unicode.IsControl(char) {
			return false
		}
		count++
	}
	return count >= minimum && count <= maximum
}

func resultURL(raw string) (string, bool) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	if parsed.Scheme == "" && parsed.Host == "duckduckgo.com" && parsed.Path == "/l/" && parsed.User == nil {
		values := parsed.Query()["uddg"]
		if len(values) != 1 {
			return "", false
		}
		raw = values[0]
	}
	if len(raw) < 1 || len(raw) > maximumURLBytes || !asciiString(raw) {
		return "", false
	}
	decoded, err := url.PathUnescape(raw)
	if err != nil || !utf8.ValidString(decoded) {
		return "", false
	}
	for _, char := range decoded {
		if unicode.IsControl(char) {
			return "", false
		}
	}
	parsed, err = url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil || parsed.Opaque != "" {
		return "", false
	}
	return raw, true
}

func asciiString(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("100:0:0:1::/64"),
	// The IETF protocol-assignment block contains mixed-purpose exceptions.
	// This fixed-destination adapter does not need any of them, so the whole
	// block stays conservatively unavailable to DNS answers.
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

func publicAddress(address netip.Addr) bool {
	if !address.IsValid() || address.Zone() != "" {
		return false
	}
	address = address.Unmap()
	if !address.IsGlobalUnicast() {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}
