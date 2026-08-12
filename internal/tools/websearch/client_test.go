package websearch

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chuanmingliu/openai-gpt-live/internal/toolpolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type resolverFunc func(context.Context, string, string) ([]netip.Addr, error)

func (f resolverFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f(ctx, network, host)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func validRequest() Request {
	return Request{Query: "golang", Policy: toolpolicy.DefaultPolicy().WebSearch}
}

func response(status int, contentType, encoding string, body []byte) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header: http.Header{
			"Content-Type":     []string{contentType},
			"Content-Encoding": []string{encoding},
		},
		Body: io.NopCloser(bytes.NewReader(body)),
	}
}

func resultHTML(title, snippet, href string) []byte {
	return []byte(`<html><body><div class="result web-result"><h2 class="result__title"><a class="result__a" href="` + href + `">` + title + `</a></h2><a class="result__snippet">` + snippet + `</a></div></body></html>`)
}

func executorWithTransport(transport http.RoundTripper) *Executor {
	return &Executor{client: &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

func TestSearchUsesExactPOSTAndPolicyForm(t *testing.T) {
	var method, target, accept, contentType, acceptEncoding string
	var form url.Values
	executor := executorWithTransport(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		method, target = request.Method, request.URL.String()
		accept = request.Header.Get("Accept")
		contentType = request.Header.Get("Content-Type")
		acceptEncoding = request.Header.Get("Accept-Encoding")
		body, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		form, err = url.ParseQuery(string(body))
		require.NoError(t, err)
		return response(http.StatusOK, "text/html; charset=utf-8", "", resultHTML("Go", "Language", "https://go.dev/")), nil
	}))

	results, err := executor.Search(t.Context(), Request{
		Query: "golang & security",
		Policy: toolpolicy.WebSearchPolicy{
			Enabled: true, SafeSearch: toolpolicy.SafeSearchStrict, Region: "us-en", MaxResults: 3, TimeoutMS: 1_000,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, method)
	assert.Equal(t, "https://html.duckduckgo.com/html/", target)
	assert.Equal(t, "text/html", accept)
	assert.Equal(t, "application/x-www-form-urlencoded", contentType)
	assert.Equal(t, "gzip", acceptEncoding)
	assert.Equal(t, url.Values{"q": {"golang & security"}, "kl": {"us-en"}, "kp": {"1"}}, form)
	require.Len(t, results, 1)
	assert.Equal(t, Result{Title: "Go", Snippet: "Language", URL: "https://go.dev/"}, results[0])
}

func TestSearchAppliesPolicyDeadline(t *testing.T) {
	executor := executorWithTransport(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		deadline, ok := request.Context().Deadline()
		require.True(t, ok)
		remaining := time.Until(deadline)
		assert.Greater(t, remaining, 700*time.Millisecond)
		assert.LessOrEqual(t, remaining, time.Second)
		return response(http.StatusOK, "text/html", "", resultHTML("Go", "Language", "https://go.dev/")), nil
	}))
	request := validRequest()
	request.Policy.TimeoutMS = 1_000
	_, err := executor.Search(t.Context(), request)
	require.NoError(t, err)
}

func TestSearchFollowsZeroRedirectsEvenToPublicHTTPS(t *testing.T) {
	var calls int
	executor := executorWithTransport(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		if calls > 1 {
			t.Fatal("redirect was followed")
		}
		redirect := response(http.StatusFound, "text/html", "", nil)
		redirect.Header.Set("Location", "https://example.com/public")
		redirect.Request = request
		return redirect, nil
	}))
	_, err := executor.Search(t.Context(), validRequest())
	require.Error(t, err)
	assert.Equal(t, 1, calls)
}

func TestSecureDialRejectsEveryNonPublicAddressBeforeDial(t *testing.T) {
	addresses := []string{
		"0.0.0.0", "10.0.0.1", "100.64.0.1", "127.0.0.1", "169.254.169.254",
		"172.16.0.1", "192.0.0.1", "192.0.2.1", "192.168.0.1", "198.18.0.1",
		"198.51.100.1", "203.0.113.1", "224.0.0.1", "240.0.0.1", "255.255.255.255",
		"::", "::1", "::ffff:127.0.0.1", "64:ff9b:1::1", "100::1", "100:0:0:1::1",
		"2001:2::1", "2001:db8::1", "3fff::1", "5f00::1", "fc00::1", "fe80::1", "ff02::1",
	}
	for _, address := range addresses {
		t.Run(address, func(t *testing.T) {
			var dialed bool
			executor, err := newExecutor(resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr(address)}, nil
			}), func(context.Context, string, string) (net.Conn, error) {
				dialed = true
				return nil, errors.New("must not dial")
			})
			require.NoError(t, err)
			_, err = executor.dialContext(t.Context(), "tcp", "html.duckduckgo.com:443")
			require.Error(t, err)
			assert.False(t, dialed)
		})
	}
}

func TestPublicAddressRejectsCompleteCurrentIANASpecialIPv6Ranges(t *testing.T) {
	for _, test := range []struct {
		prefix string
		first  string
		last   string
	}{
		{"100:0:0:1::/64", "100:0:0:1::", "100:0:0:1:ffff:ffff:ffff:ffff"},
		{"2001:2::/48", "2001:2::", "2001:2:0:ffff:ffff:ffff:ffff:ffff"},
		{"3fff::/20", "3fff::", "3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff"},
		{"5f00::/16", "5f00::", "5f00:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
	} {
		t.Run(test.prefix, func(t *testing.T) {
			prefix := netip.MustParsePrefix(test.prefix)
			for _, parsed := range []netip.Addr{netip.MustParseAddr(test.first), prefix.Addr().Next(), netip.MustParseAddr(test.last)} {
				require.True(t, prefix.Contains(parsed))
				assert.False(t, publicAddress(parsed))
			}
		})
	}
	for _, address := range []string{"2001::1", "2001:1::1", "2001:3::1", "2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff"} {
		assert.False(t, publicAddress(netip.MustParseAddr(address)), address)
	}
	assert.True(t, publicAddress(netip.MustParseAddr("2606:4700:4700::1111")))
}

func TestSecureDialRejectsMixedResolutionBeforeAnyDial(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "100:0:0:1::1", "2001:2::1", "3fff::1", "5f00::1"} {
		t.Run(address, func(t *testing.T) {
			var dialed bool
			executor, err := newExecutor(resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr(address)}, nil
			}), func(context.Context, string, string) (net.Conn, error) {
				dialed = true
				return nil, nil
			})
			require.NoError(t, err)
			_, err = executor.dialContext(t.Context(), "tcp", "html.duckduckgo.com:443")
			require.Error(t, err)
			assert.False(t, dialed)
		})
	}
}

func TestNewLocksProductionRedirectProxyAndTLSHostPolicy(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:65535")
	serverName := make(chan string, 1)
	executor, err := newExecutor(resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
	}), func(context.Context, string, string) (net.Conn, error) {
		clientConnection, serverConnection := net.Pipe()
		go func() {
			defer serverConnection.Close()
			server := tls.Server(serverConnection, &tls.Config{
				GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
					serverName <- hello.ServerName
					return nil, errors.New("test TLS stopped after ClientHello")
				},
			})
			_ = server.Handshake()
		}()
		return clientConnection, nil
	})
	require.NoError(t, err)

	transport, ok := executor.client.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Nil(t, transport.Proxy, "production transport must not honor proxy environment variables")
	assert.Nil(t, transport.TLSClientConfig, "nil preserves normal hostname certificate verification")

	redirectRequest, err := http.NewRequest(http.MethodGet, "https://example.com/redirected", nil)
	require.NoError(t, err)
	assert.ErrorIs(t, executor.client.CheckRedirect(redirectRequest, nil), http.ErrUseLastResponse)

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, searchEndpoint, nil)
	require.NoError(t, err)
	_, err = executor.client.Do(request)
	require.Error(t, err)
	assert.Equal(t, searchHost, <-serverName, "the fixed hostname must be the production TLS SNI")
}

func TestSecureDialResolvesAndValidatesAgainOnEveryDial(t *testing.T) {
	var mu sync.Mutex
	lookups := 0
	dials := 0
	resolver := resolverFunc(func(_ context.Context, network, host string) ([]netip.Addr, error) {
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, "ip", network)
		assert.Equal(t, "html.duckduckgo.com", host)
		lookups++
		if lookups == 1 {
			return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
		}
		return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
	})
	executor, err := newExecutor(resolver, func(_ context.Context, network, address string) (net.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		dials++
		assert.Equal(t, "tcp", network)
		assert.Equal(t, "93.184.216.34:443", address)
		return nil, nil
	})
	require.NoError(t, err)
	_, err = executor.dialContext(t.Context(), "tcp", "html.duckduckgo.com:443")
	require.NoError(t, err)
	_, err = executor.dialContext(t.Context(), "tcp", "html.duckduckgo.com:443")
	require.Error(t, err)
	assert.Equal(t, 2, lookups)
	assert.Equal(t, 1, dials)
}

func TestSearchRejectsCompressedAndDecompressedOverflow(t *testing.T) {
	t.Run("compressed", func(t *testing.T) {
		executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
			return response(http.StatusOK, "text/html", "identity", bytes.Repeat([]byte("x"), (512<<10)+1)), nil
		}))
		_, err := executor.Search(t.Context(), validRequest())
		require.Error(t, err)
	})

	t.Run("decompressed", func(t *testing.T) {
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, err := writer.Write(bytes.Repeat([]byte("x"), (1<<20)+1))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
			return response(http.StatusOK, "text/html", "gzip", compressed.Bytes()), nil
		}))
		_, err = executor.Search(t.Context(), validRequest())
		require.Error(t, err)
	})
}

func TestSearchAcceptsBoundedGzipHTML(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(resultHTML("Go", "Language", "https://go.dev/"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, "text/html", "gzip", compressed.Bytes()), nil
	}))
	results, err := executor.Search(t.Context(), validRequest())
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestSearchRejectsStatusTypeEncodingAndUTF8(t *testing.T) {
	tests := []struct {
		name     string
		response *http.Response
	}{
		{"status", response(http.StatusServiceUnavailable, "text/html", "", []byte("secret-result"))},
		{"non html", response(http.StatusOK, "application/json", "", []byte(`{"secret":"result"}`))},
		{"unsupported encoding", response(http.StatusOK, "text/html", "br", resultHTML("Go", "Language", "https://go.dev/"))},
		{"invalid utf8", response(http.StatusOK, "text/html", "", []byte{'<', 'p', '>', 0xff})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return test.response, nil
			}))
			_, err := executor.Search(t.Context(), Request{Query: "secret-query", Policy: toolpolicy.DefaultPolicy().WebSearch})
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "secret-query")
			assert.NotContains(t, err.Error(), "secret-result")
		})
	}
}

func TestSearchCapsCandidatesAtTwentyAndResultsAtTen(t *testing.T) {
	var document strings.Builder
	document.WriteString("<html><body>")
	for index := 0; index < 25; index++ {
		document.WriteString(`<div class="result"><a class="result__a" href="https://example.com/">Title</a><span class="result__snippet">Snippet</span></div>`)
	}
	document.WriteString("</body></html>")
	request := validRequest()
	request.Policy.MaxResults = 10
	executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, "text/html", "", []byte(document.String())), nil
	}))
	results, err := executor.Search(t.Context(), request)
	require.NoError(t, err)
	assert.Len(t, results, 10)

	document.Reset()
	document.WriteString("<html><body>")
	for index := 0; index < 20; index++ {
		document.WriteString(`<div class="result"><a class="result__a" href="http://unsafe.example/">Invalid</a><span class="result__snippet">Snippet</span></div>`)
	}
	document.WriteString(`<div class="result"><a class="result__a" href="https://example.com/">Twenty first</a><span class="result__snippet">Snippet</span></div></body></html>`)
	executor = executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, "text/html", "", []byte(document.String())), nil
	}))
	results, err = executor.Search(t.Context(), request)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestSearchValidatesResultURLsAndFieldBounds(t *testing.T) {
	unsafeURLs := []string{
		"http://example.com/", "javascript:alert(1)", "https://example.com/\nsecret",
		"https://example.com/%0a", "https://user:pass@example.com/", "https://é.example/",
		"https://example.com/" + strings.Repeat("a", 2049),
	}
	for _, unsafeURL := range unsafeURLs {
		t.Run(unsafeURL[:min(len(unsafeURL), 40)], func(t *testing.T) {
			executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return response(http.StatusOK, "text/html", "", resultHTML("Title", "Snippet", unsafeURL)), nil
			}))
			results, err := executor.Search(t.Context(), validRequest())
			require.NoError(t, err)
			assert.Empty(t, results)
		})
	}

	document := `<html><body>` +
		`<div class="result"><a class="result__a" href="https://example.com/too-long-title">` + strings.Repeat("界", 257) + `</a><span class="result__snippet">ok</span></div>` +
		`<div class="result"><a class="result__a" href="https://example.com/too-long-snippet">ok</a><span class="result__snippet">` + strings.Repeat("界", 1025) + `</span></div>` +
		`<div class="result"><a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fgo.dev%2Fdoc%2F">` + strings.Repeat("界", 256) + `</a><span class="result__snippet">` + strings.Repeat("界", 1024) + `</span></div>` +
		`</body></html>`
	executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, "text/html", "", []byte(document)), nil
	}))
	results, err := executor.Search(t.Context(), validRequest())
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "https://go.dev/doc/", results[0].URL)
	assert.Len(t, []rune(results[0].Title), 256)
	assert.Len(t, []rune(results[0].Snippet), 1024)
}

func TestSearchRejectsInvalidQueryAndPolicyWithoutHTTP(t *testing.T) {
	requests := []Request{
		{Query: "", Policy: toolpolicy.DefaultPolicy().WebSearch},
		{Query: "    ", Policy: toolpolicy.DefaultPolicy().WebSearch},
		{Query: "line\nfeed", Policy: toolpolicy.DefaultPolicy().WebSearch},
		{Query: "tab\tinside", Policy: toolpolicy.DefaultPolicy().WebSearch},
		{Query: "nul\x00byte", Policy: toolpolicy.DefaultPolicy().WebSearch},
		{Query: strings.Repeat("q", 513), Policy: toolpolicy.DefaultPolicy().WebSearch},
		{Query: string([]byte{0xff}), Policy: toolpolicy.DefaultPolicy().WebSearch},
	}
	mutations := []func(*toolpolicy.WebSearchPolicy){
		func(policy *toolpolicy.WebSearchPolicy) { policy.Enabled = false },
		func(policy *toolpolicy.WebSearchPolicy) { policy.SafeSearch = "off" },
		func(policy *toolpolicy.WebSearchPolicy) { policy.Region = "invalid" },
		func(policy *toolpolicy.WebSearchPolicy) { policy.MaxResults = 0 },
		func(policy *toolpolicy.WebSearchPolicy) { policy.MaxResults = 11 },
		func(policy *toolpolicy.WebSearchPolicy) { policy.TimeoutMS = 999 },
		func(policy *toolpolicy.WebSearchPolicy) { policy.TimeoutMS = 10_001 },
	}
	for _, mutate := range mutations {
		request := validRequest()
		mutate(&request.Policy)
		requests = append(requests, request)
	}
	for _, request := range requests {
		called := false
		executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
			called = true
			return nil, errors.New("must not execute")
		}))
		_, err := executor.Search(t.Context(), request)
		require.Error(t, err)
		assert.False(t, called)
	}
}

func TestSearchCancellationAndDependencyErrorsDoNotLeakContent(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		executor := executorWithTransport(roundTripFunc(func(request *http.Request) (*http.Response, error) {
			<-request.Context().Done()
			return nil, request.Context().Err()
		}))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := executor.Search(ctx, Request{Query: "secret-query", Policy: toolpolicy.DefaultPolicy().WebSearch})
		require.ErrorIs(t, err, context.Canceled)
		assert.NotContains(t, err.Error(), "secret-query")
	})

	t.Run("resolver", func(t *testing.T) {
		executor, err := newExecutor(resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
			return nil, errors.New("secret-resolver-content")
		}), func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("must not dial")
		})
		require.NoError(t, err)
		_, err = executor.dialContext(t.Context(), "tcp", "html.duckduckgo.com:443")
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "secret-resolver-content")
	})

	t.Run("transport", func(t *testing.T) {
		executor := executorWithTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("secret-transport-content")
		}))
		_, err := executor.Search(t.Context(), Request{Query: "secret-query", Policy: toolpolicy.DefaultPolicy().WebSearch})
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "secret-query")
		assert.NotContains(t, err.Error(), "secret-transport-content")
	})
}
