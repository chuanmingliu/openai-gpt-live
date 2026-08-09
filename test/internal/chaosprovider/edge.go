package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"
)

const edgeKeyID = "task24-chaos-edge"

func runEdge(listen, controlListen, postgresListen, caPath, voiceUpstream, deepseekUpstream, postgresUpstream, issuer string) error {
	certificate, signer, caPEM, err := newEdgeIdentity()
	if err != nil {
		return err
	}
	if err := os.WriteFile(caPath, caPEM, 0o644); err != nil {
		return err
	}
	voice, err := edgeProxy(voiceUpstream, "/voice")
	if err != nil {
		return err
	}
	deepseek, err := edgeProxy(deepseekUpstream, "/deepseek")
	if err != nil {
		return err
	}

	tlsMux := http.NewServeMux()
	tlsMux.Handle("/voice/", voice)
	tlsMux.Handle("/deepseek/", deepseek)
	tlsMux.HandleFunc("/oidc/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer": issuer, "jwks_uri": issuer + "/keys",
			"authorization_endpoint": issuer + "/authorize", "token_endpoint": issuer + "/token",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	tlsMux.HandleFunc("/oidc/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "use": "sig", "alg": "RS256", "kid": edgeKeyID,
			"n": base64.RawURLEncoding.EncodeToString(signer.PublicKey.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1}),
		}}})
	})
	tlsMux.HandleFunc("/otel", otelOK)
	tlsMux.HandleFunc("/otel/", otelOK)

	controlMux := http.NewServeMux()
	controlMux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	controlMux.HandleFunc("GET /token", func(w http.ResponseWriter, _ *http.Request) {
		token, tokenErr := signEdgeToken(signer, issuer, time.Now().UTC())
		if tokenErr != nil {
			http.Error(w, "token unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(token))
	})

	tlsServer := &http.Server{Addr: listen, Handler: tlsMux, ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second, TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}}
	controlServer := &http.Server{Addr: controlListen, Handler: controlMux, ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second}
	errorsCh := make(chan error, 3)
	go func() { errorsCh <- controlServer.ListenAndServe() }()
	go func() { errorsCh <- tlsServer.ListenAndServeTLS("", "") }()
	go func() { errorsCh <- servePostgresTLS(postgresListen, postgresUpstream, certificate) }()
	err = <-errorsCh
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tlsServer.Shutdown(shutdownCtx)
	_ = controlServer.Shutdown(shutdownCtx)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func servePostgresTLS(listen, upstream string, certificate tls.Certificate) error {
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	for {
		client, acceptErr := listener.Accept()
		if acceptErr != nil {
			return acceptErr
		}
		go proxyPostgresTLS(client, upstream, certificate)
	}
}

func proxyPostgresTLS(client net.Conn, upstream string, certificate tls.Certificate) {
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(10 * time.Second))
	var request [8]byte
	if _, err := io.ReadFull(client, request[:]); err != nil || binary.BigEndian.Uint32(request[0:4]) != 8 || binary.BigEndian.Uint32(request[4:8]) != 80877103 {
		return
	}
	if _, err := client.Write([]byte{'S'}); err != nil {
		return
	}
	secure := tls.Server(client, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
	if err := secure.Handshake(); err != nil {
		return
	}
	upstreamConn, err := net.DialTimeout("tcp", upstream, 5*time.Second)
	if err != nil {
		return
	}
	defer upstreamConn.Close()
	_ = secure.SetDeadline(time.Time{})
	_ = upstreamConn.SetDeadline(time.Time{})
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstreamConn, secure); done <- struct{}{} }()
	go func() { _, _ = io.Copy(secure, upstreamConn); done <- struct{}{} }()
	<-done
	_ = upstreamConn.Close()
	_ = client.Close()
	<-done
}

func edgeProxy(raw, prefix string) (http.Handler, error) {
	target, err := url.Parse(raw)
	if err != nil || target.Scheme != "http" || target.Host == "" {
		return nil, errors.New("invalid chaos edge upstream")
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	original := proxy.Director
	proxy.Director = func(request *http.Request) {
		original(request)
		request.URL.Path = strings.TrimPrefix(request.URL.Path, prefix)
		if request.URL.Path == "" {
			request.URL.Path = "/"
		}
	}
	return proxy, nil
}

func runPlainProxy(listen, upstream string) error {
	proxy, err := edgeProxy(upstream, "")
	if err != nil {
		return err
	}
	server := &http.Server{Addr: listen, Handler: proxy, ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second}
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func otelOK(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
}

func newEdgeIdentity() (tls.Certificate, *rsa.PrivateKey, []byte, error) {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	now := time.Now().UTC()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Task24 disposable CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "chaos-edge"}, DNSNames: []string{"chaos-edge", "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(leafKey)})
	certificate, err := tls.X509KeyPair(leafPEM, keyPEM)
	return certificate, caKey, caPEM, err
}

func signEdgeToken(key *rsa.PrivateKey, issuer string, now time.Time) (string, error) {
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": edgeKeyID, "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{"iss": issuer, "aud": "gpt-live-chaos", "sub": "chaos", "tenant_id": "chaos", "scope": "realtime live.admin", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := key.Sign(rand.Reader, digest[:], crypto.SHA256)
	if err != nil {
		return "", fmt.Errorf("sign chaos token: %w", err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
