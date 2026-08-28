# Production backend integration evidence

Date: 2026-08-28

## Revision and scope

- Plan base (uniquely resolved by subject `docs: plan production-ready backend hardening`): `97902784e8ccd2ec03ce66ab4b9d7b678a3ed4a4`
- Whole-plan review-fix base: `e0bf15f0d7b1c74cf247395ed239418faf6deb27` (`docs: record production backend integration evidence`)
- Implementation HEAD verified: `f837491432e0522964e11de086b33f8dd63d5379` (`fix: close backend integration review findings`)
- The matrix below was freshly run on that same implementation HEAD. No product or test file was modified during this gate.

## Zero-skip integration matrix

```sh
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 ./internal/config ./cmd/live-gateway ./cmd/live-worker ./demo/live-client
node deploy/scripts/run-node-test-no-skip.mjs --expect-tests 214 -- \
  node --test demo/live-client/web/*.test.mjs
./deploy/scripts/test-demo-wizard.sh
make test-credential-free-startup
test_packages=$(go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...) || exit 1
test -n "$test_packages" || exit 1
./deploy/scripts/run-go-test-no-skip.sh --expect-minimum 1 -- \
  go test -json -race -count=1 $test_packages
go vet ./...
go build ./...
go mod tidy -diff
make verify-deploy-static
git diff --check
```

Results:

- Focused Go race gate: `go_test_summary tests=501 packages=4 skips=0`.
- Browser gate: `node_test_summary tests=214 skips=0`.
- Demo wizard: printed `interactive demo wizard boundary tests passed`; its intentional cleanup-in-progress warning was captured and asserted inside the fixture, and no unexpected stderr escaped.
- Credential-free startup: `credential_free_startup_summary gateway=ready worker=live speech_connections=0`.
- Full Go race gate: 49 test-bearing packages selected; `go_test_summary tests=2948 packages=49 skips=0`.
- `go vet ./...`, `go build ./...`, `go mod tidy -diff`, and `git diff --check`: passed with exit status 0.
- `make verify-deploy-static`: passed its durable Compose wrapper, bounded module download, deployment validation, supply-evidence, and release-gate boundary checks.

There were no skipped tests in either strict runner. The full-package command was run under POSIX `sh`, preserving the brief's exact command body and the shell field splitting it relies on; interactive zsh does not split the unquoted multiline `test_packages` scalar.

## Targeted integration assertions

- `TestLoadGatewayConfigRequiresProviderCredentialAndAuthentication` and `TestGatewayRuntimeRejectsExplicitCredentialFreeConfig` prove that only an absent `-config` selects the permissive local default. Explicit loopback and non-loopback YAML require a provider credential reference plus static authentication or a complete OIDC tuple.
- `TestGatewayResolvesActiveSecretsBeforeOIDCActivity` and `TestGatewayResolvesAllActiveSecretsBeforeDatabaseOrListenerActivity` use real loopback sentinels. Invalid provider, static, database, retention, and reconnect values caused zero OIDC requests, zero PostgreSQL accepts, and zero gateway listener ownership.
- `TestIdleRuntimeForcesPartialHeaderConnectionClosedAtShutdownBound` holds a real accepted partial HTTP header past the graceful bound and proves the forced server close joins the idle runtime and closes the connection. The server also carries a bounded `ReadHeaderTimeout`.
- The focused race gate includes `TestGatewayStartupDoesNotResolveInactiveWorkerSecrets` in `cmd/live-gateway`: its sentinel lookup callback fails if the gateway attempts to resolve `DEEPSEEK_API_KEY`, `PRIVACY_HMAC`, or `TOOL_ENCRYPTION`. It passed under `-race`.
- A deployment-manifest scan found `--backend none` only in the credential-free smoke and its boundary test, not in Compose or Kubernetes manifests. Compose and both Kubernetes deployments use explicit `-config /etc/live/config.yaml`; the static deployment gate also passed.

## Implementation-range audit

`git diff --name-only 97902784e8ccd2ec03ce66ab4b9d7b678a3ed4a4..f837491432e0522964e11de086b33f8dd63d5379` contains exactly these 28 plan-owned paths: the 27 Task 1–6 and whole-plan-fix implementation/test paths plus the previously committed evidence report. The review-fix subrange `e0bf15f0d7b1c74cf247395ed239418faf6deb27..f837491432e0522964e11de086b33f8dd63d5379` contains exactly the six authorized fix paths.

```text
Makefile
cmd/live-gateway/main.go
cmd/live-gateway/main_integration_test.go
cmd/live-gateway/main_test.go
cmd/live-worker/idle.go
cmd/live-worker/idle_test.go
cmd/live-worker/main.go
cmd/live-worker/main_test.go
demo/live-client/config.go
demo/live-client/config_test.go
demo/live-client/proxy_test.go
demo/live-client/server.go
demo/live-client/server_test.go
deploy/scripts/demo-wizard.sh
deploy/scripts/run-go-test-no-skip.sh
deploy/scripts/run-node-test-no-skip.mjs
deploy/scripts/test-credential-free-startup-boundaries.sh
deploy/scripts/test-credential-free-startup.sh
deploy/scripts/test-demo-wizard.sh
deploy/scripts/test-go-test-no-skip.sh
deploy/scripts/test-node-test-no-skip.mjs
docs/superpowers/reports/2026-08-28-production-backend-integration.md
internal/cliopt/path.go
internal/cliopt/path_test.go
internal/config/default_gateway.go
internal/config/default_gateway_test.go
internal/secretbuf/text.go
internal/secretbuf/text_test.go
```

## Environment

The test harness needs local loopback listeners, so the Go race gates and credential-free smoke were run with approved loopback permission. A sandbox-only static probe could not write the shared Go cache; vet, build, tidy-diff, and deployment-static were rerun successfully with approved cache access and clean output. No environmental limitation remained in the successful evidence run.
