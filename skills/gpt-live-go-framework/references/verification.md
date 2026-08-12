# Verification reference

Run from the repository root. Use a writable `GOCACHE` when the default cache
is outside the sandbox. Do not add credentials to commands or reports.

## Documentation and skill gate

```sh
go test -count=1 ./demo/live-client
make verify-deploy-static
rg -n 'deepseek-chat|deepseek-reasoner|npx -y|server-duckduckgo' \
  README.md docs/demo deploy/config deploy/k8s configs \
  skills/gpt-live-go-framework
```

The tests and static verifier must pass. Inspect every final search match: no
retired model may remain in active configuration and no executable dynamic
package instruction is allowed. Warning prose alone is not an install recipe.

## Focused durable-tool gate

```sh
make test-demo-live-client
go test -race -count=1 ./internal/toolpolicy ./internal/tools/websearch \
  ./internal/reasoner/deepseek ./internal/worker ./internal/presentation
go test -race -tags=integration -run '^TestDurableWebSearch' \
  ./test/integration -count=1 -timeout=10m
```

The tagged boundary requires its documented disposable PostgreSQL and fake
providers. Missing prerequisites are not a production pass.

## Repository gate

```sh
go test -race -count=1 ./...
go vet ./...
go build ./...
go mod tidy -diff
make verify-deploy-static
git diff --check
```

## Manual acceptance

Record PASS/FAIL without audio, transcript, query, result body, or credential:

- speech configuration/startup names `deepseek-v4-flash` and thinking disabled;
- PostgreSQL is clean at schema v8;
- gateway, worker, and BFF content-free readiness pass;
- fixed Web Search Test and policy acknowledgement pass;
- stable progress appears while speech continues;
- the operator physically hears voice and successfully barges in;
- gateway, worker, and speech pool signals return idle; and
- browser address/assets/network/storage/console contain no credential.

Automated tests cannot claim physical audio, acoustic barge-in, real provider
quota, reviewed production secrets/identity/egress, or production readiness.
