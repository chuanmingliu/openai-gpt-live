.PHONY: test test-race vet fmt verify-deploy verify-deploy-static smoke-deploy load-unit chaos capacity barge soak load-smoke load-evidence release-gate release-gate-verify demo-live-client test-demo-live-client

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.worktrees/*')

demo-live-client:
	@test -n "$$LIVE_DEMO_BEARER"
	go run ./demo/live-client

test-demo-live-client:
	go test -race -count=1 ./demo/live-client
	node --test demo/live-client/web/audio-core.test.mjs demo/live-client/web/protocol-state.test.mjs

# Builds and validates every deployable artifact. Tool absence is an explicit
# prerequisite failure; the validator never silently skips a deployment gate.
verify-deploy:
	./deploy/scripts/verify-deploy.sh

verify-deploy-static:
	./deploy/scripts/verify-deploy-static.sh

smoke-deploy:
	./deploy/scripts/smoke-compose.sh

load-unit:
	go test -race -count=1 ./cmd/live-load ./internal/loadgen

chaos:
	test -n "$$CHAOS_TOXIPROXY_URL"
	test -n "$$CHAOS_POSTGRES_PROXY"
	test -n "$$CHAOS_VOICE_PROXY"
	test -n "$$CHAOS_DEEPSEEK_PROXY"
	test -n "$$CHAOS_VOICE_CONTROL_URL"
	test -n "$$CHAOS_DEEPSEEK_CONTROL_URL"
	test -n "$$CHAOS_GATEWAY_URL"
	test -n "$$CHAOS_GATEWAY_HEALTH_URL"
	test -n "$$CHAOS_WORKER_HEALTH_URL"
	test -n "$$CHAOS_BEARER_TOKEN"
	test -n "$$CHAOS_OIDC_CONTROL_URL"
	test -n "$$CHAOS_COMPOSE_PROJECT"
	test -n "$$CHAOS_COMPOSE_BASE"
	test -n "$$CHAOS_COMPOSE_OVERRIDE"
	test -n "$$CHAOS_DATABASE_URL"
	go test -count=1 -tags=chaos -timeout=30m ./test/chaos/...

capacity:
	go test -count=1 -tags=load -run '^TestGateway250Sessions$$' -timeout=20m ./test/load/...

barge:
	go test -count=1 -tags=load -run '^TestBargeInAcknowledgedFenceSuppressesEveryStaleFrame$$' -timeout=2m ./test/load/...

soak:
	test -n "$$LIVE_SOAK_DURATION"
	go test -count=1 -tags=soak -run '^TestGatewaySoak$$' -timeout=150m ./test/load/...

load-smoke:
	test -n "$$LIVE_EVIDENCE_DIR"
	./deploy/scripts/run-load-evidence.sh smoke

load-evidence:
	test -n "$$LIVE_EVIDENCE_DIR"
	./deploy/scripts/run-load-evidence.sh formal

RELEASE_COMMIT ?= $(shell git rev-parse HEAD)
RELEASE_CONFIG_HASH ?=
RELEASE_EVIDENCE_OUT ?= $(CURDIR)/artifacts/release/$(RELEASE_COMMIT)/$(RELEASE_CONFIG_HASH)/evidence.json

release-gate:
	test -n "$(RELEASE_CONFIG_HASH)"
	./deploy/scripts/run-release-gate.sh \
		--commit "$(RELEASE_COMMIT)" \
		--config-hash "$(RELEASE_CONFIG_HASH)" \
		--evidence-out "$(RELEASE_EVIDENCE_OUT)" \
		$${RELEASE_ATTESTATION_FILE:+--attestation "$$RELEASE_ATTESTATION_FILE"} \
		$${RELEASE_ATTESTATION_KEY:+--attestation-key "$$RELEASE_ATTESTATION_KEY"}

release-gate-verify:
	test -n "$(RELEASE_CONFIG_HASH)"
	test -n "$$RELEASE_ATTESTATION_FILE"
	test -n "$$RELEASE_ATTESTATION_KEY"
	./deploy/scripts/run-release-gate.sh \
		--verify-existing \
		--commit "$(RELEASE_COMMIT)" \
		--config-hash "$(RELEASE_CONFIG_HASH)" \
		--evidence-out "$(RELEASE_EVIDENCE_OUT)" \
		--attestation "$$RELEASE_ATTESTATION_FILE" \
		--attestation-key "$$RELEASE_ATTESTATION_KEY"
