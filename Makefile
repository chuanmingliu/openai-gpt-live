.PHONY: test test-race vet fmt verify-deploy verify-deploy-static smoke-deploy durable-demo-migrate durable-demo-up durable-demo-down test-durable-web-search load-unit chaos capacity barge soak load-smoke load-evidence release-gate release-gate-verify demo-wizard demo-live-client test-demo-live-client

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.worktrees/*')

demo-wizard:
	./deploy/scripts/demo-wizard.sh

demo-live-client:
	@test -n "$$LIVE_DEMO_BEARER"
	go run ./demo/live-client

test-demo-live-client:
	go test -race -count=1 ./demo/live-client
	node --test demo/live-client/web/*.test.mjs

# Builds and validates every deployable artifact. Tool absence is an explicit
# prerequisite failure; the validator never silently skips a deployment gate.
verify-deploy:
	./deploy/scripts/verify-deploy.sh

verify-deploy-static:
	./deploy/scripts/verify-deploy-static.sh

smoke-deploy:
	./deploy/scripts/smoke-compose.sh

durable-demo-migrate:
	@test -n "$$POSTGRES_PASSWORD"
	@test -n "$$S2S_API_KEY"
	@test -n "$$DEEPSEEK_API_KEY"
	@test -n "$$LIVE_STATIC_BEARER_TOKEN"
	@test -n "$$LIVE_ADMIN_BEARER_TOKEN"
	@test -n "$$LIVE_PRIVACY_HMAC_KEY"
	@test -n "$$LIVE_RETENTION_HMAC_KEY"
	@test -n "$$LIVE_RECONNECT_HMAC_KEY"
	@test -n "$$LIVE_TOOL_ENCRYPTION_KEY"
	@test -n "$$LIVE_EXPECTED_CONFIG_HASH"
	@test -n "$$S2S_IMAGE"
	@test -n "$$S2S_CONFIG_PATH"
	./deploy/scripts/durable-compose.sh migrate-up
	./deploy/scripts/durable-compose.sh migrate-check

durable-demo-up:
	@test -n "$$POSTGRES_PASSWORD"
	@test -n "$$S2S_API_KEY"
	@test -n "$$DEEPSEEK_API_KEY"
	@test -n "$$LIVE_STATIC_BEARER_TOKEN"
	@test -n "$$LIVE_ADMIN_BEARER_TOKEN"
	@test -n "$$LIVE_PRIVACY_HMAC_KEY"
	@test -n "$$LIVE_RETENTION_HMAC_KEY"
	@test -n "$$LIVE_RECONNECT_HMAC_KEY"
	@test -n "$$LIVE_TOOL_ENCRYPTION_KEY"
	@test -n "$$LIVE_EXPECTED_CONFIG_HASH"
	@test -n "$$S2S_IMAGE"
	@test -n "$$S2S_CONFIG_PATH"
	./deploy/scripts/durable-compose.sh up

durable-demo-down:
	./deploy/scripts/durable-compose.sh down

test-durable-web-search:
	go test -race -count=1 ./test/internal/fakesearch ./test/internal/loadgateway
	go test -race -tags=integration -run '^TestDurableWebSearch' ./test/integration -count=1 -timeout=10m

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
