.PHONY: test test-race vet fmt verify-deploy verify-deploy-static smoke-deploy

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.worktrees/*')

# Builds and validates every deployable artifact. Tool absence is an explicit
# prerequisite failure; the validator never silently skips a deployment gate.
verify-deploy:
	./deploy/scripts/verify-deploy.sh

verify-deploy-static:
	./deploy/scripts/verify-deploy-static.sh

smoke-deploy:
	./deploy/scripts/smoke-compose.sh
