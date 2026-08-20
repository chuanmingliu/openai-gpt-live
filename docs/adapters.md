# Native adapter development

Worker tools come from a closed, server-owned catalog. A profile may disable or tighten a reviewed adapter, but it cannot add an executable, destination, transport, secret, timeout, concurrency limit, or result allowance.

Create a native adapter skeleton with:

```sh
make adapter-scaffold NAME=policy_lookup
```

Names must match `^[a-z][a-z0-9_]{0,63}$`. The generator writes only `internal/adapters/<name>` and refuses collisions, symlinks, traversal, and non-canonical roots. It never installs packages or links to another repository.

Before registration, review the generated manifest and replace each `review-required` value. Register the native Go executor in `cmd/live-worker`; its adapter ID, version, destination, and transport must exactly match the manifest. Secrets are named by reviewed `secret_refs` and resolved by the server—never stored in a profile, manifest payload, browser request, log, or tool result.

MCP-backed tools still require a native adapter implementation. Pin the MCP server identity, protocol/version, destination, tool schema, and result projection in code and in the reviewed manifest. Do not add a generic shell, command, arbitrary HTTP, package installer, or runtime cross-repository adapter.

Generated tests are a starting boundary: retain cancellation, argument/result bounds, output ownership, privacy, timeout, approval/idempotency, and concurrency coverage as the implementation grows.
