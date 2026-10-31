# Mac Aria service maintenance

This branch tracks ownership and the source-recovery boundary for the **native Go
Aria application on the Mac**. It must not be used to replace the installed
service until the source gap below is resolved.

**Scope:** `codex/service-mac-aria` in `openai-gpt-live`.

## Source gap

This branch was created from verified GitHub revision `8f59fd074a50`, the
credential-free backend branch. That tree contains `cmd/live-gateway`,
`cmd/live-worker` and `demo/live-client`; **it does not contain `cmd/live-aria`**.
Consequently there is no valid `go build ./cmd/live-aria` command on this branch.

The installed Mac binary identifies itself as
`github.com/chuanmingliu/openai-gpt-live/cmd/live-aria`, revision
`051b03f4a8ef9f1fcae860c276f73249fe183e8d`, with `vcs.modified=true`.
Neither this branch nor that commit alone reproduces the deployed application.
Recover and review its exact source changes before implementing a service update.

The similarly named TypeScript Aria originator on the ThinkPad is a different
codebase. Do not deploy its package over this native Go application.

## Installed service contract

| Item | Observed configuration, 2026-10-31 |
| --- | --- |
| Supervisor | LaunchAgent `com.openai-gpt-live.aria`, with KeepAlive and RunAtLoad |
| Binary | `~/Library/Application Support/Aria/bin/live-aria` |
| State/config root | `~/Library/Application Support/Aria`; references a private telephony env file under its rollback directory |
| Logs | `~/Library/Logs/Aria/live-aria.stdout.log` and `live-aria.stderr.log` |
| Listeners | Loopback 3000, 8765 and 8768; wildcard 8767 |
| Verified health routes | `:3000/healthz`, `:3000/readyz` and `:8765/health` returned 200 |

All four listeners belong to one process. A healthy HTTP listener does not
establish that the configured PBX route or providers can complete a real call.

Read-only checks on the Mac:

```sh
launchctl print "gui/$(id -u)/com.openai-gpt-live.aria"
go version -m "$HOME/Library/Application Support/Aria/bin/live-aria"
curl --noproxy '*' --fail http://127.0.0.1:3000/healthz
```

Keep the installed binary, LaunchAgent and private referenced configuration
intact. An authorized restart uses the existing launchd service; starting the
backend/demo commands from this branch is not an Aria restart and may collide
with its ports. Before an eventual upgrade, establish idle state, preserve a
rollback copy and verify all listener contracts against the recovered build.

## Candidate source locations

The inventory found later local checkouts of the same repository:

- `~/Documents/openai-gpt-live`: branch `codex/public-operator-proxy`, observed
  revision `26f5ca36f9df4565ac16874a47546e2cc8155f7c`; includes `cmd/live-aria`.
- `~/Documents/openai-gpt-live-custom`: detached at
  `e8dea1d78206265ec37e019c8b648b9fbe8a2a2a`.

These are candidates for source reconciliation, not verified reproductions of
the installed dirty binary. Do not import their uncommitted changes or replace
this branch's history as part of a README update.

Once the matching source has been recovered and reviewed, add the actual Aria
build/check commands and its release provenance here. Until then, this branch
records service ownership and lifecycle without claiming a reproducible build.
