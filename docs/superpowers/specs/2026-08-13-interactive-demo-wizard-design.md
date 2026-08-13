# Interactive Demo Wizard Design

## Goal

Provide one repeatable terminal entry point that starts or manages the realtime
browser demo without asking operators to reconstruct commands from the complete
runbook.

## Interface

`make demo-wizard` starts a Bash wizard with four choices:

1. **Full durable demo** — PostgreSQL migration, speech service, gateway,
   worker, Web Search, and browser BFF.
2. **Client only** — browser BFF against an already-running loopback gateway.
3. **Check status** — bounded, content-free readiness checks for browser BFF,
   gateway, and worker.
4. **Stop demo** — stop only the selected wizard-owned local composition and
   print the remaining manual cleanup, if any.

Successful startup ends with the exact clickable URL
`http://127.0.0.1:3000/`.

## Security and persistence

- Secrets are entered with hidden prompts and remain only in the wizard
  process and its child processes for that run.
- The wizard never writes `.env`, credential, resolved Compose, provider-body,
  transcript, audio, or raw WebSocket files.
- Secret values never enter command-line arguments, URLs, status output, or
  error output.
- The browser receives no gateway/provider bearer; the loopback BFF injects
  the gateway bearer server-side.
- All listeners remain loopback-only. The wizard does not open tunnels or
  production endpoints.

## Full durable flow

The wizard requires `zsh` or Bash, Go, Docker Compose, `curl`, `jq`, and
`openssl`. It captures:

- hidden: PostgreSQL password, local bearer, speech bearer, DeepSeek bearer;
- visible: reviewed speech image and absolute non-secret speech config path.

It generates independent development-only privacy, retention, reconnect, and
tool-encryption keys in memory; computes the canonical
`development_durable` config hash; invokes the existing
`durable-demo-migrate` and `durable-demo-up` targets; waits for gateway and
worker readiness; then starts the BFF with the same local bearer.

The existing Compose wrapper remains the sole owner of database URL derivation
and percent-encoding. The wizard never constructs or prints a database URL.

## Client-only flow

The wizard captures a hidden local gateway bearer and optional visible
loopback gateway WebSocket URL and advertised model, defaulting to
`ws://127.0.0.1:18080/v1/realtime` and `gpt-live-real`. It validates the
gateway URL through the existing BFF and starts only the BFF. It does not start
PostgreSQL, speech, gateway, worker, or migrations.

## Lifecycle and errors

- Every external command is checked; no failed migration or readiness check is
  converted to success.
- Readiness polling has fixed attempts and per-request connect/total timeouts.
- Interrupts and startup failures stop wizard-owned BFF processes and leave a
  concise recovery command. They do not delete PostgreSQL volumes.
- `Stop demo` uses the existing `durable-demo-down` target for the durable
  Compose project and stops only a BFF PID that the wizard can prove it owns.
- Status output is limited to `ready`, `not ready`, `not running`, or the next
  safe operator action.

## Files and verification

- Create `deploy/scripts/demo-wizard.sh` for the interactive flow.
- Add `demo-wizard` to `Makefile`.
- Create `demo/live-client/README.md` as the concise entry page.
- Link the wizard from the repository `README.md` and the complete operator
  guide.
- Add shell boundary tests for menu routing, hidden/session-only credentials,
  exact child commands, bounded readiness, failure cleanup, and absence of
  credential output or files.
- Verify with shell syntax, the wizard boundary suite, demo Go/Node tests,
  static deployment verification, and `git diff --check`.

The complete operator guide remains authoritative for manual audio, barge-in,
pool-idle, DevTools privacy, troubleshooting, and production limitations.
