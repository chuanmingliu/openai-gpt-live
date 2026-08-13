# Interactive Demo Wizard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one secret-safe interactive terminal wizard that starts, checks, or stops the realtime browser demo and prints the browser URL.

**Architecture:** A Bash entry point owns menu/prompt/readiness/BFF lifecycle behavior and delegates durable database and Compose behavior to the existing Make targets. A shell boundary test substitutes `make`, `curl`, `go`, `openssl`, and process commands so every branch is verified without credentials, providers, Docker, or listeners. A concise client README becomes the first-run page; the complete operator guide remains authoritative.

**Tech Stack:** Bash, GNU/BSD command-line tools, Make, Go BFF, Docker Compose wrapper, Markdown.

## Global Constraints

- Secrets remain only in the wizard process and child environments; no `.env` or credential file is written.
- Secret values never appear in command arguments, URLs, stdout, stderr, or generated files.
- Existing browser/audio worktree changes are protected and must not be edited.
- All URLs/listeners are loopback-only; no tunnels or production operations.
- The existing durable Compose wrapper remains the only code that derives `DATABASE_URL`.
- Readiness checks are bounded and content-free.

---

### Task 1: Interactive wizard and boundary contract

**Files:**
- Create: `deploy/scripts/demo-wizard.sh`
- Create: `deploy/scripts/test-demo-wizard.sh`
- Modify: `Makefile`

**Interfaces:**
- Consumes: `make durable-demo-migrate`, `make durable-demo-up`, `make durable-demo-down`, and `go run ./demo/live-client`.
- Produces: `make demo-wizard`; menu modes `full`, `client`, `status`, and `stop`; exact success URL `http://127.0.0.1:3000/`.

- [ ] **Step 1: Write the failing boundary suite**

  Add fixture commands and scripted stdin covering full startup, client-only startup, status, stop, failed migration/readiness, interrupt cleanup, session-only secrets, exact child environments, and secret absence from output/files.

- [ ] **Step 2: Run the boundary suite to verify RED**

  Run: `bash deploy/scripts/test-demo-wizard.sh`

  Expected: FAIL because `deploy/scripts/demo-wizard.sh` and `make demo-wizard` do not exist.

- [ ] **Step 3: Implement the minimal wizard**

  Implement strict menu parsing, hidden nonempty secret prompts, visible validated paths/options, generated development keys, exact config hash capture, durable Make delegation, bounded readiness, BFF PID ownership, traps, status, and stop behavior. Add the Make target.

- [ ] **Step 4: Run focused GREEN gates**

  Run:

  ```sh
  bash -n deploy/scripts/demo-wizard.sh deploy/scripts/test-demo-wizard.sh
  bash deploy/scripts/test-demo-wizard.sh
  ```

  Expected: PASS with no provider, database, Docker, or listener contact.

### Task 2: Concise first-run README

**Files:**
- Create: `demo/live-client/README.md`
- Modify: `README.md`
- Modify: `docs/demo/realtime-browser-client.md`
- Modify: `demo/live-client/docs_test.go`

**Interfaces:**
- Consumes: `make demo-wizard` from Task 1.
- Produces: a concise three-step quick start and links to the advanced operator guide.

- [ ] **Step 1: Add failing documentation assertions**

  Require the client README to contain the wizard command, four menu modes, exact browser URL, microphone/speaker permission, session-only secret statement, stop instruction, and advanced-guide link. Require repository and operator guides to link to it.

- [ ] **Step 2: Run the focused docs test to verify RED**

  Run: `go test ./demo/live-client -run 'QuickStart|OperatorGuide' -count=1`

  Expected: FAIL because the quick-start file/link does not exist.

- [ ] **Step 3: Write the concise README and links**

  Document prerequisites, `make demo-wizard`, menu selection, hidden/public values, exact link, browser permissions, Tools flow, status/stop choices, and the advanced guide. Do not duplicate the full runbook.

- [ ] **Step 4: Run focused GREEN gates**

  Run:

  ```sh
  go test -race -count=1 ./demo/live-client
  node --test demo/live-client/web/*.test.mjs
  ```

  Expected: PASS.

### Task 3: Repository verification

**Files:**
- Verify only; fix Task 1–2 files if a gate exposes a defect.

**Interfaces:**
- Consumes: all Task 1–2 artifacts.
- Produces: verified handoff with no runtime or release claim.

- [ ] **Step 1: Run static and privacy gates**

  ```sh
  make verify-deploy-static
  git diff --check
  rg -n 'POSTGRES_PASSWORD|S2S_API_KEY|DEEPSEEK_API_KEY|LIVE_.*(TOKEN|KEY)' \
    deploy/scripts/demo-wizard.sh deploy/scripts/test-demo-wizard.sh demo/live-client/README.md
  ```

  Review every match as a variable name, hidden prompt, or negative fixture; no literal value may remain.

- [ ] **Step 2: Review status and scope**

  Confirm the seven protected browser/audio files are byte-untouched and no generated credential/state file exists.

- [ ] **Step 3: Commit only wizard/docs files**

  Stage the wizard, boundary test, Makefile, quick README, guide links, docs tests, and this plan. Exclude protected changes and the unrelated research note.
