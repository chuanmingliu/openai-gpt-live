const STEPS = [
  {
    id: "topology",
    title: "Start PostgreSQL, gateway, worker, and speech-to-speech",
    command: "cd deploy && docker compose up -d",
    hint: "Or run focused processes from the repo README. Compose publishes the gateway on 127.0.0.1:18080.",
  },
  {
    id: "bearer",
    title: "Export the demo bearer from the local static token",
    command: "export LIVE_DEMO_BEARER=\"$LIVE_STATIC_BEARER_TOKEN\"",
    hint: "The browser never sees this value. The BFF injects it on the upstream dial.",
  },
  {
    id: "bff",
    title: "Start this client BFF",
    command: "make demo-live-client",
    hint: "Default listen is 127.0.0.1:3000. Stop with Ctrl+C; that does not delete PostgreSQL data.",
  },
  {
    id: "open",
    title: "Open the local origin and allow the microphone",
    command: "open http://127.0.0.1:3000",
    hint: "Click Start on this page. Handoffs stay off in local Compose.",
  },
];

export function setupChecks(status, extras = {}) {
  const ready = Boolean(status && status.ready);
  const gateway = Boolean(status && status.gateway_reachable);
  const secureOk = !status || !status.https_origin || extras.secureContext !== false;
  const micOk = extras.mediaDevices !== false;
  return [
    { id: "bff", label: "Client BFF ready", ok: ready },
    { id: "gateway", label: status && status.gateway_host ? `Gateway ${status.gateway_host}` : "Gateway host", ok: gateway },
    { id: "origin", label: status && status.https_origin ? "HTTPS origin" : "Local HTTP origin", ok: secureOk },
    { id: "mic", label: "Microphone API available", ok: micOk },
  ];
}

export function renderSetupPanel(root, status, extras = {}) {
  if (!root) {
    return;
  }
  const checks = setupChecks(status, extras);
  const allOk = checks.every((check) => check.ok);
  root.replaceChildren();
  const heading = document.createElement("h2");
  heading.textContent = "Setup";
  const summary = document.createElement("p");
  summary.className = "setup-summary";
  summary.textContent = allOk
    ? "Dependencies look ready. Click Start to talk."
    : "Finish the steps below, then Recheck.";
  const list = document.createElement("ul");
  list.className = "setup-checks";
  for (const check of checks) {
    const item = document.createElement("li");
    item.className = check.ok ? "ok" : "wait";
    item.textContent = `${check.ok ? "ready" : "waiting"} · ${check.label}`;
    list.appendChild(item);
  }
  const steps = document.createElement("ol");
  steps.className = "setup-steps";
  for (const step of STEPS) {
    const item = document.createElement("li");
    const title = document.createElement("p");
    title.textContent = step.title;
    const pre = document.createElement("pre");
    pre.textContent = step.command;
    const hint = document.createElement("p");
    hint.className = "setup-hint";
    hint.textContent = step.hint;
    item.append(title, pre, hint);
    steps.appendChild(item);
  }
  const meta = document.createElement("p");
  meta.className = "setup-hint";
  if (status) {
    meta.textContent = `mode ${status.mode} · model ${status.model} · origin ${status.origin}`;
  }
  root.append(heading, summary, list, steps, meta);
}

export { STEPS };
