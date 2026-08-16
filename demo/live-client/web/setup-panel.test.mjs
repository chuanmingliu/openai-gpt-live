import test from "node:test";
import assert from "node:assert/strict";
import { STEPS, setupChecks } from "./setup-panel.mjs";

function status(overrides = {}) {
  return {
    mode: "local",
    ready: true,
    origin: "http://127.0.0.1:3000",
    model: "gpt-live-simulator",
    gateway_host: "127.0.0.1:18080",
    gateway_reachable: true,
    bearer_env: "LIVE_DEMO_BEARER",
    listen: "127.0.0.1:3000",
    https_origin: false,
    ...overrides,
  };
}

test("setup checks fail closed when gateway is down", () => {
  const checks = setupChecks(status({ ready: true, gateway_reachable: false }));
  assert.equal(checks.find((check) => check.id === "bff").ok, true);
  assert.equal(checks.find((check) => check.id === "gateway").ok, false);
});

test("setup commands never embed a bearer value", () => {
  const text = STEPS.map((step) => `${step.title}\n${step.command}\n${step.hint}`).join("\n");
  assert.match(text, /make demo-live-client/);
  assert.match(text, /LIVE_DEMO_BEARER="\$LIVE_STATIC_BEARER_TOKEN"/);
  assert.doesNotMatch(text, /Authorization/);
  assert.doesNotMatch(text, /LIVE_DEMO_BEARER="[^$]/);
});
