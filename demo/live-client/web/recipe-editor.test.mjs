import assert from 'node:assert/strict';
import test from 'node:test';

import {recipeHelp, recipeSettings} from './recipe-editor.mjs';

test('worker objective is described as the async prompt rather than handoff policy', () => {
  assert.match(recipeHelp(), /asynchronous worker objective/i);
  assert.doesNotMatch(recipeHelp(), /decides.*handoff/i);
});

test('recipe settings keep only bounded reviewed form values', () => {
  assert.deepEqual(recipeSettings({executionTimeoutMS: 30000, toolTimeoutMS: 5000, overallTimeoutMS: 60000, maxObjectiveBytes: 8192, maxEvidenceBytes: 32768, maxToolResultBytes: 16384, maxFinalResultBytes: 16384, requireApproval: false}), {
    require_approval: false,
    execution_timeout_ms: 30000,
    tool_timeout_ms: 5000,
    overall_timeout_ms: 60000,
    max_objective_bytes: 8192,
    max_evidence_bytes: 32768,
    max_tool_result_bytes: 16384,
    max_final_result_bytes: 16384,
  });
  assert.throws(() => recipeSettings({executionTimeoutMS: 0}));
});
