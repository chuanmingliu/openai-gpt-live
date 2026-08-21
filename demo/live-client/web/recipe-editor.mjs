const MAX_TIMEOUT_MS = 86400000;
const MAX_BYTES = 1048576;

export function recipeHelp() {
  return 'The objective template is the asynchronous worker objective prompt. It runs after a handoff has already been accepted; it does not decide whether a handoff occurs.';
}

export function recipeSettings(values) {
  const numeric = [values?.executionTimeoutMS, values?.toolTimeoutMS, values?.overallTimeoutMS, values?.maxObjectiveBytes, values?.maxEvidenceBytes, values?.maxToolResultBytes, values?.maxFinalResultBytes];
  if (typeof values?.requireApproval !== 'boolean' || numeric.some((value) => !Number.isSafeInteger(value) || value < 1)
      || numeric.slice(0, 3).some((value) => value > MAX_TIMEOUT_MS)
      || numeric.slice(3).some((value) => value > MAX_BYTES)) {
    throw new TypeError('invalid recipe settings');
  }
  return Object.freeze({
    require_approval: values.requireApproval,
    execution_timeout_ms: values.executionTimeoutMS,
    tool_timeout_ms: values.toolTimeoutMS,
    overall_timeout_ms: values.overallTimeoutMS,
    max_objective_bytes: values.maxObjectiveBytes,
    max_evidence_bytes: values.maxEvidenceBytes,
    max_tool_result_bytes: values.maxToolResultBytes,
    max_final_result_bytes: values.maxFinalResultBytes,
  });
}
