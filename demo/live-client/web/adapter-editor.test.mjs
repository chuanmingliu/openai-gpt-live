import assert from 'node:assert/strict';
import test from 'node:test';

import {decodeAdapterCatalog, bindingRequest} from './adapter-editor.mjs';

test('adapter editor exposes reviewed identity and tighten-only controls', () => {
  const catalog = decodeAdapterCatalog({
    registry_version: 'registry-v1',
    adapters: [{id: 'knowledge.search', version: 'v1', tool_name: 'knowledge_search', description: 'Pinned search', tool_schema: {type: 'object', additionalProperties: false, required: ['query'], properties: {query: {type: 'string'}}}, read_only: true, side_effecting: false, parallel_safe: true, approval_required: false, health_class: 'required', max_result_bytes: 98304}],
  });
  assert.deepEqual(Object.keys(catalog.adapters[0]), ['id', 'version', 'toolName', 'description', 'toolSchema', 'readOnly', 'sideEffecting', 'parallelSafe', 'approvalRequired', 'healthClass', 'maxResultBytes']);
  assert.deepEqual(catalog.adapters[0].toolSchema.required, ['query']);
  assert.deepEqual(bindingRequest(catalog.adapters[0], {maxResultBytes: 8192, requireApproval: true}), {max_result_bytes: 8192, require_approval: true});
  assert.throws(() => bindingRequest(catalog.adapters[0], {maxResultBytes: 98305, requireApproval: false}));
});

test('adapter catalog rejects executable or destination-bearing projections', () => {
  const base = {registry_version: 'registry-v1', adapters: []};
  for (const key of ['destination', 'transport', 'secret_refs', 'command', 'url']) {
    assert.throws(() => decodeAdapterCatalog({...base, [key]: 'forbidden'}));
  }
});
