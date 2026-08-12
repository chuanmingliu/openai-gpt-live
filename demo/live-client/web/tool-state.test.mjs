import test from 'node:test';
import assert from 'node:assert/strict';

import {toolPolicyStatusText} from './app.js';
import {ToolState, buildToolPolicyUpdate, normalizeToolPolicy} from './tool-state.mjs';

const DEFAULT_POLICY = {
  web_search: {
    enabled: true,
    safe_search: 'moderate',
    region: 'wt-wt',
    max_results: 5,
    timeout_ms: 8000,
  },
};

function createdEvent(id, sequence = 1) {
  return {type: 'live.handoff.created', handoff_id: id, sequence_id: sequence, status: 'pending', details: {}};
}

function progressEvent({id = 'h1', sequence_id = 2, phase = 'searching', summary = 'Searching the web.'} = {}) {
  return {type: 'live.handoff.progress', handoff_id: id, sequence_id, status: 'running', details: {phase, summary}};
}

function completedToolState() {
  const tools = new ToolState();
  tools.consume(createdEvent('h1'));
  tools.consume({
    type: 'live.handoff.completed',
    handoff_id: 'h1',
    sequence_id: 3,
    status: 'succeeded',
    details: {summary: 'Finished.', sources: []},
  });
  return tools;
}

test('progress updates one card monotonically', () => {
  const tools = new ToolState();
  tools.consume(createdEvent('h1'));
  tools.consume(progressEvent());
  tools.consume(progressEvent({summary: 'duplicate'}));
  assert.deepEqual(tools.snapshot().cards.map(({id, phase}) => ({id, phase})), [{id: 'h1', phase: 'searching'}]);
  assert.equal(tools.snapshot().cards[0].summary, 'Searching the web.');
});

test('terminal card rejects a later progress regression', () => {
  const tools = completedToolState();
  tools.consume(progressEvent({sequence_id: 99, phase: 'searching'}));
  assert.equal(tools.snapshot().cards[0].phase, 'completed');
});

test('needs-input cards can resume through running phases while terminals remain absorbing', () => {
  const tools = new ToolState();
  tools.consume(createdEvent('resume'));
  tools.consume({
    type: 'live.handoff.needs_input', handoff_id: 'resume', sequence_id: 2,
    status: 'needs_input', details: {summary: 'Choose a region.'},
  });
  assert.equal(tools.consume({
    type: 'live.handoff.progress', handoff_id: 'resume', sequence_id: 3,
    status: 'running', details: {},
  }).accepted, true);
  for (const [sequence_id, phase] of [[4, 'searching'], [5, 'reading_sources'], [6, 'preparing_answer']]) {
    assert.equal(tools.consume(progressEvent({id: 'resume', sequence_id, phase})).accepted, true);
    assert.equal(tools.snapshot().cards[0].phase, phase);
  }
  tools.consume({
    type: 'live.handoff.completed', handoff_id: 'resume', sequence_id: 7,
    status: 'succeeded', details: {summary: 'Done.'},
  });
  assert.equal(tools.consume(progressEvent({id: 'resume', sequence_id: 8, phase: 'searching'})).accepted, false);
  assert.equal(tools.snapshot().cards[0].phase, 'completed');
});

test('active card pressure reports overflow without evicting live state while terminal pressure evicts oldest terminal', () => {
	const tools = new ToolState();
	for (let index = 0; index < 64; index += 1) tools.consume(createdEvent(`active-${index}`, index + 1));
	const overflow = tools.consume(createdEvent('active-64', 65));
	assert.equal(overflow.overflow, true);
	let cards = tools.snapshot().cards;
	assert.equal(cards.filter(({terminal}) => !terminal).length, 64);
	assert.equal(cards.some(({id}) => id === 'active-0'), true);
	assert.equal(cards.some(({id}) => id === 'active-64'), false);

	const terminalTools = new ToolState();
	for (let index = 0; index < 65; index += 1) {
		terminalTools.consume(createdEvent(`terminal-${index}`, 1000 + index * 2));
		terminalTools.consume({
			type: 'live.handoff.completed', handoff_id: `terminal-${index}`, sequence_id: 1001 + index * 2,
			status: 'succeeded', details: {summary: `done ${index}`},
		});
	}
	cards = terminalTools.snapshot().cards;
	assert.equal(cards.filter(({terminal}) => terminal).length, 64);
	assert.equal(cards.some(({id}) => id === 'terminal-0'), false);
});

test('card summaries and sources stay within fixed per-card bounds', () => {
	const tools = new ToolState();
	tools.consume(createdEvent('bounded'));
	tools.consume({
		type: 'live.handoff.completed', handoff_id: 'bounded', sequence_id: 2,
		status: 'succeeded',
		details: {
			summary: '界'.repeat(5000),
			sources: Array.from({length: 12}, (_, source) => ({
				title: `source-${source}`,
				url: `https://example.com/${source}`,
			})),
		},
	});
	const snapshot = tools.snapshot();
	assert.equal(snapshot.cards.every(({summary}) => Buffer.byteLength(summary) <= 8 << 10), true);
	assert.equal(snapshot.cards.every(({sources}) => sources.length <= 10), true);
});

test('aggregate text pressure reports overflow without discarding retained cards', () => {
	const tools = new ToolState();
	for (let index = 0; index < 31; index += 1) {
		tools.consume(createdEvent(`aggregate-${index}`, index * 2 + 1));
		assert.equal(tools.consume(progressEvent({id: `aggregate-${index}`, sequence_id: index * 2 + 2, summary: 'x'.repeat(8 << 10)})).overflow, false);
	}
	tools.consume(createdEvent('aggregate-overflow', 1000));
	const before = tools.snapshot();
	const result = tools.consume({
		type: 'live.handoff.progress', handoff_id: 'aggregate-overflow', sequence_id: 1001,
		status: 'running', details: {
			phase: 'searching', summary: 'x'.repeat(8 << 10),
			sources: [{title: 'extra', url: 'https://example.com/extra'}],
		},
	});
	assert.equal(result.overflow, true);
	assert.deepEqual(tools.snapshot().cards, before.cards);
});

test('source projection keeps only bounded HTTPS titles and ASCII URLs', () => {
  const tools = new ToolState();
  tools.consume(createdEvent('sources'));
  tools.consume({
    type: 'live.handoff.completed', handoff_id: 'sources', sequence_id: 2, status: 'succeeded',
    details: {
      summary: 'Done.',
      query: 'must not be retained',
      reasoning_content: 'must not be retained',
      sources: [
        {title: 'Docs', url: 'https://example.com/docs'},
        {title: 'Plain HTTP', url: 'http://example.com/'},
        {title: 'Credentials', url: 'https://user:pass@example.com/'},
        {title: 'Control\ncharacter', url: 'https://example.com/control'},
        {title: 'x'.repeat(257), url: 'https://example.com/title'},
        {title: 'Unicode URL', url: 'https://例子.example/'},
        {title: 'Long URL', url: `https://example.com/${'x'.repeat(2049)}`},
      ],
    },
  });
  const card = tools.snapshot().cards[0];
  assert.deepEqual(card.sources, [{title: 'Docs', url: 'https://example.com/docs'}]);
  assert.equal('query' in card, false);
  assert.equal('reasoning_content' in card, false);
});

test('unknown detail fields and caller mutations never enter owned card snapshots', () => {
  const tools = new ToolState();
  const event = progressEvent();
  event.details.command = 'npx';
  event.details.snippet = 'private snippet';
  tools.consume(event);
  event.details.summary = 'mutated';
  const first = tools.snapshot();
  first.cards[0].summary = 'caller mutation';
  assert.deepEqual(tools.snapshot().cards[0], {
    id: 'h1', sequence: 2, status: 'running', phase: 'searching', summary: 'Searching the web.', sources: [], terminal: false,
  });
});

test('announcements are emitted only when a stable phase changes', () => {
  const tools = new ToolState();
  tools.consume(createdEvent('h1'));
  assert.deepEqual(tools.drainAnnouncements(), ['Background task queued.']);
  tools.consume(progressEvent());
  tools.consume(progressEvent({sequence_id: 2, summary: 'duplicate'}));
  assert.deepEqual(tools.drainAnnouncements(), ['Searching the web.']);
  assert.deepEqual(tools.drainAnnouncements(), []);
});

test('disabled policy remains canonical and cannot carry executable or unknown fields', () => {
  const disabled = structuredClone(DEFAULT_POLICY);
  disabled.web_search.enabled = false;
  assert.deepEqual(normalizeToolPolicy(disabled), disabled);
  assert.throws(() => normalizeToolPolicy({...disabled, command: 'npx'}), /invalid tool policy/);
  assert.throws(() => normalizeToolPolicy({web_search: {...disabled.web_search, safe_search: 'off'}}), /invalid tool policy/);
	assert.throws(() => normalizeToolPolicy({web_search: {...disabled.web_search, max_results: 11}}), /invalid tool policy/);
	assert.throws(() => normalizeToolPolicy({web_search: {...disabled.web_search, max_results: 6}}), /invalid tool policy/);
	assert.throws(() => normalizeToolPolicy({web_search: {...disabled.web_search, timeout_ms: 8001}}), /invalid tool policy/);
});

test('new tool state is disabled and strict until the BFF policy is restored', () => {
	assert.deepEqual(new ToolState().snapshot().policy, {
		web_search: {enabled: false, safe_search: 'strict', region: 'wt-wt', max_results: 5, timeout_ms: 8000},
	});
});

test('policy save keeps persisted policy authoritative until its exact gateway acknowledgement', () => {
	const tools = new ToolState(DEFAULT_POLICY);
  const disabled = structuredClone(DEFAULT_POLICY);
  disabled.web_search.enabled = false;
  tools.beginSave(disabled);
  assert.deepEqual(tools.snapshot().policy, DEFAULT_POLICY);
  tools.commitSave(disabled);
  tools.markPolicySent('policy-2', disabled);
  assert.equal(tools.snapshot().saveStatus, 'awaiting-ack');
  assert.equal(tools.acceptPolicy('policy-1', disabled, 'deepseek-v4-flash'), false);
  assert.equal(tools.snapshot().saveStatus, 'awaiting-ack');
  assert.equal(tools.acceptPolicy('policy-2', DEFAULT_POLICY, 'deepseek-v4-flash'), false);
  assert.equal(tools.acceptPolicy('policy-2', disabled, 'deepseek-v4-flash'), true);
  assert.equal(tools.snapshot().saveStatus, 'saved');
  assert.equal(tools.snapshot().model, 'deepseek-v4-flash');
});

test('policy acknowledgement accepts only canonical server tightening for the exact request', () => {
	const tools = new ToolState(DEFAULT_POLICY);
	tools.markPolicySent('policy-tightened', DEFAULT_POLICY);
	const tightened = structuredClone(DEFAULT_POLICY);
	tightened.web_search.safe_search = 'strict';
	assert.equal(tools.acceptPolicy('policy-tightened', tightened, 'deepseek-v4-flash'), true);
	assert.deepEqual(tools.snapshot().policy, tightened);

	tools.markPolicySent('policy-loosened', tightened);
	assert.equal(tools.acceptPolicy('policy-loosened', DEFAULT_POLICY, 'deepseek-v4-flash'), false);
});

test('failed save restores a late persisted load instead of retaining the draft', () => {
	const tools = new ToolState(DEFAULT_POLICY);
  const loaded = structuredClone(DEFAULT_POLICY);
  loaded.web_search.region = 'uk-en';
  const disabled = structuredClone(DEFAULT_POLICY);
  disabled.web_search.enabled = false;
  tools.beginSave(disabled);
  tools.loadPolicy(loaded);
  assert.deepEqual(tools.snapshot().policy, DEFAULT_POLICY);
  assert.equal(tools.snapshot().saveStatus, 'saving');
  tools.failSave();
  assert.deepEqual(tools.snapshot().policy, loaded);
  assert.equal(tools.snapshot().saveStatus, 'error');
});

test('a policy load failure cannot cancel a save already in flight', () => {
  const tools = new ToolState();
  const disabled = structuredClone(DEFAULT_POLICY);
  disabled.web_search.enabled = false;
  tools.beginSave(disabled);
  assert.equal(tools.failLoad(), false);
  assert.equal(tools.snapshot().saveStatus, 'saving');
  assert.equal(tools.commitSave(disabled), true);
  assert.deepEqual(tools.snapshot().policy, disabled);
});

test('the latest operation selects Test then later Save success and failure status', () => {
  const tools = new ToolState();
  const disabled = structuredClone(DEFAULT_POLICY);
  disabled.web_search.enabled = false;
  tools.beginTest();
  tools.finishTest(true);
  assert.equal(toolPolicyStatusText(tools.snapshot()), 'Web Search test passed.');

  tools.beginSave(disabled);
  assert.equal(toolPolicyStatusText(tools.snapshot()), 'Saving policy…');
  tools.commitSave(disabled);
  tools.markPolicySent('policy-success', disabled);
  tools.acceptPolicy('policy-success', disabled, 'deepseek-v4-flash');
  assert.equal(toolPolicyStatusText(tools.snapshot()), 'Policy saved and active.');

  tools.beginTest();
  tools.finishTest(true);
  tools.beginSave(DEFAULT_POLICY);
  tools.failSave();
  assert.equal(toolPolicyStatusText(tools.snapshot()), 'Policy action failed. Try again.');
});

test('an older Test completion cannot reclaim status from a later successful Save', () => {
  for (const passed of [true, false]) {
    const tools = new ToolState();
    const saved = structuredClone(DEFAULT_POLICY);
    saved.web_search.region = 'uk-en';
    const testOperation = tools.beginTest();
    const saveOperation = tools.beginSave(saved);
    tools.commitSave(saved, saveOperation);

    assert.equal(tools.finishTest(passed, testOperation), true);
    assert.equal(tools.snapshot().testStatus, passed ? 'passed' : 'error');
    assert.equal(tools.snapshot().lastOperation, 'save');
    assert.equal(toolPolicyStatusText(tools.snapshot()), 'Policy saved and active.');
  }
});

test('a load started before a committed Save cannot later replace or fail it', () => {
  const tools = new ToolState();
  const saved = structuredClone(DEFAULT_POLICY);
  saved.web_search.region = 'uk-en';
  const staleLoad = tools.beginPolicyLoad();
  const saveOperation = tools.beginSave(saved);
  tools.commitSave(saved, saveOperation);

  assert.equal(tools.loadPolicy(DEFAULT_POLICY, staleLoad), false);
  assert.deepEqual(tools.snapshot().policy, saved);
  assert.equal(tools.failLoad(staleLoad), false);
  assert.equal(tools.snapshot().saveStatus, 'saved');
});

test('tool policy update is a closed-world canonical WebSocket event', () => {
  assert.deepEqual(buildToolPolicyUpdate(DEFAULT_POLICY, 'policy-1'), {
    type: 'live.tool_policy.update', event_id: 'policy-1', tools: DEFAULT_POLICY,
  });
  assert.throws(() => buildToolPolicyUpdate(DEFAULT_POLICY, ''), /invalid event id/);
  assert.throws(() => buildToolPolicyUpdate(DEFAULT_POLICY, 'x\n'), /invalid event id/);
});
