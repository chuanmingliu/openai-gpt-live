import test from 'node:test';
import assert from 'node:assert/strict';

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

test('tool cards evict oldest active and terminal entries at independent 64-card bounds', () => {
  const tools = new ToolState();
  for (let index = 0; index < 65; index += 1) tools.consume(createdEvent(`active-${index}`, index + 1));
  let cards = tools.snapshot().cards;
  assert.equal(cards.filter(({terminal}) => !terminal).length, 64);
  assert.equal(cards.some(({id}) => id === 'active-0'), false);

  for (let index = 0; index < 65; index += 1) {
    tools.consume(createdEvent(`terminal-${index}`, 1000 + index * 2));
    tools.consume({
      type: 'live.handoff.completed', handoff_id: `terminal-${index}`, sequence_id: 1001 + index * 2,
      status: 'succeeded', details: {summary: `done ${index}`},
    });
  }
  cards = tools.snapshot().cards;
  assert.equal(cards.filter(({terminal}) => terminal).length, 64);
  assert.equal(cards.some(({id}) => id === 'terminal-0'), false);
});

test('card summaries, sources, and aggregate retained text stay within fixed byte bounds', () => {
  const tools = new ToolState();
  for (let index = 0; index < 40; index += 1) {
    tools.consume(createdEvent(`bounded-${index}`, index * 2 + 1));
    tools.consume({
      type: 'live.handoff.completed', handoff_id: `bounded-${index}`, sequence_id: index * 2 + 2,
      status: 'succeeded',
      details: {
        summary: '界'.repeat(5000),
        sources: Array.from({length: 12}, (_, source) => ({
          title: `source-${source}`,
          url: `https://example.com/${index}/${source}`,
        })),
      },
    });
  }
  const snapshot = tools.snapshot();
  assert.equal(snapshot.cards.every(({summary}) => Buffer.byteLength(summary) <= 8 << 10), true);
  assert.equal(snapshot.cards.every(({sources}) => sources.length <= 10), true);
  assert.ok(snapshot.totalTextBytes <= 256 << 10);
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
});

test('policy save stays pending until the exact gateway acknowledgement', () => {
  const tools = new ToolState();
  const disabled = structuredClone(DEFAULT_POLICY);
  disabled.web_search.enabled = false;
  tools.beginSave(disabled);
  tools.markPolicySent();
  assert.equal(tools.snapshot().saveStatus, 'awaiting-ack');
  assert.equal(tools.acceptPolicy(DEFAULT_POLICY, 'deepseek-v4-flash'), false);
  assert.equal(tools.snapshot().saveStatus, 'awaiting-ack');
  assert.equal(tools.acceptPolicy(disabled, 'deepseek-v4-flash'), true);
  assert.equal(tools.snapshot().saveStatus, 'saved');
  assert.equal(tools.snapshot().model, 'deepseek-v4-flash');
});

test('late initial policy load cannot overwrite a save already in flight', () => {
  const tools = new ToolState();
  const disabled = structuredClone(DEFAULT_POLICY);
  disabled.web_search.enabled = false;
  tools.beginSave(disabled);
  tools.loadPolicy(DEFAULT_POLICY);
  assert.deepEqual(tools.snapshot().policy, disabled);
  assert.equal(tools.snapshot().saveStatus, 'saving');
});

test('tool policy update is a closed-world canonical WebSocket event', () => {
  assert.deepEqual(buildToolPolicyUpdate(DEFAULT_POLICY, 'policy-1'), {
    type: 'live.tool_policy.update', event_id: 'policy-1', tools: DEFAULT_POLICY,
  });
  assert.throws(() => buildToolPolicyUpdate(DEFAULT_POLICY, ''), /invalid event id/);
  assert.throws(() => buildToolPolicyUpdate(DEFAULT_POLICY, 'x\n'), /invalid event id/);
});
