const MAXIMUM_ACTIVE_CARDS = 64;
const MAXIMUM_TERMINAL_CARDS = 64;
const MAXIMUM_SUMMARY_BYTES = 8 << 10;
const MAXIMUM_SOURCES = 10;
const MAXIMUM_SOURCE_TITLE_SCALARS = 256;
const MAXIMUM_SOURCE_URL_BYTES = 2048;
const MAXIMUM_AGGREGATE_TEXT_BYTES = 256 << 10;
const textEncoder = new TextEncoder();

const DEFAULT_POLICY = Object.freeze({
  web_search: Object.freeze({
    enabled: false,
    safe_search: 'strict',
    region: 'wt-wt',
    max_results: 5,
    timeout_ms: 8000,
  }),
});

const POLICY_KEYS = ['web_search'];
const WEB_SEARCH_KEYS = ['enabled', 'safe_search', 'region', 'max_results', 'timeout_ms'];
const REGIONS = new Set(['wt-wt', 'us-en', 'uk-en', 'au-en', 'ca-en', 'cn-zh']);
const SAFE_SEARCH = new Set(['strict', 'moderate']);
const EVENT_ID_PATTERN = /^[\x20-\x7e]{1,128}$/;
const HANDOFF_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;
const PROGRESS_PHASES = new Set(['queued', 'searching', 'reading_sources', 'preparing_answer']);
const RESUME_PHASES = new Set(['searching', 'reading_sources', 'preparing_answer']);
const PHASE_ORDER = new Map([
  ['queued', 0],
  ['searching', 1],
  ['reading_sources', 2],
  ['preparing_answer', 3],
  ['needs_input', 4],
  ['completed', 5],
  ['failed', 5],
  ['cancelled', 5],
]);
const TERMINAL_PHASES = new Set(['completed', 'failed', 'cancelled']);
const STABLE_PHASE_TEXT = {
  queued: 'Background task queued.',
  searching: 'Searching the web.',
  reading_sources: 'Reading sources.',
  preparing_answer: 'Preparing the answer.',
  needs_input: 'Background task needs input.',
  completed: 'Background task completed.',
  failed: 'Background task failed.',
  cancelled: 'Background task stopped.',
};
const STATUS_BY_TYPE = new Map([
  ['live.handoff.created', new Set(['pending'])],
  ['live.handoff.progress', new Set(['pending', 'leased', 'running', 'needs_input'])],
  ['live.handoff.needs_input', new Set(['needs_input'])],
  ['live.handoff.completed', new Set(['succeeded', 'completed'])],
  ['live.handoff.failed', new Set(['failed'])],
  ['live.handoff.cancelled', new Set(['cancelled', 'expired'])],
]);

function exactKeys(value, expected) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const keys = Object.keys(value).sort();
  const wanted = [...expected].sort();
  return keys.length === wanted.length && keys.every((key, index) => key === wanted[index]);
}

function clonePolicy(policy) {
  return {web_search: {...policy.web_search}};
}

function policiesEqual(left, right) {
  return WEB_SEARCH_KEYS.every((key) => left.web_search[key] === right.web_search[key]);
}

function policyIsCanonicalTightening(requested, accepted) {
  const before = requested.web_search;
  const after = accepted.web_search;
  return before.enabled === after.enabled
    && before.region === after.region
    && (before.safe_search === after.safe_search || (before.safe_search === 'moderate' && after.safe_search === 'strict'))
    && after.max_results <= before.max_results
    && after.timeout_ms <= before.timeout_ms;
}

function byteLength(value) {
  return textEncoder.encode(value).byteLength;
}

function boundedText(value, maximumBytes) {
  if (typeof value !== 'string' || value.length === 0) return '';
  if (byteLength(value) <= maximumBytes) return value;
  let output = '';
  let bytes = 0;
  for (const scalar of value) {
    const scalarBytes = byteLength(scalar);
    if (bytes + scalarBytes > maximumBytes) break;
    output += scalar;
    bytes += scalarBytes;
  }
  return output;
}

function printableTitle(value) {
  return typeof value === 'string' && value.length > 0 && [...value].length <= MAXIMUM_SOURCE_TITLE_SCALARS && !/[\p{Cc}\p{Cf}]/u.test(value);
}

function safeHTTPSURL(value) {
  if (typeof value !== 'string' || value.length === 0 || value.length > MAXIMUM_SOURCE_URL_BYTES || !/^[\x20-\x7e]+$/.test(value)) return false;
  try {
    const parsed = new URL(value);
    return parsed.protocol === 'https:' && parsed.username === '' && parsed.password === '';
  } catch {
    return false;
  }
}

function normalizeSources(value) {
  if (!Array.isArray(value)) return [];
  const sources = [];
  for (const candidate of value) {
    if (sources.length >= MAXIMUM_SOURCES) break;
    if (!candidate || typeof candidate !== 'object' || Array.isArray(candidate)) continue;
    const title = candidate.title ?? candidate.Title;
    const url = candidate.url ?? candidate.URL;
    if (printableTitle(title) && safeHTTPSURL(url)) sources.push({title, url});
  }
  return sources;
}

function nestedValue(value, ...path) {
  let current = value;
  for (const key of path) {
    if (!current || typeof current !== 'object' || Array.isArray(current)) return undefined;
    current = current[key];
  }
  return current;
}

function displayDetails(details) {
  const phase = details.phase ?? details.Phase;
  const summary = details.summary ?? details.Summary
    ?? nestedValue(details, 'result', 'summary') ?? nestedValue(details, 'Result', 'Summary')
    ?? nestedValue(details, 'failure', 'message') ?? nestedValue(details, 'Failure', 'Message');
  const sources = details.sources ?? details.Sources
    ?? nestedValue(details, 'result', 'structured', 'sources') ?? nestedValue(details, 'Result', 'Structured', 'sources');
  const output = {};
  if (phase !== undefined) output.phase = phase;
  if (typeof summary === 'string') output.summary = boundedText(summary, MAXIMUM_SUMMARY_BYTES);
  const normalizedSources = normalizeSources(sources);
  if (normalizedSources.length > 0 || Array.isArray(sources)) output.sources = normalizedSources;
  return output;
}

function phaseForEvent(event, details) {
  switch (event.type) {
    case 'live.handoff.created': return 'queued';
    case 'live.handoff.needs_input': return 'needs_input';
    case 'live.handoff.completed': return 'completed';
    case 'live.handoff.failed': return 'failed';
    case 'live.handoff.cancelled': return 'cancelled';
    default: return details.phase;
  }
}

function validExplicitPhase(type, phase) {
  if (phase === undefined) return true;
  switch (type) {
    case 'live.handoff.created': return phase === 'queued';
    case 'live.handoff.progress': return PROGRESS_PHASES.has(phase);
    case 'live.handoff.needs_input': return phase === 'needs_input';
    case 'live.handoff.completed': return phase === 'completed';
    case 'live.handoff.failed': return phase === 'failed';
    case 'live.handoff.cancelled': return phase === 'cancelled';
    default: return false;
  }
}

export function normalizeToolPolicy(value) {
  if (!exactKeys(value, POLICY_KEYS) || !exactKeys(value.web_search, WEB_SEARCH_KEYS)) throw new TypeError('invalid tool policy');
  const web = value.web_search;
  if (typeof web.enabled !== 'boolean' || !SAFE_SEARCH.has(web.safe_search) || !REGIONS.has(web.region)
      || !Number.isInteger(web.max_results) || web.max_results < 1 || web.max_results > 5
      || !Number.isInteger(web.timeout_ms) || web.timeout_ms < 1000 || web.timeout_ms > 8000) {
    throw new TypeError('invalid tool policy');
  }
  return clonePolicy(value);
}

export function buildToolPolicyUpdate(policy, eventID) {
  if (typeof eventID !== 'string' || !EVENT_ID_PATTERN.test(eventID)) throw new TypeError('invalid event id');
  return {type: 'live.tool_policy.update', event_id: eventID, tools: normalizeToolPolicy(policy)};
}

export function normalizeHandoffEvent(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || !STATUS_BY_TYPE.has(value.type)) throw new TypeError('invalid handoff event');
  const allowedKeys = new Set(['type', 'event_id', 'sequence_id', 'handoff_id', 'status', 'details']);
  if (Object.keys(value).some((key) => !allowedKeys.has(key))) throw new TypeError('invalid handoff event');
  if (value.event_id !== undefined && (typeof value.event_id !== 'string' || !EVENT_ID_PATTERN.test(value.event_id))) throw new TypeError('invalid handoff event');
  if (!Number.isSafeInteger(value.sequence_id) || value.sequence_id < 1 || !HANDOFF_ID_PATTERN.test(value.handoff_id)
      || !STATUS_BY_TYPE.get(value.type).has(value.status)) throw new TypeError('invalid handoff event');
  const rawDetails = value.details ?? {};
  if (!rawDetails || typeof rawDetails !== 'object' || Array.isArray(rawDetails)) throw new TypeError('invalid handoff event');
  const details = displayDetails(rawDetails);
  if (!validExplicitPhase(value.type, details.phase)) throw new TypeError('invalid handoff event');
  const outputDetails = {...details};
  const phase = phaseForEvent(value, details);
  if (phase !== undefined) outputDetails.phase = phase;
  return {
    type: value.type,
    handoff_id: value.handoff_id,
    sequence_id: value.sequence_id,
    status: value.status,
    details: outputDetails,
  };
}

function cloneCard(card) {
  return {
    id: card.id,
    sequence: card.sequence,
    status: card.status,
    phase: card.phase,
    summary: card.summary,
    sources: card.sources.map((source) => ({...source})),
    terminal: card.terminal,
  };
}

function cardTextBytes(card) {
  return byteLength(card.summary) + card.sources.reduce((total, source) => total + byteLength(source.title) + byteLength(source.url), 0);
}

export class ToolState {
  constructor(policy = DEFAULT_POLICY) {
    this.policy = normalizeToolPolicy(policy);
    this.cards = [];
    this.byID = new Map();
    this.announcements = [];
    this.saveStatus = 'ready';
    this.testStatus = 'idle';
    this.lastOperation = 'save';
    this.pendingPolicy = null;
    this.pendingPolicyEventID = null;
    this.pendingAcknowledgementPolicy = null;
    this.pendingAcknowledgementOperation = null;
    this.deferredLoadedPolicy = null;
    this.policyCommitGeneration = 0;
    this.operationGeneration = 0;
    this.latestOperationGeneration = 0;
    this.pendingSaveOperation = null;
    this.persistedSaveOperation = 0;
    this.pendingTestOperation = null;
    this.model = 'deepseek-v4-flash';
  }

  beginPolicyLoad() {
    return this.policyCommitGeneration;
  }

  loadPolicy(policy, loadGeneration = this.policyCommitGeneration) {
    const loaded = normalizeToolPolicy(policy);
    if (loadGeneration !== this.policyCommitGeneration) return false;
    if (this.saveStatus === 'saving') {
      this.deferredLoadedPolicy = loaded;
      return false;
    }
    this.policy = loaded;
    this.saveStatus = 'ready';
    return true;
  }

  beginSave(policy) {
    const operation = ++this.operationGeneration;
    this.latestOperationGeneration = operation;
    this.pendingPolicy = normalizeToolPolicy(policy);
    this.pendingSaveOperation = operation;
    this.pendingPolicyEventID = null;
    this.pendingAcknowledgementPolicy = null;
    this.pendingAcknowledgementOperation = null;
    this.deferredLoadedPolicy = null;
    this.saveStatus = 'saving';
    this.lastOperation = 'save';
    return operation;
  }

  commitSave(policy, operation = this.pendingSaveOperation) {
    const persisted = normalizeToolPolicy(policy);
    if (operation !== this.pendingSaveOperation || !this.pendingPolicy || !policiesEqual(persisted, this.pendingPolicy)) return false;
    this.policy = persisted;
    this.pendingPolicy = null;
    this.pendingSaveOperation = null;
    this.persistedSaveOperation = operation;
    this.policyCommitGeneration += 1;
    this.deferredLoadedPolicy = null;
    this.saveStatus = 'saved';
    if (operation === this.latestOperationGeneration) this.lastOperation = 'save';
    return true;
  }

  markPolicySent(eventID, policy = this.policy, operation = this.persistedSaveOperation) {
    if (typeof eventID !== 'string' || !EVENT_ID_PATTERN.test(eventID)) return false;
    let sent;
    try {
      sent = normalizeToolPolicy(policy);
    } catch {
      return false;
    }
    if (!policiesEqual(sent, this.policy)) return false;
    this.pendingPolicyEventID = eventID;
    this.pendingAcknowledgementPolicy = sent;
    this.pendingAcknowledgementOperation = operation;
    this.saveStatus = 'awaiting-ack';
    if (operation === this.latestOperationGeneration) this.lastOperation = 'save';
    return true;
  }

  failSave(operation = this.pendingSaveOperation) {
    if (this.pendingSaveOperation !== null && operation !== this.pendingSaveOperation) return false;
    if (operation === null) {
      operation = ++this.operationGeneration;
      this.latestOperationGeneration = operation;
    }
    this.pendingPolicy = null;
    this.pendingSaveOperation = null;
    this.pendingPolicyEventID = null;
    this.pendingAcknowledgementPolicy = null;
    this.pendingAcknowledgementOperation = null;
    if (this.deferredLoadedPolicy) this.policy = this.deferredLoadedPolicy;
    this.deferredLoadedPolicy = null;
    this.saveStatus = 'error';
    if (operation === this.latestOperationGeneration) this.lastOperation = 'save';
    return true;
  }

  failLoad(loadGeneration = this.policyCommitGeneration) {
    if (loadGeneration !== this.policyCommitGeneration) return false;
    if (this.saveStatus === 'saving' || this.saveStatus === 'awaiting-ack') return false;
    this.saveStatus = 'error';
    return true;
  }

  acceptPolicy(eventID, policy, model) {
    let accepted;
    try {
      accepted = normalizeToolPolicy(policy);
    } catch {
      return false;
    }
    if (eventID !== this.pendingPolicyEventID || model !== 'deepseek-v4-flash'
        || !this.pendingAcknowledgementPolicy || !policyIsCanonicalTightening(this.pendingAcknowledgementPolicy, accepted)) return false;
    this.policy = accepted;
    this.pendingPolicyEventID = null;
    this.pendingAcknowledgementPolicy = null;
    const operation = this.pendingAcknowledgementOperation;
    this.pendingAcknowledgementOperation = null;
    this.model = model;
    this.saveStatus = 'saved';
    if (operation === this.latestOperationGeneration) this.lastOperation = 'save';
    return true;
  }

  beginTest() {
    const operation = ++this.operationGeneration;
    this.latestOperationGeneration = operation;
    this.pendingTestOperation = operation;
    this.testStatus = 'running';
    this.lastOperation = 'test';
    return operation;
  }

  finishTest(passed, operation = this.pendingTestOperation) {
    if (operation !== this.pendingTestOperation) return false;
    this.pendingTestOperation = null;
    this.testStatus = passed ? 'passed' : 'error';
    if (operation === this.latestOperationGeneration) this.lastOperation = 'test';
    return true;
  }

  consume(rawEvent) {
    let event;
    try {
      event = normalizeHandoffEvent(rawEvent);
    } catch {
      return {accepted: false, created: false, phaseChanged: false, overflow: false};
    }
    const current = this.byID.get(event.handoff_id);
    if (current && (event.sequence_id <= current.sequence || current.terminal)) {
      return {accepted: false, created: false, phaseChanged: false, overflow: false};
    }
    const nextPhase = event.details.phase ?? current?.phase ?? 'queued';
    const resumesFromNeedsInput = current?.phase === 'needs_input' && RESUME_PHASES.has(nextPhase);
    if (current && !resumesFromNeedsInput && !TERMINAL_PHASES.has(nextPhase) && PHASE_ORDER.get(nextPhase) < PHASE_ORDER.get(current.phase)) {
      return {accepted: false, created: false, phaseChanged: false, overflow: false};
    }
    const created = !current;
    const phaseChanged = created || current.phase !== nextPhase;
    const card = current ? {...current, sources: current.sources.map((source) => ({...source}))} : {
      id: event.handoff_id,
      sequence: 0,
      status: event.status,
      phase: nextPhase,
      summary: '',
      sources: [],
      terminal: false,
    };
    card.sequence = event.sequence_id;
    card.status = event.status;
    card.phase = nextPhase;
    card.terminal = TERMINAL_PHASES.has(nextPhase);
    if (event.details.summary) card.summary = event.details.summary;
    if (!card.summary) card.summary = STABLE_PHASE_TEXT[nextPhase];
    if (event.details.sources) card.sources = event.details.sources.map((source) => ({...source}));
    const activeCount = this.cards.filter((candidate) => !candidate.terminal).length
      - (current && !current.terminal ? 1 : 0) + (!card.terminal ? 1 : 0);
    const aggregateBytes = this.totalTextBytes() - (current ? cardTextBytes(current) : 0) + cardTextBytes(card);
    if (activeCount > MAXIMUM_ACTIVE_CARDS || aggregateBytes > MAXIMUM_AGGREGATE_TEXT_BYTES) {
      return {accepted: false, created: false, phaseChanged: false, overflow: true};
    }
    if (created) {
      this.cards.push(card);
    } else {
      this.cards[this.cards.indexOf(current)] = card;
    }
    this.byID.set(card.id, card);
    if (phaseChanged) {
      this.announcements.push(STABLE_PHASE_TEXT[nextPhase]);
      if (this.announcements.length > 128) this.announcements.shift();
    }
    this.enforceBounds();
    return {accepted: true, created, phaseChanged, overflow: false};
  }

  enforceBounds() {
    while (this.cards.filter((card) => card.terminal).length > MAXIMUM_TERMINAL_CARDS) {
      this.removeCard(this.cards.find((card) => card.terminal));
    }
  }

  removeCard(card) {
    if (!card) return;
    const index = this.cards.indexOf(card);
    if (index >= 0) this.cards.splice(index, 1);
    this.byID.delete(card.id);
  }

  totalTextBytes() {
    return this.cards.reduce((total, card) => total + cardTextBytes(card), 0);
  }

  snapshot() {
    return {
      policy: clonePolicy(this.policy),
      saveStatus: this.saveStatus,
      testStatus: this.testStatus,
      lastOperation: this.lastOperation,
      model: this.model,
      cards: this.cards.map(cloneCard),
      totalTextBytes: this.totalTextBytes(),
    };
  }

  drainAnnouncements() {
    const output = [...this.announcements];
    this.announcements = [];
    return output;
  }
}
