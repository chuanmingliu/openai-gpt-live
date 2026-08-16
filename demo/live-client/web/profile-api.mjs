const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const HASH_PATTERN = /^[0-9a-f]{64}$/;
const TEMPLATE_PATH_PATTERN = /^[a-z0-9][a-z0-9_./-]{0,254}$/;
const MAX_LIST_ENTRIES = 100;
const MAX_FILE_ENTRIES = 128;
const MAX_SOURCE_BYTES = 64 << 10;
const MAX_DESCRIPTION_CHARS = 4096;

export class ProfileAPIError extends Error {
  constructor(code) {
    super(code);
    this.code = code;
  }
}

function fail(code) {
  throw new ProfileAPIError(code);
}

function decodeErrorCode(body, status) {
  if (body !== null && typeof body === 'object' && body.error && typeof body.error.code === 'string'
      && /^[a-z][a-z_]{0,63}$/.test(body.error.code)) {
    return body.error.code;
  }
  return `http_${status}`;
}

function validProfileID(value) {
  return typeof value === 'string' && ID_PATTERN.test(value) && value !== 'builtin';
}

function validVersion(value) {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 1;
}

function decodeFileInfos(value) {
  if (!Array.isArray(value) || value.length > MAX_FILE_ENTRIES) fail('invalid_response');
  return value.map((entry) => {
    if (entry === null || typeof entry !== 'object'
        || typeof entry.path !== 'string' || !TEMPLATE_PATH_PATTERN.test(entry.path)
        || typeof entry.hash !== 'string' || !HASH_PATTERN.test(entry.hash)
        || typeof entry.bytes !== 'number' || !Number.isSafeInteger(entry.bytes) || entry.bytes < 0) {
      fail('invalid_response');
    }
    return Object.freeze({path: entry.path, hash: entry.hash, bytes: entry.bytes});
  });
}

const RECIPE_ID_PATTERN = /^[a-z0-9][a-z0-9_-]{0,63}$/;

function decodeRecipes(value) {
  if (!Array.isArray(value) || value.length > MAX_FILE_ENTRIES) fail('invalid_response');
  return value.map((entry) => {
    if (entry === null || typeof entry !== 'object'
        || typeof entry.id !== 'string' || !RECIPE_ID_PATTERN.test(entry.id)
        || typeof entry.version !== 'number' || !Number.isSafeInteger(entry.version) || entry.version < 1
        || typeof entry.objective !== 'string' || entry.objective !== `workers/${entry.id}.j2`
        || typeof entry.require_approval !== 'boolean') {
      fail('invalid_response');
    }
    for (const key of ['execution_timeout_ms', 'tool_timeout_ms', 'overall_timeout_ms',
      'max_objective_bytes', 'max_evidence_bytes', 'max_tool_result_bytes', 'max_final_result_bytes']) {
      if (typeof entry[key] !== 'number' || !Number.isSafeInteger(entry[key]) || entry[key] < 0) fail('invalid_response');
    }
    return Object.freeze({
      id: entry.id,
      version: entry.version,
      objective: entry.objective,
      requireApproval: entry.require_approval,
      executionTimeoutMS: entry.execution_timeout_ms,
      toolTimeoutMS: entry.tool_timeout_ms,
      overallTimeoutMS: entry.overall_timeout_ms,
      maxObjectiveBytes: entry.max_objective_bytes,
      maxEvidenceBytes: entry.max_evidence_bytes,
      maxToolResultBytes: entry.max_tool_result_bytes,
      maxFinalResultBytes: entry.max_final_result_bytes,
    });
  });
}

export function decodeDraft(value) {
  if (value === null || typeof value !== 'object'
      || !validProfileID(value.id)
      || typeof value.generation !== 'number' || !Number.isSafeInteger(value.generation) || value.generation < 1
      || typeof value.description !== 'string' || value.description.length > MAX_DESCRIPTION_CHARS) {
    fail('invalid_response');
  }
  return Object.freeze({
    id: value.id,
    generation: value.generation,
    description: value.description,
    templates: Object.freeze(decodeFileInfos(value.templates)),
    fixtures: Object.freeze(decodeFileInfos(value.fixtures)),
    recipes: Object.freeze(decodeRecipes(value.recipes ?? [])),
  });
}

export function decodePublishedList(value) {
  if (value === null || typeof value !== 'object' || !Array.isArray(value.profiles)
      || value.profiles.length > MAX_LIST_ENTRIES) {
    fail('invalid_response');
  }
  const profiles = value.profiles.map((entry) => {
    if (entry === null || typeof entry !== 'object'
        || !validProfileID(entry.id) || !validVersion(entry.version)
        || typeof entry.active !== 'boolean' || typeof entry.revoked !== 'boolean'
        || typeof entry.snapshot_hash !== 'string' || !HASH_PATTERN.test(entry.snapshot_hash)) {
      fail('invalid_response');
    }
    return Object.freeze({
      id: entry.id,
      version: entry.version,
      active: entry.active,
      revoked: entry.revoked,
      snapshotHash: entry.snapshot_hash,
    });
  });
  return Object.freeze(profiles);
}

export function decodePreview(value) {
  if (value === null || typeof value !== 'object'
      || typeof value.bundle_hash !== 'string' || !HASH_PATTERN.test(value.bundle_hash)
      || typeof value.snapshot_hash !== 'string' || !HASH_PATTERN.test(value.snapshot_hash)
      || value.outputs === null || typeof value.outputs !== 'object' || Array.isArray(value.outputs)) {
    fail('invalid_response');
  }
  const outputs = {};
  for (const [name, output] of Object.entries(value.outputs)) {
    if (typeof output !== 'string' || output.length > MAX_SOURCE_BYTES) fail('invalid_response');
    outputs[name] = output;
  }
  return Object.freeze({bundleHash: value.bundle_hash, snapshotHash: value.snapshot_hash, outputs: Object.freeze(outputs)});
}

export function decodePublished(value) {
  if (value === null || typeof value !== 'object'
      || !validProfileID(value.id) || !validVersion(value.version)
      || typeof value.snapshot_hash !== 'string' || !HASH_PATTERN.test(value.snapshot_hash)
      || typeof value.active !== 'boolean') {
    fail('invalid_response');
  }
  return Object.freeze({id: value.id, version: value.version, snapshotHash: value.snapshot_hash, active: value.active});
}

// createProfileAPI wraps the same-origin setup endpoints with strict response
// decoding. All requests are abortable and cache-free; no credential, tenant,
// or database detail ever appears in a request.
export function createProfileAPI(fetchImpl) {
  if (typeof fetchImpl !== 'function') throw new TypeError('profile API requires fetch');

  async function request(method, path, {body, contentType, ifMatch, signal} = {}) {
    const headers = {};
    if (contentType) headers['Content-Type'] = contentType;
    if (ifMatch !== undefined) headers['If-Match'] = `"${ifMatch}"`;
    let response;
    try {
      response = await fetchImpl(path, {
        method,
        headers,
        body,
        signal,
        cache: 'no-store',
        credentials: 'same-origin',
      });
    } catch (error) {
      if (error && error.name === 'AbortError') throw error;
      fail('network_unavailable');
    }
    return response;
  }

  async function jsonBody(response) {
    try {
      return await response.json();
    } catch {
      fail('invalid_response');
    }
    return null;
  }

  async function expectJSON(response, decode) {
    if (!response.ok) fail(decodeErrorCode(await jsonBody(response).catch(() => null), response.status));
    return decode(await jsonBody(response));
  }

  function requireTemplatePath(path) {
    if (typeof path !== 'string' || !TEMPLATE_PATH_PATTERN.test(path) || path.includes('..') || !path.endsWith('.j2')) {
      fail('invalid_template_path');
    }
    return path;
  }

  return Object.freeze({
    async listPublished(signal) {
      const response = await request('GET', '/api/setup/v1/profiles?state=published', {signal});
      return expectJSON(response, decodePublishedList);
    },
    async createProfile(id, description, signal) {
      const response = await request('POST', '/api/setup/v1/profiles', {
        body: JSON.stringify({id, description}),
        contentType: 'application/json',
        signal,
      });
      return expectJSON(response, decodeDraft);
    },
    async loadDraft(id, signal) {
      const response = await request('GET', `/api/setup/v1/profiles/${encodeURIComponent(id)}/draft`, {signal});
      if (response.status === 404) return null;
      return expectJSON(response, decodeDraft);
    },
    async saveDescription(id, generation, description, signal) {
      const response = await request('PUT', `/api/setup/v1/profiles/${encodeURIComponent(id)}/draft`, {
        body: JSON.stringify({description}),
        contentType: 'application/json',
        ifMatch: generation,
        signal,
      });
      return expectJSON(response, decodeDraft);
    },
    async templateSource(id, path, signal) {
      const response = await request('GET', `/api/setup/v1/profiles/${encodeURIComponent(id)}/templates/${requireTemplatePath(path)}`, {signal});
      if (!response.ok) fail(`http_${response.status}`);
      let text;
      try {
        text = await response.text();
      } catch {
        fail('invalid_response');
      }
      if (typeof text !== 'string' || text.length > MAX_SOURCE_BYTES) fail('invalid_response');
      return text;
    },
    async saveTemplate(id, generation, path, source, signal) {
      const response = await request('PUT', `/api/setup/v1/profiles/${encodeURIComponent(id)}/templates/${requireTemplatePath(path)}`, {
        body: source,
        contentType: 'text/plain; charset=utf-8',
        ifMatch: generation,
        signal,
      });
      return expectJSON(response, decodeDraft);
    },
    async deleteTemplate(id, generation, path, signal) {
      const response = await request('DELETE', `/api/setup/v1/profiles/${encodeURIComponent(id)}/templates/${requireTemplatePath(path)}`, {
        ifMatch: generation,
        signal,
      });
      if (!response.ok) fail(decodeErrorCode(await jsonBody(response).catch(() => null), response.status));
      return true;
    },
    async saveRecipe(id, generation, recipeID, settings, signal) {
      if (typeof recipeID !== 'string' || !RECIPE_ID_PATTERN.test(recipeID)) fail('invalid_recipe_id');
      const response = await request('PUT', `/api/setup/v1/profiles/${encodeURIComponent(id)}/recipes/${recipeID}`, {
        body: JSON.stringify(settings),
        contentType: 'application/json',
        ifMatch: generation,
        signal,
      });
      return expectJSON(response, decodeDraft);
    },
    async deleteRecipe(id, generation, recipeID, signal) {
      if (typeof recipeID !== 'string' || !RECIPE_ID_PATTERN.test(recipeID)) fail('invalid_recipe_id');
      const response = await request('DELETE', `/api/setup/v1/profiles/${encodeURIComponent(id)}/recipes/${recipeID}`, {
        ifMatch: generation,
        signal,
      });
      if (!response.ok) fail(decodeErrorCode(await jsonBody(response).catch(() => null), response.status));
      return true;
    },
    async validate(id, generation, signal) {
      const response = await request('POST', `/api/setup/v1/profiles/${encodeURIComponent(id)}/validate`, {
        body: JSON.stringify({expected_generation: generation}),
        contentType: 'application/json',
        signal,
      });
      if (!response.ok) fail(decodeErrorCode(await jsonBody(response).catch(() => null), response.status));
      return true;
    },
    async preview(id, generation, signal) {
      const response = await request('POST', `/api/setup/v1/profiles/${encodeURIComponent(id)}/preview`, {
        body: JSON.stringify({expected_generation: generation}),
        contentType: 'application/json',
        signal,
      });
      return expectJSON(response, decodePreview);
    },
    async publish(id, generation, activate, signal) {
      const response = await request('POST', `/api/setup/v1/profiles/${encodeURIComponent(id)}/publish`, {
        body: JSON.stringify({expected_generation: generation, activate: Boolean(activate)}),
        contentType: 'application/json',
        signal,
      });
      return expectJSON(response, decodePublished);
    },
    async activate(id, version, signal) {
      const response = await request('POST', `/api/setup/v1/profiles/${encodeURIComponent(id)}/activate`, {
        body: JSON.stringify({version}),
        contentType: 'application/json',
        signal,
      });
      if (!response.ok) fail(decodeErrorCode(await jsonBody(response).catch(() => null), response.status));
      return true;
    },
    async revoke(id, version, signal) {
      const response = await request('POST', `/api/setup/v1/profiles/${encodeURIComponent(id)}/revoke`, {
        body: JSON.stringify({version}),
        contentType: 'application/json',
        signal,
      });
      if (!response.ok) fail(decodeErrorCode(await jsonBody(response).catch(() => null), response.status));
      return true;
    },
  });
}
