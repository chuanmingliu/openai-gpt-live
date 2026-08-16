const PACK_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const HASH_PATTERN = /^[0-9a-f]{64}$/;
const MAX_VERSIONS = 100;
const MAX_EXCERPTS = 2048;
const MAX_EXCERPT_CHARS = 4096;
// Mirrors the server ceiling so an oversize file is refused before it is read.
export const MAX_UPLOAD_BYTES = 8 << 20;
export const ACCEPTED_EXTENSIONS = Object.freeze(['.md', '.markdown', '.txt', '.text']);

export class KnowledgeError extends Error {
  constructor(code) {
    super(code);
    this.code = code;
  }
}

function fail(code) {
  throw new KnowledgeError(code);
}

function decodeErrorCode(body, status) {
  if (body !== null && typeof body === 'object' && body.error && typeof body.error.code === 'string'
      && /^[a-z][a-z_]{0,63}$/.test(body.error.code)) {
    return body.error.code;
  }
  return `http_${status}`;
}

export function decodePreview(value) {
  if (value === null || typeof value !== 'object'
      || typeof value.pack_id !== 'string' || !PACK_ID_PATTERN.test(value.pack_id)
      || typeof value.hash !== 'string' || !HASH_PATTERN.test(value.hash)
      || typeof value.title !== 'string' || value.title === ''
      || typeof value.media_type !== 'string'
      || !Array.isArray(value.excerpts) || value.excerpts.length === 0
      || value.excerpts.length > MAX_EXCERPTS) {
    fail('invalid_response');
  }
  const excerpts = value.excerpts.map((entry, index) => {
    if (entry === null || typeof entry !== 'object'
        || entry.ordinal !== index
        || typeof entry.text !== 'string' || entry.text === '' || entry.text.length > MAX_EXCERPT_CHARS
        || typeof entry.section !== 'string') {
      fail('invalid_response');
    }
    return Object.freeze({ordinal: entry.ordinal, text: entry.text, section: entry.section});
  });
  return Object.freeze({
    packID: value.pack_id,
    hash: value.hash,
    title: value.title,
    mediaType: value.media_type,
    excerpts: Object.freeze(excerpts),
  });
}

export function decodeVersions(value) {
  if (value === null || typeof value !== 'object' || !Array.isArray(value.versions)
      || value.versions.length > MAX_VERSIONS) {
    fail('invalid_response');
  }
  const versions = value.versions.map((entry) => {
    if (entry === null || typeof entry !== 'object'
        || typeof entry.version !== 'number' || !Number.isSafeInteger(entry.version) || entry.version < 1
        || typeof entry.hash !== 'string' || !HASH_PATTERN.test(entry.hash)
        || typeof entry.title !== 'string'
        || typeof entry.excerpt_count !== 'number' || !Number.isSafeInteger(entry.excerpt_count) || entry.excerpt_count < 0
        || typeof entry.tombstoned !== 'boolean') {
      fail('invalid_response');
    }
    return Object.freeze({
      version: entry.version,
      hash: entry.hash,
      title: entry.title,
      excerptCount: entry.excerpt_count,
      tombstoned: entry.tombstoned,
    });
  });
  return Object.freeze(versions);
}

// acceptedFile reports whether a File may be uploaded at all. The extension
// check is a courtesy that keeps an obvious mistake local; the server repeats
// it against the bytes, which is where the decision actually binds.
export function acceptedFile(file) {
  if (!file || typeof file.name !== 'string' || typeof file.size !== 'number') return false;
  if (file.size <= 0 || file.size > MAX_UPLOAD_BYTES) return false;
  const lowered = file.name.toLowerCase();
  return ACCEPTED_EXTENSIONS.some((extension) => lowered.endsWith(extension));
}

// createKnowledgeAPI wraps the same-origin knowledge endpoints. Uploads are
// multipart; nothing else about the file is sent, and no response is trusted
// before it is decoded.
export function createKnowledgeAPI(fetchImpl, formDataImpl) {
  if (typeof fetchImpl !== 'function') throw new TypeError('knowledge API requires fetch');
  const FormDataCtor = formDataImpl || (typeof FormData === 'function' ? FormData : null);
  if (typeof FormDataCtor !== 'function') throw new TypeError('knowledge API requires FormData');

  async function send(method, path, body, signal) {
    let response;
    try {
      response = await fetchImpl(path, {
        method,
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

  function requirePackID(packID) {
    if (typeof packID !== 'string' || !PACK_ID_PATTERN.test(packID) || packID === 'builtin') {
      fail('invalid_pack_id');
    }
    return packID;
  }

  return Object.freeze({
    async listVersions(packID, signal) {
      const response = await send('GET', `/api/setup/v1/knowledge/${requirePackID(packID)}`, undefined, signal);
      if (!response.ok) fail(decodeErrorCode(await jsonBody(response).catch(() => null), response.status));
      return decodeVersions(await jsonBody(response));
    },
    async preview(packID, file, signal) {
      requirePackID(packID);
      if (!acceptedFile(file)) fail('unsupported_content');
      const form = new FormDataCtor();
      form.append('document', file, file.name);
      const response = await send('POST', `/api/setup/v1/knowledge/${packID}`, form, signal);
      if (!response.ok) fail(decodeErrorCode(await jsonBody(response).catch(() => null), response.status));
      return decodePreview(await jsonBody(response));
    },
    // publish re-sends the same file with the previewed hash. The server
    // re-derives excerpts and refuses if they no longer match, so a file that
    // changed between preview and publish cannot slip through.
    async publish(packID, file, previewedHash, signal) {
      requirePackID(packID);
      if (!acceptedFile(file)) fail('unsupported_content');
      if (typeof previewedHash !== 'string' || !HASH_PATTERN.test(previewedHash)) fail('invalid_response');
      const form = new FormDataCtor();
      form.append('hash', previewedHash);
      form.append('document', file, file.name);
      const response = await send('POST', `/api/setup/v1/knowledge/${packID}/publish`, form, signal);
      if (!response.ok) fail(decodeErrorCode(await jsonBody(response).catch(() => null), response.status));
      const body = await jsonBody(response);
      if (body === null || typeof body !== 'object'
          || typeof body.version !== 'number' || !Number.isSafeInteger(body.version) || body.version < 1
          || typeof body.hash !== 'string' || !HASH_PATTERN.test(body.hash)) {
        fail('invalid_response');
      }
      return Object.freeze({packID, version: body.version, hash: body.hash});
    },
    async tombstone(packID, version, signal) {
      requirePackID(packID);
      if (!Number.isSafeInteger(version) || version < 1) fail('invalid_response');
      const response = await send('POST', `/api/setup/v1/knowledge/${packID}/${version}/tombstone`, undefined, signal);
      if (!response.ok) fail(decodeErrorCode(await jsonBody(response).catch(() => null), response.status));
      return true;
    },
  });
}

// createKnowledgeController owns the Knowledge tab's state. One action runs at
// a time, and the selected file plus its preview are dropped on destroy so no
// customer material outlives the page.
export function createKnowledgeController({api, render}) {
  if (!api || typeof render !== 'function') {
    throw new TypeError('knowledge controller requires api and render');
  }
  let packID = '';
  let file = null;
  let preview = null;
  let versions = Object.freeze([]);
  let notice = '';
  let busy = false;
  let controller = null;
  let destroyed = false;

  function snapshot() {
    return Object.freeze({
      packID,
      filename: file === null ? '' : file.name,
      preview,
      versions,
      notice,
      busy,
      // Publishing requires a preview of the file currently selected.
      publishDisabled: busy || preview === null || file === null,
    });
  }

  function emit() {
    if (!destroyed) render(snapshot());
  }

  async function act(run, successCode) {
    if (destroyed) return false;
    if (busy) {
      notice = 'busy';
      emit();
      return false;
    }
    busy = true;
    controller = new AbortController();
    const signal = controller.signal;
    emit();
    try {
      await run(signal);
      if (!destroyed && !signal.aborted) notice = successCode;
      return true;
    } catch (error) {
      if (destroyed || signal.aborted) return false;
      notice = typeof error?.code === 'string' ? error.code : 'network_unavailable';
      return false;
    } finally {
      busy = false;
      controller = null;
      emit();
    }
  }

  return Object.freeze({
    snapshot,
    setPack(next) {
      packID = typeof next === 'string' ? next.trim() : '';
      // A pack change invalidates any preview: the hash is pack-scoped.
      preview = null;
      versions = Object.freeze([]);
      emit();
    },
    // selectFile drops any previous preview, so the publish button can never
    // send one file under another file's reviewed hash.
    selectFile(next) {
      file = next || null;
      preview = null;
      notice = file !== null && !acceptedFile(file) ? 'unsupported_content' : '';
      emit();
    },
    previewFile() {
      return act(async (signal) => {
        preview = await api.preview(packID, file, signal);
      }, 'previewed');
    },
    publishPreviewed() {
      return act(async (signal) => {
        const reviewed = preview;
        if (reviewed === null) throw new KnowledgeError('unsupported_content');
        await api.publish(packID, file, reviewed.hash, signal);
        versions = await api.listVersions(packID, signal);
        preview = null;
        file = null;
      }, 'published');
    },
    refreshVersions() {
      return act(async (signal) => {
        versions = await api.listVersions(packID, signal);
      }, '');
    },
    tombstoneVersion(version) {
      return act(async (signal) => {
        await api.tombstone(packID, version, signal);
        versions = await api.listVersions(packID, signal);
      }, 'tombstoned');
    },
    destroy() {
      destroyed = true;
      if (controller !== null) controller.abort();
      controller = null;
      file = null;
      preview = null;
      versions = Object.freeze([]);
      packID = '';
    },
  });
}
