const ID = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$/;
const SAFE_CATALOG_KEYS = new Set(['registry_version', 'adapters']);
const SAFE_ADAPTER_KEYS = new Set(['id', 'version', 'tool_name', 'description', 'tool_schema', 'read_only', 'side_effecting', 'parallel_safe', 'approval_required', 'health_class', 'max_result_bytes']);

function exactKeys(value, allowed) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).every((key) => allowed.has(key));
}

function cloneToolSchema(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new TypeError('invalid adapter catalog');
  const encoded = JSON.stringify(value);
  if (encoded.length > 65536) throw new TypeError('invalid adapter catalog');
  const clone = JSON.parse(encoded);
  const freeze = (node, depth = 0) => {
    if (depth > 32 || node === null || typeof node !== 'object') return node;
    for (const child of Object.values(node)) freeze(child, depth + 1);
    return Object.freeze(node);
  };
  return freeze(clone);
}

export function decodeAdapterCatalog(value) {
  if (!exactKeys(value, SAFE_CATALOG_KEYS) || !ID.test(value.registry_version) || !Array.isArray(value.adapters) || value.adapters.length > 256) {
    throw new TypeError('invalid adapter catalog');
  }
  const seen = new Set();
  const adapters = value.adapters.map((entry) => {
    if (!exactKeys(entry, SAFE_ADAPTER_KEYS) || !ID.test(entry.id) || !ID.test(entry.version) || !ID.test(entry.tool_name)
        || typeof entry.description !== 'string' || entry.description.length > 1024
        || typeof entry.read_only !== 'boolean' || typeof entry.side_effecting !== 'boolean'
        || typeof entry.parallel_safe !== 'boolean' || typeof entry.approval_required !== 'boolean'
        || !['none', 'required'].includes(entry.health_class)
        || !Number.isSafeInteger(entry.max_result_bytes) || entry.max_result_bytes < 1 || entry.max_result_bytes > 262144
        || seen.has(entry.id)) {
      throw new TypeError('invalid adapter catalog');
    }
    seen.add(entry.id);
    return Object.freeze({
      id: entry.id,
      version: entry.version,
      toolName: entry.tool_name,
      description: entry.description,
      toolSchema: cloneToolSchema(entry.tool_schema),
      readOnly: entry.read_only,
      sideEffecting: entry.side_effecting,
      parallelSafe: entry.parallel_safe,
      approvalRequired: entry.approval_required,
      healthClass: entry.health_class,
      maxResultBytes: entry.max_result_bytes,
    });
  });
  return Object.freeze({registryVersion: value.registry_version, adapters: Object.freeze(adapters)});
}

export function bindingRequest(adapter, values) {
  if (adapter === null || typeof adapter !== 'object' || !Number.isSafeInteger(values?.maxResultBytes)
      || values.maxResultBytes < 1 || values.maxResultBytes > adapter.maxResultBytes
      || typeof values.requireApproval !== 'boolean' || (adapter.approvalRequired && !values.requireApproval)) {
    throw new TypeError('invalid adapter binding');
  }
  return Object.freeze({max_result_bytes: values.maxResultBytes, require_approval: values.requireApproval});
}
