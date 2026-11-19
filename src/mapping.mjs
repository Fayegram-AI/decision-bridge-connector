import { assert, freeze, getPath, identifier, isRecord, keysOnly, snapshot } from './util.mjs';

const own = (value, key) => Object.hasOwn(value, key);

/** Check declarative selectors without reading evidence or running user code. */
export function validateProjection(spec) {
  const clean = snapshot(spec);
  let visited = 0;
  function visit(node, depth) {
    assert(++visited <= 10000 && depth <= 48, 'Projection exceeds its resource limit.', 'RESOURCE_LIMIT');
    if (Array.isArray(node)) { node.forEach(child => visit(child, depth + 1)); return; }
    if (!isRecord(node)) return;
    if (own(node, '$literal')) {
      keysOnly(node, ['$literal'], 'Literal projection');
      return;
    }
    if (own(node, '$path')) {
      keysOnly(node, ['$path', 'default', 'required'], 'Path projection');
      assert(typeof node.$path === 'string' && node.$path.length <= 4096, 'Projection path must be a string of at most 4096 characters.');
      assert(node.required === undefined || typeof node.required === 'boolean', 'Projection required must be boolean.');
      // The shared accessor rejects prototype traversal even when no input exists.
      validatePath(node.$path);
      return;
    }
    assert(!Object.keys(node).some(key => key.startsWith('$')), 'Unknown projection directive; wrap literal dollar-prefixed keys in $literal.');
    Object.values(node).forEach(child => visit(child, depth + 1));
  }
  visit(clean, 0);
  return freeze(clean);
}

export function validatePath(path) {
  assert(typeof path === 'string' && path.length <= 4096, 'Path must be a string of at most 4096 characters.');
  if (path === '' || path === '$') return path;
  if (path.startsWith('/')) assert(!/~(?![01])/u.test(path), 'JSON pointer escapes must be ~0 or ~1.');
  const parts = path.startsWith('/') ? path.slice(1).split('/').map(part => part.replace(/~1/g, '/').replace(/~0/g, '~'))
    : path.replace(/^\$\./u, '').split('.');
  assert(path.startsWith('/') || parts.every(part => part.length > 0), 'Dotted paths cannot contain empty segments.');
  assert(!parts.some(part => ['__proto__', 'prototype', 'constructor'].includes(part)), 'Unsafe property path.');
  return path;
}

/** A missing optional selector produces JSON null; defaults are literal JSON. */
export function project(value, spec) {
  return projectCaptured(snapshot(value), spec);
}

/** Map a value that has already been captured at its own boundary. */
export function projectCaptured(source, spec) {
  const projection = validateProjection(spec);
  function resolve(node) {
    if (Array.isArray(node)) return node.map(resolve);
    if (!isRecord(node)) return node;
    if (own(node, '$literal')) return node.$literal;
    if (own(node, '$path')) {
      const selected = getPath(source, node.$path);
      if (selected !== undefined) return selected;
      if (own(node, 'default')) return node.default;
      assert(node.required === false, `Projection path is missing: ${node.$path}`, 'MAPPING_MISSING_PATH');
      return null;
    }
    return Object.fromEntries(Object.entries(node).map(([key, child]) => [key, resolve(child)]));
  }
  // Selected values and literals never share mutable storage with either caller input.
  return freeze(snapshot(resolve(projection)));
}

export function validateTemplate(template) {
  const clean = snapshot(template);
  keysOnly(clean, ['id', 'revision', 'request'], 'Request template');
  if (own(clean, 'id')) identifier(clean.id, 'Template id');
  identifier(clean.revision, 'Template revision');
  assert(own(clean, 'request'), 'Request template requires a request projection.');
  validateProjection(clean.request);
  return freeze(clean);
}

/** Mapping builds only a request. Task identity and revisions stay in the envelope. */
export function mapRequest(event, template) {
  const configured = validateTemplate(template);
  const request = project(event, configured.request);
  assert(isRecord(request), 'A request template must produce a JSON object.', 'MAPPING_INVALID_REQUEST');
  return request;
}
