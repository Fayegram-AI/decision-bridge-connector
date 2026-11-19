import { assert, identifier } from '../util.mjs';

const kinds = new Set(['incoming', 'outgoing', 'evaluator', 'validator', 'policy', 'transform', 'store']);

/** Capture factory metadata without losing prototype methods or their original receiver. */
export function extensionDefinition(extension, revision = extension?.revision) {
    const { apiVersion, create, schema, runtime } = extension ?? {};
    assert(apiVersion === 1 && typeof create === 'function', 'Extension requires apiVersion 1 and create().');
    identifier(revision, 'Extension revision');
    assert(schema && typeof schema === 'object', 'Extension requires a configuration schema.');
    return Object.freeze({ ...extension, apiVersion, revision, schema, runtime, create: create.bind(extension) });
}

/** An omitted selector never selects the registration literally named "undefined". */
export function registeredEntry(entries, name) {
    return typeof name === 'string' && entries && Object.hasOwn(entries, name) ? entries[name] : undefined;
}

/** Explicit registration keeps application modules out of untrusted submissions. */
export function createRegistry() {
    const entries = new Map();
    return Object.freeze({
        register(kind, name, extension) {
            assert(kinds.has(kind), 'Unknown extension kind.');
            identifier(name);
            const key = `${kind}:${name}`;
            assert(!entries.has(key), `Extension already registered: ${key}`);
            entries.set(key, extensionDefinition(extension));
            return this;
        },
        get(kind, name) {
            assert(kinds.has(kind), 'Unknown extension kind.');
            identifier(name, 'Extension name');
            const entry = entries.get(`${kind}:${name}`);
            assert(entry, `Unregistered ${kind} extension: ${name}`);
            return entry;
        },
        describe() {
            return [...entries].map(([key, value]) => ({ key, apiVersion: 1, revision: value.revision,
                runtime: value.runtime ?? 'portable', schema: value.schema }));
        },
    });
}
