import { resolve, dirname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { hostname } from 'node:os';
import Ajv from 'ajv';
import { createBridge, createProvider } from '@fayegram-ai/decision-bridge';
import { createGenerativeProvider } from '@fayegram-ai/decision-bridge/custom/generative';
import { createExtractionProvider } from '@fayegram-ai/decision-bridge/extraction';
import { createDecisionRouter, createExtractionRouter } from '@fayegram-ai/decision-bridge/routing';
import { decisionEvaluator, extractionEvaluator, inheritExtractionPreflight, routedDecisionEvaluator, routedExtractionEvaluator } from '../operations.mjs';
import { abortError, assert, bounded, fingerprint, freeze, identifier, isRecord, keysOnly, runnerIdentity, snapshot, stableStringify, validateSignal } from '../util.mjs';
import { readJson } from './io.mjs';
import { createRegistry, extensionDefinition } from '../engine/registry.mjs';
import { createRunner } from '../engine/runner.mjs';
import { createDurableAggregator } from '../engine/aggregation.mjs';
import { createMemoryStore } from '../storage/memory.mjs';
import { createFileConnector, createHttpConnector, createProcessConnector } from './connectors.mjs';
import { createResourceOwner } from './resources.mjs';
import { closeEvery, startupCleanupError } from './cleanup.mjs';
import { createExtensionLoader, EXTENSION_LOADER } from './extension-loader.mjs';
import { canonicalModule } from './extension-resolution.mjs';
import { createConfiguredRedisConnector } from './redis-connector.mjs';
import { aggregationBinding, SOURCE_BINDING, validateSourceBinding } from './source-recovery.mjs';
import { providerCredentials, routingCredentials } from './provider-credentials.mjs';

const objectSchema = { type: 'object' };
const storageIdentities = new WeakMap();

/** Reload safety metadata stays private; resolved settings never enter saved plans or reports. */
export function configuredStorageIdentity(runner) { return storageIdentities.get(runner); }

export function validateConfig(input) {
    const config = snapshot(input, { maxBytes: 4 * 1024 * 1024 });
    keysOnly(config, ['schemaVersion', 'namespace', 'workerId', 'placements', 'limits', 'store', 'extensions', 'evaluators',
        'validators', 'policies', 'transforms', 'connectors', 'flows', 'incoming', 'host', 'aggregations'], 'Configuration');
    assert(config.schemaVersion === 3, 'Configuration schemaVersion must be 3. Use fresh state; older schemas are unsupported.', 'UNSUPPORTED_SCHEMA');
    runnerIdentity(config);
    assert(isRecord(config.flows), 'Named flows are required.');
    for (const key of ['evaluators', 'validators', 'policies', 'transforms', 'connectors', 'incoming']) {
        config[key] ??= {}; assert(isRecord(config[key]), `Invalid ${key} registry.`);
    }
    for (const [name, descriptor] of Object.entries(config.incoming)) {
        identifier(name, 'Incoming source name');
        keysOnly(descriptor, ['type', 'options', 'aggregation'], 'Incoming instance');
        if (descriptor.aggregation !== undefined) {
            const binding = aggregationBinding(descriptor.aggregation, name);
            const rule = Object.hasOwn(config.aggregations ?? {}, binding.rule) ? config.aggregations[binding.rule] : undefined;
            assert(rule && Array.isArray(rule.sources), 'Incoming aggregation rule is unavailable.');
            aggregationBinding(binding, name, rule);
        }
    }
    config.store ??= { type: 'memory', options: {} };
    config.extensions ??= [];
    assert(Array.isArray(config.extensions), 'Extensions must be an array.');
    if (config.host) keysOnly(config.host, ['role', 'pollMs', 'maintenanceMs', 'drainMs'], 'Host settings');
    return freeze(config);
}

export async function loadConfig(path, { signal } = {}) {
    const filename = resolve(path);
    const value = await readJson(filename, { maxBytes: 4 * 1024 * 1024, signal });
    return { config: validateConfig(value), baseDir: dirname(filename), path: filename };
}

/** Credential references are resolved only in trusted connection configuration. */
function resolveReferences(value, env) {
    if (Array.isArray(value)) return value.map(item => resolveReferences(item, env));
    if (!isRecord(value)) return value;
    if (Object.hasOwn(value, '$env')) {
        keysOnly(value, ['$env'], 'Credential reference');
        assert(typeof value.$env === 'string' && value.$env.length > 0, 'Environment reference requires a nonempty variable name.');
        assert(typeof env[value.$env] === 'string' && env[value.$env].length > 0, `Missing environment reference: ${value.$env}`, 'CREDENTIAL_UNAVAILABLE');
        return env[value.$env];
    }
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, resolveReferences(item, env)]));
}

export async function createConfiguredRunner(input, { baseDir = process.cwd(), env = process.env, registry = createRegistry(), onEvent, signal,
    maxExtensionGraphs = 64, [EXTENSION_LOADER]: suppliedLoader } = {}) {
    validateSignal(signal);
    if (signal?.aborted) throw abortError('startup');
    const config = validateConfig(input); const resources = createResourceOwner();
    const loader = suppliedLoader ?? createExtensionLoader({ maxExtensionGraphs }); let graph;
    let storageIdentity;
    const ajv = new Ajv({ allErrors: true, strict: true }); const schemas = new Map();
    // Reuse schema identities across stored-plan clones without sharing state between runners.
    function compileSchema(schema) {
        const key = stableStringify(schema);
        if (!schemas.has(key)) {
            let validate;
            // Ajv also throws plain Error values for invalid schema authorship.
            try { validate = ajv.compile(schema); }
            catch { assert(false, 'Invalid JSON schema.'); }
            schemas.set(key, validate);
        }
        return schemas.get(key);
    }
    async function checkSchema(validate, value, message, code) {
        try {
            if (validate.$async) await validate(value);
            else assert(validate(value), message, code);
        } catch (error) {
            if (error?.validation === true) assert(false, message, code);
            throw error;
        }
        return value;
    }
    const define = (kind, name, create, schema = objectSchema) => registry.register(kind, name, { apiVersion: 1, revision: '1', schema, create });
    const path = value => value === ':memory:' ? value : resolve(baseDir, value);
    const createBackend = settings => {
        const options = providerCredentials(settings, env);
        return ['openai-compatible', 'openrouter', 'anthropic'].includes(options?.type)
            ? createGenerativeProvider(options) : createProvider(options);
    };
    define('store', 'memory', settings => createMemoryStore({ ...settings, ...config.limits }));
    define('store', 'sqlite', async settings => (await import('../storage/sqlite.mjs')).createSQLiteStore(path(settings.path), config.limits));
    define('store', 'postgres', async settings => (await import('../storage/postgres.mjs')).createPostgresStore({ ...settings, ...config.limits }));
    define('evaluator', 'decision', settings => {
        const provider = createBackend(settings.provider);
        return decisionEvaluator(createBridge({ provider }), { revision: settings.revision ?? '1',
            validationOptions: request => provider.getResultValidationOptions?.(request.model) ?? {} });
    });
    define('evaluator', 'extraction', settings => extractionEvaluator(createExtractionProvider(providerCredentials({ type: 'cloudflare-extraction', ...settings.provider }, env)), { revision: settings.revision ?? '1' }));
    define('evaluator', 'routing', settings => settings.operation === 'extraction'
        ? routedExtractionEvaluator(createExtractionRouter(routingCredentials(settings.routing, env)), { revision: settings.revision ?? '1' })
        : routedDecisionEvaluator(createDecisionRouter(routingCredentials(settings.routing, env)), { revision: settings.revision ?? '1' }));
    define('validator', 'live', async () => {
        const { validateLiveDecisionNotification } = await import('../integrations/live.mjs');
        return { revision: '1', provenance: 'structural', validate: value => ({ notification: validateLiveDecisionNotification(value.notification) }) };
    });
    define('validator', 'schema', settings => {
        const validate = compileSchema(settings.schema);
        return { revision: settings.revision ?? '1', provenance: 'structural',
            validate: value => checkSchema(validate, value, 'Result does not match its configured schema.', 'INVALID_RESULT') };
    });
    define('outgoing', 'http', settings => createHttpConnector(settings));
    define('outgoing', 'file', settings => createFileConnector({ ...settings, directory: path(settings.directory) }));
    define('outgoing', 'process', settings => createProcessConnector({ ...settings, ...(settings.cwd ? { cwd: path(settings.cwd) } : {}) }));
    define('outgoing', 'redis', createConfiguredRedisConnector);
    define('outgoing', 'mcp', async (settings, { signal }) =>
        (await import('../integrations/mcp-client.mjs')).createMcpConnector(settings, { signal }));
    for (const type of ['http', 'file', 'stdin', 'process', 'spool', 'redis', 'mcp'])
        define('incoming', type, async settings => (await import('./incoming.mjs')).createIncoming(type, settings, { baseDir }));
    try {
        graph = await loader.prepare(config.extensions.map(descriptor => pathToFileURL(path(descriptor.module)).href), { signal });
        for (const descriptor of config.extensions) {
            keysOnly(descriptor, ['kind', 'name', 'module', 'export'], 'Extension registration');
            const filename = path(descriptor.module);
            const { module, digest } = graph.modules.get(await canonicalModule(pathToFileURL(filename).href));
            const extension = module[descriptor.export ?? 'default'];
            const declared = identifier(extension?.revision, 'Extension revision');
            // Preserve established identities while bounding valid long declarations.
            const revision = declared.length <= 191 ? `${declared}:${digest}` : await fingerprint([declared, digest]);
            registry.register(descriptor.kind, descriptor.name, extensionDefinition(extension, revision));
        }
        async function instantiate(kind, descriptor) {
            keysOnly(descriptor, ['type', 'options', ...(kind === 'incoming' ? ['aggregation'] : [])], 'Extension instance');
            const extension = registry.get(kind, descriptor.type);
            const settings = resolveReferences(descriptor.options ?? {}, env);
            await bounded(() => checkSchema(compileSchema(extension.schema), settings,
                `Invalid ${kind} configuration for ${descriptor.type}.`, 'CONFIGURATION_ERROR'), { signal, timeoutMs: 60000, stage: 'startup-validation' });
            if (kind === 'store') storageIdentity = await fingerprint({ type: descriptor.type, factory: extension.revision, settings,
                baseDir: resolve(baseDir), ...(descriptor.type === 'postgres'
                    ? { driverEnvironment: Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith('PG'))) } : {}) });
            const instance = await resources.create(signal => extension.create(settings, { signal, baseDir }), { signal });
            if (kind === 'store') return instance;
            if (kind === 'incoming') {
                const source = { ...instance };
                // Bind class methods/getters to their original instance, including private fields.
                for (const key of ['name', 'revision', 'capabilities']) {
                    const value = instance[key]; if (value !== undefined) source[key] = value;
                }
                for (const method of ['open', 'start', 'health']) if (typeof instance[method] === 'function') source[method] = instance[method].bind(instance);
                if (typeof instance.close === 'function') source.close = options => resources.close(instance, options);
                return Object.freeze(source);
            }
            const revision = await fingerprint({ extension: extension.revision, descriptor });
            const methods = {};
            for (const method of ['execute', 'validate', 'validateSaved', 'apply', 'transform', 'send', 'close'])
                if (typeof instance[method] === 'function') methods[method] = method === 'close' ? () => resources.close(instance) : instance[method].bind(instance);
            // Class metadata may live on prototype getters; spreading loses authority and placement declarations.
            const metadata = {};
            for (const key of ['name', 'capabilities', 'operation', 'provenance']) {
                const value = instance[key]; if (value !== undefined) metadata[key] = freeze(snapshot(value));
            }
            const configured = Object.freeze({ ...instance, ...metadata, ...methods, revision });
            if (kind === 'evaluator') inheritExtractionPreflight(instance, configured);
            return configured;
        }
        const registrations = {};
        for (const [key, kind] of Object.entries({ evaluators: 'evaluator', validators: 'validator', policies: 'policy', transforms: 'transform', connectors: 'outgoing' })) {
            registrations[key] = Object.create(null);
            for (const [name, descriptor] of Object.entries(config[key])) registrations[key][name] = await instantiate(kind, descriptor);
        }
        const store = await instantiate('store', config.store);
        // Compile before inference: a malformed configured schema is a startup error.
        for (const flow of Object.values(config.flows)) for (const connection of flow.connections ?? [])
            if (connection.outputSchema !== undefined) compileSchema(connection.outputSchema);
        const runner = createRunner({ ...registrations, store, flows: config.flows, namespace: config.namespace,
            workerId: config.workerId, placements: config.placements ?? [hostname()], limits: config.limits, onEvent,
            async validateOutput(schema, value) { await checkSchema(compileSchema(schema), value, 'Outgoing payload does not match its schema.', 'OUTPUT_SCHEMA'); } });
        await bounded(() => runner.ready, { signal, timeoutMs: 60000, stage: 'startup' });
        const aggregators = Object.create(null);
        for (const [name, rule] of Object.entries(config.aggregations ?? {})) {
            assert(Object.hasOwn(config.flows, rule.flow), 'Aggregation target flow is unavailable.');
            assert((await runner.ready)[rule.flow].inputs.includes('event'), 'Aggregation target must accept events.');
            aggregators[name] = createDurableAggregator({ runner, rule: { ...rule, id: name } });
        }
        const sources = Object.create(null);
        for (const [name, descriptor] of Object.entries(config.incoming)) {
            const source = await instantiate('incoming', descriptor);
            validateSourceBinding(source[SOURCE_BINDING], await runner.ready, Boolean(descriptor.aggregation));
            if (descriptor.aggregation) assert(aggregators[descriptor.aggregation.rule] && source.open, 'Aggregation requires a registered rule and an iterable source.');
            sources[name] = descriptor.aggregation ? { ...source, open: source.open.bind(source),
                ...(source.close ? { close: source.close.bind(source) } : {}), aggregation: descriptor.aggregation } : source;
        }
        let closing;
        await bounded(() => graph.verify(), { signal, timeoutMs: 60000, stage: 'extension-graph' });
        const configured = Object.freeze({ ...runner, config, sources, registry, aggregators,
            async close(options) {
                if (closing) { if (options?.cancel) await runner.close(options); return closing; }
                closing = (async () => {
                    // Cleanup must still run if draining the runner fails.
                    let draining;
                    try { await runner.close(options); } catch (error) { draining = { error }; }
                    try { await closeEvery([...(draining ? [() => { throw draining.error; }] : []), () => resources.closeAll()]); }
                    finally { graph.release(); if (!suppliedLoader) loader.close(); }
                })();
                return closing;
            } });
        storageIdentities.set(configured, storageIdentity);
        return configured;
    } catch (error) {
        try { await resources.closeAll(); } catch (cleanup) { throw startupCleanupError(error, cleanup); }
        finally { graph?.release(); if (!suppliedLoader) loader.close(); }
        throw error;
    }
}

export function inspectConfiguration(input) {
    const config = validateConfig(input);
    return { schemaVersion: 3, namespace: config.namespace ?? 'default', flows: config.flows,
        incoming: Object.fromEntries(Object.entries(config.incoming).map(([name, value]) => [name, { type: value.type, flow: value.options?.flow }])),
        credentials: 'Environment references remain unresolved.' };
}
