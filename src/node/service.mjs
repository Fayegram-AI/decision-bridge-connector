import { assert, publicError } from '../util.mjs';
import { configuredStorageIdentity, createConfiguredRunner } from './configuration.mjs';
import { createHost, prepareHost, startHost, HOST_DEPLOYMENT } from './host.mjs';
import { AGGREGATION_GENERATION } from '../engine/aggregation.mjs';
import { ADMISSION_LIFETIME } from '../engine/admission-lifetime.mjs';
import { hasRevisionWork } from '../storage/revision-work.mjs';
import { cleanupDiagnostic, closeEvery, startupCleanupError } from './cleanup.mjs';
import { createExtensionLoader, EXTENSION_LOADER } from './extension-loader.mjs';

/** Reload admits new work to a new generation while prior revisions keep workers. */
export async function createService(configuration, { baseDir, env, signal, maxGenerations = 8, maxExtensionGraphs = 64, ...hostOptions } = {}) {
    assert(Number.isSafeInteger(maxGenerations) && maxGenerations > 0, 'maxGenerations must be a positive safe integer.');
    let current; const retired = []; let closed = false; let reloading = false; let closing;
    let lifecycle = Promise.resolve();
    const extensions = createExtensionLoader({ maxExtensionGraphs });
    const cleanupFailures = [];
    // Reaping, cutover and shutdown all mutate the same owned generation set.
    function serialize(operation) {
        const result = lifecycle.then(operation);
        lifecycle = result.catch(() => {});
        return result;
    }
    async function create(config) {
        const runner = await createConfiguredRunner(config, { baseDir, env, signal, [EXTENSION_LOADER]: extensions });
        try { return { runner, host: await createHost({ ...runner.config.host, ...hostOptions, runner, signal }) }; }
        catch (error) {
            try { await runner.close({ cancel: true }); } catch (cleanup) { throw startupCleanupError(error, cleanup); }
            throw error;
        }
    }
    try { current = await create(configuration); } catch (error) { extensions.close(); throw error; }
    async function reap() {
        // Newer retained workers may cover an older generation; never let two
        // candidates mutually justify retiring every worker for the same contract.
        const replacements = [current];
        for (const generation of [...retired].reverse()) {
            const admissions = generation.runner[ADMISSION_LIFETIME];
            const checkpoint = admissions.checkpoint();
            if (checkpoint === null) { replacements.push(generation); continue; }
            const deployment = generation.host[HOST_DEPLOYMENT];
            let needed = false;
            if (deployment.role !== 'ingress') {
                const eligible = replacements.filter(item => item.host[HOST_DEPLOYMENT].role !== 'ingress');
                for (const flow of Object.values(await generation.runner.ready)) {
                    const compatible = [];
                    for (const replacement of eligible) if (Object.values(await replacement.runner.ready).some(item => item.runtimeRevision === flow.runtimeRevision)) compatible.push(replacement);
                    const labels = new Set(compatible.flatMap(item => item.runner.placements));
                    const requiredLabels = flow.connections.map(connection => connection.placement).filter(label => label && deployment.placements.includes(label));
                    if ((!compatible.length || !requiredLabels.every(label => labels.has(label))) &&
                        await hasRevisionWork(generation.runner.store, generation.runner.namespace, flow)) needed = true;
                }
                for (const aggregator of Object.values(generation.runner.aggregators)) {
                    const revision = await aggregator[AGGREGATION_GENERATION]();
                    let covered = false;
                    for (const replacement of eligible) for (const other of Object.values(replacement.runner.aggregators))
                        if ((await other[AGGREGATION_GENERATION]()).runtimeRevision === revision.runtimeRevision) covered = true;
                    if (!covered && await generation.runner.store.pendingAggregates(generation.runner.namespace, revision.runtimeRevision)) needed = true;
                }
            }
            if (needed || !admissions.fence(checkpoint)) replacements.push(generation);
            else {
                try { await generation.host.close(); }
                catch (error) {
                    cleanupFailures.push(publicError(cleanupDiagnostic(error)));
                    if (cleanupFailures.length > 32) cleanupFailures.shift();
                } finally { retired.splice(retired.indexOf(generation), 1); }
            }
        }
    }
    return Object.freeze({
        get runner() { return current.runner; },
        async health() {
            assert(!closed, 'Service is closed.', 'CONFLICT');
            return serialize(async () => { await reap(); return { ...(await current.host.health()),
                retainedGenerations: retired.length, cleanupFailures: [...cleanupFailures] }; });
        },
        async reload(config) {
            assert(!closed && !reloading, 'Service is closed or already reloading.', 'CONFLICT'); reloading = true;
            return serialize(async () => {
                let next; let primary;
                try {
                    await reap(); assert(retired.length + 1 < maxGenerations, 'Pending revisions reached the generation limit.', 'RESOURCE_LIMIT');
                    next = await createConfiguredRunner(config, { baseDir, env, [EXTENSION_LOADER]: extensions });
                    assert(next.namespace === current.runner.namespace, 'Reload cannot change the namespace.');
                    assert(configuredStorageIdentity(next) === configuredStorageIdentity(current.runner), 'Reload cannot change resolved storage configuration or its factory.');
                    assert(next.capabilities.durable, 'Reload requires shared durable storage.');
                    // A replacement must still hydrate evidence admitted by prior generations.
                    assert(next.store.limits.maxBytes >= current.runner.store.limits.maxBytes,
                        'Reload cannot lower maxBytes while retaining existing execution data.');
                    const prepared = await prepareHost({ ...next.config.host, ...hostOptions, runner: next });
                    const previous = current.host[HOST_DEPLOYMENT];
                    let host;
                    try {
                        host = await startHost(prepared, { deferActivation: true, beforeSource: previous.suspendConflicts });
                    } catch (error) {
                        try { await previous.restoreListeners(); }
                        catch (restoreError) { throw new AggregateError([error, restoreError], 'Reload failed and previous ingress could not be restored.'); }
                        throw error;
                    }
                    await current.host.stopIngress();
                    retired.push(current); current = { runner: next, host }; next = null;
                    host[HOST_DEPLOYMENT].activate();
                } catch (error) { primary = { error }; throw error; }
                finally {
                    reloading = false;
                    try { await next?.close({ cancel: true }); }
                    catch (cleanup) { if (primary) throw startupCleanupError(primary.error, cleanup); throw cleanup; }
                }
            });
        },
        close() {
            if (closing) return closing;
            closed = true;
            closing = serialize(async () => {
                try { await closeEvery([current, ...retired].map(generation => () => generation.host.close())); }
                finally { extensions.close(); }
            });
            return closing;
        },
    });
}
