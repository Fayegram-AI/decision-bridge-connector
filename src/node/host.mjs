import { abortError, assert, bounded, ConnectorError, delay, publicError, getPath, validateSignal } from '../util.mjs';
import { hostSources, PREPARED_LISTENER, SOURCE_PREFLIGHT, SUSPEND_LISTENER } from './source-recovery.mjs';
import { ADMISSION_LIFETIME } from '../engine/admission-lifetime.mjs';
import { overlapsListener } from './http-security.mjs';
import { SOURCE_ACTIVE, sourceActivation } from './host-activation.mjs';
import { acknowledgementFailure, replyFailure } from '../engine/receipts.mjs';
import { cleanupDiagnostic, cleanupError, startupCleanupError } from './cleanup.mjs';
export const HOST_DEPLOYMENT = Symbol('host-deployment');

function conflicts(left, right) {
    if (!left || !right) return false;
    if (left.stdio || right.stdio) return Boolean(left.stdio && right.stdio &&
        (left.stdio.input === right.stdio.input || left.stdio.output === right.stdio.output));
    return overlapsListener(left, right);
}

/** Capture and validate a deployment before starting workers, sources or service cutover. */
export async function prepareHost({ runner, sources = runner.sources ?? {}, aggregators = runner.aggregators ?? {}, role = 'all', pollMs = 50, maintenanceMs = 1000,
    drainMs = 30000, onError = () => {}, signal } = {}) {
    validateSignal(signal);
    if (signal?.aborted) throw abortError('host-startup');
    assert(['all', 'ingress', 'worker'].includes(role), 'Invalid host role.');
    for (const value of [pollMs, maintenanceMs, drainMs]) assert(Number.isSafeInteger(value) && value > 0 && value <= 2147483647, 'Invalid host time limit.');
    const flows = signal ? await bounded(() => runner.ready, { signal, timeoutMs: 60000, stage: 'host-preparation' }) : await runner.ready;
    const sourceEntries = role === 'worker' ? [] : hostSources(sources, flows, aggregators, runner.capabilities?.durable);
    const listeners = [];
    for (const entry of sourceEntries) {
        const { name, source } = entry;
        if (!source[SOURCE_PREFLIGHT]) continue;
        const address = await bounded(() => source[SOURCE_PREFLIGHT]({ runner, role }),
            { signal, timeoutMs: drainMs, stage: 'source-preflight' });
        if (!address) continue;
        const conflict = listeners.find(listener => conflicts(listener.address, address));
        assert(!conflict, `Incoming listeners ${conflict?.name} and ${name} have conflicting bind addresses.`);
        listeners.push({ name, address });
        entry.address = address;
    }
    return { runner, aggregators, role, pollMs, maintenanceMs, drainMs, onError, sourceEntries, signal };
}

/** Service lifecycle owns scheduling; admission remains separate from execution. */
export async function createHost(options = {}) {
    return startHost(await prepareHost(options));
}

// A replacement stages sources before committing ingress and worker activation.
export async function startHost({ runner, aggregators, role, pollMs, maintenanceMs, drainMs, onError, sourceEntries, signal },
    { deferActivation = false, beforeSource } = {}) {
    if (signal?.aborted) throw abortError('host-startup');
    const controller = new AbortController(); const ingress = new AbortController(); const running = []; const incoming = []; const activeSources = [];
    const activation = sourceActivation(runner, ingress.signal);
    const starters = []; const suspended = new Set();
    let stopped = false; let lastMaintenance = 0; let ready = false; let ingressClosing; let closing;
    // The caller owns startup cancellation; the returned host owns its lifetime.
    const cancelStartup = () => { controller.abort(); ingress.abort(); };
    signal?.addEventListener('abort', cancelStartup, { once: true });
    const errors = []; const failures = new Map(); const cleanupFailures = [];
    function record(error, activity, cleanup = false) {
        if (controller.signal.aborted && !cleanup) return;
        const value = publicError(cleanupDiagnostic(error), cleanup ? activity : undefined); errors.push(value); if (errors.length > 32) errors.shift();
        if (activity) failures.set(activity, value);
        try { void Promise.resolve(onError(value)).catch(() => {}); } catch {}
    }
    async function attempt(activity, operation) {
        try { await operation(); failures.delete(activity); } catch (error) { record(error, activity); }
    }
    async function closeSources() {
        await Promise.all(activeSources.map(({ name, source }) => bounded(signal => source.close?.({ signal, timeoutMs: drainMs }),
            { timeoutMs: drainMs, stage: `source-close:${name}` }).catch(error => {
                cleanupFailures.push(error); record(error, `source-close:${name}`, true);
            })));
    }
    async function refreshSources() {
        for (const { name, source } of sourceEntries) if (source.health) {
            const activity = `source:${name}`;
            try {
                const health = await bounded(() => source.health(), { timeoutMs: 1000, stage: 'source-health' });
                assert(typeof health?.ready === 'boolean', 'Source health requires a ready flag.');
                if (health.ready) failures.delete(activity);
                else {
                    const value = { code: 'SOURCE_UNREADY', message: 'Incoming source is unavailable.', stage: activity };
                    if (!failures.has(activity)) record(new ConnectorError(value.code, value.message, { stage: activity }), activity);
                    failures.set(activity, value);
                }
            } catch (error) { if (!failures.has(activity)) record(error, activity); }
        }
    }
    if (role !== 'ingress') {
        try {
            if (signal) await bounded(() => runner.recover(), { signal, timeoutMs: 60000, stage: 'host-recovery' });
            else await runner.recover();
        }
        catch (error) { cancelStartup(); signal?.removeEventListener('abort', cancelStartup); throw error; }
        starters.push(() => running.push((async () => {
            while (!controller.signal.aborted) {
                await attempt('worker', () => runner.tick());
                if (Date.now() - lastMaintenance >= maintenanceMs) {
                    await refreshSources();
                    // A failed rule cannot starve another rule or retention indefinitely.
                    for (const [activity, operation] of [['recovery', () => runner.recover()],
                        ...Object.entries(aggregators).map(([name, aggregator]) => [`aggregation:${name}`, () => aggregator.flush()]), ['retention', () => runner.maintain()]]) {
                        await attempt(activity, operation);
                    }
                    lastMaintenance = Date.now();
                }
                try { await delay(pollMs, controller.signal); } catch { break; }
            }
        })()));
    }
    const startSource = ({ source, name, address }) => bounded(signal => source.start({
        runner: activation.runner, signal: AbortSignal.any([ingress.signal, signal]), name, role,
        whenActive: activation.whenActive, [SOURCE_ACTIVE]: activation.active,
        ...(address ? { [PREPARED_LISTENER]: address } : {}),
    }), { signal: ingress.signal, timeoutMs: drainMs, stage: 'source-start' });
    try { for (const entry of sourceEntries) {
        const { name, source, aggregation, aggregator, address } = entry;
        if (address) await beforeSource?.(address);
        activeSources.push(entry);
        if (source.start) {
            await startSource(entry);
        } else starters.push(() => incoming.push((async () => {
            try {
                for await (const message of await source.open({ signal: ingress.signal })) {
                    if (ingress.signal.aborted) break;
                    let acknowledgementInvoked = false; let acknowledgement; let externalAcknowledgement;
                    try {
                        const acknowledge = message.acknowledge;
                        const reply = message.reply;
                        assert(reply === undefined || typeof reply === 'function', 'Source reply must be callable.');
                        if (acknowledge !== undefined) {
                            assert(typeof acknowledge === 'function', 'Source acknowledgement must be callable.');
                            assert(runner.capabilities.durable, 'Source acknowledgement requires durable admission.');
                        }
                        const add = () => {
                            if (ingress.signal.aborted) throw abortError('admission');
                            const event = message.input[message.input.kind];
                            return aggregator.add({
                                source: aggregation.member ?? name, event,
                                eventId: aggregation.eventIdPath === undefined ? message.input.idempotencyKey : getPath(event, aggregation.eventIdPath),
                                correlationKey: getPath(event, aggregation.correlationPath),
                            });
                        };
                        const admitted = aggregation
                            ? await (runner[ADMISSION_LIFETIME] ? runner[ADMISSION_LIFETIME].track(add) : add())
                            : await runner.submit(message.input, { source: name, signal: ingress.signal });
                        if (acknowledge) {
                            const invokeAcknowledgement = async () => {
                                acknowledgementInvoked = true;
                                try {
                                    const value = await acknowledge.call(message, admitted);
                                    externalAcknowledgement = ['failed', 'unknown', 'submitted'].includes(value?.status) ? value.status : 'acknowledged';
                                    return value;
                                } catch (error) {
                                    externalAcknowledgement = publicError(error).ambiguous === false ? 'failed' : 'unknown';
                                    throw error;
                                }
                            };
                            if (admitted.taskId) {
                                acknowledgement = await runner.acknowledge(admitted.taskId, invokeAcknowledgement, { timeoutMs: drainMs });
                            } else {
                                const received = await bounded(invokeAcknowledgement, { timeoutMs: drainMs });
                                assert(received?.status === undefined || ['acknowledged', 'submitted', 'unknown', 'failed'].includes(received.status),
                                    'Invalid source acknowledgement receipt.', 'INVALID_RECEIPT');
                                acknowledgement = { ...received, status: received?.status === 'submitted' ? 'unknown' : received?.status ?? 'acknowledged' };
                            }
                            const failure = acknowledgementFailure(acknowledgement);
                            if (failure) record(failure);
                        }
                        if (reply) {
                            const disposition = admitted.taskId
                                ? await runner.reply(admitted.taskId, (value, context) => reply.call(message, value, context))
                                : await bounded(signal => reply.call(message, admitted, { signal }), { timeoutMs: 10000 });
                            const failure = replyFailure(disposition);
                            if (failure) record(failure);
                        }
                    } catch (error) {
                        // A lost settlement write does not prove that the source did not ACK.
                        const canReject = acknowledgement?.status === 'failed' || !acknowledgement &&
                            (!acknowledgementInvoked || externalAcknowledgement === 'failed');
                        try { if (canReject) await bounded(() => message.reject?.(publicError(error)), { timeoutMs: drainMs }); }
                        catch (rejection) { record(rejection); }
                        record(error);
                    }
                }
            } catch (error) { if (!ingress.signal.aborted) record(error, `source:${name}`); }
        })()));
    } } catch (error) {
        controller.abort(); ingress.abort();
        await closeSources();
        await bounded(() => Promise.allSettled([...running, ...incoming]), { timeoutMs: drainMs })
            .catch(failure => cleanupFailures.push(failure));
        if (cleanupFailures.length) throw startupCleanupError(error, cleanupError(cleanupFailures));
        throw error;
    } finally { signal?.removeEventListener('abort', cancelStartup); }
    function activate() {
        ready = true;
        activation.activate();
        for (const start of starters.splice(0)) start();
    }
    async function suspendConflicts(address) {
        for (const entry of sourceEntries) {
            if (!entry.source[SUSPEND_LISTENER] || suspended.has(entry) || !conflicts(entry.address, address)) continue;
            suspended.add(entry);
            await bounded(() => entry.source[SUSPEND_LISTENER](), { timeoutMs: drainMs, stage: 'source-suspend' });
        }
    }
    async function restoreListeners() {
        const errors = [];
        for (const entry of suspended) {
            try { await startSource(entry); suspended.delete(entry); failures.delete(`listener:${entry.name}`); }
            catch (error) { errors.push(error); record(error, `listener:${entry.name}`); }
        }
        if (errors.length) throw new AggregateError(errors, 'Previous listeners could not be restored.');
    }
    if (!deferActivation) activate();
    function stopIngress() {
        if (ingressClosing) return ingressClosing;
        ready = false; ingress.abort();
        ingressClosing = (async () => {
            await closeSources();
            await bounded(() => Promise.allSettled(incoming), { timeoutMs: drainMs }).catch(error => {
                // Reload retains admissions beyond its ingress budget; final shutdown owns cleanup failure.
                if (stopped) cleanupFailures.push(error);
                record(error, 'ingress-drain', true);
            });
        })();
        return ingressClosing;
    }
    return Object.freeze({
        [HOST_DEPLOYMENT]: Object.freeze({ role, placements: runner.placements ?? [], activate, suspendConflicts, restoreListeners }),
        async health() { await refreshSources(); return { ready: ready && !stopped && failures.size === 0, role,
            errors: [...errors], failures: Object.fromEntries(failures), ...(await runner.inspect()) }; },
        stopIngress,
        close() {
            if (closing) return closing;
            stopped = true; ready = false; controller.abort();
            closing = (async () => {
                await stopIngress();
                await bounded(() => Promise.allSettled(running), { timeoutMs: drainMs }).catch(error => {
                    cleanupFailures.push(error); record(error, 'worker-drain', true);
                });
                try { await bounded(() => runner.close(), { timeoutMs: drainMs }); }
                catch (error) {
                    if (error?.code === 'TIMEOUT') {
                        try { await bounded(() => runner.close({ cancel: true }), { timeoutMs: drainMs }); }
                        catch (failure) { cleanupFailures.push(failure); record(failure, 'runner-close', true); }
                    } else { cleanupFailures.push(error); record(error, 'runner-close', true); }
                }
                if (cleanupFailures.length) throw cleanupError(cleanupFailures);
            })();
            return closing;
        },
    });
}
