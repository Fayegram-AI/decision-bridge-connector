import { abortError, assert, bounded, delay, identifier, keysOnly, publicError } from '../util.mjs';
import { attachLiveController } from './live.mjs';
import { SOURCE_BINDING, sourceBinding, validateSourceBinding } from '../source-binding.mjs';
import { cleanupDiagnostic, cleanupError, closeEvery } from '../cleanup.mjs';

async function waitForDisconnect(closed, signal) {
    let abort;
    try {
        await Promise.race([closed, new Promise(resolve => {
            abort = resolve;
            if (signal.aborted) resolve(); else signal.addEventListener('abort', abort, { once: true });
        })]);
    } finally { signal.removeEventListener('abort', abort); }
}

/** Reconnect an application-supplied transport factory, retaining a stable session identity. */
export function createLiveSource({ connect, flow, sessionId, reconnect = {}, onError = () => {}, ...attachmentOptions } = {}) {
    assert(typeof connect === 'function', 'Live reconnect requires connect() and a stable sessionId.');
    identifier(flow, 'Live flow'); identifier(sessionId, 'Live session ID');
    const binding = sourceBinding({ flow, inputMode: 'result' });
    assert(typeof onError === 'function', 'Live onError must be a function.');
    keysOnly(reconnect, ['maxAttempts', 'delayMs', 'maxDelayMs', 'timeoutMs'], 'Live reconnect options');
    const policy = { maxAttempts: 10, delayMs: 250, maxDelayMs: 30000, timeoutMs: 30000, ...reconnect };
    for (const value of Object.values(policy)) assert(Number.isSafeInteger(value) && value > 0, 'Invalid reconnect bound.');
    for (const key of ['delayMs', 'maxDelayMs', 'timeoutMs']) assert(policy[key] <= 2147483647, 'Live reconnect timers exceed the timer limit.');
    let stop; let loop; let active; let closing; let started = false; let closed = false; let state = 'idle'; let reconnects = 0;
    const cleanupFailures = [];
    async function recordCleanup(error) {
        cleanupFailures.push(error); if (cleanupFailures.length > 32) cleanupFailures.shift();
        await bounded(() => onError(publicError(cleanupDiagnostic(error), 'live-close')), { timeoutMs: 1000 }).catch(() => {});
    }
    return Object.freeze({
        [SOURCE_BINDING]: binding,
        health() { return { ready: state === 'listening' && active !== null && !cleanupFailures.length, state }; },
        async start({ runner, signal, role = 'all', whenActive }) {
            assert(!started && !closed, 'Live source is already started or closed.');
            assert(['all', 'ingress'].includes(role), 'Live sources require an incoming host role.');
            assert(whenActive === undefined || typeof whenActive?.then === 'function', 'Live activation must be a promise.');
            started = true;
            stop = new AbortController();
            const lifetime = signal ? AbortSignal.any([signal, stop.signal]) : stop.signal;
            const flows = await bounded(() => runner.ready, { signal: lifetime, timeoutMs: policy.timeoutMs, stage: 'live-start' });
            validateSourceBinding(binding, flows);
            const processing = role === 'ingress' ? 'admit' : 'run';
            assert(typeof runner[processing === 'admit' ? 'submit' : 'run'] === 'function', 'Live ingress requires a runner.');
            loop = (async () => {
                // A staged source owns no transport and cannot consume non-replayable decisions.
                if (whenActive !== undefined) {
                    state = 'waiting_activation';
                    try { await waitForDisconnect(whenActive, lifetime); }
                    catch (error) {
                        if (!lifetime.aborted) await bounded(() => onError(publicError(error, 'live-activation')), { timeoutMs: 1000 }).catch(() => {});
                        state = 'stopped'; return;
                    }
                }
                let failures = 0;
                while (!lifetime.aborted) {
                    let connection; let offered; let disposal; let attached = false;
                    const dispose = resource => {
                        if (!resource) return Promise.resolve();
                        disposal ??= bounded(() => closeEvery([
                            ...(!attached ? [() => resource.controller?.dispose?.()] : []), () => resource.close?.(),
                        ]), { timeoutMs: policy.timeoutMs, stage: 'live-close' }).catch(async error => {
                            await recordCleanup(error); throw error;
                        });
                        return disposal;
                    };
                    try {
                        state = 'connecting';
                        connection = await bounded(async child => {
                            const created = await connect({ signal: child, attempt: reconnects });
                            offered = created;
                            // Disposal of a late connection may reject its disconnect promise.
                            void Promise.resolve(created?.closed).catch(() => {});
                            if (child.aborted) {
                                await dispose(created).catch(() => {});
                                throw abortError('live-connect');
                            }
                            return created;
                        }, { signal: lifetime, timeoutMs: policy.timeoutMs });
                        assert(connection?.controller && connection.closed?.then && typeof connection.close === 'function',
                            'Live factory must return controller, closed promise, and close().');
                        active = attachLiveController({ ...attachmentOptions, runner, flow, sessionId, controller: connection.controller,
                            processing, ownsController: true, ownsConnection: false });
                        attached = true;
                        state = 'listening';
                        await waitForDisconnect(connection.closed, lifetime);
                    } catch (error) {
                        if (!lifetime.aborted) await bounded(() => onError(publicError(error, 'live-connection')), { timeoutMs: 1000 }).catch(() => {});
                    } finally {
                        await bounded(() => active?.stop({ drain: !lifetime.aborted }), { timeoutMs: policy.timeoutMs }).catch(recordCleanup);
                        active = null;
                        await dispose(connection ?? offered).catch(() => {});
                    }
                    if (lifetime.aborted) break;
                    reconnects++; failures++;
                    if (failures >= policy.maxAttempts) { state = 'exhausted'; return; }
                    state = 'backoff';
                    await delay(Math.min(policy.maxDelayMs, policy.delayMs * 2 ** Math.min(failures - 1, 16)), lifetime).catch(() => {});
                }
                state = 'stopped';
            })();
        },
        status() { return { state, reconnects, attachment: active?.status() ?? null }; },
        close() {
            if (closing) return closing;
            closed = true; stop?.abort();
            closing = Promise.resolve(loop).then(() => { if (cleanupFailures.length) throw cleanupError([...cleanupFailures]); });
            return closing;
        },
    });
}
