import { assert, bounded, validateSignal } from '../util.mjs';

/** Keep yielded settlements alive without retaining message objects in a registry. */
export function createRedisSourceDrain({ owned, stop, destroy }) {
    const controller = new AbortController();
    let outstanding = 0; let active = 0; let closing; let finished = false; let drained;
    const idle = () => outstanding === 0 && active === 0;
    function notify() { if (idle()) drained?.resolve(); }
    return {
        signal: controller.signal,
        track(callbacks) {
            if (!owned) return callbacks;
            outstanding++;
            let settled = false;
            return Object.fromEntries(Object.entries(callbacks).map(([name, callback]) => [name, async (...args) => {
                assert(!finished, 'Redis source settlement is closed.', 'SOURCE_CLOSED');
                active++;
                try { return await callback(...args); }
                finally {
                    active--;
                    if (!settled) { settled = true; outstanding--; }
                    notify();
                }
            }]));
        },
        close({ signal, timeoutMs = 10000 } = {}) {
            validateSignal(signal);
            assert(Number.isSafeInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 2147483647, 'Invalid Redis source close timeout.');
            if (closing) return closing;
            stop();
            // Borrowed connections also support acknowledgement after source closure.
            if (!owned) { closing = Promise.resolve(); return closing; }
            closing = (async () => {
                let failure;
                try {
                    await bounded(() => {
                        if (idle()) return;
                        drained = Promise.withResolvers();
                        return drained.promise;
                    }, { signal, timeoutMs, stage: 'redis-source-close' });
                } catch (error) { failure = error; }
                finally {
                    finished = true; controller.abort();
                    try { destroy(); } catch (error) { failure ??= error; }
                }
                if (failure) throw failure;
            })();
            return closing;
        },
    };
}
