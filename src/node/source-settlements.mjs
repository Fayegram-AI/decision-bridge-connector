import { assert, bounded, validateSignal } from '../util.mjs';

/** Claim heartbeats belong to yielded messages until their settlement or drain deadline. */
export function createSourceSettlements(stop) {
    const entries = new Set(); let closing; let closed = false; let idle;
    return {
        track(callbacks, release) {
            assert(!closing && !closed, 'Source settlement is closed.', 'SOURCE_CLOSED');
            const entry = { release }; entries.add(entry);
            let selected; let result;
            return Object.fromEntries(Object.entries(callbacks).map(([name, callback]) => [name, (...args) => {
                assert(!closed, 'Source settlement is closed.', 'SOURCE_CLOSED');
                assert(selected === undefined || selected === name, 'Source message already has another disposition.', 'CONFLICT');
                if (!selected) {
                    selected = name;
                    result = Promise.resolve().then(() => callback(...args)).finally(() => {
                        release(); entries.delete(entry); if (!entries.size) idle?.resolve();
                    });
                }
                return result;
            }]));
        },
        close({ signal, timeoutMs = 10000 } = {}) {
            validateSignal(signal);
            assert(Number.isSafeInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 2147483647, 'Invalid source close timeout.');
            if (closing) return closing;
            stop();
            closing = (async () => {
                try {
                    await bounded(() => {
                        if (!entries.size) return;
                        idle = Promise.withResolvers(); return idle.promise;
                    }, { signal, timeoutMs, stage: 'source-settlement-drain' });
                } finally {
                    closed = true;
                    for (const entry of entries) entry.release();
                    entries.clear();
                }
            })();
            return closing;
        },
    };
}
