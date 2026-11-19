import { abortError, bounded } from '../util.mjs';
import { closeEvery } from './cleanup.mjs';

/** Configured resources share one close promise, including host and startup cleanup. */
export function createResourceOwner() {
    const owned = new Set(); const closing = new WeakMap();
    function close(resource, options) {
        if (typeof resource?.close !== 'function') return Promise.resolve();
        if (!closing.has(resource)) closing.set(resource, Promise.resolve().then(() => resource.close(options)));
        return closing.get(resource);
    }
    const closeBounded = resource => bounded(() => close(resource), { timeoutMs: 10000, stage: 'resource-close' });
    return Object.freeze({
        async create(factory, { signal } = {}) {
            return bounded(async signal => {
                const resource = await factory(signal);
                // A timed-out factory can still resolve after closeAll took its snapshot.
                if (signal.aborted) {
                    await closeBounded(resource).catch(() => {});
                    throw abortError('startup');
                }
                if (typeof resource?.close === 'function') owned.add(resource);
                return resource;
            }, { signal, timeoutMs: 60000, stage: 'startup' });
        },
        close,
        async closeAll() { await closeEvery([...owned].reverse().map(resource => () => closeBounded(resource))); },
    });
}
