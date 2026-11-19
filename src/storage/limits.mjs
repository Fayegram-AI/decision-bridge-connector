import { assert } from '../util.mjs';
import { LIMITS } from '../engine/flows.mjs';

/** Validate resource bounds before stores acquire database handles or connections. */
export function storeLimits(options = {}) {
    const limits = { ...LIMITS, ...options };
    for (const [key, value] of Object.entries(limits)) assert(Object.hasOwn(LIMITS, key) && Number.isSafeInteger(value) && value > 0, `Invalid store limit: ${key}`);
    assert(limits.maxBytes <= 64 * 1024 * 1024 && limits.optionalConcurrency <= limits.deliveryConcurrency, 'Invalid resource limits.');
    assert(limits.leaseMs <= 2147483647, 'Store lease exceeds the timer limit.');
    assert(limits.pageSize <= 100, 'Default list pageSize must be 1–100.');
    assert(limits.maxTransfers <= 1000 && limits.transferIdleMs <= 2147483647, 'Invalid transfer limits.');
    return Object.freeze(limits);
}
