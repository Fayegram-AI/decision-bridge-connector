import { assert, freeze } from '../util.mjs';

/** Bounded aggregate metrics; payloads and credentials are never exported. */
export function createTelemetryObserver({ exportEvent, maxSeries = 256 } = {}) {
    const series = new Map();
    async function onEvent(event) {
        const safe = { type: event.type, flow: event.flow ?? 'unknown', status: event.status ?? 'observed',
            ...(event.taskId ? { taskId: event.taskId } : {}), ...(event.deliveryId ? { deliveryId: event.deliveryId } : {}),
            ...(event.durationMs !== undefined ? { durationMs: event.durationMs } : {}) };
        const key = JSON.stringify([safe.type, safe.flow, safe.status]);
        if (!series.has(key) && series.size < maxSeries) series.set(key, { type: safe.type, flow: safe.flow, status: safe.status, count: 0, totalDurationMs: 0 });
        const entry = series.get(key); if (entry) { entry.count++; entry.totalDurationMs += safe.durationMs ?? 0; }
        await exportEvent?.(freeze(safe));
    }
    assert(Number.isSafeInteger(maxSeries) && maxSeries > 0, 'Invalid telemetry cardinality limit.');
    return Object.freeze({ onEvent, snapshot: () => [...series.values()].map(value => freeze({ ...value })) });
}
