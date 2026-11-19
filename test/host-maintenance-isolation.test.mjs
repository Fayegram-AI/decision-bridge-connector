import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createHost } from '../src/node/host.mjs';
import { bounded, ConnectorError } from '../src/util.mjs';

test('worker and rule failures cannot starve independent aggregation or retention maintenance', async () => {
    let recovered = 0; let healthy = 0; let maintained = 0;
    const runner = { ready: Promise.resolve(), async tick() { throw new Error('Worker unavailable'); },
        async recover() { if (++recovered > 1) throw null; }, async maintain() { maintained++; }, async close() {}, async inspect() { return {}; } };
    const host = await createHost({ runner, pollMs: 5, maintenanceMs: 1000, aggregators: {
        blocked: { async flush() { throw new ConnectorError('RESOURCE_LIMIT', 'Blocked emission'); } },
        healthy: { async flush() { healthy++; } },
    } });
    try {
        await bounded(async () => { while (!healthy || !maintained) await sleep(5); }, { timeoutMs: 1000 });
        const health = await host.health(); assert.equal(health.ready, false);
        assert(health.errors.some(error => error.code === 'RESOURCE_LIMIT'));
        assert(health.errors.some(error => error.code === 'UNEXPECTED_ERROR'));
        assert.equal(healthy, 1); assert.equal(maintained, 1);
    } finally { await host.close(); }
});

test('host readiness recovers per activity while retaining its diagnostic history', async () => {
    let ticks = 0; let recovered = 0; let flushed = 0; let maintained = 0;
    const runner = { ready: Promise.resolve(), async tick() { if (++ticks === 1) throw new Error('Transient worker failure'); },
        async recover() { if (++recovered === 2) throw null; }, async maintain() { if (++maintained === 1) throw new Error('Transient retention failure'); },
        async close() {}, async inspect() { return {}; } };
    const host = await createHost({ runner, pollMs: 1, maintenanceMs: 1, aggregators: { one: {
        async flush() { if (++flushed === 1) throw new Error('Transient rule failure'); },
    } } });
    try {
        await bounded(async () => { while (ticks < 2 || maintained < 2 || !(await host.health()).ready) await sleep(5); }, { timeoutMs: 1000 });
        const health = await host.health(); assert.equal(health.ready, true); assert.deepEqual(health.failures, {});
        assert.equal(health.errors.length, 4);
    } finally { await host.close(); }
});

test('rejected messages do not poison readiness while a stopped source remains unready', async () => {
    for (const stopped of [false, true]) {
        let admitted = 0; let rejected = 0;
        const runner = { ready: Promise.resolve(), async submit() { if (++admitted === 1) throw new Error('Rejected message'); return { taskId: 'accepted' }; },
            async close() {}, async inspect() { return {}; } };
        const host = await createHost({ runner, role: 'ingress', sources: { input: { async *open() {
            if (stopped) throw new Error('Source stopped');
            yield { input: { kind: 'event', event: 1 }, reject() { rejected++; } };
            yield { input: { kind: 'event', event: 2 } };
        } } } });
        try {
            await bounded(async () => { while (!(await host.health()).errors.length || !stopped && admitted < 2) await sleep(5); }, { timeoutMs: 1000 });
            const health = await host.health(); assert.equal(health.ready, !stopped);
            assert.equal(Object.hasOwn(health.failures, 'source:input'), stopped);
            if (!stopped) { assert.equal(admitted, 2); assert.equal(rejected, 1); }
        } finally { await host.close(); }
    }
});
