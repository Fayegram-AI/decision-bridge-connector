import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

const stores = [['memory', options => createMemoryStore(options)], ['sqlite', options => createSQLiteStore(':memory:', options)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', options => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...options })]] : [])];

for (const [name, factory] of stores) test(`${name}: prior delivery uncertainty survives expiry, cancellation and definite retry failure`, { timeout: 30000 }, async () => {
    const store = await factory({ retentionMs: 1 });
    try {
        for (const mode of ['ttl', 'freshness', 'cancel', 'rejection', 'reconciled']) {
            let sends = 0;
            const runner = createRunner({ store, namespace: `ambiguity-${crypto.randomUUID()}`,
                validators: { v: { revision: '1', validate: value => value } },
                connectors: { sink: createCallbackConnector(async () => {
                    sends++;
                    if (sends === 1) throw new Error('Receipt was lost.');
                    throw new ConnectorError('REJECTED', 'This attempt was rejected.', { ambiguous: false, retryable: false });
                }, { idempotent: true }) },
                flows: { a: { revision: '1', inputs: ['result'], validator: 'v', maxAgeMs: 60000, stale: 'expire', connections: [
                    { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', retry: { ambiguous: 'retry', delayMs: 60000, maxDelayMs: 60000 } },
                ] } } });
            try {
                const task = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
                await bounded(async () => {
                    while ((await runner.get(task.taskId)).deliveries[0].status !== 'retry_pending') { await runner.tick(); await sleep(5); }
                }, { timeoutMs: 5000 });
                const deliveryId = `${task.taskId}:delivery:out`;
                if (mode === 'cancel') await runner.cancel(task.taskId);
                else {
                    if (mode === 'reconciled') {
                        await runner.cancel(task.taskId);
                        await runner.resolveDelivery(deliveryId, { status: 'failed', receipt: { evidence: 'No attempt was accepted.' } });
                        await runner.retryDelivery(deliveryId);
                    }
                    const claimed = await store.claim(runner.namespace, 'delivery', { worker: 'setup', taskId: task.taskId });
                    // The ordinary delayed retry is not due yet; operator retry makes it claimable.
                    if (!claimed) await runner.retryDelivery(deliveryId);
                    const claim = claimed ?? await store.claim(runner.namespace, 'delivery', { worker: 'setup', taskId: task.taskId });
                    assert(claim);
                    await store.commit(claim.job, { task: mode === 'freshness' ? { evidenceAt: '2020-01-01T00:00:00Z' } : undefined,
                        job: { state: 'pending', runAt: Date.now(), token: null, leaseUntil: null, ...(mode === 'ttl' ? { expiresAt: 0 } : {}) } });
                    await runner.execute(task.taskId);
                }
                const result = await runner.get(task.taskId);
                assert.equal(result.status, mode === 'reconciled' ? 'partial' : 'unknown', mode);
                assert.equal(result.deliveries[0].status, mode === 'reconciled' ? 'failed' : 'unknown', mode);
                assert.equal(sends, ['rejection', 'reconciled'].includes(mode) ? 2 : 1);
                if (mode === 'rejection') {
                    assert.equal(result.deliveries[0].attempts[1].status, 'failed');
                    assert.equal(result.deliveries[0].error.code, 'ACCEPTANCE_UNCONFIRMED');
                }
                if (mode !== 'reconciled') {
                    await sleep(5); await runner.maintain(); assert(await runner.get(task.taskId));
                    await runner.resolveDelivery(deliveryId, { status: 'acknowledged', receipt: { evidence: 'Recipient receipt verified.' } });
                    assert.equal((await runner.get(task.taskId)).status, 'completed');
                }
            } finally { await runner.close(); }
        }
    } finally { await store.close(); }
});
