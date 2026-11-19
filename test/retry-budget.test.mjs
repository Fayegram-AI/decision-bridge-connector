import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

for (const [name, factory] of stores) test(`${name}: recipient backoff cannot defer delivery stopping checks beyond their budget`, { timeout: 60000 }, async () => {
    const store = await factory();
    try {
        for (const age of [false, true]) for (const ambiguous of [false, true]) {
            let sends = 0;
            const runner = createRunner({ store, namespace: `retry-budget-${crypto.randomUUID()}`,
                validators: { v: { revision: '1', validate: value => value } },
                connectors: { sink: createCallbackConnector(async () => {
                    sends++; throw new ConnectorError('BACKPRESSURE', 'Retry tomorrow.', { ambiguous, retryable: true, retryAfterMs: 86400000 });
                }, { idempotent: true }) },
                flows: { a: { revision: '1', inputs: ['result'], validator: 'v', deliveryTtlMs: age ? 7200000 : 3600000,
                    ...(age ? { maxAgeMs: 3600000, stale: 'hold' } : {}), connections: [
                        { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', retry: { ambiguous: 'retry', maxAttempts: 2 } },
                    ] } } });
            try {
                const task = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
                await bounded(async () => {
                    while ((await runner.get(task.taskId)).deliveries[0].status !== 'retry_pending') { await runner.tick(); await sleep(5); }
                }, { timeoutMs: 5000 });
                const record = await store.get(task.taskId, runner.namespace); const job = record.jobs.find(item => item.kind === 'delivery');
                const expected = age ? Date.parse(record.task.evidenceAt) + 3600001 : job.expiresAt;
                assert.equal(job.runAt, expected); assert.equal(sends, 1);
                await runner.tick({ taskId: task.taskId }); assert.equal(sends, 1, 'The shortened check must not replay a recipient early.');
                // Make the stopping boundary due without relying on wall-clock sleeps.
                await runner.retryDelivery(job.id);
                const claim = await store.claim(runner.namespace, 'delivery', { worker: 'setup', taskId: task.taskId }); assert(claim);
                await store.commit(claim.job, { task: age ? { evidenceAt: '2020-01-01T00:00:00Z' } : undefined,
                    job: { state: 'pending', runAt: 0, token: null, leaseUntil: null, ...(!age ? { expiresAt: 0 } : {}) } });
                const stopped = await runner.execute(task.taskId);
                assert.equal(stopped.deliveries[0].status, ambiguous ? 'unknown' : age ? 'held' : 'expired');
                assert.equal(sends, 1);
            } finally { await runner.close(); }
        }
    } finally { await store.close(); }
});
