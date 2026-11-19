import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];
const configuration = (store, send, connection = {}) => ({ store, namespace: `prepared-${crypto.randomUUID()}`,
    validators: { v: { revision: '1', validate: value => value } }, connectors: { sink: createCallbackConnector(send) },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
        { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', ...connection },
    ] } } });

for (const [name, factory] of stores) {
    test(`${name}: planning failures cannot bypass validation through manual retry`, { timeout: 30000 }, async () => {
        const store = await factory(); let calls = 0;
        try {
            for (const mode of ['schema', 'transform', 'text']) {
                const connection = mode === 'schema' ? { outputSchema: false } : mode === 'transform'
                    ? { transforms: [{ type: 'coerce', to: 'number' }] } : { encoding: 'text' };
                const options = configuration(store, async () => { calls++; return { status: 'acknowledged' }; }, connection);
                options.validateOutput = () => false;
                const runner = createRunner(options);
                try {
                    const task = await runner.run({ flow: 'a', kind: 'result', result: {} });
                    const delivery = task.deliveries[0]; assert.equal(delivery.status, 'failed');
                    for (const resolved of [false, true]) {
                        if (resolved) await runner.resolveDelivery(delivery.id, { status: 'failed', receipt: 'Confirmed no acceptance.' });
                        await assert.rejects(runner.retryDelivery(delivery.id), { code: 'CONFLICT' });
                        const current = await runner.execute(task.taskId);
                        assert.equal(current.deliveries[0].status, 'failed'); assert.equal(current.deliveries[0].attempts.length, 0);
                    }
                    assert.equal(calls, 0);
                } finally { await runner.close(); }
            }
        } finally { await store.close(); }
    });

    test(`${name}: saved unprepared deliveries are fenced without losing earlier uncertainty`, { timeout: 30000 }, async () => {
        const store = await factory();
        try {
            for (const uncertain of [false, true]) {
                let calls = 0;
                const options = configuration({ ...store, async commit(claim, update) {
                    // Model a previously saved retry of an output-planning failure.
                    if (claim.kind === 'evaluation' && update.jobs?.length) update = { ...update, jobs: update.jobs.map(job => ({ ...job,
                        state: 'pending', attempts: uncertain ? [{ id: 'earlier', status: 'unknown', startedAt: 0,
                            error: { code: 'OLD_UNKNOWN', message: 'Earlier receipt lost.' } }] : [] })) };
                    return store.commit(claim, update);
                } }, async () => { calls++; return { status: 'acknowledged' }; }, { outputSchema: false });
                options.validateOutput = () => false;
                const runner = createRunner(options);
                try {
                    const task = await runner.run({ flow: 'a', kind: 'result', result: 1 });
                    const delivery = task.deliveries[0];
                    assert.equal(delivery.status, uncertain ? 'unknown' : 'failed');
                    assert.equal(delivery.error.code, 'OUTPUT_NOT_PREPARED'); assert.equal(delivery.error.ambiguous, uncertain);
                    assert.equal(delivery.attempts.length, uncertain ? 1 : 0); assert.equal(calls, 0);
                    await runner.tick({ taskId: task.taskId }); assert.equal(calls, 0);
                    if (uncertain) {
                        const resolved = await runner.resolveDelivery(delivery.id, { status: 'acknowledged', receipt: 'Recipient evidence verified.' });
                        assert.equal(resolved.deliveries[0].error, null);
                        assert.equal(resolved.deliveries[0].attempts[0].error.code, 'OLD_UNKNOWN');
                    }
                } finally { await runner.close(); }
            }
        } finally { await store.close(); }
    });

    test(`${name}: prepared falsy payloads retry and acknowledged outcomes clear only current errors`, { timeout: 30000 }, async () => {
        const store = await factory();
        try {
            for (const value of [null, false, 0, '']) {
                const received = [];
                const runner = createRunner(configuration(store, async payload => {
                    received.push(payload);
                    if (received.length === 1) throw new ConnectorError('BUSY', 'Rejected this attempt.', { ambiguous: false, retryable: false });
                    return { status: 'acknowledged' };
                }));
                try {
                    const first = await runner.run({ flow: 'a', kind: 'result', result: value });
                    assert.equal(first.deliveries[0].error.code, 'BUSY');
                    await runner.retryDelivery(first.deliveries[0].id);
                    const current = await runner.execute(first.taskId);
                    assert.equal(current.deliveries[0].status, 'acknowledged'); assert.equal(current.deliveries[0].error, null);
                    assert.equal(current.deliveries[0].attempts[0].error.code, 'BUSY');
                    assert.deepEqual(received, [value, value]);
                } finally { await runner.close(); }
            }
        } finally { await store.close(); }
    });
}
