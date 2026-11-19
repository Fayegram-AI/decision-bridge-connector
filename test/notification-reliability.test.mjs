import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', createMemoryStore], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];
const validator = { revision: '1', validate: value => value };
const output = (id, connector, extra = {}) => ({ id, connector, when: 'always', payload: 'raw-result', ...extra });

for (const [name, factory] of stores) {
    test(`${name}: delayed notifications contain full errors and satisfy output schemas`, async () => {
        const store = await factory(); const notices = [];
        const runner = createRunner({ store, namespace: `notification-${crypto.randomUUID()}`, validators: { v: validator },
            validateOutput: (_schema, value) => assert(value.errors.every(error => typeof error.message === 'string' && !error.$data)),
            connectors: { reject: createCallbackConnector(() => { throw new ConnectorError('REJECTED', 'Recipient refused.', { ambiguous: false, retryable: false }); }),
                notice: createCallbackConnector(value => { notices.push(value); return { status: 'acknowledged' }; }) },
            flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [output('out', 'reject'),
                output('notice', 'notice', { when: 'failure', payload: 'failure', outputSchema: {} })] } } });
        try {
            const report = await runner.run({ flow: 'a', kind: 'result', result: 1 }, { waitFor: 'settled', waitMs: name === 'postgres' ? 15000 : 3000 });
            assert.equal(report.settled, true); assert.equal(report.errorCount, 1); assert.equal(notices.length, 1);
            assert.equal(notices[0].errors[0].message, 'Recipient refused.');
        } finally { await runner.close(); await store.close(); }
    });

    test(`${name}: output planning diagnostics are retained once`, async () => {
        const store = await factory(); const notices = [];
        const runner = createRunner({ store, namespace: `notification-${crypto.randomUUID()}`, validators: { v: validator }, connectors: {
            sink: createCallbackConnector(value => { notices.push(value); return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            output('bad', 'sink', { projection: { $path: 'missing' } }), output('notice', 'sink', { when: 'failure', payload: 'failure' }),
        ] } } });
        try {
            const report = await runner.run({ flow: 'a', kind: 'result', result: {} }, { waitFor: 'settled', waitMs: name === 'postgres' ? 15000 : 3000 });
            assert.equal(report.errorCount, 1); assert.equal(notices.length, 1); assert.equal(notices[0].errors.length, 1);
        } finally { await runner.close(); await store.close(); }
    });

    test(`${name}: stale evidence activates a fresh diagnostic notification`, async () => {
        const store = await factory(); const notices = [];
        const runner = createRunner({ store, namespace: `notification-${crypto.randomUUID()}`, validators: { v: validator }, connectors: {
            sink: createCallbackConnector(value => { notices.push(value); return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', maxAgeMs: 1, stale: 'expire', connections: [
            output('out', 'sink'), output('notice', 'sink', { when: 'failure', payload: 'failure' }),
        ] } } });
        try {
            const report = await runner.run({ flow: 'a', kind: 'result', result: 1, evidenceAt: new Date(Date.now() - 1000).toISOString() },
                { waitFor: 'settled', waitMs: name === 'postgres' ? 15000 : 3000 });
            assert.equal(report.settled, true); assert.equal(report.deliveryCounts.expired, 1); assert.equal(notices.length, 1);
            const record = await store.get(report.taskId, runner.namespace);
            const notice = record.jobs.find(job => job.plan?.id === 'notice');
            assert.equal(notice.expiresAt - notice.eligibleAt, record.task.plan.deliveryTtlMs);
        } finally { await runner.close(); await store.close(); }
    });

    test(`${name}: delivery expiry gives notifications a new deadline and retries keep it`, async () => {
        const store = await factory(); const deadlines = []; let taskId;
        const runner = createRunner({ store, namespace: `notification-${crypto.randomUUID()}`, validators: { v: validator }, connectors: {
            sink: createCallbackConnector(() => { assert.fail('Expired business output must not dispatch.'); }),
            notice: createCallbackConnector(async (_value, context) => {
                const saved = await store.getJob(runner.namespace, context.deliveryId); deadlines.push(saved.expiresAt);
                if (deadlines.length === 1) throw new ConnectorError('RETRY', 'Retry notification.', { ambiguous: false, retryable: true });
                return { status: 'acknowledged' };
            }) }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                output('out', 'sink'), output('notice', 'notice', { when: 'failure', payload: 'failure',
                    retry: { maxAttempts: 2, delayMs: 1, maxDelayMs: 1 } }),
            ] } } });
        try {
            ({ taskId } = await runner.submit({ flow: 'a', kind: 'result', result: 1 }));
            await runner.execute(taskId, { waitFor: 'evaluated' });
            // Saved results already satisfy evaluated; prepare jobs explicitly without dispatch.
            const evaluation = await store.claim(runner.namespace, 'evaluation', { taskId, worker: 'setup' });
            await store.commit(evaluation.job, { task: { plannedAt: Date.now() }, job: { state: 'acknowledged' }, jobs: [
                { id: `${taskId}:delivery:out`, state: 'pending', payload: 1, expiresAt: Date.now() - 1 },
                { id: `${taskId}:delivery:notice`, state: 'skipped' },
            ] });
            const report = await runner.execute(taskId, { waitFor: 'settled', waitMs: name === 'postgres' ? 15000 : 3000 });
            assert.equal(report.settled, true); assert.equal(deadlines.length, 2); assert.equal(deadlines[0], deadlines[1]);
            assert.equal(report.deliveryCounts.expired, 1); assert.equal(report.deliveryCounts.acknowledged, 1);
        } finally { await runner.close(); await store.close(); }
    });

    test(`${name}: concurrent equal recipient failures retain separate diagnostics`, async () => {
        const store = await factory(); const arrived = Promise.withResolvers(); let calls = 0;
        const runner = createRunner({ store, namespace: `notification-${crypto.randomUUID()}`, validators: { v: validator }, connectors: {
            sink: createCallbackConnector(async () => {
                if (++calls === 2) arrived.resolve(); await arrived.promise;
                throw new ConnectorError('REJECTED', 'Identical recipient error.', { ambiguous: false, retryable: false });
            }), notice: createCallbackConnector(() => ({ status: 'acknowledged' })) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [output('one', 'sink'), output('two', 'sink'),
            output('notice', 'notice', { when: 'failure', payload: 'failure' })] } } });
        try {
            const report = await runner.run({ flow: 'a', kind: 'result', result: 1 }, { waitFor: 'settled', waitMs: name === 'postgres' ? 15000 : 3000 });
            assert.equal(report.errorCount, 2); assert.equal(report.settled, true);
            const record = await store.get(report.taskId, runner.namespace);
            assert.equal(new Set(record.task.errors.map(error => error.originJobId)).size, 2);
        } finally { arrived.resolve(); await runner.close(); await store.close(); }
    });

    for (const required of [true, false]) test(`${name}: ${required ? 'required' : 'optional'} chain errors are retained without recursive alerts`, async () => {
        const store = await factory(); const notices = [];
        const runner = createRunner({ store, namespace: `notification-${crypto.randomUUID()}`, validators: { v: validator }, evaluators: { e: { revision: '1', execute: value => value } }, connectors: {
            sink: createCallbackConnector(() => ({ status: 'acknowledged' })),
            notice: createCallbackConnector(value => { notices.push(value); return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            output('out', 'sink', { required, nextFlow: 'child', chainRetry: { maxAttempts: 1, delayMs: 1, maxDelayMs: 1 } }),
            output('notice', 'notice', { when: 'failure', payload: 'failure' }),
        ] }, child: { revision: '1', inputs: ['event'], template: { $path: '$' }, evaluator: 'e' } } });
        try {
            const report = await runner.run({ flow: 'a', kind: 'result', result: 1 }, { waitFor: 'settled', waitMs: name === 'postgres' ? 15000 : 3000 });
            assert.equal(report.status, required ? 'partial' : 'completed'); assert.equal(report.errorCount, 1);
            assert.equal(notices.length, 1); assert.equal(notices[0].errors[0].code, 'RESPONSE_UNAVAILABLE');
            await runner.recover(); await runner.tick(); assert.equal(notices.length, 1);
        } finally { await runner.close(); await store.close(); }
    });
}
