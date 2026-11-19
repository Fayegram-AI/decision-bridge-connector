import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', createMemoryStore], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

for (const [name, factory] of stores) {
    test(`${name}: annotating a retained diagnostic cannot prevent terminal settlement`, async () => {
        const store = await factory(); const notices = []; const message = 'x'.repeat(65440);
        const runner = createRunner({ store, namespace: `diagnostic-boundary-${crypto.randomUUID()}`, validators: { v: { revision: '1', validate: value => value } }, connectors: {
            sink: createCallbackConnector(value => { notices.push(value); return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
            { id: 'notice', connector: 'sink', when: 'failure', payload: 'failure' },
        ] } } });
        try {
            const { taskId } = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
            const claim = await store.claim(runner.namespace, 'evaluation', { taskId, worker: 'setup' });
            await store.commit(claim.job, { task: { plannedAt: Date.now() }, job: { state: 'acknowledged' }, jobs: [
                { id: `${taskId}:delivery:out`, state: 'failed', error: { code: 'REJECTED', stage: 'delivery', message } },
                { id: `${taskId}:delivery:notice`, state: 'skipped' },
            ] });
            const report = await runner.execute(taskId, { waitFor: 'settled', waitMs: name === 'postgres' ? 15000 : 3000 });
            assert.equal(report.errorCount, 1); assert.equal(report.settled, true); assert.equal(notices.length, 1);
            assert.equal(notices[0].errors[0].code, 'REJECTED'); assert.equal(notices[0].errors[0].detailsUnavailable, true);
            assert.equal((await store.get(taskId, runner.namespace)).jobs.find(job => job.plan?.id === 'out').error.message, message);
        } finally { await runner.close(); await store.close(); }
    });
}
