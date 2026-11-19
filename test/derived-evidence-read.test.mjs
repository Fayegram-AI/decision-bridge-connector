import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createCallbackConnector } from '../src/index.mjs';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createExecutionStore } from '../src/storage/store.mjs';

test('derived executions use intact task evidence independently of unavailable delivery bodies', async () => {
    const repository = createMemoryRepository(); const store = createExecutionStore(repository); let evaluations = 0;
    const runner = createRunner({ store, evaluators: { e: { revision: '1', execute() { evaluations++; return { ok: true }; }, validateSaved: value => value } },
        connectors: { sink: createCallbackConnector(async () => ({ status: 'acknowledged' })) },
        flows: { original: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', projection: { $literal: 'delivery-only-data' } },
        ] }, recovered: { revision: '1', inputs: ['request', 'result'], evaluator: 'e' } } });
    try {
        const original = await runner.run({ flow: 'original', kind: 'request', request: { original: true } });
        const deliveryId = `${original.taskId}:delivery:out`;
        // Model loss of an unrelated stored job body without damaging task evidence.
        await repository.transaction(async tx => {
            const delivery = await tx.get('jobs', deliveryId);
            for (const chunk of await tx.find('chunks', { blobId: delivery.payload.$data.id })) await tx.remove('chunks', chunk.id);
        });
        await assert.rejects(store.getJob(runner.namespace, deliveryId), { code: 'DATA_UNAVAILABLE' });
        for (const action of ['reprocess', 'redeliver', 'reevaluate']) {
            const derived = await runner[action](original.taskId, { flow: 'recovered' });
            assert.equal((await runner.execute(derived.taskId)).status, 'completed');
        }
        assert.equal(evaluations, 2);
        assert.equal((await runner.list()).items.length, 4);
        await assert.rejects(store.getJob(runner.namespace, deliveryId), { code: 'DATA_UNAVAILABLE' });
    } finally { await runner.close(); await store.close(); }
});
