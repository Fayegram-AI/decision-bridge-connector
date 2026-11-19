import { assert, bounded, snapshot } from './util.mjs';
import { createRunner } from './engine/runner.mjs';
import { createCallbackConnector } from './operations.mjs';

/** Run only against a test destination: conformance deliberately invokes send(). */
export async function checkConnector(connector, { payload = { conformance: true }, timeoutMs = 1000 } = {}) {
    assert(connector?.revision && typeof connector.send === 'function', 'Connector requires revision and send().');
    const result = await bounded(signal => connector.send(snapshot(payload), {
        signal, taskId: 'conformance-task', eventId: 'conformance-task', deliveryId: 'conformance-delivery',
        attemptId: 'conformance-attempt', idempotencyKey: 'conformance-delivery', encoding: 'json',
    }), { timeoutMs });
    assert(['acknowledged', 'submitted', 'unknown'].includes(result?.status), 'Invalid connector receipt.');
    snapshot(result);
    return { passed: true, status: result.status, capabilities: snapshot(connector.capabilities ?? {}) };
}

/** The caller owns the temporary store, including closing and removing it. */
export async function checkStore(store) {
    const namespace = `conformance-${crypto.randomUUID()}`;
    const options = { store, namespace, validators: { json: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async () => ({ status: 'acknowledged' })) },
        flows: { test: { revision: '1', inputs: ['result'], validator: 'json', concurrency: 1,
            connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] } } };
    const first = createRunner(options); const second = createRunner(options);
    try {
        for (const operation of [() => store.claim(namespace, 'evaluation', { worker: 'invalid', placements: 'local' }),
            () => store.pause(namespace, 'destination', undefined, true)]) {
            let rejected = false;
            try { await operation(); } catch (error) { if (error?.code !== 'CONFIGURATION_ERROR') throw error; rejected = true; }
            assert(rejected, 'Store accepted malformed placement or pause authority.');
        }
        const input = { flow: 'test', kind: 'result', result: { value: 1 }, idempotencyKey: 'once' };
        const [a, b] = await Promise.all([first.submit(input), second.submit(input)]);
        assert(a.taskId === b.taskId, 'Store did not deduplicate concurrent admission.');
        const result = await second.execute(a.taskId);
        assert(result.status === 'completed' && (await first.list()).items.length === 1, 'Store visibility differs between workers.');
        const planned = await first.submit({ ...input, idempotencyKey: 'planned' });
        await store.pause(namespace, 'destination', 'undefined', true);
        const planning = await store.claim(namespace, 'evaluation', { worker: 'conformance', taskId: planned.taskId });
        assert(planning, 'A destination pause blocked destinationless evaluation.');
        await store.pause(namespace, 'destination', 'undefined', false);
        // A saved-result evaluation and its readied output exercise cross-kind slots
        // within one task, including stores configured for a single active task.
        await store.commit(planning.job, { task: { plannedAt: Date.now() }, job: { state: 'pending', token: null, leaseUntil: null },
            jobs: [{ id: `${planned.taskId}:delivery:out`, state: 'pending', payload: 1 }] });
        const claims = await Promise.all([store.claim(namespace, 'delivery', { worker: 'one', taskId: planned.taskId }),
            store.claim(namespace, 'evaluation', { worker: 'two', taskId: planned.taskId })]);
        assert(claims.filter(Boolean).length === 1, 'Store did not enforce overall flow concurrency.');
        await first.cancel(planned.taskId);
        const checkpointed = await first.submit({ ...input, idempotencyKey: 'acceptance' });
        const evaluation = await store.claim(namespace, 'evaluation', { worker: 'checkpoint', taskId: checkpointed.taskId });
        await store.commit(evaluation.job, { task: { plannedAt: Date.now() }, job: { state: 'acknowledged' },
            jobs: [{ id: `${checkpointed.taskId}:delivery:out`, state: 'pending', payload: 1 }] });
        const delivery = await store.claim(namespace, 'delivery', { worker: 'checkpoint', taskId: checkpointed.taskId });
        const attempt = { id: 'checkpoint-attempt', status: 'acknowledged', startedAt: Date.now() };
        await store.commit(delivery.job, { job: { stage: 'invoked', attempts: [attempt] } });
        await store.commit(delivery.job, { job: { acceptance: { attemptId: attempt.id, receipt: { status: 'acknowledged' } } } });
        await store.commit(delivery.job, { job: { leaseUntil: 0 } }); await store.recover(namespace);
        const recovered = await store.getJob(namespace, `${checkpointed.taskId}:delivery:out`);
        assert(recovered.state === 'acknowledged' && recovered.receipt.receipt.responseUnavailable === true,
            'Store did not preserve checkpointed acceptance during recovery.');
        const unprepared = await first.submit({ ...input, idempotencyKey: 'unprepared' });
        const preparing = await store.claim(namespace, 'evaluation', { worker: 'unprepared', taskId: unprepared.taskId });
        await store.commit(preparing.job, { task: { plannedAt: Date.now() }, job: { state: 'acknowledged' },
            jobs: [{ id: `${unprepared.taskId}:delivery:out`, state: 'failed' }] });
        let retryRejected = false;
        try { await first.retryDelivery(`${unprepared.taskId}:delivery:out`); }
        catch (error) { if (error?.code !== 'CONFLICT') throw error; retryRejected = true; }
        assert(retryRejected, 'Store allowed retry without a prepared payload.');
        await store.aggregate(namespace, 'rule-one', () => ({ ruleId: 'one', state: 'pending' }), 1);
        await store.aggregate(namespace, 'rule-two', () => ({ ruleId: 'two', state: 'pending' }), 1);
        let rejected = false;
        try { await store.aggregate(namespace, 'rule-one-more', () => ({ ruleId: 'one', state: 'pending' }), 1); }
        catch (error) { if (error?.code !== 'RESOURCE_LIMIT') throw error; rejected = true; }
        assert(rejected, 'Store did not enforce the named aggregation rule quota.');
        return { passed: true, capabilities: store.capabilities };
    } finally { await first.close(); await second.close(); }
}
