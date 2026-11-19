import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createHttpServer } from '../src/node/http-service.mjs';
import { createMcpServer } from '../src/integrations/mcp-service.mjs';

async function fixture() {
    const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async () => ({ status: 'unknown' })) }, flows: {
            a: { revision: '1', inputs: ['result'], validator: 'v' },
            b: { revision: '1', inputs: ['result'], validator: 'v', connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] },
        } });
    const allowed = await runner.run({ flow: 'a', kind: 'result', result: 1 });
    const other = await runner.run({ flow: 'b', kind: 'result', result: 2 });
    return { runner, allowed, other, args: { taskId: allowed.taskId, deliveryId: other.deliveries[0].id,
        resolution: { status: 'acknowledged' }, completion: { status: 'completed' } } };
}

test('HTTP administration authorizes the actual delivery parent despite a conflicting taskId', async () => {
    const { runner, other, args } = await fixture();
    const server = createHttpServer({ runner, flow: 'a', reply: 'complete', allowAdmin: true });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    try {
        for (const action of ['resolveDelivery', 'retryDelivery', 'recordCompletion']) {
            const response = await fetch(`http://127.0.0.1:${server.address().port}/operations/${action}`, {
                method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(args),
            });
            assert.equal(response.status, 404); assert.equal((await response.json()).error.code, 'NOT_FOUND');
        }
        assert.equal((await runner.get(other.taskId)).deliveries[0].status, 'unknown');
    } finally { await server.drain(); await runner.close(); }
});

test('MCP administration authorizes the actual delivery parent despite a conflicting taskId', async () => {
    const [{ Client }, { InMemoryTransport }] = await Promise.all([
        import('@modelcontextprotocol/sdk/client/index.js'), import('@modelcontextprotocol/sdk/inMemory.js'),
    ]);
    const { runner, other, args } = await fixture();
    const server = await createMcpServer({ runner, flows: ['a'], allowAdmin: true });
    const client = new Client({ name: 'boundary-test', version: '1' }); const [a, b] = InMemoryTransport.createLinkedPair();
    await server.connect(b); await client.connect(a);
    try {
        for (const action of ['resolveDelivery', 'retryDelivery', 'recordCompletion']) {
            const response = await client.callTool({ name: `connectors_${action}`, arguments: args });
            assert.equal(response.isError, true); assert.equal(response.structuredContent.error.code, 'NOT_FOUND');
        }
        assert.equal((await runner.get(other.taskId)).deliveries[0].status, 'unknown');
    } finally { await client.close(); await server.close(); await runner.close(); }
});

test('cancelled required chains report partial completion after accepted parent delivery', async () => {
    const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
        evaluators: { e: { revision: '1', execute: async value => value } },
        connectors: { sink: createCallbackConnector(async () => ({ status: 'acknowledged', receipt: { response: 1 } })) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', nextFlow: 'child', chainRetry: { maxAttempts: 3, delayMs: 10, maxDelayMs: 50 } },
        ] }, child: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
    try {
        const task = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
        const planning = await runner.store.claim('default', 'evaluation', { worker: 'test', taskId: task.taskId });
        await runner.store.commit(planning.job, { task: { plannedAt: Date.now() }, job: { state: 'acknowledged' },
            jobs: [{ id: task.deliveries[0].id, state: 'pending', payload: 1 }] });
        const delivery = await runner.store.claim('default', 'delivery', { worker: 'test', taskId: task.taskId });
        await runner.store.commit(delivery.job, { job: { state: 'acknowledged', receipt: { status: 'acknowledged', receipt: { response: 1 } } } });
        const cancelled = await runner.cancel(task.taskId);
        assert.equal(cancelled.chains[0].status, 'cancelled'); assert.equal(cancelled.settled, true);
        assert.equal(cancelled.status, 'partial'); assert.equal(cancelled.requiredSettled, true);
    } finally { await runner.close(); }
});
