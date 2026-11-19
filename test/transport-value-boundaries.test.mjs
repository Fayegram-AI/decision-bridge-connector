import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { createRunner, createCallbackConnector } from '../src/index.mjs';
import { createHttpServer } from '../src/node/http-service.mjs';
import { createHttpConnector } from '../src/node/connectors.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createMcpServer, createMcpHttpHandler } from '../src/integrations/mcp-service.mjs';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

function nested(depth = 100) { let value = 1; while (depth--) value = { value }; return value; }
const configuration = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
async function listen(server) { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server; }
async function close(server) { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }

test('HTTP chaining preserves boundary JSON through storage and treats over-depth JSON as unavailable', async () => {
    let response;
    const store = createSQLiteStore(':memory:');
    const sink = createHttpConnector({ url: 'http://127.0.0.1/', captureResponse: true,
        fetchImpl: async () => new Response(response, { headers: { 'content-type': 'application/json' } }) });
    const runner = createRunner({ store, validators: configuration.validators, connectors: { sink },
        evaluators: { e: { revision: '1', execute: value => value } },
        flows: { a: { ...configuration.flows.a, connections: [{ id: 'next', connector: 'sink', when: 'always',
            payload: 'raw-result', nextFlow: 'child', chainRetry: { maxAttempts: 1, delayMs: 1, maxDelayMs: 1 } }] },
        child: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
    try {
        for (const depth of [99, 100, 101]) {
            const value = nested(depth);
            response = JSON.stringify(value);
            const task = await runner.run({ flow: 'a', kind: 'result', result: depth }, { waitFor: 'settled' });
            const { jobs } = await store.get(task.taskId, runner.namespace);
            const delivery = jobs.find(job => job.kind === 'delivery');
            const chain = jobs.find(job => job.kind === 'chain');
            assert.equal(delivery.state, 'acknowledged');
            if (depth === 101) {
                assert.equal(delivery.receipt.receipt.responseUnavailable, true);
                assert.equal(chain.error?.code, 'RESPONSE_UNAVAILABLE');
                assert.equal(chain.childTaskId, undefined);
                continue;
            }
            assert.deepEqual(delivery.receipt.receipt.response, value);
            assert.equal(chain.state, 'acknowledged');
            assert.deepEqual((await store.get(chain.childTaskId, runner.namespace)).task.input.event, value);
            if (depth === 100) {
                const frames = [];
                for await (const frame of runner.exportTask(task.taskId)) frames.push(frame);
                assert.equal((await runner.importTask(frames, { flow: 'a' })).flow, 'a');
            }
        }
        response = 'ordinary text';
        assert.equal((await sink.send(1)).receipt.response, response);
    } finally { await runner.close({ cancel: true }); await store.close(); }
});

for (const path of ['/tasks', '/operations/submit']) test(`HTTP ${path} preserves application depth independently of protocol metadata`, async () => {
    const runner = createRunner(configuration);
    const server = await listen(createHttpServer({ runner, flow: 'a', reply: 'complete', maxBytes: 4096 }));
    const url = `http://127.0.0.1:${server.address().port}${path}`;
    const post = input => fetch(url, { method: 'POST', body: JSON.stringify(path === '/tasks' ? input : { input }) });
    try {
        const response = await post({ kind: 'result', result: nested() });
        assert.equal(response.status, path === '/tasks' ? 200 : 202);
        assert.equal((await response.json()).ok, true);
        assert.equal((await runner.list()).items.length, 1);
        for (const result of [nested(101), 'x'.repeat(4096)]) {
            const rejected = await post({ kind: 'result', result }).catch(error => ({ ok: false, error }));
            assert.equal(rejected.ok, false); if (rejected.arrayBuffer) await rejected.arrayBuffer();
        }
        assert.equal((await runner.list()).items.length, 1, 'Value and wire limits still apply before admission.');
    } finally { await server.drain(); await runner.close(); }
});

test('HTTP operator resolution preserves independent recovered-result depth', async () => {
    const runner = createRunner({ ...configuration, evaluators: { e: { revision: '1', execute() { throw new Error('Uncertain provider.'); } } },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v' } } });
    const task = await runner.run({ flow: 'a', kind: 'request', request: 1 });
    const server = await listen(createHttpServer({ runner, flow: 'a', allowAdmin: true, reply: 'complete' }));
    try {
        const response = await fetch(`http://127.0.0.1:${server.address().port}/operations/resolveEvaluation`, {
            method: 'POST', body: JSON.stringify({ taskId: task.taskId,
                resolution: { status: 'succeeded', reason: 'Recovered output.', result: nested() } }),
        });
        assert.equal(response.status, 200);
        assert.equal((await response.json()).value.evaluation.hasResult, true);
    } finally { await server.drain(); await runner.close(); }
});

test('HTTP delivery resolution and completion preserve independent evidence depth', async () => {
    const runner = createRunner({ ...configuration, connectors: { sink: createCallbackConnector(async () => ({ status: 'unknown' })) },
        flows: { a: { ...configuration.flows.a, connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] } } });
    const server = await listen(createHttpServer({ runner, flow: 'a', allowAdmin: true, reply: 'complete' }));
    try {
        for (const receipt of [nested(), { response: nested() }]) {
            const task = await runner.run({ flow: 'a', kind: 'result', result: 1 });
            const deliveryId = `${task.taskId}:delivery:out`;
            const invoke = (action, evidence) => fetch(`http://127.0.0.1:${server.address().port}/operations/${action}`, {
                method: 'POST', body: JSON.stringify({ deliveryId, ...evidence }),
            });
            const tooDeep = await invoke('resolveDelivery', { resolution: { status: 'acknowledged', receipt: { response: nested(101) } } });
            assert.equal((await tooDeep.json()).error.code, 'RESOURCE_LIMIT');
            const resolved = await invoke('resolveDelivery', { resolution: { status: 'acknowledged', receipt } });
            assert.equal(resolved.status, 200); assert.equal((await resolved.json()).ok, true);
            const completed = await invoke('recordCompletion', { completion: { status: 'completed', evidence: nested() } });
            assert.equal(completed.status, 200); assert.equal((await completed.json()).ok, true);
            const saved = await runner.store.getJob(runner.namespace, deliveryId);
            assert.deepEqual(saved.receipt.receipt, receipt);
            assert.deepEqual(saved.completion, { status: 'completed', evidence: nested() });
        }
    } finally { await server.drain(); await runner.close(); }
});

test('a boundary-depth receipt retains large scalar evidence within the store value limit', async () => {
    const runner = createRunner({ ...configuration, limits: { maxBytes: 6 * 1024 * 1024 },
        connectors: { sink: createCallbackConnector(() => ({ status: 'unknown' })) },
        flows: { a: { ...configuration.flows.a, connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] } } });
    try {
        const task = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        const deliveryId = `${task.taskId}:delivery:out`;
        const receipt = { response: nested(), note: 'x'.repeat(4200000) };
        await runner.resolveDelivery(deliveryId, { status: 'acknowledged', receipt });
        const job = await runner.store.getJob(runner.namespace, deliveryId);
        assert.equal(job.state, 'acknowledged');
        assert.deepEqual(job.receipt.receipt, receipt);
    } finally { await runner.close(); }
});

test('official MCP arguments preserve value depth and the combined argument byte limit', async () => {
    const runner = createRunner(configuration); const server = await createMcpServer({ runner, flows: ['a'] });
    const client = new Client({ name: 'value-boundary', version: '1' });
    const [a, b] = InMemoryTransport.createLinkedPair(); await server.connect(b); await client.connect(a);
    try {
        const response = await client.callTool({ name: 'connectors_run', arguments: { input: { flow: 'a', kind: 'result', result: nested() } } });
        assert.equal(response.isError, false); assert.equal(response.structuredContent.value.status, 'completed');
        const rejected = await client.callTool({ name: 'connectors_submit', arguments: { input: {
            flow: 'a', kind: 'result', result: 'r'.repeat(600000), validation: 'v'.repeat(600000),
        } } });
        assert.equal(rejected.isError, true); assert.equal(rejected.structuredContent.error.code, 'RESOURCE_LIMIT');
        assert.equal((await runner.list()).items.length, 1);
    } finally { await client.close(); await server.close(); await runner.close(); }
});

for (const parsedBody of [false, true]) test(`MCP HTTP ${parsedBody ? 'parsed' : 'wire'} bodies preserve tool value boundaries`, async () => {
    const runner = createRunner(configuration); const handler = await createMcpHttpHandler({ runner, flows: ['a'] });
    const server = await listen(createServer((request, response) => {
        void (async () => {
            if (!parsedBody || request.method !== 'POST') return handler(request, response);
            const chunks = []; for await (const chunk of request) chunks.push(chunk);
            return handler(request, response, JSON.parse(Buffer.concat(chunks).toString('utf8')));
        })().catch(() => response.destroy());
    }));
    const client = new Client({ name: 'http-value-boundary', version: '1' });
    try {
        await client.connect(new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${server.address().port}`)));
        const value = await client.callTool({ name: 'connectors_run', arguments: { input: { flow: 'a', kind: 'result', result: nested() } } });
        assert.equal(value.isError, false); assert.equal(value.structuredContent.value.status, 'completed');
    } finally { await client.close(); await close(server); await runner.close(); }
});
