import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createCallbackConnector, VERSION } from '../src/index.mjs';
import { createRunner, createConfiguredRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createHttpServer } from '../src/node/http-service.mjs';

import { createHost } from '../src/node/host.mjs';
import { createSpoolSource } from '../src/node/incoming.mjs';
import { createMcpServer } from '../src/integrations/mcp-service.mjs';
import { createMcpConnector } from '../src/integrations/mcp-client.mjs';
import { createRedisSource } from '../src/integrations/redis-source.mjs';

function fixture(store = createMemoryStore()) {
    const calls = [];
    const runner = createRunner({ store, validators: { any: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async value => { calls.push(value); return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'any', connections: [{ id: 'save', connector: 'sink', when: 'evaluation.succeeded', payload: 'raw-result' }] },
            b: { revision: '1', inputs: ['result'], validator: 'any', connections: [] } } });
    return { runner, store, calls };
}

test('HTTP binds flows and permissions while sharing the result pipeline', async () => {
    const { runner, store, calls } = fixture();
    const server = createHttpServer({ runner, flow: 'a', reply: 'complete', authToken: 'test-only' });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const url = `http://127.0.0.1:${server.address().port}`;
    const post = (path, body, auth = true) => fetch(url + path, { method: 'POST', headers: { 'content-type': 'application/json', ...(auth ? { authorization: 'Bearer test-only' } : {}) }, body: JSON.stringify(body) });
    try {
        assert.equal((await post('/tasks', {}, false)).status, 401);
        assert.equal((await post('/tasks', { flow: 'b', kind: 'result', result: {} })).status, 403);
        const response = await post('/tasks', { kind: 'result', result: { answer: 42 } });
        assert.equal(response.status, 200); assert.equal((await response.json()).value.status, 'completed');
        assert.deepEqual(calls, [{ answer: 42 }]);
        assert.equal((await post('/operations/reprocess', {})).status, 403);
    } finally { await server.drain(); await runner.close(); await store.close(); }
});

test('MCP server and outgoing client interoperate with official peers', async () => {
    const { Client } = await import('@modelcontextprotocol/sdk/client/index.js');
    const { InMemoryTransport } = await import('@modelcontextprotocol/sdk/inMemory.js');
    const { runner, store } = fixture();
    const server = await createMcpServer({ runner, flows: ['a'] });
    const client = new Client({ name: 'test', version: '1' });
    const [a, b] = InMemoryTransport.createLinkedPair();
    await server.connect(b); await client.connect(a);
    try {
        assert.equal(client.getServerVersion().version, VERSION);
        const connector = await createMcpConnector({ client, tool: 'connectors_run' });
        const receipt = await connector.send({ input: { flow: 'a', kind: 'result', result: { answer: 1 } } }, {});
        assert.equal(receipt.status, 'acknowledged'); assert.equal(receipt.completion.status, 'completed');
        assert.equal(receipt.receipt.response.structuredContent.value.status, 'completed');
        const forbidden = await client.callTool({ name: 'connectors_run', arguments: { input: { flow: 'b', kind: 'result', result: {} } } });
        assert.equal(forbidden.isError, true);
        assert(!(await client.listTools()).tools.some(tool => tool.name === 'connectors_resolveDelivery'));
    } finally { await client.close(); await server.close(); await runner.close(); await store.close(); }
});

test('configuration registry loads a custom evaluator without engine changes', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-extension-'));
    await writeFile(join(directory, 'extension.mjs'), `export default { apiVersion: 1, revision: '1', schema: {type:'object'}, create() { return { revision:'1', execute: async value => ({echo:value}) }; } };`);
    const runner = await createConfiguredRunner({ schemaVersion: 3, extensions: [{ kind: 'evaluator', name: 'custom', module: './extension.mjs' }],
        evaluators: { test: { type: 'custom' } }, flows: { a: { revision: '1', evaluator: 'test', inputs: ['request'], connections: [] } } }, { baseDir: directory });
    try { assert.deepEqual((await runner.run({ flow: 'a', kind: 'request', request: 3 })).evaluation.result, { echo: 3 }); }
    finally { await runner.close(); await rm(directory, { recursive: true }); }
});

test('directory spool admits durably before archiving', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-'));
    const store = createSQLiteStore(join(directory, 'state.sqlite'));
    const { runner, calls } = fixture(store);
    const source = createSpoolSource({ directory: join(directory, 'in'), flow: 'a', inputMode: 'result', pollMs: 10 });
    const host = await createHost({ runner, sources: { spool: source }, pollMs: 10 });
    try {
        await sleep(50); await writeFile(join(directory, 'in', 'one.json'), JSON.stringify({ value: 4 }));
        for (let i = 0; i < 100 && !calls.length; i++) await sleep(20);
        assert.deepEqual(calls, [{ value: 4 }]);
    } finally { await host.close(); await store.close(); await rm(directory, { recursive: true }); }
});

test('Redis consumer acknowledges durable admission and preserves entry identity', { skip: !process.env.DBC_REDIS_URL }, async () => {
    const { createClient } = await import('redis');
    const client = createClient({ url: process.env.DBC_REDIS_URL }); client.on('error', () => {}); await client.connect();
    const stream = `dbc-test-${crypto.randomUUID()}`; const directory = await mkdtemp(join(tmpdir(), 'dbc-redis-'));
    const store = createSQLiteStore(join(directory, 'state.sqlite')); const { runner, calls } = fixture(store);
    const source = createRedisSource({ client, stream, group: 'test', flow: 'a', inputMode: 'result', pollMs: 10 });
    const host = await createHost({ runner, sources: { redis: source }, pollMs: 10 });
    try {
        await client.sendCommand(['XADD', stream, '*', 'payload', JSON.stringify({ value: 5 })]);
        for (let i = 0; i < 100 && !calls.length; i++) await sleep(20);
        assert.deepEqual(calls, [{ value: 5 }]);
        assert.equal((await client.sendCommand(['XPENDING', stream, 'test']))[0], 0);
    } finally { await host.close(); await store.close(); await client.del(stream); client.destroy(); await rm(directory, { recursive: true }); }
});
