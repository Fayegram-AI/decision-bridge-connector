import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createRunner } from './helpers/runner.mjs';
import { createMcpHttpHandler, createMcpServer } from '../src/integrations/mcp-service.mjs';
import { ConnectorError } from '../src/util.mjs';
import { createRunner as createProductionRunner, createMemoryStore } from '../src/index.mjs';
import { createMcpConnector } from '../src/integrations/mcp-client.mjs';
import { fileURLToPath } from 'node:url';

test('raised MCP argument budgets work over official stdio framing', async () => {
    const connector = await createMcpConnector({ transport: 'stdio', command: process.execPath,
        args: [fileURLToPath(new URL('./fixtures/mcp-large-peer.mjs', import.meta.url))], tool: 'connectors_submit' });
    try {
        const receipt = await connector.send({ input: { flow: 'a', kind: 'result', result: 'x'.repeat(1100000) } });
        assert.equal(receipt.status, 'acknowledged'); assert.equal(receipt.receipt.response.isError, false);
    } finally { await connector.close(); }
});

test('raised MCP budgets admit values above 1 MiB through raw HTTP, parsed HTTP and direct tools', async () => {
    const store = createMemoryStore({ maxBytes: 2097152 });
    const runner = createProductionRunner({ store, validators: { v: { revision: '1', validate: value => value } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    const args = { input: { flow: 'a', kind: 'result', result: 'x'.repeat(1100000) } };
    const frame = { jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'connectors_submit', arguments: args } };
    const handler = await createMcpHttpHandler({ runner, flows: ['a'], maxBytes: 2097152 });
    const server = createServer((request, response) => handler(request, response, request.url === '/parsed' ? frame : undefined));
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const [{ Client }, { InMemoryTransport }] = await Promise.all([
        import('@modelcontextprotocol/sdk/client/index.js'), import('@modelcontextprotocol/sdk/inMemory.js'),
    ]);
    const direct = await createMcpServer({ runner, flows: ['a'], maxBytes: 2097152 });
    const client = new Client({ name: 'large-budget', version: '1' }); const [a, b] = InMemoryTransport.createLinkedPair();
    await direct.connect(b); await client.connect(a);
    try {
        for (const path of ['/raw', '/parsed']) {
            const response = await fetch(`http://127.0.0.1:${server.address().port}${path}`, { method: 'POST', body: JSON.stringify(frame),
                headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream' } });
            assert.equal(response.status, 200);
            const line = (await response.text()).split('\n').find(value => value.startsWith('data: '));
            assert.equal(JSON.parse(line.slice(6)).result.isError, false);
        }
        assert.equal((await client.callTool({ name: 'connectors_submit', arguments: args })).isError, false);
    } finally {
        await client.close(); await direct.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
        await runner.close(); await store.close();
    }
});

test('MCP byte settings fail startup and both HTTP body paths enforce the limit', async () => {
    const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    let dispatches = 0;
    const hostRunner = { ...runner, inspect() { dispatches++; return {}; } };
    const frame = { jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'connectors_inspect', arguments: { extra: '猫'.repeat(200) } } };
    const handler = await createMcpHttpHandler({ runner: hostRunner, flows: ['a'], maxBytes: 256 });
    const server = createServer((request, response) => handler(request, response, request.url === '/parsed' ? frame : request.url === '/invalid' ? { value: () => 1 } : undefined));
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    try {
        for (const maxBytes of [0, -1, 1.5, 67108865]) {
            await assert.rejects(createMcpHttpHandler({ runner, flows: ['a'], maxBytes }), { code: 'CONFIGURATION_ERROR' });
            await assert.rejects(createMcpServer({ runner, flows: ['a'], maxBytes }), { code: 'CONFIGURATION_ERROR' });
        }
        for (const path of ['/raw', '/parsed']) {
            const response = await fetch(`http://127.0.0.1:${server.address().port}${path}`, { method: 'POST', body: JSON.stringify(frame),
                headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream' } });
            assert.equal(response.status, 413); assert.equal(await response.text(), '');
        }
        const malformed = await fetch(`http://127.0.0.1:${server.address().port}/raw`, { method: 'POST', body: '{malformed' });
        assert.equal(malformed.status, 400); assert.equal(dispatches, 0);
        const invalid = await fetch(`http://127.0.0.1:${server.address().port}/invalid`, { method: 'POST', body: '{}' });
        assert.equal(invalid.status, 400); assert.equal(await invalid.text(), ''); assert.equal(dispatches, 0);
    } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); await runner.close(); }
});

test('MCP initialization resource errors remain sanitized HTTP 500 responses', async () => {
    let failing = false;
    const runner = { get ready() { if (failing) throw new ConnectorError('RESOURCE_LIMIT', 'private startup failure'); return Promise.resolve({ a: {} }); }, get() {} };
    const handler = await createMcpHttpHandler({ runner, flows: ['a'] }); failing = true;
    const server = createServer((request, response) => handler(request, response));
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    try {
        const response = await fetch(`http://127.0.0.1:${server.address().port}`, { method: 'POST', body: '{}' });
        assert.equal(response.status, 500); assert.equal(await response.text(), '');
    } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
});

test('MCP raw numbers reject overflow while retaining the actual wire-byte budget', async () => {
    for (const literal of ['1e400', '-1e400', '1e20']) {
        let dispatches = 0;
        const runner = { ready: Promise.resolve({ a: {} }), get() {}, inspect() { dispatches++; return {}; } };
        const frame = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"connectors_inspect","arguments":{"value":${literal}}}}`;
        const handler = await createMcpHttpHandler({ runner, flows: ['a'], maxBytes: Buffer.byteLength(frame) });
        const server = createServer((request, response) => handler(request, response, request.url === '/parsed' ? JSON.parse(frame) : undefined));
        server.listen(0, '127.0.0.1'); await once(server, 'listening');
        try {
            for (const path of ['/raw', '/parsed']) {
                const response = await fetch(`http://127.0.0.1:${server.address().port}${path}`, { method: 'POST', body: frame,
                    headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream' } });
                const expected = literal === '1e20' ? path === '/raw' ? 200 : 413 : 400;
                assert.equal(response.status, expected);
                if (expected !== 200) assert.equal(await response.text(), ''); else assert.match(await response.text(), /"ok":true/);
            }
            assert.equal(dispatches, literal === '1e20' ? 1 : 0);
        } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
    }
});
