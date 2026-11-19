import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer, request as httpRequest } from 'node:http';
import { createRunner } from './helpers/runner.mjs';
import { createHttpHandler, createHttpServer } from '../src/node/http-service.mjs';
import { createMcpServer, createMcpHttpHandler } from '../src/integrations/mcp-service.mjs';
import { bounded } from '../src/util.mjs';

const configuration = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };

test('HTTP and MCP reject nonboolean permission flags before exposing operations', async () => {
    const runner = createRunner(configuration);
    try {
        for (const key of ['allowAdmin', 'allowRun']) for (const value of ['false', 'true', 0, 1, null, [], {}]) {
            assert.throws(() => createHttpHandler({ runner, flow: 'a', reply: 'complete', [key]: value }), { code: 'CONFIGURATION_ERROR' });
            await assert.rejects(createMcpServer({ runner, flows: ['a'], [key]: value }), { code: 'CONFIGURATION_ERROR' });
            await assert.rejects(createMcpHttpHandler({ runner, flows: ['a'], [key]: value }), { code: 'CONFIGURATION_ERROR' });
        }
    } finally { await runner.close(); }
});

for (const transport of ['direct', 'http']) test(`MCP ${transport} host captures flow and authorization settings`, async () => {
    const [{ Client }, { InMemoryTransport }, { StreamableHTTPClientTransport }] = await Promise.all([
        import('@modelcontextprotocol/sdk/client/index.js'), import('@modelcontextprotocol/sdk/inMemory.js'),
        import('@modelcontextprotocol/sdk/client/streamableHttp.js'),
    ]);
    const runner = createRunner({ ...configuration, flows: { ...configuration.flows, b: configuration.flows.a } });
    const options = { runner, flows: ['a'], allowAdmin: false, authorizationTimeoutMs: 1000, authorize: () => false };
    const client = new Client({ name: 'captured-authority', version: '1' }); let server;
    try {
        if (transport === 'direct') {
            server = await createMcpServer(options);
            const [a, b] = InMemoryTransport.createLinkedPair(); await server.connect(b); await client.connect(a);
        } else {
            server = createServer(await createMcpHttpHandler(options)); server.listen(0, '127.0.0.1'); await once(server, 'listening');
            await client.connect(new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${server.address().port}`)));
        }
        options.flows.push('b'); options.allowAdmin = true; options.authorize = undefined;
        const tools = (await client.listTools()).tools;
        assert.equal(tools.some(tool => tool.name === 'connectors_cancel'), false);
        assert.deepEqual(tools.find(tool => tool.name === 'connectors_submit').inputSchema.properties.input.properties.flow.enum, ['a']);
        for (const flow of ['a', 'b']) {
            const result = await client.callTool({ name: 'connectors_submit', arguments: { input: { flow, kind: 'result', result: 1 } } });
            assert.equal(result.isError, true); assert.equal(result.structuredContent.error.code, 'FORBIDDEN');
        }
        assert.equal((await runner.list()).items.length, 0);
    } finally {
        await client.close();
        if (transport === 'direct') await server?.close();
        else if (server) await new Promise(resolve => { server.close(resolve); server.closeIdleConnections(); });
        await runner.close();
    }
});

test('authorization requires an explicit supported timeout for HTTP and MCP', async () => {
    const runner = createRunner(configuration);
    try {
        for (const authorizationTimeoutMs of [undefined, 0, -1, 1.5, 2147483648]) {
            assert.throws(() => createHttpHandler({ runner, flow: 'a', reply: 'complete', authorize: () => true, authorizationTimeoutMs }), /authorizationTimeoutMs/);
            await assert.rejects(createMcpHttpHandler({ runner, flows: ['a'], authorize: () => true, authorizationTimeoutMs }), /authorizationTimeoutMs/);
        }
        for (const flows of [[], ['a', 'a'], ['missing'], [undefined], ['']])
            await assert.rejects(createMcpHttpHandler({ runner, flows }), { code: 'CONFIGURATION_ERROR' });
    } finally { await runner.close(); }
});

for (const outcome of ['resolve', 'reject']) test(`HTTP authorization times out and observes late ${outcome}`, async () => {
    const runner = createRunner(configuration); const gate = Promise.withResolvers(); let signal; let dispatches = 0;
    const server = createHttpServer({ runner: { ...runner, inspect() { dispatches++; return {}; } }, flow: 'a', reply: 'complete',
        authorizationTimeoutMs: 30, authorize(context) { signal = context.signal; return gate.promise; } });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    try {
        const response = await fetch(`http://127.0.0.1:${server.address().port}/health`);
        assert.equal(response.status, 503); assert.equal((await response.json()).error.code, 'AUTHORIZATION_TIMEOUT');
        assert(signal.aborted); assert.equal(dispatches, 0);
        if (outcome === 'resolve') gate.resolve(true); else gate.reject(new Error('late rejection'));
        await new Promise(resolve => setImmediate(resolve)); assert.equal(dispatches, 0);
    } finally { gate.resolve(false); await server.drain(); await runner.close(); }
});

for (const mode of ['disconnect', 'drain', 'host-abort']) test(`HTTP ${mode} cancels an uncooperative authorization hook`, async () => {
    const runner = createRunner(configuration); const entered = Promise.withResolvers(); const controller = new AbortController();
    let dispatches = 0; let signal;
    const server = createHttpServer({ runner: { ...runner, inspect() { dispatches++; return {}; } }, flow: 'a', reply: 'complete', signal: controller.signal,
        authorizationTimeoutMs: 10000, authorize(context) { signal = context.signal; entered.resolve(); return new Promise(() => {}); } });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const request = httpRequest(`http://127.0.0.1:${server.address().port}/health`); request.on('error', () => {}); request.end();
    try {
        await bounded(() => entered.promise, { timeoutMs: 5000 });
        if (mode === 'disconnect') request.destroy(); else if (mode === 'host-abort') controller.abort();
        else await bounded(() => server.drain(), { timeoutMs: 1000 });
        await bounded(async () => { while (!signal.aborted) await new Promise(resolve => setImmediate(resolve)); }, { timeoutMs: 1000 });
        assert.equal(dispatches, 0);
    } finally { request.destroy(); await server.drain(); await runner.close(); }
});

test('HTTP authorization allows true and denies false without dispatching denied operations', async () => {
    const runner = createRunner(configuration); let allow = false; let dispatches = 0;
    const server = createHttpServer({ runner: { ...runner, inspect() { dispatches++; return {}; } }, flow: 'a', reply: 'complete',
        authorizationTimeoutMs: 1000, authorize: () => allow });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    try {
        const url = `http://127.0.0.1:${server.address().port}/health`;
        assert.equal((await fetch(url)).status, 403); assert.equal(dispatches, 0);
        allow = true; assert.equal((await fetch(url)).status, 200); assert.equal(dispatches, 1);
    } finally { await server.drain(); await runner.close(); }
});

for (const mode of ['timeout', 'shutdown', 'success', 'denial', 'throw']) test(`MCP authorization handles ${mode}`, async () => {
    const [{ Client }, { InMemoryTransport }] = await Promise.all([
        import('@modelcontextprotocol/sdk/client/index.js'), import('@modelcontextprotocol/sdk/inMemory.js'),
    ]);
    const runner = createRunner(configuration); const controller = new AbortController(); const entered = Promise.withResolvers();
    const gate = Promise.withResolvers(); let signal; let dispatches = 0;
    const server = await createMcpServer({ runner: { ...runner, inspect() { dispatches++; return {}; } }, flows: ['a'], signal: controller.signal,
        authorizationTimeoutMs: mode === 'timeout' ? 30 : 1000, authorize(context) {
            signal = context.signal; entered.resolve();
            if (mode === 'success') return true; if (mode === 'denial') return false; if (mode === 'throw') throw new Error('private detail');
            return gate.promise;
        } });
    const client = new Client({ name: 'authorization', version: '1' }); const [a, b] = InMemoryTransport.createLinkedPair();
    await server.connect(b); await client.connect(a);
    try {
        const result = client.callTool({ name: 'connectors_inspect', arguments: {} });
        await entered.promise; if (mode === 'shutdown') controller.abort();
        const response = await result;
        assert.equal(response.isError, mode !== 'success'); assert.equal(dispatches, mode === 'success' ? 1 : 0);
        if (mode === 'timeout' || mode === 'shutdown') {
            assert(signal.aborted); assert.equal(response.structuredContent.error.code, mode === 'timeout' ? 'AUTHORIZATION_TIMEOUT' : 'ABORTED');
            gate.reject(new Error('late rejection')); await new Promise(resolve => setImmediate(resolve)); assert.equal(dispatches, 0);
        }
        if (mode === 'throw') assert(!JSON.stringify(response).includes('private detail'));
    } finally { gate.resolve(false); await client.close(); await server.close(); await runner.close(); }
});

test('MCP HTTP contains per-request setup failure after startup validation', async () => {
    let reads = 0;
    const runner = { get() {}, get ready() { if (++reads === 1) return Promise.resolve({ a: {} }); throw new Error('private setup detail'); } };
    const handler = await createMcpHttpHandler({ runner, flows: ['a'] });
    const server = createServer(handler); server.listen(0, '127.0.0.1'); await once(server, 'listening');
    try {
        const response = await fetch(`http://127.0.0.1:${server.address().port}`);
        assert.equal(response.status, 500); assert.equal(await response.text(), '');
    } finally { await new Promise(resolve => { server.close(resolve); server.closeIdleConnections(); }); }
});

test('MCP HTTP closes partial resources once and observes cleanup errors', async () => {
    const [{ Server }, { StreamableHTTPServerTransport }] = await Promise.all([
        import('@modelcontextprotocol/sdk/server/index.js'), import('@modelcontextprotocol/sdk/server/streamableHttp.js'),
    ]);
    const originalServerClose = Server.prototype.close; const originalTransportClose = StreamableHTTPServerTransport.prototype.close;
    let serverCloses = 0; let transportCloses = 0;
    Server.prototype.close = async function () { serverCloses++; await originalServerClose.call(this); throw new Error('private cleanup failure'); };
    StreamableHTTPServerTransport.prototype.close = async function () { transportCloses++; return originalTransportClose.call(this); };
    const runner = createRunner(configuration); let server;
    try {
        server = createServer(await createMcpHttpHandler({ runner, flows: ['a'] }));
        server.listen(0, '127.0.0.1'); await once(server, 'listening');
        const response = await fetch(`http://127.0.0.1:${server.address().port}`, { method: 'POST', body: '{broken' });
        assert.equal(response.status, 400); await response.text();
        await new Promise(resolve => setImmediate(resolve));
        assert.equal(serverCloses, 1); assert.equal(transportCloses, 1);
    } finally {
        if (server) await new Promise(resolve => { server.close(resolve); server.closeIdleConnections(); });
        Server.prototype.close = originalServerClose; StreamableHTTPServerTransport.prototype.close = originalTransportClose; await runner.close();
    }
});
