import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer, request } from 'node:http';
import { createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createHttpHandler, createHttpServer } from '../src/node/http-service.mjs';
import { createMcpHttpHandler } from '../src/integrations/mcp-service.mjs';
import { createIncoming } from '../src/node/incoming.mjs';

function fixture() {
    let effects = 0; let authorizations = 0;
    const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async () => { effects++; return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
        ] } } });
    return { runner, authorize() { authorizations++; return true; }, get effects() { return effects; },
        get authorizations() { return authorizations; } };
}

async function listen(server) {
    server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server;
}

function send(server, headers = {}, body, method = 'POST') {
    return new Promise((resolve, reject) => {
        const req = request({ hostname: '127.0.0.1', port: server.address().port, path: '/tasks', method,
            headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream', ...headers } }, response => {
            const chunks = [];
            response.on('data', chunk => chunks.push(chunk));
            response.on('error', reject);
            response.on('end', () => resolve({ status: response.statusCode, body: Buffer.concat(chunks).toString() }));
        });
        req.on('error', reject); req.setTimeout(3000, () => req.destroy(new Error('Request stalled.')));
        req.end(body);
    });
}

const inputs = {
    http: JSON.stringify({ kind: 'result', result: 1 }),
    mcp: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: {
        name: 'connectors_run', arguments: { input: { flow: 'a', kind: 'result', result: 1 } },
    } }),
};

test('HTTP server enforces authentication on the actual listen address and defaults to loopback', async () => {
    const { runner } = fixture();
    const server = createHttpServer({ runner, flow: 'a', reply: 'complete' });
    const authenticated = createHttpServer({ runner, flow: 'a', reply: 'complete', authToken: 'test-only' });
    try {
        assert.throws(() => server.listen(0, '0.0.0.0'), { code: 'CONFIGURATION_ERROR', message: /authentication/ });
        assert.throws(() => server.listen({ port: 0, host: '::' }), { code: 'CONFIGURATION_ERROR', message: /authentication/ });
        assert.throws(() => server.listen({ fd: 0 }), { code: 'CONFIGURATION_ERROR', message: /authentication/ });
        await new Promise(resolve => server.listen(0, resolve));
        assert.equal(server.address().address, '127.0.0.1');
        assert.equal((await send(server, {}, inputs.http)).status, 200);
        await server.drain();
        assert.throws(() => authenticated.listen({ port: 0, host: {} }), { code: 'CONFIGURATION_ERROR', message: /host/ });
        await new Promise(resolve => authenticated.listen({ port: 0, host: '0.0.0.0' }, resolve));
        assert.equal((await send(authenticated, {}, inputs.http)).status, 401);
        assert.equal((await send(authenticated, { authorization: 'Bearer test-only' }, inputs.http)).status, 200);
    } finally { await Promise.all([server.drain(), authenticated.drain()]); await runner.close(); }
});

test('HTTP and MCP listeners reject malformed configured bearer credentials', async () => {
    const { runner } = fixture();
    try {
        for (const authToken of [{ missingSecret: true }, true, null, '', '  ']) {
            assert.throws(() => createHttpServer({ runner, flow: 'a', host: '0.0.0.0', reply: 'complete', authToken }),
                { code: 'CONFIGURATION_ERROR', message: /authToken/ });
            const incoming = await createIncoming('mcp', { transport: 'http', flow: 'a', port: 0, authToken });
            try {
                await assert.rejects(incoming.start({ runner, role: 'all' }),
                    { code: 'CONFIGURATION_ERROR', message: /authToken/ });
            } finally { await incoming.close(); }
        }
        assert.equal((await runner.list()).items.length, 0);
    } finally { await runner.close(); }
});

async function host(protocol, state, options = {}) {
    const settings = { runner: state.runner, authorize: state.authorize, authorizationTimeoutMs: 1000, ...options };
    return listen(protocol === 'http' ? createHttpServer({ ...settings, flow: 'a', reply: 'complete' })
        : createServer(await createMcpHttpHandler({ ...settings, flows: ['a'] })));
}

async function close(server, runner) {
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); await runner.close();
}

for (const protocol of ['http', 'mcp']) {
    test(`${protocol}: untrusted Host and Origin fail before parsing, authorization or effects`, async () => {
        const state = fixture(); const server = await host(protocol, state);
        try {
            for (const headers of [
                { origin: 'https://untrusted.example', 'content-type': 'text/plain' },
                { origin: 'null' }, { host: 'untrusted.example' }, { host: 'localhost.untrusted.example' },
                { host: 'untrusted.example', 'x-forwarded-host': 'localhost' },
            ]) {
                const denied = await send(server, headers, inputs[protocol]);
                assert.equal(denied.status, 403);
            }
            assert.equal((await send(server, { origin: 'https://untrusted.example' }, '{invalid')).status, 403);
            assert.equal((await send(server, { host: 'untrusted.example' }, undefined, 'GET')).status, 403);
            assert.equal(state.authorizations, 0); assert.equal(state.effects, 0);
            assert.equal((await state.runner.list()).items.length, 0);
            const accepted = await send(server, { host: '[::1]:8787' }, inputs[protocol]);
            assert.equal(accepted.status, 200); assert.equal(state.effects, 1);
        } finally { await close(server, state.runner); }
    });

    test(`${protocol}: explicit hostnames and exact origins are captured and preserve trusted execution`, async () => {
        const state = fixture(); const options = { allowedHosts: ['bridge.example'], allowedOrigin: 'https://app.example' };
        const server = await host(protocol, state, options);
        options.allowedHosts.push('untrusted.example'); options.allowedOrigin = 'https://untrusted.example';
        try {
            for (const headers of [{ host: 'untrusted.example' }, { host: 'bridge.example', origin: 'https://untrusted.example' },
                { host: 'bridge.example', origin: 'https://app.example.untrusted.example' }, { host: '127.0.0.1' }])
                assert.equal((await send(server, headers, inputs[protocol])).status, 403);
            assert.equal(state.effects, 0); assert.equal(state.authorizations, 0);
            for (const origin of [undefined, 'https://app.example']) {
                const accepted = await send(server, { host: 'BRIDGE.EXAMPLE:8787', ...(origin ? { origin } : {}) }, inputs[protocol]);
                assert.equal(accepted.status, 200);
            }
            assert.equal(state.effects, 2); assert.equal(state.authorizations, 2);
        } finally { await close(server, state.runner); }
    });
}

test('HTTP security configuration rejects malformed allowlists before hosting', async () => {
    const state = fixture();
    try {
        for (const options of [
            ...[[], null, '*', ['*'], ['localhost:8787'], ['https://bridge.example'], [1], ['user@localhost']].map(allowedHosts => ({ allowedHosts })),
            ...['*', 'null', null, 1, 'https://app.example/', 'https://user@app.example', 'https://app.example/path'].map(allowedOrigin => ({ allowedOrigin })),
        ]) {
            assert.throws(() => createHttpHandler({ runner: state.runner, flow: 'a', reply: 'complete', ...options }), { code: 'CONFIGURATION_ERROR' });
            await assert.rejects(createMcpHttpHandler({ runner: state.runner, flows: ['a'], ...options }), { code: 'CONFIGURATION_ERROR' });
        }
    } finally { await state.runner.close(); }
});

test('configured MCP HTTP listeners use the same request policy', async () => {
    const state = fixture();
    const source = await createIncoming('mcp', { transport: 'http', flow: 'a', port: 0,
        allowedHosts: ['bridge.example'], allowedOrigin: 'https://app.example',
        authorizationTimeoutMs: 1000, authorize: state.authorize });
    // Capture the bound port without changing the source's public lifecycle contract.
    const originalListen = (await import('node:net')).Server.prototype.listen;
    let server;
    (await import('node:net')).Server.prototype.listen = function (...args) { server = this; return originalListen.apply(this, args); };
    try {
        await source.start({ runner: state.runner, role: 'all' });
        assert.equal((await send(server, { host: 'bridge.example', origin: 'https://untrusted.example' }, inputs.mcp)).status, 403);
        assert.equal((await send(server, { host: 'untrusted.example' }, inputs.mcp)).status, 403);
        assert.equal(state.effects, 0);
        assert.equal((await send(server, { host: 'bridge.example', origin: 'https://app.example' }, inputs.mcp)).status, 200);
        assert.equal(state.effects, 1);
    } finally {
        (await import('node:net')).Server.prototype.listen = originalListen;
        await source.close(); await state.runner.close();
    }
});
