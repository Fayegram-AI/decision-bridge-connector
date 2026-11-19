import test from 'node:test';
import assert from 'node:assert/strict';
import { createMcpConnector } from '../src/integrations/mcp-client.mjs';

test('MCP construction cancellation preserves borrowed clients and detaches after success', async () => {
    let calls = 0; let closes = 0; const controller = new AbortController();
    const client = { callTool() { calls++; return { content: [] }; }, close() { closes++; } };
    await assert.rejects(createMcpConnector({ tool: 'run', client }, { signal: AbortSignal.abort() }), { code: 'ABORTED' });
    const connector = await createMcpConnector({ tool: 'run', client }, { signal: controller.signal });
    controller.abort(); assert.equal((await connector.send({}, {})).status, 'acknowledged');
    await connector.close(); assert.equal(calls, 1); assert.equal(closes, 0);
});

test('MCP connector validates factory settings before accessing a client', async () => {
    let calls = 0;
    const client = { callTool() { calls++; return { content: [] }; } };
    const invalid = [
        ...[0, -1, 1.5, NaN, Infinity, 2147483648, '1000', null].map(timeoutMs => ({ timeoutMs })),
        { name: '' }, { revision: '' }, { tool: '' }, { transport: 'unknown' },
        { client: {} }, { client: null }, { unexpected: true },
    ];
    for (const options of invalid) await assert.rejects(createMcpConnector({ client, tool: 'run', ...options }), { code: 'CONFIGURATION_ERROR' });
    for (const options of [{}, { command: '' }, { command: 'node', args: ['\0'] }])
        await assert.rejects(createMcpConnector({ transport: 'stdio', tool: 'run', ...options }), { code: 'CONFIGURATION_ERROR' });
    assert.equal(calls, 0);
});

test('MCP close fences new calls, drains admitted calls and retains borrowed client ownership', async () => {
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let calls = 0; let closes = 0; let closed = false;
    const connector = await createMcpConnector({ tool: 'run', client: {
        async callTool() { calls++; entered.resolve(); return release.promise; }, close() { closes++; },
    } });
    try {
        const sending = connector.send({}, {}); await entered.promise;
        const closing = connector.close(); closing.then(() => { closed = true; });
        assert.equal(connector.close(), closing);
        await assert.rejects(connector.send({}, {}), { code: 'CONNECTOR_CLOSED', ambiguous: false });
        assert.equal(closed, false); assert.equal(calls, 1);
        release.resolve({ content: [] }); assert.equal((await sending).status, 'acknowledged');
        await closing; assert.equal(closed, true); assert.equal(closes, 0);
    } finally { release.resolve({ content: [] }); await connector.close(); }
});

test('MCP close finishes after the admitted call budget when the borrowed client ignores cancellation', async t => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    let signal; const entered = Promise.withResolvers();
    const connector = await createMcpConnector({ tool: 'run', timeoutMs: 1000, client: {
        callTool(_args, _schema, context) { signal = context.signal; entered.resolve(); return new Promise(() => {}); },
    } });
    const sending = connector.send({}, {}); const rejected = assert.rejects(sending, { code: 'MCP_DELIVERY', ambiguous: true });
    await entered.promise; const closing = connector.close();
    t.mock.timers.tick(1001); await rejected; await closing;
    assert.equal(signal.aborted, true);
    await assert.rejects(connector.send({}, {}), { code: 'CONNECTOR_CLOSED' });
});
