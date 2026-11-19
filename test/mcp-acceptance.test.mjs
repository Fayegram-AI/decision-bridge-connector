import test from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { createMcpConnector } from '../src/integrations/mcp-client.mjs';
import { createMemoryStore } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

// Actual SDK peers establish acceptance independently of local response retention limits.
for (const isError of [false, true]) test(`MCP keeps official peer acceptance when response capture exceeds its budget (isError=${isError})`, async () => {
    let calls = 0;
    const server = new Server({ name: 'large-tool-result', version: '1' }, { capabilities: { tools: {} } });
    server.setRequestHandler(ListToolsRequestSchema, () => ({ tools: [{ name: 'deliver', inputSchema: { type: 'object' } }] }));
    server.setRequestHandler(CallToolRequestSchema, () => {
        calls++; return { content: [{ type: 'text', text: 'x'.repeat(1048576) }], isError };
    });
    const client = new Client({ name: 'acceptance-consumer', version: '1' });
    const [a, b] = InMemoryTransport.createLinkedPair();
    let connector;
    try {
        await server.connect(b); await client.connect(a);
        connector = await createMcpConnector({ client, tool: 'deliver' });
        const checkpoints = [];
        const result = await connector.send({}, { onAcceptance(value) { checkpoints.push(value); } });
        assert.equal(result.status, 'acknowledged');
        assert.deepEqual(result.receipt, { responseUnavailable: true });
        assert.deepEqual(result.completion, { status: isError ? 'failed' : 'completed', scope: 'mcp-tool' });
        assert.equal(checkpoints.length, 1);
        assert.equal(checkpoints[0].status, 'acknowledged');
        assert.equal(checkpoints[0].completion.status, result.completion.status);
        assert.equal(calls, 1);
        await connector.close();
        assert.equal((await client.listTools()).tools.length, 1);
    } finally { await connector?.close(); await client.close(); await server.close(); }
});

const stores = [['memory', () => createMemoryStore({ maxBytes: 512 })],
    ['sqlite', () => createSQLiteStore(':memory:', { maxBytes: 512 })],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, maxBytes: 512 })]] : [])];

for (const [name, factory] of stores) test(`${name}: MCP acceptance survives a smaller store receipt budget without replay`, async () => {
    const store = await factory(); let calls = 0;
    const connector = await createMcpConnector({ tool: 'deliver', idempotent: true, idempotencyArgument: 'deliveryKey',
        client: { async callTool() { calls++; return { content: [{ type: 'text', text: 'x'.repeat(1024) }] }; } } });
    const runner = createRunner({ store, namespace: `mcp-budget-${crypto.randomUUID()}`,
        validators: { v: { revision: '1', validate: value => value } }, connectors: { sink: connector },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result',
                retry: { ambiguous: 'retry', maxAttempts: 2, delayMs: 1, maxDelayMs: 1 } },
        ] } } });
    try {
        const result = await runner.run({ flow: 'a', kind: 'result', result: { value: 1 } });
        assert.equal(result.status, 'completed');
        assert.equal(result.deliveries[0].status, 'acknowledged');
        assert.equal(result.deliveries[0].receipt.receipt.responseUnavailable, true);
        assert.equal(result.deliveries[0].completion.status, 'completed');
        assert.equal(result.deliveries[0].attempts.length, 1);
        assert.equal(result.deliveries[0].error, null);
        await runner.recover(); await runner.execute(result.taskId); assert.equal(calls, 1);
        const saved = await store.get(result.taskId, runner.namespace);
        assert.equal(saved.jobs.find(job => job.kind === 'delivery').acceptance.receipt.status, 'acknowledged');
    } finally { await runner.close(); await connector.close(); await store.close(); }
});

test('MCP awaits its acceptance hook and preserves acceptance when that wait expires', async t => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let calls = 0;
    const connector = await createMcpConnector({ tool: 'deliver', timeoutMs: 1000, client: {
        async callTool() { calls++; return { content: [], isError: true }; },
    } });
    try {
        let finished = false;
        const sending = connector.send({}, { async onAcceptance() { entered.resolve(); await release.promise; } });
        sending.then(() => { finished = true; });
        // The timer gate also makes a missing checkpoint fail promptly on the baseline.
        await Promise.race([entered.promise, sending]);
        assert.equal(finished, false);
        t.mock.timers.tick(1001);
        const result = await sending;
        assert.equal(result.status, 'acknowledged');
        assert.deepEqual(result.receipt, { responseUnavailable: true });
        assert.equal(result.completion.status, 'failed'); assert.equal(calls, 1);
    } finally { release.resolve(); await connector.close(); }
});

test('MCP failures before a valid tool response cannot checkpoint acceptance', async () => {
    for (const response of [undefined, null, 42, { isError: 'false' }]) {
        let checkpoints = 0;
        const connector = await createMcpConnector({ tool: 'deliver', client: { async callTool() { return response; } } });
        try {
            await assert.rejects(connector.send({}, { onAcceptance() { checkpoints++; } }), { code: 'MCP_DELIVERY', ambiguous: true });
            assert.equal(checkpoints, 0);
        } finally { await connector.close(); }
    }
});
