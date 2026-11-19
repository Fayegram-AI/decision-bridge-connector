import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { createRunner } from './helpers/runner.mjs';
import { createMcpHttpHandler } from '../src/integrations/mcp-service.mjs';
import { createMcpConnector } from '../src/integrations/mcp-client.mjs';

test('MCP stdio exchanges framed JSON-RPC with an independent official SDK process', async () => {
    const startup = new AbortController();
    const connector = await createMcpConnector({ transport: 'stdio', command: process.execPath,
        args: [fileURLToPath(new URL('./fixtures/mcp-peer.mjs', import.meta.url))], tool: 'connectors_run' }, { signal: startup.signal });
    startup.abort();
    try {
        const receipt = await connector.send({ input: { flow: 'test', kind: 'result', result: { wire: 'stdio' } } }, {});
        assert.equal(receipt.status, 'acknowledged');
        assert.equal(receipt.receipt.response.structuredContent.value.status, 'completed');
    } finally { await connector.close(); }
});

test('MCP Streamable HTTP exchanges official client and server messages', async () => {
    const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
        flows: { test: { revision: '1', inputs: ['result'], validator: 'v' } } });
    const handler = await createMcpHttpHandler({ runner, flows: ['test'] });
    const server = createServer(handler); server.listen(0, '127.0.0.1'); await once(server, 'listening');
    let connector;
    try {
        connector = await createMcpConnector({ transport: 'http', url: `http://127.0.0.1:${server.address().port}`, tool: 'connectors_run' });
        const receipt = await connector.send({ input: { flow: 'test', kind: 'result', result: { wire: 'http' } } }, {});
        assert.equal(receipt.completion.status, 'completed');
        assert.equal(receipt.receipt.response.structuredContent.value.evaluation.result.wire, 'http');
    } finally { await connector?.close(); await new Promise(resolve => { server.close(resolve); server.closeIdleConnections(); }); await runner.close(); }
});
