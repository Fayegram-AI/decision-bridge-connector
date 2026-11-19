import test from 'node:test';
import assert from 'node:assert/strict';
import Ajv from 'ajv';
import { createRunner } from './helpers/runner.mjs';
import { createMcpServer } from '../src/integrations/mcp-service.mjs';
import { toolSchema } from '../src/integrations/mcp-schemas.mjs';

test('MCP advertised schemas represent actual operation options and JSON validation contexts', () => {
    const ajv = new Ajv({ strict: true }); const check = action => ajv.compile(toolSchema(action, ['a']));
    for (const validation of [null, 42, 'context', ['context'], { request: {} }]) {
        assert(check('run')({ input: { flow: 'a', kind: 'result', result: 1, validation }, options: { waitFor: 'required', waitMs: 1000 } }));
    }
    assert.equal(check('run')({ input: { flow: 'a', kind: 'result', result: 1 }, options: { flow: 'a' } }), false);
    assert.equal(check('run')({ input: { flow: 'a', kind: 'result', request: 1 } }), false);
    assert.equal(check('run')({ input: { flow: 'a', kind: 'result', result: 1, request: 1 } }), false);
    assert.equal(check('reprocess')({ taskId: 'task', options: { waitMs: 1000 } }), false);
    for (const action of ['redeliver', 'openImport']) {
        const base = action === 'redeliver' ? { taskId: 'task' } : {};
        assert.equal(check(action)(base), false); assert.equal(check(action)({ ...base, options: {} }), false);
        assert(check(action)({ ...base, options: { flow: 'a' } }));
    }
});

test('official MCP clients can send scalar and null saved-result validation contexts', async () => {
    const [{ Client }, { InMemoryTransport }] = await Promise.all([
        import('@modelcontextprotocol/sdk/client/index.js'), import('@modelcontextprotocol/sdk/inMemory.js'),
    ]);
    const contexts = [];
    const runner = createRunner({ validators: { v: { revision: '1', validate(value, context) { contexts.push(context.validation); return value; } } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    const server = await createMcpServer({ runner, flows: ['a'] }); const client = new Client({ name: 'schema-contract', version: '1' });
    const [a, b] = InMemoryTransport.createLinkedPair(); await server.connect(b); await client.connect(a);
    try {
        await client.listTools();
        for (const validation of [42, 'context', ['context'], null]) {
            const response = await client.callTool({ name: 'connectors_run', arguments: { input: { flow: 'a', kind: 'result', result: 1, validation } } });
            assert.equal(response.isError, false); assert.equal(response.structuredContent.value.status, 'completed');
        }
        // Null retains admission's documented default validation object.
        assert.deepEqual(contexts, [42, 'context', ['context'], {}]);
    } finally { await client.close(); await server.close(); await runner.close(); }
});
