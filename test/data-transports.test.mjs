import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createExecutionStore } from '../src/storage/store.mjs';
import { createRunner } from '../src/engine/runner.mjs';
import { createHttpServer } from '../src/node/http-service.mjs';
import { createMcpServer } from '../src/integrations/mcp-service.mjs';

const setup = () => ({ validators: { v: { revision: '1', validate: value => value } }, flows: {
    a: { revision: '1', inputs: ['result'], validator: 'v' }, b: { revision: '1', inputs: ['result'], validator: 'v' },
} });

test('HTTP streams archives independently of JSON-body limits and enforces captured flow authority', async () => {
    const runner = createRunner(setup()); const actions = [];
    const options = { runner, flow: 'a', reply: 'complete', allowAdmin: true, maxBytes: 1024, replyTimeoutMs: 10000,
        authorizationTimeoutMs: 1000, authorize: ({ action }) => { actions.push(action); return true; } };
    const server = createHttpServer(options); const other = createHttpServer({ ...options, flow: 'b' });
    server.listen(0, '127.0.0.1'); other.listen(0, '127.0.0.1'); await Promise.all([once(server, 'listening'), once(other, 'listening')]);
    const root = `http://127.0.0.1:${server.address().port}`; const foreign = `http://127.0.0.1:${other.address().port}`;
    const post = (url, body) => fetch(url, { method: 'POST', body: JSON.stringify(body), headers: { 'content-type': 'application/json' } });
    try {
        const task = await runner.run({ flow: 'a', kind: 'result', result: { text: 'é猫🌉'.repeat(25000) } });
        const response = await fetch(`${root}/tasks/${task.taskId}/export`);
        assert.equal(response.status, 200); assert.match(response.headers.get('content-type'), /ndjson/);
        const archive = new Uint8Array(await response.arrayBuffer()); assert(archive.length > 1024);
        const imported = await fetch(`${root}/tasks/import`, { method: 'POST', body: archive, headers: { 'content-type': 'application/x-ndjson' } });
        assert.equal(imported.status, 202); const summary = (await imported.json()).value;
        assert.notEqual(summary.taskId, task.taskId); assert.equal(summary.evaluation.hasResult, true); assert(!Object.hasOwn(summary.evaluation, 'result'));
        assert(actions.includes('exportTask')); assert(actions.includes('importTask'));
        assert.equal((await post(root + '/operations/importTask', { value: {}, options: { flow: 'a' } })).status, 404);
        const lease = (await (await post(`${root}/operations/openSnapshot`, { taskId: task.taskId })).json()).value;
        const forbidden = await post(`${foreign}/operations/listSnapshot`, { snapshotId: lease.id });
        assert.equal(forbidden.status, 404);
        assert.equal((await fetch(`${foreign}/tasks/${task.taskId}/export`)).status, 404);
        assert.equal((await fetch(`${root}/tasks/${task.taskId}/export`, { headers: { origin: 'https://untrusted.example' } })).status, 403);
        const page = await post(`${root}/operations/listSnapshot`, { snapshotId: lease.id, options: { collection: 'data' } });
        assert.equal(page.status, 200); assert((await page.json()).value.items.length > 0);
        await runner.closeSnapshot(lease.id);
    } finally { await server.drain(); await other.drain(); await runner.close(); }
});

test('MCP exposes paged transfer sessions and rejects cross-flow snapshot handles', async () => {
    const [{ Client }, { InMemoryTransport }] = await Promise.all([import('@modelcontextprotocol/sdk/client/index.js'), import('@modelcontextprotocol/sdk/inMemory.js')]);
    const runner = createRunner(setup()); const server = await createMcpServer({ runner, flows: ['a'], allowAdmin: true });
    const client = new Client({ name: 'data-session', version: '1' }); const [left, right] = InMemoryTransport.createLinkedPair();
    await server.connect(right); await client.connect(left);
    const call = async (action, args) => {
        const result = await client.callTool({ name: `connectors_${action}`, arguments: args });
        assert.equal(result.isError, false, JSON.stringify(result.structuredContent)); return result.structuredContent.value;
    };
    try {
        const tools = (await client.listTools()).tools.map(tool => tool.name);
        assert(tools.includes('connectors_readExportFrames')); assert(tools.includes('connectors_appendImport'));
        assert(!tools.includes('connectors_exportTask')); assert(!tools.includes('connectors_importTask'));
        const task = await runner.run({ flow: 'a', kind: 'result', result: { tool: true } });
        const lease = await call('openSnapshot', { taskId: task.taskId }); const transfer = await call('openImport', { options: { flow: 'a' } });
        let after;
        do {
            const page = await call('readExportFrames', { snapshotId: lease.id, options: { ...(after === undefined ? {} : { after }), limit: 1 } });
            for (const frame of page.frames) await call('appendImport', { transferId: transfer.id, frame });
            after = page.next;
        } while (after);
        const imported = await call('finishImport', { transferId: transfer.id }); assert.notEqual(imported.taskId, task.taskId);
        await call('closeImport', { transferId: transfer.id }); await call('closeSnapshot', { snapshotId: lease.id });
        const other = await runner.submit({ flow: 'b', kind: 'result', result: 1 }); const foreign = await runner.openSnapshot(other.taskId);
        const denied = await client.callTool({ name: 'connectors_listSnapshot', arguments: { snapshotId: foreign.id } });
        assert.equal(denied.isError, true); assert.equal(denied.structuredContent.error.code, 'NOT_FOUND'); await runner.closeSnapshot(foreign.id);
    } finally { await client.close(); await server.close(); await runner.close(); }
});

test('authorization reads only summary metadata even when SDK inline reports are enabled', async () => {
    const raw = createMemoryRepository(); let chunks = 0;
    const store = createExecutionStore({ ...raw, transaction: callback => raw.transaction(tx => callback({ ...tx, get(table, ...args) {
        if (table === 'chunks') chunks++; return tx.get(table, ...args);
    } })) });
    const runner = createRunner({ ...setup(), store, report: { includeData: true } });
    const server = createHttpServer({ runner, flow: 'a', reply: 'complete', authorizationTimeoutMs: 1000, authorize: () => false });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    try {
        const task = await runner.submit({ flow: 'a', kind: 'result', result: { private: true } }, { report: {} }); chunks = 0;
        const response = await fetch('http://127.0.0.1:' + server.address().port + '/tasks/' + task.taskId);
        assert.equal(response.status, 403); assert.equal(chunks, 0);
    } finally { await server.drain(); await runner.close(); await store.close(); }
});
