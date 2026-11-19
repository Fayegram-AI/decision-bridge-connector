import test from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { createMcpServer } from '../src/integrations/mcp-service.mjs';
import { createRunner } from './helpers/runner.mjs';
import { bounded } from '../src/util.mjs';

// Host cancellation and protocol cancellation must share the lifetime forwarded after authorization.
for (const action of ['run', 'submit']) for (const mode of ['host', 'request']) test(`MCP ${mode} cancellation reaches ${action} after authorization`, async () => {
    const host = new AbortController(); const request = new AbortController();
    const entered = Promise.withResolvers(); const released = Promise.withResolvers();
    let signal;
    const runner = { namespace: 'run-lifetime', ready: Promise.resolve({ a: {} }), get() {},
        async [action](_input, options) {
            signal = options.signal; entered.resolve();
            await released.promise;
            return { taskId: 'result' };
        } };
    const server = await createMcpServer({ runner, flows: ['a'], signal: host.signal,
        authorize: () => true, authorizationTimeoutMs: 1000 });
    const client = new Client({ name: 'run-lifetime', version: '1' });
    const [a, b] = InMemoryTransport.createLinkedPair();
    await server.connect(b); await client.connect(a);
    let call;
    try {
        call = client.callTool({ name: `connectors_${action}`, arguments: { input: { flow: 'a', kind: 'result', result: 1 } } }, undefined,
            { signal: request.signal });
        // Observe protocol cancellation before triggering it.
        const result = call.then(value => value, error => error);
        await entered.promise;
        if (mode === 'host') host.abort(); else request.abort();
        await new Promise(resolve => setImmediate(resolve));
        assert.equal(signal.aborted, true);
        released.resolve(); await result;
    } finally { released.resolve(); await call?.catch(() => {}); await client.close(); await server.close(); }
});

for (const action of ['run', 'submit']) for (const phase of action === 'run' ? ['admission', 'wait'] : ['admission'])
test(`MCP ${action} host cancellation during ${phase} preserves the admission boundary`, async () => {
    const host = new AbortController(); const entered = Promise.withResolvers(); const release = Promise.withResolvers();
    let taskId;
    const runner = createRunner({ validators: { v: { revision: '1', async validate(value) {
        if (phase === 'admission') { entered.resolve(); await release.promise; }
        return value;
    } } }, onEvent(event) { if (phase === 'wait' && event.type === 'task.admitted') { taskId = event.taskId; entered.resolve(); } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    if (phase === 'wait') await runner.pause('flow', 'a');
    const server = await createMcpServer({ runner, flows: ['a'], signal: host.signal });
    const client = new Client({ name: 'admission-lifetime', version: '1' });
    const [a, b] = InMemoryTransport.createLinkedPair(); await server.connect(b); await client.connect(a);
    let call;
    try {
        call = client.callTool({ name: `connectors_${action}`, arguments: { input: { flow: 'a', kind: 'result', result: 1 },
            ...(action === 'run' ? { options: { waitMs: 1000 } } : {}) } });
        await entered.promise; host.abort(); release.resolve();
        const result = await bounded(() => call, { timeoutMs: 500 });
        assert.equal(result.isError, true);
        if (phase === 'admission') assert.equal((await runner.list()).items.length, 0);
        else {
            assert.equal((await runner.get(taskId)).settled, false);
            await runner.pause('flow', 'a', false);
            assert.equal((await runner.execute(taskId)).status, 'completed');
        }
    } finally { release.resolve(); await call?.catch(() => {}); await client.close(); await server.close(); await runner.close(); }
});
