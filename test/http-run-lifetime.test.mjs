import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createHttpServer } from '../src/node/http-service.mjs';
import { createMemoryStore } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { bounded } from '../src/util.mjs';

// Complete task replies and explicit run operations share the HTTP request lifetime.
for (const action of ['run', 'submit']) for (const mode of ['host', 'disconnect', 'drain'])
for (const phase of action === 'run' ? ['validation', 'wait'] : ['validation'])
test(`HTTP ${mode} cancellation during ${phase} reaches ${action} without losing admitted work`, async () => {
    const host = new AbortController(); const request = new AbortController();
    const entered = Promise.withResolvers(); const release = Promise.withResolvers();
    const finished = Promise.withResolvers(); const disconnected = Promise.withResolvers();
    const store = createMemoryStore(); let taskId; let runSignal;
    const runner = createRunner({ store, validators: { v: { revision: '1', async validate(value) {
        if (phase === 'validation') { entered.resolve(); await release.promise; }
        return value;
    } } }, onEvent(event) { if (phase === 'wait' && event.type === 'task.admitted') { taskId = event.taskId; entered.resolve(); } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    if (phase === 'wait') await runner.pause('flow', 'a');
    const server = createHttpServer({ runner: { ...runner, async [action](input, options) {
        runSignal = options.signal;
        try { return await runner[action](input, options); } finally { finished.resolve(); }
    } }, flow: 'a', reply: 'complete', signal: host.signal, authorize: () => true, authorizationTimeoutMs: 1000 });
    server.on('connection', socket => socket.once('close', () => disconnected.resolve()));
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const path = action === 'submit' ? '/operations/submit' : phase === 'validation' ? '/tasks' : '/operations/run';
    const input = { flow: 'a', kind: 'result', result: 1 };
    const body = path === '/tasks' ? input : { input, ...(action === 'run' ? { options: { waitMs: 1000 } } : {}) };
    const response = fetch(`http://127.0.0.1:${server.address().port}${path}`, {
        method: 'POST', body: JSON.stringify(body), signal: request.signal,
    }).then(value => value.text(), () => null);
    let draining;
    try {
        await bounded(() => entered.promise, { timeoutMs: 5000 });
        if (mode === 'host') host.abort();
        else if (mode === 'drain') draining = server.drain();
        else { request.abort(); await bounded(() => disconnected.promise, { timeoutMs: 1000 }); }
        assert.equal(runSignal?.aborted, true);
        release.resolve();
        await bounded(() => finished.promise, { timeoutMs: 500 });
        await response;
        if (phase === 'validation') assert.equal((await runner.list()).items.length, 0);
        else {
            assert.equal((await runner.get(taskId)).settled, false);
            await runner.pause('flow', 'a', false);
            assert.equal((await runner.execute(taskId)).status, 'completed');
        }
    } finally {
        release.resolve(); request.abort();
        await runner.pause('flow', 'a', false);
        await bounded(() => finished.promise, { timeoutMs: 2500 });
        await response; await draining; await server.drain(); await runner.close(); await store.close();
    }
});
