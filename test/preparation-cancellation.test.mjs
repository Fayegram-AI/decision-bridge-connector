import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createRunner, createMemoryStore, ConnectorError } from '../src/index.mjs';
import { invoke, createHttpServer } from '../src/node/http-service.mjs';
import { createMcpServer } from '../src/integrations/mcp-service.mjs';
import { bounded, delay } from '../src/util.mjs';

const actions = ['preview', 'reprocess', 'redeliver', 'reevaluate', 'resolveEvaluation'];

/** Keep validation pending so cancellation must settle without callback cooperation. */
async function fixture(action) {
    const entered = Promise.withResolvers(); const release = Promise.withResolvers();
    const store = createMemoryStore({ maxTransfers: 1 }); let gated = false; let validationSignal;
    const runner = createRunner({ store,
        evaluators: { e: { revision: '1', async validate(value) {
            if (gated && action === 'reevaluate') { entered.resolve(); await release.promise; }
            return value;
        }, execute() {
            if (action === 'resolveEvaluation') throw new ConnectorError('TIMEOUT', 'Synthetic uncertain evaluation.');
            return 1;
        } } },
        validators: { v: { revision: '1', async validate(value, { signal }) {
            if (gated) { validationSignal = signal; entered.resolve(); await release.promise; }
            return value;
        } } },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v', timeoutMs: 10000 } } });
    const original = await runner.run({ flow: 'a', kind: 'request', request: 1 }); gated = true;
    const args = action === 'preview' ? { input: { flow: 'a', kind: 'result', result: 1 } }
        : { taskId: original.taskId, options: { flow: 'a' },
            ...(action === 'resolveEvaluation' ? { resolution: { status: 'succeeded', result: 1, reason: 'Recovered evidence.' } } : {}) };
    return { runner, store, entered, release, args, get validationSignal() { return validationSignal; },
        call(signal, dispatch) {
            if (dispatch) return invoke(runner, action, args, { signal });
            if (action === 'preview') return runner.preview(args.input, { signal });
            if (action === 'resolveEvaluation') return runner.resolveEvaluation(args.taskId, args.resolution, { signal });
            return runner[action](args.taskId, { ...args.options, signal });
        }, async close() { release.resolve(); await runner.close({ cancel: true }); await store.close(); } };
}

for (const action of actions) for (const dispatch of [false, true])
test(`${dispatch ? 'transport dispatch' : 'SDK'} cancels ${action} before committing prepared work`, async () => {
    const f = await fixture(action); const controller = new AbortController(); let pending;
    try {
        pending = f.call(controller.signal, dispatch); pending.catch(() => {});
        await bounded(() => Promise.race([f.entered.promise, pending]), { timeoutMs: 2000 });
        controller.abort();
        await bounded(() => assert.rejects(pending, { code: 'ABORTED' }), { timeoutMs: 1000 });
        if (action !== 'reevaluate') assert.equal(f.validationSignal.aborted, true);
        assert.equal((await f.runner.list()).items.length, 1);
        if (action === 'resolveEvaluation') assert.equal((await f.runner.get(f.args.taskId)).evaluation.status, 'unknown');
        // A cancelled preview must leave the single lease slot usable.
        if (action === 'preview') {
            f.release.resolve();
            const next = await f.runner.preview(f.args.input);
            await f.runner.closeSnapshot(next.snapshot.id);
        }
    } finally { f.release.resolve(); await pending?.catch(() => {}); await f.close(); }
});

test('preparation APIs reject pre-aborted and invalid signals before validation or writes', async () => {
    for (const action of actions) {
        const f = await fixture(action);
        try {
            await assert.rejects(f.call(AbortSignal.abort(), false), { code: 'ABORTED' });
            await assert.rejects(f.call({ aborted: false }, false), { code: 'CONFIGURATION_ERROR' });
            assert.equal(f.validationSignal, undefined);
            assert.equal((await f.runner.list()).items.length, 1);
        } finally { await f.close(); }
    }
});

test('HTTP disconnection cancels real preview validation and leaves no captured lease', async () => {
    const f = await fixture('preview'); const request = new AbortController();
    const disconnected = Promise.withResolvers();
    const server = createHttpServer({ runner: f.runner, flow: 'a', reply: 'complete' });
    server.on('connection', socket => socket.once('close', () => disconnected.resolve()));
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const response = fetch(`http://127.0.0.1:${server.address().port}/operations/preview`, {
        method: 'POST', body: JSON.stringify(f.args), signal: request.signal,
    }).then(value => value.text(), () => null);
    try {
        await bounded(() => f.entered.promise, { timeoutMs: 2000 }); request.abort();
        await bounded(() => disconnected.promise, { timeoutMs: 1000 });
        await bounded(async signal => { while (!f.validationSignal.aborted) await delay(1, signal); }, { timeoutMs: 1000 });
        f.release.resolve(); await response;
        const next = await f.runner.preview(f.args.input); await f.runner.closeSnapshot(next.snapshot.id);
    } finally { request.abort(); f.release.resolve(); await response; await server.drain(); await f.close(); }
});

test('official MCP request cancellation reaches real preview validation', async () => {
    const [{ Client }, { InMemoryTransport }] = await Promise.all([
        import('@modelcontextprotocol/sdk/client/index.js'), import('@modelcontextprotocol/sdk/inMemory.js'),
    ]);
    const f = await fixture('preview'); const request = new AbortController();
    const server = await createMcpServer({ runner: f.runner, flows: ['a'] });
    const client = new Client({ name: 'preview-cancellation', version: '1' });
    const [a, b] = InMemoryTransport.createLinkedPair(); await server.connect(b); await client.connect(a);
    let pending;
    try {
        pending = client.callTool({ name: 'connectors_preview', arguments: f.args }, undefined, { signal: request.signal });
        pending.catch(() => {}); await bounded(() => f.entered.promise, { timeoutMs: 2000 }); request.abort();
        await bounded(async signal => { while (!f.validationSignal.aborted) await delay(1, signal); }, { timeoutMs: 1000 });
        f.release.resolve(); await pending.catch(() => {});
        const next = await f.runner.preview(f.args.input); await f.runner.closeSnapshot(next.snapshot.id);
    } finally { request.abort(); f.release.resolve(); await pending?.catch(() => {}); await client.close(); await server.close(); await f.close(); }
});

test('failed or cancelled post-capture previews release their transfer quota', async () => {
    for (const mode of ['read-failure', 'cancel']) {
        const store = createMemoryStore({ maxTransfers: 1 }); const controller = new AbortController();
        const captured = Promise.withResolvers(); const release = Promise.withResolvers(); let fault = true;
        const runner = createRunner({ store: { ...store,
            async openPreview(...args) {
                const snapshot = await store.openPreview(...args);
                if (mode === 'cancel' && fault) { fault = false; captured.resolve(); await release.promise; }
                return snapshot;
            }, async snapshotRecord(...args) {
                if (mode === 'read-failure' && fault) { fault = false; throw new ConnectorError('READ_FAULT', 'Synthetic projection failure.'); }
                return store.snapshotRecord(...args);
            } }, validators: { v: { revision: '1', validate: value => value } },
            flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
        const input = { flow: 'a', kind: 'result', result: 1 }; let pending;
        try {
            pending = runner.preview(input, { signal: controller.signal }); pending.catch(() => {});
            if (mode === 'cancel') { await captured.promise; controller.abort(); release.resolve(); }
            await assert.rejects(pending, { code: mode === 'cancel' ? 'ABORTED' : 'READ_FAULT' });
            const next = await runner.preview(input); await runner.closeSnapshot(next.snapshot.id);
        } finally { release.resolve(); await pending?.catch(() => {}); await runner.close({ cancel: true }); await store.close(); }
    }
});
