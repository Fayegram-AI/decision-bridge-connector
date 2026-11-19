import test from 'node:test';
import assert from 'node:assert/strict';
import { attachLiveController, createLiveSource } from '../src/integrations/live.mjs';

const runner = { ready: Promise.resolve({ a: { inputs: ['result'] } }), run() {} };
const controller = () => ({ subscribe: () => () => {}, dispose() {} });

test('Live disposes a connection when its deadline expires during ownership handoff', async () => {
    const failed = Promise.withResolvers(); let closes = 0; let disposed = 0;
    const source = createLiveSource({ flow: 'a', sessionId: 'handoff', reconnect: { maxAttempts: 1, timeoutMs: 10 },
        onError: () => { failed.resolve(); }, connect({ signal }) {
            const end = performance.now() + 25;
            while (performance.now() < end) { /* Model a synchronous factory consuming the attempt budget. */ }
            assert.equal(signal.aborted, false);
            return { controller: { ...controller(), dispose() { disposed++; } }, closed: new Promise(() => {}), close() { closes++; } };
        } });
    await source.start({ runner }); await failed.promise; await source.close();
    assert.equal(closes, 1); assert.equal(disposed, 1);
});

test('Live startup disposes connections returned after timeout or cancellation', async t => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    for (const mode of ['timeout', 'cancelled']) {
        const entered = Promise.withResolvers(); const release = Promise.withResolvers(); const disconnected = Promise.withResolvers();
        let closes = 0; let disposed = 0;
        const source = createLiveSource({ flow: 'a', sessionId: mode, reconnect: { maxAttempts: 1, timeoutMs: 25 },
            connect({ signal }) { entered.resolve(signal); return release.promise; } });
        await source.start({ runner }); const signal = await entered.promise;
        if (mode === 'timeout') {
            t.mock.timers.tick(26); await new Promise(resolve => setImmediate(resolve));
            assert.equal(source.status().state, 'exhausted');
        }
        await source.close(); assert.equal(signal.aborted, true);
        release.resolve({ controller: { ...controller(), dispose() { disposed++; } }, closed: disconnected.promise, close() {
            closes++; disconnected.reject(new Error('Transport closed after startup expired.'));
        } });
        await new Promise(resolve => setImmediate(resolve));
        assert.equal(closes, 1); assert.equal(disposed, 1); await source.close(); assert.equal(closes, 1);
    }
});

test('repeated Live source close calls wait for the same in-progress shutdown', async () => {
    const connected = Promise.withResolvers(); const closing = Promise.withResolvers(); const release = Promise.withResolvers();
    let closes = 0; let disposed = 0;
    const source = createLiveSource({ flow: 'a', sessionId: 'shared-close', reconnect: { timeoutMs: 5000 }, async connect() {
        connected.resolve(); return { controller: { ...controller(), dispose() { disposed++; } }, closed: new Promise(() => {}),
            async close() { closes++; closing.resolve(); await release.promise; } };
    } });
    await source.start({ runner }); await connected.promise;
    // Allow attachment before requesting shutdown; the factory's own promise resolves first.
    await new Promise(resolve => setImmediate(resolve));
    const first = source.close(); await closing.promise; const second = source.close(); let finished = false;
    second.then(() => { finished = true; });
    try {
        assert.equal(first, second); await new Promise(resolve => setImmediate(resolve));
        assert.equal(finished, false); assert.equal(closes, 1);
    } finally { release.resolve(); await Promise.all([first, second]); }
    assert.equal(disposed, 1); assert.equal(closes, 1);
});

test('Live source rejects invalid reconnect settings before calling its factory', () => {
    let calls = 0; const options = { flow: 'a', sessionId: 'bounded', connect: async () => { calls++; return {}; } };
    for (const reconnect of [{ timeoutMs: 0 }, { timeoutMs: 2147483648 }, { delayMs: 2147483648 }, { maxDelayMs: 2147483648 },
        { maxAttempts: 0 }, { timeotMs: 10 }]) assert.throws(() => createLiveSource({ ...options, reconnect }), { code: 'CONFIGURATION_ERROR' });
    assert.throws(() => createLiveSource({ ...options, sessionId: '' }), { code: 'CONFIGURATION_ERROR' });
    assert.equal(calls, 0);
});

test('Live attachment attempts every owned cleanup and preserves aggregate causes', async () => {
    const calls = []; const failures = ['unsubscribe', 'dispose', 'connection'].map(name => new Error(name));
    const attachment = attachLiveController({ runner, flow: 'a', ownsController: true, ownsConnection: true,
        controller: { subscribe() { return async () => { calls.push('unsubscribe'); throw failures[0]; }; },
            dispose() { calls.push('dispose'); throw failures[1]; } },
        closeConnection() { calls.push('connection'); throw failures[2]; } });
    const closing = attachment.stop(); assert.equal(attachment.stop(), closing);
    await assert.rejects(closing, error => error instanceof AggregateError &&
        error.code === 'RESOURCE_CLOSE_FAILED' && error.errors.every((cause, index) => cause === failures[index]));
    assert.deepEqual(calls, ['unsubscribe', 'dispose', 'connection']);
});

test('Live source reports sanitized disposal failures and rejects repeated close with the same causes', async () => {
    const subscribed = Promise.withResolvers(); const events = []; const calls = [];
    const source = createLiveSource({ flow: 'a', sessionId: 'failed-cleanup', onError: error => events.push(error),
        async connect() { return { controller: {
            subscribe() { subscribed.resolve(); return () => { calls.push('unsubscribe'); }; },
            dispose() { calls.push('dispose'); throw new Error('private-controller-detail'); },
        }, closed: new Promise(() => {}), close() { calls.push('connection'); throw new Error('private-transport-detail'); } }; } });
    await source.start({ runner }); await subscribed.promise;
    const closing = source.close(); assert.equal(source.close(), closing);
    await assert.rejects(closing, error => error instanceof AggregateError && error.errors.length === 2);
    assert.deepEqual(calls, ['unsubscribe', 'dispose', 'connection']); assert.equal(events.length, 2);
    assert(events.every(error => error.code === 'RESOURCE_CLOSE_FAILED' && error.stage === 'live-close'));
    assert(!JSON.stringify(events).includes('private-'));
});

test('Live reports cleanup failures from a factory that returns after source closure', async () => {
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); const reported = Promise.withResolvers();
    let closes = 0;
    const source = createLiveSource({ flow: 'a', sessionId: 'late-failed-cleanup',
        connect() { entered.resolve(); return release.promise; }, onError: error => reported.resolve(error) });
    await source.start({ runner }); await entered.promise; await source.close();
    release.resolve({ controller: controller(), closed: new Promise(() => {}), close() { closes++; throw new Error('private-late-close'); } });
    const error = await reported.promise; assert.equal(error.code, 'RESOURCE_CLOSE_FAILED'); assert.equal(closes, 1);
    assert(!JSON.stringify(error).includes('private-')); await source.close();
});
