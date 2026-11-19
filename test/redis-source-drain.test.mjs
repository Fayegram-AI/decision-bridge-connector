import test from 'node:test';
import assert from 'node:assert/strict';
import { createRedisSource } from '../src/integrations/redis-source.mjs';
import { redisPeer } from './helpers/redis-peer.mjs';
import { bounded } from '../src/util.mjs';

const settings = { stream: 'events', group: 'workers', flow: 'a', inputMode: 'result' };

for (const settlement of ['acknowledge', 'quarantine', 'retry']) test(`owned Redis close drains yielded ${settlement}`, async () => {
    const peer = await redisPeer(); const source = createRedisSource({ ...settings, url: peer.url });
    const iterator = source.open(); let closing;
    try {
        const message = (await iterator.next()).value;
        assert.equal(message.input.result, 42);
        closing = source.close({ timeoutMs: 2000 }); let finished = false;
        closing.then(() => { finished = true; });
        assert.equal(source.close({ timeoutMs: 1 }), closing);
        await new Promise(resolve => setImmediate(resolve)); assert.equal(finished, false);
        if (settlement === 'acknowledge') assert.equal((await message.acknowledge()).status, 'acknowledged');
        else await message.reject(settlement === 'quarantine' ? { code: 'INVALID_JSON' } : { code: 'RESOURCE_LIMIT', retryable: true });
        await closing;
        assert.equal(peer.commands.filter(args => args[0] === 'XACK').length, settlement === 'retry' ? 0 : 1);
        assert.equal(peer.commands.filter(args => args[0] === 'XADD').length, settlement === 'quarantine' ? 1 : 0);
        await assert.rejects(message.acknowledge(), { code: 'SOURCE_CLOSED' });
    } finally { await closing?.catch(() => {}); await iterator.return(); await source.close().catch(() => {}); await peer.close(); }
});

for (const mode of ['timeout', 'abort']) test(`owned Redis ${mode} bounds an abandoned yielded message without ACK`, async () => {
    const peer = await redisPeer(); const source = createRedisSource({ ...settings, url: peer.url });
    const iterator = source.open(); const controller = new AbortController();
    try {
        const message = (await iterator.next()).value;
        const closing = source.close({ timeoutMs: mode === 'timeout' ? 20 : 2000, signal: controller.signal });
        const rejected = assert.rejects(closing, { code: mode === 'timeout' ? 'TIMEOUT' : 'ABORTED' });
        if (mode === 'abort') controller.abort();
        await bounded(() => rejected, { timeoutMs: 1000 });
        await assert.rejects(message.acknowledge(), { code: 'SOURCE_CLOSED' });
        assert.equal(peer.commands.some(args => args[0] === 'XACK'), false);
    } finally { await iterator.return(); await source.close().catch(() => {}); await peer.close(); }
});

test('owned Redis drain cancels an active acknowledgement at its close deadline', async () => {
    const entered = Promise.withResolvers(); const release = Promise.withResolvers();
    const peer = await redisPeer(args => { if (args[0] === 'XACK') { entered.resolve(); return release.promise; } });
    const source = createRedisSource({ ...settings, url: peer.url }); const iterator = source.open();
    try {
        const message = (await iterator.next()).value;
        const acknowledgement = message.acknowledge(); const outcome = acknowledgement.catch(error => error);
        await entered.promise;
        await assert.rejects(source.close({ timeoutMs: 20 }), { code: 'TIMEOUT' });
        assert.equal((await bounded(() => outcome, { timeoutMs: 1000 })).code, 'ABORTED');
        assert.equal(peer.commands.filter(args => args[0] === 'XACK').length, 1);
    } finally { release.resolve(1); await iterator.return(); await source.close().catch(() => {}); await peer.close(); }
});

test('real Redis owned-source acknowledgement finishes during close', { skip: !process.env.DBC_REDIS_URL }, async () => {
    const { createClient } = await import('redis'); const client = createClient({ url: process.env.DBC_REDIS_URL });
    client.on('error', () => {}); await client.connect();
    const stream = `dbc-drain-${crypto.randomUUID()}`;
    const source = createRedisSource({ ...settings, stream, url: process.env.DBC_REDIS_URL }); const iterator = source.open();
    try {
        await client.sendCommand(['XADD', stream, '*', 'payload', '42']);
        const message = (await iterator.next()).value;
        const closing = source.close({ timeoutMs: 2000 });
        assert.equal((await message.acknowledge()).status, 'acknowledged'); await closing;
        assert.equal((await client.sendCommand(['XPENDING', stream, settings.group]))[0], 0);
    } finally { await iterator.return(); await source.close().catch(() => {}); await client.del(stream); client.destroy(); }
});
