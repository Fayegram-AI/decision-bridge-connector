import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createRedisSource } from '../src/integrations/redis-source.mjs';
import { bounded } from '../src/util.mjs';

const settings = { stream: 'events', group: 'workers', flow: 'a', inputMode: 'result', pollMs: 1 };
const validReply = () => [['events', [['1-0', ['payload', '1']]]]];
const reply = command => command === 'XGROUP' ? 'OK' : command === 'XAUTOCLAIM' ? ['0-0', []] : validReply();

test('Redis source rejects a quarantine destination equal to its input before issuing commands', () => {
    let commands = 0;
    assert.throws(() => createRedisSource({ ...settings, quarantine: 'events', client: {
        async sendCommand() { commands++; },
    } }), { code: 'CONFIGURATION_ERROR' });
    assert.equal(commands, 0);
});

// Noncooperative clients must not extend the source lifetime or emit late batches.
for (const phase of ['XGROUP', 'XAUTOCLAIM', 'XREADGROUP']) for (const mode of ['abort', 'close'])
test(`Redis ${mode} cancels pending ${phase} while retaining its borrowed client`, async () => {
    const controller = new AbortController(); const entered = Promise.withResolvers(); const release = Promise.withResolvers();
    let commandSignal; let closed = 0;
    const source = createRedisSource({ ...settings, client: { async sendCommand(args, options) {
        if (args[0] === phase) { commandSignal = options.abortSignal; entered.resolve(); await release.promise; }
        return reply(args[0]);
    }, destroy() { closed++; }, close() { closed++; } } });
    const iterator = source.open({ signal: controller.signal });
    const pending = iterator.next(); const outcome = pending.then(value => value, error => error);
    try {
        await bounded(() => entered.promise, { timeoutMs: 5000 });
        if (mode === 'abort') controller.abort(); else await source.close();
        const stopped = await bounded(() => outcome, { timeoutMs: 500 });
        assert.equal(stopped.code, 'ABORTED');
        assert.equal(commandSignal.aborted, true);
        assert.equal(closed, 0);
    } finally { controller.abort(); release.resolve(); await outcome; await iterator.return(); await source.close(); }
});

test('closed Redis sources reject reopening before using a borrowed client', async () => {
    let commands = 0;
    const source = createRedisSource({ ...settings, client: { async sendCommand(args) { commands++; return reply(args[0]); } } });
    await source.close(); await source.close();
    const iterator = source.open({ signal: new AbortController().signal });
    try { await assert.rejects(iterator.next(), { code: 'SOURCE_CLOSED' }); assert.equal(commands, 0); }
    finally { await iterator.return(); }
});

test('Redis source admits one consumer per instance before starting another read', async () => {
    let commands = 0;
    const source = createRedisSource({ ...settings, client: { async sendCommand(args) { commands++; return reply(args[0]); } } });
    const controller = new AbortController();
    const first = source.open({ signal: controller.signal }); const second = source.open({ signal: controller.signal });
    try {
        assert.equal((await first.next()).value.input.result, 1);
        const before = commands;
        await assert.rejects(second.next(), { code: 'SOURCE_BUSY' }); assert.equal(commands, before);
    } finally { controller.abort(); await first.return(); await second.return(); await source.close(); }
});

test('Redis cancellation leaves the rest of a read batch pending and preserves yielded acknowledgement', async () => {
    const commands = []; const controller = new AbortController();
    const source = createRedisSource({ ...settings, client: { async sendCommand(args) {
        commands.push(args);
        if (args[0] !== 'XREADGROUP') return args[0] === 'XACK' ? 1 : reply(args[0]);
        return [['events', [['1-0', ['payload', '1']], ['2-0', ['payload', 'invalid']], ['3-0', ['payload', '2']]]]];
    } } });
    const iterator = source.open({ signal: controller.signal });
    try {
        const first = (await iterator.next()).value; controller.abort(); await source.close();
        assert.equal((await first.acknowledge()).status, 'acknowledged');
        assert.equal((await iterator.next()).done, true);
        assert.deepEqual(commands.filter(args => ['XADD', 'XACK'].includes(args[0])), [['XACK', 'events', 'workers', '1-0']]);
    } finally { controller.abort(); await iterator.return(); await source.close(); }
});

test('owned Redis source close stops real polling and fences a later open', { skip: !process.env.DBC_REDIS_URL }, async () => {
    const { createClient } = await import('redis'); const client = createClient({ url: process.env.DBC_REDIS_URL });
    client.on('error', () => {}); await client.connect();
    const stream = `dbc-owned-source-${crypto.randomUUID()}`; const controller = new AbortController();
    const source = createRedisSource({ ...settings, url: process.env.DBC_REDIS_URL, stream });
    const iterator = source.open({ signal: controller.signal });
    const pending = iterator.next(); const outcome = pending.then(value => value, error => error);
    try {
        await bounded(async () => {
            while (!(await client.sendCommand(['XINFO', 'GROUPS', stream]).catch(() => [])).length) await sleep(2);
        }, { timeoutMs: 5000 });
        await source.close();
        assert.equal((await bounded(() => outcome, { timeoutMs: 1000 })).code, 'ABORTED');
        await source.close();
        const reopened = source.open({ signal: controller.signal });
        await assert.rejects(reopened.next(), { code: 'SOURCE_CLOSED' });
        assert.equal(await client.ping(), 'PONG');
    } finally { controller.abort(); await source.close(); await outcome; await iterator.return(); await client.del(stream); client.destroy(); }
});
