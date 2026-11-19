import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { mkdtemp, mkdir, readFile, readdir, rm, utimes, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createDurableAggregator, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSpoolSource } from '../src/node/incoming.mjs';
import { createRedisSource } from '../src/integrations/redis-source.mjs';

test('spool recovery and rejection preserve a newer file with the same name', { timeout: 10000 }, async () => {
    for (const rejected of [false, true]) {
        const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-collision-')); const controller = new AbortController();
        const source = createSpoolSource({ directory, flow: 'a', inputMode: 'result', leaseMs: 30, pollMs: 1 });
        let iterator = source.open({ signal: controller.signal });
        try {
            if (rejected) {
                await writeFile(join(directory, 'event.json'), '1');
                const old = (await iterator.next()).value;
                await writeFile(join(directory, 'event.json'), '2');
                await old.reject({ code: 'RESOURCE_LIMIT' });
                assert.equal(await readFile(join(directory, 'event.json'), 'utf8'), '2');
                await iterator.return(); await sleep(50);
                iterator = createSpoolSource({ directory, flow: 'a', inputMode: 'result', leaseMs: 30, pollMs: 1 }).open({ signal: controller.signal });
            } else {
                await mkdir(join(directory, '.processing'));
                const claim = join(directory, '.processing', 'event.json~old');
                await writeFile(claim, '1'); await utimes(claim, new Date(0), new Date(0));
                await writeFile(join(directory, 'event.json'), '2');
            }
            const newer = (await iterator.next()).value;
            assert.equal(newer.input.result, 2); await newer.acknowledge();
            const older = (await iterator.next()).value;
            assert.equal(older.input.result, 1); await older.acknowledge();
            assert.equal((await readdir(join(directory, '.processed'))).length, 2);
            assert.equal((await readdir(join(directory, '.quarantine'))).length, 0);
        } finally { controller.abort(); await iterator.return(); await rm(directory, { recursive: true }); }
    }
});

test('spool preserves transient admission failures and quarantines malformed JSON', { timeout: 10000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-transient-')); const controller = new AbortController();
    const source = createSpoolSource({ directory, flow: 'a', inputMode: 'result', pollMs: 1 });
    const iterator = source.open({ signal: controller.signal });
    try {
        await writeFile(join(directory, 'bad.json'), '{'); await writeFile(join(directory, 'valid.json'), '1');
        const message = (await iterator.next()).value;
        assert.equal(message.input.result, 1);
        await message.reject({ code: 'UNEXPECTED_ERROR', message: 'Temporary database failure.' });
        assert.equal(await readFile(join(directory, 'valid.json'), 'utf8'), '1');
        assert.equal((await readdir(join(directory, '.quarantine'))).length, 1);
    } finally { controller.abort(); await iterator.return(); await rm(directory, { recursive: true }); }
});

test('spool quarantines invalid submission envelopes and continues to the next input', { timeout: 10000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-envelope-')); const controller = new AbortController();
    const source = createSpoolSource({ directory, flow: 'a', pollMs: 1 });
    const iterator = source.open({ signal: controller.signal });
    try {
        await writeFile(join(directory, 'a-invalid.json'), 'null');
        await writeFile(join(directory, 'b-valid.json'), JSON.stringify({ kind: 'result', result: null }));
        const message = (await iterator.next()).value;
        assert.equal(message.input.result, null); await message.acknowledge();
        assert.equal((await readdir(join(directory, '.quarantine'))).length, 1);
        assert.equal((await readdir(join(directory, '.processed'))).length, 1);
    } finally { controller.abort(); await iterator.return(); await rm(directory, { recursive: true }); }
});

test('Redis keeps transient admission failures pending for recovery', { skip: !process.env.DBC_REDIS_URL, timeout: 10000 }, async () => {
    const { createClient } = await import('redis'); const client = createClient({ url: process.env.DBC_REDIS_URL });
    client.on('error', () => {}); await client.connect();
    const stream = `dbc-recovery-${crypto.randomUUID()}`; const controller = new AbortController();
    const source = createRedisSource({ client, stream, group: 'test', flow: 'a', inputMode: 'result', pollMs: 1 });
    const iterator = source.open({ signal: controller.signal });
    try {
        await client.sendCommand(['XADD', stream, '*', 'payload', '1']);
        const message = (await iterator.next()).value;
        for (const error of [{ code: 'UNEXPECTED_ERROR' }, { code: 'TIMEOUT', retryable: true }, { code: 'CONFIGURATION_ERROR', ambiguous: true }]) {
            await message.reject(error);
            assert.equal((await client.sendCommand(['XPENDING', stream, 'test']))[0], 1);
            assert.equal(await client.sendCommand(['XLEN', `${stream}:quarantine`]), 0);
        }
        await message.reject({ code: 'INVALID_RESULT' });
        assert.equal((await client.sendCommand(['XPENDING', stream, 'test']))[0], 0);
        assert.equal(await client.sendCommand(['XLEN', `${stream}:quarantine`]), 1);
    } finally {
        controller.abort(); await iterator.return(); await source.close();
        await client.del([stream, `${stream}:quarantine`]); client.destroy();
    }
});

function aggregateRunner() {
    return createRunner({ evaluators: { e: { revision: '1', execute: async value => value } },
        flows: { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
}

test('aggregation does not replace un-emitted evidence after the deduplication interval', async () => {
    const runner = aggregateRunner(); let reject = true;
    const aggregate = createDurableAggregator({ runner: { ...runner, submit(...args) {
        if (reject) throw new ConnectorError('RESOURCE_LIMIT', 'Temporarily full');
        return runner.submit(...args);
    } }, rule: { id: 'one', revision: '1', sources: ['one'], windowMs: 10, dedupeMs: 10, flow: 'a' } });
    try {
        await assert.rejects(aggregate.add({ source: 'one', eventId: 'old', correlationKey: 'case', event: 1 }), { code: 'RESOURCE_LIMIT' });
        await sleep(25); reject = false;
        await assert.rejects(aggregate.add({ source: 'one', eventId: 'new', correlationKey: 'case', event: 2 }), { code: 'LATE_EVENT' });
        await aggregate.flush(); const [task] = (await runner.list()).items;
        const completed = await runner.execute(task.taskId);
        assert.equal(completed.evaluation.result.events.one.eventId, 'old');
        assert.equal(completed.evaluation.result.events.one.event, 1);
    } finally { await runner.close(); }
});

test('a late emission completion cannot mark a replacement aggregation group emitted', { timeout: 10000 }, async () => {
    const runner = aggregateRunner(); const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let calls = 0;
    const aggregate = createDurableAggregator({ runner: { ...runner, async submit(...args) {
        if (++calls === 1) { entered.resolve(); await release.promise; }
        return runner.submit(...args);
    } }, rule: { id: 'pair', revision: '1', sources: ['a', 'b'], windowMs: 10000, dedupeMs: 10, flow: 'a' } });
    let delayed;
    try {
        await aggregate.add({ source: 'a', eventId: 'a1', correlationKey: 'case', event: 1 });
        const old = { source: 'b', eventId: 'b1', correlationKey: 'case', event: 1 };
        delayed = aggregate.add(old); const observed = delayed.catch(error => error); await entered.promise;
        await aggregate.add(old); await sleep(25);
        const newer = { source: 'a', eventId: 'a2', correlationKey: 'case', event: 2 };
        const replacement = await aggregate.add(newer); assert.equal(replacement.emitted, false);
        release.resolve(); assert.equal((await observed).code, 'CONFLICT');
        const current = await aggregate.add(newer);
        assert.equal(current.aggregateId, replacement.aggregateId); assert.equal(current.emitted, false);
        await aggregate.add({ source: 'b', eventId: 'b2', correlationKey: 'case', event: 2 });
        assert.equal((await runner.list()).items.length, 2);
    } finally { release.resolve(); await delayed?.catch(() => {}); await runner.close(); }
});

test('a stale aggregation flush snapshot cannot close a replacement window early', async () => {
    const runner = aggregateRunner(); let replace;
    const aggregate = createDurableAggregator({ runner: { ...runner, store: { ...runner.store, async dueAggregates(...args) {
        const rows = await runner.store.dueAggregates(...args);
        if (replace) { const action = replace; replace = null; await action(); }
        return rows;
    } } }, rule: { id: 'pair', revision: '1', sources: ['a', 'b'], windowMs: 10000, dedupeMs: 10, incomplete: 'emit', flow: 'a' } });
    try {
        let previous;
        for (const source of ['a', 'b']) previous = await aggregate.add({ source, eventId: `${source}1`, correlationKey: 'case', event: 1 });
        // Simulate a crash after child admission but before its emission marker was saved.
        await runner.store.aggregate(runner.namespace, previous.key, current => ({ ...current, emitted: false, expiresAt: 0 }));
        const newer = { source: 'a', eventId: 'a2', correlationKey: 'case', event: 2 };
        replace = async () => {
            await aggregate.add({ source: 'b', eventId: 'b1', correlationKey: 'case', event: 1 });
            await sleep(25); await aggregate.add(newer);
        };
        assert.deepEqual(await aggregate.flush(), []);
        const current = await aggregate.add(newer);
        assert.equal(current.state, 'pending'); assert.equal(current.emitted, false);
        assert.equal((await runner.list()).items.length, 1);
    } finally { await runner.close(); }
});
