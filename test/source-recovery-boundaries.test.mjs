import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile, utimes } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createIterableSource, consumeSource, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSpoolSource } from '../src/node/spool.mjs';
import { bounded } from '../src/util.mjs';

test('opaque source-open and iteration errors become reported outcomes with cleanup', async () => {
    const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    try {
        for (const stage of ['open', 'next']) {
            let closed = 0; let returned = 0;
            const failure = Object.defineProperty(new ConnectorError('BROKEN', 'Private detail'), 'code', { get() { throw new Error('Unreadable'); } });
            const source = createIterableSource({ [Symbol.asyncIterator]() { return this; },
                async next() { throw failure; }, async return() { returned++; return { done: true }; } }, { name: 'opaque' });
            const result = await consumeSource(runner, { ...source,
                open: stage === 'open' ? async () => { throw failure; } : source.open,
                async close() { closed++; await source.close(); },
            });
            assert.equal(result.stopped, 'failed'); assert.equal(result.sourceErrors, 1);
            assert.equal(result.errors[0].code, 'UNEXPECTED_ERROR'); assert.equal(closed, 1);
            assert.equal(returned, stage === 'next' ? 1 : 0);
        }
    } finally { await runner.close(); }
});

test('spool recovery progresses beyond a full page of live processing leases', { timeout: 10000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-pages-')); const controller = new AbortController();
    const working = join(directory, '.processing'); await mkdir(working);
    for (let index = 0; index < 100; index++) await writeFile(join(working, `a${String(index).padStart(3, '0')}.json~live`), '{}');
    const stale = join(working, 'z.json~stale'); await writeFile(stale, '{"kind":"result","result":42}'); await utimes(stale, new Date(0), new Date(0));
    const iterator = createSpoolSource({ directory, flow: 'a', pollMs: 1, leaseMs: 60000 }).open({ signal: controller.signal });
    try {
        const item = await bounded(() => iterator.next(), { timeoutMs: 5000 });
        assert.equal(item.value.input.result, 42); await item.value.acknowledge();
    } finally {
        controller.abort(); await bounded(() => iterator.return(), { timeoutMs: 1000 }).catch(() => {});
        await rm(directory, { recursive: true, force: true });
    }
});

test('spool queue pages progress past retryable files and wrap without dropping evidence', { timeout: 10000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-retries-')); const controller = new AbortController();
    for (let index = 0; index < 101; index++) await writeFile(join(directory, `${String(index).padStart(3, '0')}.json`),
        JSON.stringify({ kind: 'result', result: index }));
    const iterator = createSpoolSource({ directory, flow: 'a', pollMs: 1 }).open({ signal: controller.signal });
    const seen = [];
    try {
        for (let index = 0; index < 102; index++) {
            const item = await bounded(() => iterator.next(), { timeoutMs: 1000 });
            seen.push(item.value.input.result);
            await item.value.reject({ code: 'RESOURCE_LIMIT', message: 'Try later.', retryable: true });
        }
        assert.deepEqual(seen, [...Array.from({ length: 101 }, (_, index) => index), 0]);
        assert.equal((await readdir(directory)).filter(name => name.endsWith('.json')).length, 101);
        assert.deepEqual(JSON.parse(await readFile(join(directory, '100.json'), 'utf8')), { kind: 'result', result: 100 });
    } finally {
        controller.abort(); await iterator.return();
        await rm(directory, { recursive: true, force: true });
    }
});

test('spool preserves long and multibyte filenames through retry, restart recovery and archival', { timeout: 10000 }, async () => {
    for (const filename of [`${'a'.repeat(230)}.json`, `${'é'.repeat(120)}.json`]) {
        const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-filename-')); const controller = new AbortController();
        const options = { directory, flow: 'a', pollMs: 1, leaseMs: 60000 };
        const open = () => createSpoolSource(options).open({ signal: controller.signal });
        let iterator = open();
        try {
            await writeFile(join(directory, filename), '{"kind":"result","result":42}');
            const first = await bounded(() => iterator.next(), { timeoutMs: 1000 });
            const key = first.value.input.idempotencyKey;
            await first.value.reject({ code: 'RESOURCE_LIMIT', retryable: true });
            assert.equal(await readFile(join(directory, filename), 'utf8'), '{"kind":"result","result":42}');
            assert.deepEqual(await readdir(join(directory, '.processing')), []);
            await iterator.return(); iterator = open();
            const abandoned = await bounded(() => iterator.next(), { timeoutMs: 1000 });
            assert.equal(abandoned.value.input.idempotencyKey, key);
            await iterator.return();
            const [claim] = await readdir(join(directory, '.processing'));
            await utimes(join(directory, '.processing', claim), new Date(0), new Date(0));
            iterator = open();
            const recovered = await bounded(() => iterator.next(), { timeoutMs: 1000 });
            assert.equal(recovered.value.input.result, 42); assert.equal(recovered.value.input.idempotencyKey, key);
            await recovered.value.acknowledge();
            const [archived] = await readdir(join(directory, '.processed'));
            assert.equal(await readFile(join(directory, '.processed', archived, filename), 'utf8'), '{"kind":"result","result":42}');
            assert.deepEqual(await readdir(join(directory, '.processing')), []);
        } finally {
            controller.abort(); await iterator.return(); await rm(directory, { recursive: true, force: true });
        }
    }
});

test('spool quarantines invalid JSON shape while preserving valid expanded numbers and later input', { timeout: 10000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-json-contract-')); const controller = new AbortController();
    const malformed = ['{"kind":"result","result":1e400}',
        `{"kind":"result","result":${'['.repeat(105)}1${']'.repeat(105)}}`];
    const iterator = createSpoolSource({ directory, flow: 'a', pollMs: 1 }).open({ signal: controller.signal });
    try {
        for (const [index, content] of malformed.entries()) await writeFile(join(directory, `a${index}.json`), content);
        await writeFile(join(directory, 'b-expanded.json'), `{"kind":"result","result":[${Array(60000).fill('1e20').join(',')}]}`);
        await writeFile(join(directory, 'z-good.json'), '{"kind":"result","result":42}');
        const expanded = await bounded(() => iterator.next(), { timeoutMs: 5000 });
        assert.equal(expanded.value.input.result.length, 60000); assert.equal(expanded.value.input.result[0], 1e20);
        assert(JSON.stringify(expanded.value.input.result).length > 1048576); await expanded.value.acknowledge();
        const message = await bounded(() => iterator.next(), { timeoutMs: 5000 });
        assert.equal(message.value.input.result, 42); await message.value.acknowledge();
        const claims = await readdir(join(directory, '.quarantine')); assert.equal(claims.length, 2);
        const evidence = [];
        for (const claim of claims) {
            const [file] = await readdir(join(directory, '.quarantine', claim));
            evidence.push(await readFile(join(directory, '.quarantine', claim, file), 'utf8'));
        }
        assert.deepEqual(evidence.sort(), [...malformed].sort());
        assert.deepEqual(await readdir(join(directory, '.processing')), []);
    } finally { controller.abort(); await iterator.return(); await rm(directory, { recursive: true, force: true }); }
});
