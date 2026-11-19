import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createRunner, createMemoryStore, ConnectorError } from '../src/index.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

const stores = [['memory', options => createMemoryStore(options)],
    ['sqlite', options => createSQLiteStore(':memory:', options)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', options => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...options })]] : [])];

async function archive(runner) {
    const task = await runner.run({ flow: 'a', kind: 'result', result: { original: true } });
    const frames = [];
    for await (const bytes of runner.exportTask(task.taskId)) frames.push(JSON.parse(new TextDecoder().decode(bytes)));
    return { taskId: task.taskId, frames };
}

async function open(runner, frames) {
    const session = await runner.openImport({ flow: 'a' });
    for (const frame of frames) await runner.appendImport(session.id, frame);
    return session;
}

for (const [name, factory] of stores) test(`${name}: live import sessions retain one execution across finish failures and retention`, async () => {
    const store = await factory({ retentionMs: 1, dedupeMs: 1 }); let fail = true;
    const runner = createRunner({ store: { ...store, async completeImport(...args) {
        if (fail) { fail = false; throw new ConnectorError('WRITE_FAULT', 'Synthetic interrupted import completion.'); }
        return store.completeImport(...args);
    } }, namespace: `import-retention-${crypto.randomUUID()}`,
    validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    let first; let second;
    try {
        const original = await archive(runner); const { frames } = original;
        first = await open(runner, frames); second = await open(runner, frames);
        await assert.rejects(runner.finishImport(first.id), { code: 'WRITE_FAULT' });
        const imported = (await runner.list()).items.find(task => task.taskId !== original.taskId); assert(imported);
        await runner.execute(imported.taskId); await sleep(10); await runner.maintain();
        assert(await runner.get(imported.taskId), 'An unfinished live import must pin its admitted execution.');
        assert.equal((await runner.finishImport(first.id)).taskId, imported.taskId);
        await runner.closeImport(first.id); await runner.maintain();
        assert(await runner.get(imported.taskId), 'Another matching session must retain the shared admission.');
        assert.equal((await runner.finishImport(second.id)).taskId, imported.taskId);
        await runner.maintain();
        assert.equal((await runner.finishImport(second.id)).taskId, imported.taskId,
            'A completed live session must keep its finish response available.');
        await runner.closeImport(second.id); await runner.maintain(); await runner.maintain();
        assert.equal(await runner.get(imported.taskId), null, 'Closing all matching sessions releases retention.');
    } finally {
        if (first) await runner.closeImport(first.id);
        if (second) await runner.closeImport(second.id);
        await runner.close(); await store.close();
    }
});

for (const [name, factory] of stores) for (const disposition of ['closes', 'expires'])
test(`${name}: stale import validation cannot recreate work after its session ${disposition}`, async t => {
    const store = await factory({ retentionMs: 1, dedupeMs: 1 });
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let gate = false;
    const runner = createRunner({ store, namespace: `import-stale-${crypto.randomUUID()}`,
        validators: { v: { revision: '1', async validate(value) {
            if (gate) { gate = false; entered.resolve(); await release.promise; }
            return value;
        } } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    let session; let pending;
    try {
        session = await open(runner, (await archive(runner)).frames); gate = true;
        pending = runner.finishImport(session.id); pending.catch(() => {});
        await bounded(() => entered.promise, { timeoutMs: 5000 });
        const winner = await runner.finishImport(session.id); await runner.execute(winner.taskId);
        if (disposition === 'closes') await runner.closeImport(session.id);
        else {
            const timestamp = Date.now() + 2 * store.limits.transferIdleMs;
            t.mock.method(Date, 'now', () => timestamp);
        }
        await sleep(10); await runner.maintain();
        assert.equal((await runner.list()).items.length, 0);
        release.resolve(); await assert.rejects(pending, { code: 'ADMISSION_OWNER_SETTLED' });
        assert.equal((await runner.list()).items.length, 0, 'A stale finish cannot insert another task.');
    } finally {
        release.resolve(); await pending?.catch(() => {});
        if (session) await runner.closeImport(session.id);
        await runner.close(); await store.close();
    }
});
