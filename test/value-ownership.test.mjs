import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createValueStorage, CHUNK_BYTES } from '../src/storage/values.mjs';
import { storeLimits } from '../src/storage/limits.mjs';

test('shared values and index descendants survive until their last owner is removed', async () => {
    const raw = createMemoryRepository(); const values = createValueStorage(raw, storeLimits());
    const stage = await values.stage('ownership');
    try {
        const body = await stage.put({ text: 'shared' });
        const app = await stage.put([body], 'json'); const index = await stage.put([body], 'index');
        assert.notEqual(app.$data.id, index.$data.id, 'Opaque JSON must not share index ownership semantics.');
        await values.transaction(async tx => {
            await tx.insert('tasks', { id: 'one', namespace: 'ownership', value: index });
            await tx.insert('records', { id: 'two', namespace: 'ownership', value: index });
        });
        await stage.close();
        await values.transaction(tx => tx.remove('tasks', 'one')); await values.maintain('ownership');
        assert.deepEqual(await values.transaction(tx => values.hydrate(tx, 'ownership', index)), [{ text: 'shared' }]);
        await values.transaction(tx => tx.remove('records', 'two'));
        for (let pass = 0; pass < 5; pass++) await values.maintain('ownership');
        assert.equal(await raw.transaction(tx => tx.count('blobs')), 0);
        assert.equal(await raw.transaction(tx => tx.count('chunks')), 0);
        assert.equal(await raw.transaction(tx => tx.count('refs')), 0);
    } finally { await stage.close(); await raw.close(); }
});

test('value garbage collection progresses past a full page of retained owners', async () => {
    const raw = createMemoryRepository(); const values = createValueStorage(raw, storeLimits()); const stage = await values.stage('gc-progress');
    try {
        const refs = [];
        for (let index = 0; index < 107; index++) refs.push(await stage.put({ index }));
        // Sort physical IDs to place all unowned values after the first GC page.
        refs.sort((a, b) => a.$data.id.localeCompare(b.$data.id));
        await values.transaction(async tx => {
            for (const [index, value] of refs.slice(0, 105).entries()) await tx.insert('tasks', { id: String(index), namespace: 'gc-progress', value });
        });
        await stage.close();
        for (let pass = 0; pass < 4; pass++) await values.maintain('gc-progress');
        assert.equal(await raw.transaction(tx => tx.count('blobs')), 105);
        for (const value of refs.slice(105)) await assert.rejects(values.transaction(tx => values.read(tx, 'gc-progress', value.$data)), { code: 'DATA_UNAVAILABLE' });
        const retained = await values.transaction(tx => values.read(tx, 'gc-progress', refs[0].$data));
        assert(Number.isInteger(retained.index)); assert(retained.index >= 0 && retained.index < 107);
    } finally { await stage.close(); await raw.close(); }
});

test('expired stages cannot acquire fresh pins on existing values', async t => {
    const raw = createMemoryRepository(); const values = createValueStorage(raw, storeLimits({ transferIdleMs: 10 })); let clock = Date.now();
    t.mock.method(Date, 'now', () => clock);
    const first = await values.stage('stage'); const body = await first.put(1);
    await values.transaction(tx => tx.insert('tasks', { id: 'keep', namespace: 'stage', body })); await first.close();
    const expired = await values.stage('stage'); clock += 11;
    try {
        await values.maintain('stage'); await assert.rejects(expired.put(1), { code: 'TRANSFER_EXPIRED' });
        assert.equal(await raw.transaction(tx => tx.count('refs', { ownerId: expired.ownerId })), 0);
    } finally { await expired.close(); await raw.close(); }
});

test('partial collection cannot regain owners and resumes after storage reconstruction', async () => {
    const raw = createMemoryRepository(); const limits = storeLimits();
    const values = createValueStorage(raw, limits); const stage = await values.stage('partial-plan');
    const value = 'x'.repeat(CHUNK_BYTES * 101); let replacementStage;
    try {
        // Plans permit this size even with the default application-value limit.
        const previous = await stage.put(value, 'plan'); await stage.close();
        await values.maintain('partial-plan');
        assert(await raw.transaction(tx => tx.count('chunks', { blobId: previous.$data.id })) > 0);
        await assert.rejects(values.transaction(tx => tx.insert('records', {
            id: 'stale-owner', namespace: 'partial-plan', value: previous,
        })), { code: 'DATA_UNAVAILABLE' });
        assert.equal(await raw.transaction(tx => tx.get('records', 'stale-owner')), null);

        const reopened = createValueStorage(raw, limits);
        replacementStage = await reopened.stage('partial-plan');
        const replacement = await replacementStage.put(value, 'plan');
        assert.notEqual(replacement.$data.id, previous.$data.id);
        for (let pass = 0; pass < 3; pass++) await reopened.maintain('partial-plan');
        assert.equal(await raw.transaction(tx => tx.get('blobs', previous.$data.id)), null);
        assert.equal(await raw.transaction(tx => tx.count('chunks', { blobId: previous.$data.id })), 0);
        assert(await reopened.transaction(tx => reopened.read(tx, 'partial-plan', replacement.$data)) === value);
    } finally { await stage.close(); await replacementStage?.close(); await raw.close(); }
});
