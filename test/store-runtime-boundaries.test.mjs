import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, unlink, writeFile, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createMemoryStore } from '../src/index.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { createHost } from '../src/node/host.mjs';

test('invalid store and host timer limits fail before resources are acquired', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-store-limits-')); let acquired = 0;
    try {
        for (const limits of [{ maxPending: 0 }, { leaseMs: 2147483648 }]) {
            assert.throws(() => createMemoryStore(limits), { code: 'CONFIGURATION_ERROR' });
            assert.throws(() => createSQLiteStore(join(directory, 'state.sqlite'), limits), { code: 'CONFIGURATION_ERROR' });
            await assert.rejects(createPostgresStore({ ...limits, pool: { connect() { acquired++; throw new Error('Unexpected acquisition'); } } }), { code: 'CONFIGURATION_ERROR' });
        }
        for (const option of ['pollMs', 'maintenanceMs', 'drainMs']) {
            await assert.rejects(createHost({ runner: { ready: Promise.resolve(), recover() { acquired++; } }, [option]: 2147483648 }), { code: 'CONFIGURATION_ERROR' });
        }
        assert.equal(acquired, 0); assert.deepEqual(await readdir(directory), []);
    } finally { await rm(directory, { recursive: true, force: true }); }
});

test('failed SQLite setup releases its file and failed PostgreSQL setup preserves borrowed ownership', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-store-setup-')); const filename = join(directory, 'bad.sqlite');
    try {
        await writeFile(filename, Buffer.alloc(512, 1));
        assert.throws(() => createSQLiteStore(filename));
        await unlink(filename); // Windows will reject this if SQLite retains its opened handle.
        let released = 0; let ended = 0;
        await assert.rejects(createPostgresStore({ pool: { async connect() { return {
            async query() { throw new Error('Setup failed'); }, release() { released++; },
        }; }, async end() { ended++; } } }), /Setup failed/);
        assert.equal(released, 1); assert.equal(ended, 0);
        await assert.rejects(createPostgresStore({ pool: { async connect() { throw new Error('Connect failed'); }, async end() { ended++; } } }), /Connect failed/);
        assert.equal(ended, 0);
    } finally { await rm(directory, { recursive: true, force: true }); }
});
