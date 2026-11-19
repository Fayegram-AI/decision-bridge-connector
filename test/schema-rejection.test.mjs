import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

test('SQLite rejects earlier schema without modifying existing bytes', async () => {
    for (const version of [1, 2]) {
        const directory = await mkdtemp(join(tmpdir(), 'dbc-unsupported-')); const filename = join(directory, 'earlier.sqlite');
        try {
            const db = new DatabaseSync(filename);
            try { db.exec('CREATE TABLE dbc' + version + '_tasks (id TEXT PRIMARY KEY,value TEXT); INSERT INTO dbc' + version + '_tasks VALUES ("one","{}")'.replaceAll('"', "'")); }
            finally { db.close(); }
            const before = await readFile(filename);
            assert.throws(() => createSQLiteStore(filename), { code: 'UNSUPPORTED_SCHEMA' });
            assert.deepEqual(await readFile(filename), before);
        } finally { await rm(directory, { recursive: true, force: true }); }
    }
});

test('current SQLite stores reopen while an existing asynchronous writer can resume', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-current-reopen-')); const filename = join(directory, 'current.sqlite');
    const first = createSQLiteStore(filename); let second; let pending;
    try {
        pending = first.aggregate('reopen', 'group', () => ({ ruleId: 'one', state: 'pending', evidence: 42 }));
        // The repository has entered its transaction and yielded at the first await.
        await Promise.resolve();
        second = createSQLiteStore(filename);
        await pending;
        assert.equal((await second.aggregate('reopen', 'group', value => value)).evidence, 42);
    } finally { await pending; await second?.close(); await first.close(); await rm(directory, { recursive: true, force: true }); }
});

test('SQLite reopening still repairs a missing ownership index', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-index-reopen-')); const filename = join(directory, 'current.sqlite');
    const initial = createSQLiteStore(filename); await initial.close();
    try {
        const db = new DatabaseSync(filename); try { db.exec('DROP INDEX dbc3_aggregates_rule'); } finally { db.close(); }
        const reopened = createSQLiteStore(filename); await reopened.close();
        const check = new DatabaseSync(filename);
        try { assert(check.prepare("SELECT name FROM sqlite_schema WHERE name='dbc3_aggregates_rule'").get()); } finally { check.close(); }
    } finally { await rm(directory, { recursive: true, force: true }); }
});

test('PostgreSQL rejects earlier schema before DDL and preserves existing tables', { skip: !process.env.DBC_POSTGRES_URL }, async () => {
    const { Pool } = await import('pg'); const schema = 'dbc_old_' + crypto.randomUUID().replaceAll('-', '');
    const admin = new Pool({ connectionString: process.env.DBC_POSTGRES_URL });
    await admin.query('CREATE SCHEMA ' + schema);
    const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL, options: '-c search_path=' + schema });
    try {
        await pool.query("CREATE TABLE dbc2_tasks (id TEXT PRIMARY KEY,value TEXT); INSERT INTO dbc2_tasks VALUES ('one','{}')");
        const before = (await pool.query('SELECT * FROM dbc2_tasks')).rows;
        await assert.rejects(createPostgresStore({ pool }), { code: 'UNSUPPORTED_SCHEMA' });
        assert.deepEqual((await pool.query('SELECT * FROM dbc2_tasks')).rows, before);
        assert.deepEqual((await pool.query('SELECT tablename FROM pg_tables WHERE schemaname=$1', [schema])).rows, [{ tablename: 'dbc2_tasks' }]);
    } finally { await pool.end(); await admin.query('DROP SCHEMA ' + schema + ' CASCADE'); await admin.end(); }
});
