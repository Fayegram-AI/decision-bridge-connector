import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, lstatSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { assert, delay } from '../util.mjs';
import { COLUMNS, orderSql, querySql, rowValues, readRow, schemaSql, tableName } from './repository.mjs';
import { createExecutionStore } from './store.mjs';
import { storeLimits } from './limits.mjs';

const queues = new Map();

/** Current stores reopen through reads, allowing an existing writer to resume. */
function initializeSchema(db) {
    assert(!db.prepare("SELECT 1 FROM sqlite_schema WHERE type='table' AND name GLOB 'dbc[12]_*' LIMIT 1").get(),
        'Unsupported saved schema. Use a fresh schema-v3 database; existing state was preserved.', 'UNSUPPORTED_SCHEMA');
    const statements = schemaSql('sqlite');
    const existing = new Set(db.prepare("SELECT name FROM sqlite_schema WHERE type IN ('table','index')").all().map(row => row.name));
    const missing = statements.filter(sql => !existing.has(sql.split(' ')[5]));
    if (!missing.length) return;
    db.exec('BEGIN IMMEDIATE');
    try {
        for (const statement of missing) db.exec(statement);
        db.exec('COMMIT');
    } catch (error) { try { db.exec('ROLLBACK'); } catch {} throw error; }
}

/** SQLite is a single-host durable profile; distributed hosts use PostgreSQL. */
export function createSQLiteStore(filename, options = {}) {
    const limits = storeLimits(options);
    const path = filename === ':memory:' ? filename : resolve(filename);
    if (path !== ':memory:') {
        mkdirSync(dirname(path), { recursive: true });
        try { assert(lstatSync(path).isFile(), 'SQLite path must identify a regular file.'); }
        catch (error) { if (error.code !== 'ENOENT') throw error; }
    }
    const db = new DatabaseSync(path);
    try {
        assert(!db.prepare("SELECT 1 FROM sqlite_schema WHERE type='table' AND name GLOB 'dbc[12]_*' LIMIT 1").get(),
            'Unsupported saved schema. Use a fresh schema-v3 database; existing state was preserved.', 'UNSUPPORTED_SCHEMA');
        db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;');
        initializeSchema(db);
        // Busy waits must yield the event loop so independent leases can renew.
        db.exec('PRAGMA busy_timeout=0;');
    } catch (error) { db.close(); throw error; }
    let closed = false; let closing;
    const key = path === ':memory:' ? db : path;
    const repository = {
        capabilities: { durable: path !== ':memory:', distributed: false },
        async transaction(callback) {
            assert(!closed, 'Store is closed.', 'STORE_CLOSED');
            const previous = queues.get(key) ?? Promise.resolve(); let release;
            const queued = new Promise(resolveQueue => { release = resolveQueue; }); queues.set(key, queued);
            await previous;
            const tx = {
                async lock() {},
                async get(table, id, _lock, namespace) {
                    const row = db.prepare(`SELECT value,data FROM ${tableName(table)} WHERE id=?${namespace === undefined ? '' : ' AND namespace=?'}`)
                        .get(...(namespace === undefined ? [id] : [id, namespace]));
                    return readRow(row);
                },
                async insert(table, row) {
                    db.prepare(`INSERT INTO ${tableName(table)} (${COLUMNS.join(',')},value,data) VALUES (${Array(COLUMNS.length + 2).fill('?').join(',')})`).run(...rowValues(row));
                },
                async put(table, row) {
                    db.prepare(`INSERT INTO ${tableName(table)} (${COLUMNS.join(',')},value,data) VALUES (${Array(COLUMNS.length + 2).fill('?').join(',')}) ON CONFLICT(id) DO UPDATE SET ${[...COLUMNS.slice(1), 'value', 'data'].map(column => `${column}=excluded.${column}`).join(',')}`).run(...rowValues(row));
                },
                async remove(table, id) { db.prepare(`DELETE FROM ${tableName(table)} WHERE id=?`).run(id); },
                async count(table, where = {}) {
                    const query = querySql(where, 'sqlite');
                    return db.prepare(`SELECT count(*) AS count FROM ${tableName(table)} WHERE ${query.text}`).get(...query.parameters).count;
                },
                async find(table, where = {}, { limit = 100, order = ['id'], after } = {}) {
                    assert(Number.isSafeInteger(limit) && limit > 0 && limit <= 1000, 'Invalid page limit.');
                    const query = querySql(where, 'sqlite', 0, after && { columns: order, values: order.map(key => after[key]) });
                    return db.prepare(`SELECT value,data FROM ${tableName(table)} WHERE ${query.text} ORDER BY ${orderSql(order)} LIMIT ${limit}`).all(...query.parameters).map(readRow);
                },
            };
            try {
                const until = Date.now() + 10000;
                while (true) {
                    try { db.exec('BEGIN IMMEDIATE'); break; }
                    catch (error) {
                        if (error.errcode !== 5 || Date.now() >= until) throw error;
                        await delay(2 + Math.floor(Math.random() * 8));
                    }
                }
                const result = await callback(tx); db.exec('COMMIT'); return result;
            } catch (error) { try { db.exec('ROLLBACK'); } catch {} throw error; }
            finally { release(); if (queues.get(key) === queued) queues.delete(key); }
        },
        close() {
            if (!closing) {
                // New calls cannot enter behind the transaction tail being drained.
                closed = true;
                closing = Promise.resolve(queues.get(key)).then(() => db.close());
            }
            return closing;
        },
    };
    return createExecutionStore(repository, limits);
}
