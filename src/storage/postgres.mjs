import { assert, delay } from '../util.mjs';
import { COLUMNS, orderSql, querySql, rowValues, readRow, schemaSql, tableName } from './repository.mjs';
import { createExecutionStore } from './store.mjs';
import { storeLimits } from './limits.mjs';

/** A pool may be borrowed; only pools created here are closed by the store. */
export async function createPostgresStore({ pool, connectionString, max = 10, ...options } = {}) {
    const limits = storeLimits(options);
    assert(Number.isSafeInteger(max) && max > 0, 'Invalid PostgreSQL pool size.');
    const owned = !pool;
    if (!pool) {
        const { Pool } = await import('pg');
        pool = new Pool({ connectionString, max, connectionTimeoutMillis: 10000, statement_timeout: 10000 });
        // pg removes broken idle clients; their EventEmitter error must not kill the host.
        pool.on('error', () => {});
    }
    let setup;
    try {
        setup = await pool.connect();
        for (let attempt = 0; ; attempt++) try {
            await setup.query('BEGIN');
            await setup.query("SELECT pg_advisory_xact_lock(hashtext('dbc3-schema'))");
            const { rows: legacy } = await setup.query("SELECT tablename FROM pg_tables WHERE schemaname=current_schema() AND left(tablename,5) IN ('dbc1_','dbc2_') LIMIT 1");
            assert(!legacy.length, 'Unsupported saved schema. Use a fresh schema-v3 database; existing state was preserved.', 'UNSUPPORTED_SCHEMA');
            const statements = schemaSql('pg');
            await setup.query(statements.filter(value => value.startsWith('CREATE TABLE')).join(';'));
            const { rows: indexes } = await setup.query("SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() AND tablename LIKE 'dbc3_%'");
            const existing = new Set(indexes.map(row => row.indexname));
            // Even IF NOT EXISTS takes table locks; reopening a store must avoid unnecessary DDL.
            const missing = statements.filter(value => value.startsWith('CREATE INDEX') && !existing.has(value.split(' ')[5]));
            if (missing.length) await setup.query(missing.join(';'));
            await setup.query('COMMIT');
            break;
        } catch (error) {
            await setup.query('ROLLBACK').catch(() => {});
            if (['40001', '40P01'].includes(databaseCode(error)) && attempt < 5) { await delay(5 * 2 ** attempt); continue; }
            throw error;
        }
    } catch (error) {
        setup?.release(); setup = null;
        if (owned) await pool.end().catch(() => {});
        throw error;
    } finally { setup?.release(); }
    let closed = false; let closing;
    const active = new Set();
    const repository = {
        capabilities: { durable: true, distributed: true },
        async transaction(callback) {
            assert(!closed, 'Store is closed.', 'STORE_CLOSED');
            // Include pool acquisition and retry delays in the shutdown drain.
            let finish;
            const completed = new Promise(resolve => { finish = resolve; });
            active.add(completed);
            try {
                for (let attempt = 0; ; attempt++) {
                    const client = await pool.connect();
                    const tx = {
                        async lock(key) { await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [key]); },
                        async get(table, id, lock = false, namespace) {
                            const { rows } = await client.query(`SELECT value,data FROM ${tableName(table)} WHERE id=$1${namespace === undefined ? '' : ' AND namespace=$2'}${lock === 'reference' ? ' FOR KEY SHARE' : lock ? ' FOR UPDATE' : ''}`,
                                namespace === undefined ? [id] : [id, namespace]);
                            return readRow(rows[0]);
                        },
                        async insert(table, row) {
                            await client.query(`INSERT INTO ${tableName(table)} (${COLUMNS.join(',')},value,data) VALUES (${Array.from({ length: COLUMNS.length + 2 }, (_, i) => `$${i + 1}`).join(',')})`, rowValues(row));
                        },
                        async put(table, row) {
                            await client.query(`INSERT INTO ${tableName(table)} (${COLUMNS.join(',')},value,data) VALUES (${Array.from({ length: COLUMNS.length + 2 }, (_, i) => `$${i + 1}`).join(',')}) ON CONFLICT(id) DO UPDATE SET ${[...COLUMNS.slice(1), 'value', 'data'].map(column => `${column}=excluded.${column}`).join(',')}`, rowValues(row));
                        },
                        async remove(table, id) { await client.query(`DELETE FROM ${tableName(table)} WHERE id=$1`, [id]); },
                        async count(table, where = {}) {
                            const query = querySql(where, 'pg');
                            const { rows } = await client.query(`SELECT count(*) AS count FROM ${tableName(table)} WHERE ${query.text}`, query.parameters);
                            return Number(rows[0].count);
                        },
                        async find(table, where = {}, { limit = 100, order = ['id'], lock = false, skipLocked = true, after } = {}) {
                            assert(Number.isSafeInteger(limit) && limit > 0 && limit <= 1000, 'Invalid page limit.');
                            const query = querySql(where, 'pg', 0, after && { columns: order, values: order.map(key => after[key]) });
                            const { rows } = await client.query(`SELECT value,data FROM ${tableName(table)} WHERE ${query.text} ORDER BY ${orderSql(order)} LIMIT ${limit}${lock ? ` FOR UPDATE${skipLocked ? ' SKIP LOCKED' : ''}` : ''}`, query.parameters);
                            return rows.map(readRow);
                        },
                    };
                    try {
                        await client.query('BEGIN');
                        const result = await callback(tx); await client.query('COMMIT'); return result;
                    } catch (error) {
                        await client.query('ROLLBACK').catch(() => {});
                        if (!['40001', '40P01', '23505'].includes(databaseCode(error)) || attempt >= 5) throw error;
                    } finally { client.release(); }
                    await delay(5 * 2 ** attempt);
                }
            } finally { active.delete(completed); finish(); }
        },
        close() {
            if (!closing) {
                closed = true;
                closing = Promise.all([...active]).then(() => owned ? pool.end() : undefined);
            }
            return closing;
        },
    };
    return createExecutionStore(repository, limits);
}

function databaseCode(error) {
    try { return error?.code; } catch { return undefined; }
}
