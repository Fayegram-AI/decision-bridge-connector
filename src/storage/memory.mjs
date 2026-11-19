import { assert, snapshot } from '../util.mjs';
import { compareRows, matches, TABLES } from './repository.mjs';
import { restoreReferences } from './data-ref.mjs';
import { createExecutionStore } from './store.mjs';

/** Volatile repository. A serialized transaction isolates asynchronous callers. */
export function createMemoryRepository() {
    let tables = Object.fromEntries(TABLES.map(name => [name, new Map()]));
    let tail = Promise.resolve(); let closed = false;
    const copy = row => {
        const { data, ...metadata } = row;
        return { ...restoreReferences(snapshot(metadata, { maxBytes: 65536 })), ...(data === undefined ? {} : { data: new Uint8Array(data) }) };
    };
    return {
        capabilities: { durable: false, distributed: false },
        async transaction(callback) {
            assert(!closed, 'Store is closed.', 'STORE_CLOSED');
            const previous = tail; let release;
            tail = new Promise(resolve => { release = resolve; });
            await previous;
            const working = Object.fromEntries(TABLES.map(name => [name, new Map(tables[name])]));
            const tx = {
                async lock() {},
                async get(table, key, _lock, namespace) {
                    const row = working[table].get(key);
                    return row && (namespace === undefined || row.namespace === namespace) ? copy(row) : null;
                },
                async put(table, row) { working[table].set(row.id, copy(row)); },
                async insert(table, row) { assert(!working[table].has(row.id), 'Duplicate record.', 'STORE_CONFLICT'); await tx.put(table, row); },
                async remove(table, key) { working[table].delete(key); },
                async count(table, where = {}) { return [...working[table].values()].filter(row => matches(row, where)).length; },
                async find(table, where = {}, { limit = 100, order = ['id'], after } = {}) {
                    assert(limit > 0 && limit <= 1000, 'Invalid page limit.');
                    return [...working[table].values()].filter(row => matches(row, where) &&
                        (!after || compareRows(row, after, order) > 0))
                        .sort((a, b) => compareRows(a, b, order)).slice(0, limit).map(copy);
                },
            };
            try { const result = await callback(tx); tables = working; return result; }
            finally { release(); }
        },
        // Fence admission before yielding; the captured tail drains accepted transactions.
        close() { closed = true; return tail; },
    };
}

export function createMemoryStore(options) { return createExecutionStore(createMemoryRepository(), options); }
