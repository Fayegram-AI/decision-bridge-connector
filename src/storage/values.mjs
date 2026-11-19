import { assert, id, isRecord, snapshot, stableStringify } from '../util.mjs';
import { dataReference, referenceNode } from './data-ref.mjs';

export const CHUNK_BYTES = 65536;
export const METADATA_BYTES = 65536;
const key = (...parts) => JSON.stringify(parts);
const budgets = limits => ({ json: limits.maxBytes, plan: 67108864, diagnostic: 65536, index: 4194304 });

export async function digest(bytes) {
    return [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

/** References occur only in generated record metadata, never inside application JSON. */
export function references(value) {
    const found = new Map();
    function visit(item) {
        if (!item || typeof item !== 'object') return;
        if (referenceNode(item)) { found.set(item.$data.id, item.$data); return; }
        Object.values(item).forEach(visit);
    }
    visit(value);
    return [...found.values()];
}

/** Immutable values are published before the short transaction linking live metadata. */
export function createValueStorage(repository, limits) {
    const max = budgets(limits);
    async function link(tx, namespace, ownerId, values) {
        for (const ref of references(values).sort((a, b) => a.id.localeCompare(b.id))) {
            // Compatible reference pins block deletion without serializing independent owners.
            const blob = await tx.get('blobs', ref.id, 'reference');
            assert(blob?.namespace === namespace && blob.state === 'complete' && blob.sha256 === ref.sha256 && blob.bytes === ref.bytes && blob.kind === ref.kind,
                'Referenced data is unavailable.', 'DATA_UNAVAILABLE');
            await tx.put('refs', { id: key(ownerId, ref.id), namespace, ownerId, blobId: ref.id });
        }
    }
    async function unlink(tx, ownerId) {
        while (true) {
            const rows = await tx.find('refs', { ownerId }, { limit: 128 });
            for (const row of rows) await tx.remove('refs', row.id);
            if (rows.length < 128) break;
        }
    }
    async function read(tx, namespace, ref) {
        const blob = await tx.get('blobs', ref.id);
        assert(blob?.namespace === namespace && blob.state === 'complete' && blob.sha256 === ref.sha256 && blob.bytes === ref.bytes,
            'Referenced data is unavailable.', 'DATA_UNAVAILABLE');
        assert(Object.hasOwn(max, ref.kind) && blob.bytes <= max[ref.kind], 'Stored value exceeds its limit.', 'RESOURCE_LIMIT');
        const bytes = new Uint8Array(blob.bytes); let offset = 0; let ordinal = 0;
        while (offset < bytes.length) {
            const chunk = await tx.get('chunks', key(ref.id, ordinal++));
            assert(chunk?.namespace === namespace && chunk.data?.length > 0 && chunk.data.length <= CHUNK_BYTES &&
                offset + chunk.data.length <= bytes.length, 'Stored data is incomplete.', 'DATA_UNAVAILABLE');
            bytes.set(chunk.data, offset); offset += chunk.data.length;
        }
        const value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
        return snapshot(value, { maxBytes: max[ref.kind] });
    }
    async function hydrate(tx, namespace, value, cache = new Map()) {
        if (value === null || typeof value !== 'object') return value;
        if (referenceNode(value)) {
            const ref = value.$data;
            if (!cache.has(ref.id)) cache.set(ref.id, read(tx, namespace, ref));
            const data = await cache.get(ref.id);
            if (ref.kind === 'index') return hydrate(tx, namespace, data, cache);
            // Recovery may annotate optional response capture without reading its body.
            return isRecord(data) && value.responseUnavailable === true ? { ...data, responseUnavailable: true } : data;
        }
        if (Array.isArray(value)) return Promise.all(value.map(item => hydrate(tx, namespace, item, cache)));
        const entries = await Promise.all(Object.entries(value).map(async ([name, child]) => [name, await hydrate(tx, namespace, child, cache)]));
        return Object.fromEntries(entries);
    }
    async function stage(namespace) {
        const ownerId = id('stage'); const cache = new Map(); let closed = false;
        await repository.transaction(tx => tx.insert('owners', { id: ownerId, namespace, kind: 'stage', expiresAt: Date.now() + limits.transferIdleMs }));
        return {
            ownerId,
            maxValueBytes: max.json,
            async put(value, kind = 'json', projection = {}) {
                assert(!closed && Object.hasOwn(max, kind), 'Data stage is closed or invalid.');
                const captured = snapshot(value, { maxBytes: max[kind] });
                const encoded = new TextEncoder().encode(stableStringify(captured));
                const sha256 = await digest(encoded);
                const cacheKey = `${kind}:${sha256}`;
                if (!cache.has(cacheKey)) cache.set(cacheKey, (async () => {
                    const existing = await repository.transaction(async tx => {
                        await tx.lock(key(namespace, 'blob', sha256));
                        const owner = await tx.get('owners', ownerId, true);
                        assert(owner && owner.expiresAt > Date.now(), 'Data stage expired.', 'TRANSFER_EXPIRED');
                        await tx.put('owners', { ...owner, expiresAt: Date.now() + limits.transferIdleMs });
                        const [blob] = await tx.find('blobs', { namespace, rootId: sha256, kind, state: 'complete' }, { limit: 1 });
                        if (blob) await tx.put('refs', { id: key(ownerId, blob.id), namespace, ownerId, blobId: blob.id });
                        return blob;
                    });
                    if (existing) return { id: existing.id, bytes: existing.bytes, sha256, kind };
                    const blobId = id('blob');
                    await repository.transaction(async tx => {
                        const owner = await tx.get('owners', ownerId, true);
                        assert(owner && owner.expiresAt > Date.now(), 'Data stage expired.', 'TRANSFER_EXPIRED');
                        await tx.insert('blobs', { id: blobId, namespace, rootId: sha256, sha256, bytes: encoded.length, kind, state: 'writing' });
                        await tx.put('refs', { id: key(ownerId, blobId), namespace, ownerId, blobId });
                    });
                    // Chunk batches yield without holding task/job locks or a long database transaction.
                    for (let start = 0; start < encoded.length; start += CHUNK_BYTES * 32) await repository.transaction(async tx => {
                        const owner = await tx.get('owners', ownerId, true);
                        assert(owner && owner.expiresAt > Date.now(), 'Data stage expired.', 'TRANSFER_EXPIRED');
                        await tx.put('owners', { ...owner, expiresAt: Date.now() + limits.transferIdleMs });
                        for (let offset = start; offset < Math.min(encoded.length, start + CHUNK_BYTES * 32); offset += CHUNK_BYTES) {
                            const ordinal = offset / CHUNK_BYTES;
                            await tx.insert('chunks', { id: key(blobId, ordinal), namespace, blobId, ordinal,
                                data: encoded.slice(offset, offset + CHUNK_BYTES) });
                        }
                    });
                    return repository.transaction(async tx => {
                        await tx.lock(key(namespace, 'blob', sha256));
                        const owner = await tx.get('owners', ownerId, true);
                        assert(owner && owner.expiresAt > Date.now(), 'Data stage expired.', 'TRANSFER_EXPIRED');
                        const [duplicate] = await tx.find('blobs', { namespace, rootId: sha256, kind, state: 'complete' }, { limit: 1 });
                        const chosen = duplicate ?? { id: blobId, namespace, rootId: sha256, sha256, bytes: encoded.length, kind, state: 'complete' };
                        if (!duplicate) {
                            await tx.put('blobs', chosen);
                            if (kind === 'index') await link(tx, namespace, `blob:${blobId}`, captured);
                        }
                        await tx.put('refs', { id: key(ownerId, chosen.id), namespace, ownerId, blobId: chosen.id });
                        return { id: chosen.id, bytes: chosen.bytes, sha256, kind };
                    });
                })());
                return dataReference({ ...projection, $data: await cache.get(cacheKey) });
            },
            async close() {
                if (closed) return; closed = true;
                await repository.transaction(async tx => { await unlink(tx, ownerId); await tx.remove('owners', ownerId); });
            },
        };
    }
    /** Small diagnostic/index writes join an existing domain transaction atomically. */
    async function putSmall(tx, namespace, value, kind, projection = {}) {
        assert(['index', 'diagnostic'].includes(kind), 'Only diagnostic metadata can be written inline.');
        const captured = snapshot(value, { maxBytes: max[kind] });
        const bytes = new TextEncoder().encode(stableStringify(captured)); const sha256 = await digest(bytes);
        await tx.lock(key(namespace, 'blob', sha256));
        const [existing] = await tx.find('blobs', { namespace, rootId: sha256, kind, state: 'complete' }, { limit: 1 });
        const blob = existing ?? { id: id('blob'), namespace, rootId: sha256, sha256, bytes: bytes.length, kind, state: 'complete' };
        if (!existing) {
            await tx.insert('blobs', blob);
            for (let offset = 0; offset < bytes.length; offset += CHUNK_BYTES) await tx.insert('chunks', {
                id: key(blob.id, offset / CHUNK_BYTES), namespace, blobId: blob.id, ordinal: offset / CHUNK_BYTES,
                data: bytes.slice(offset, offset + CHUNK_BYTES) });
            if (kind === 'index') await link(tx, namespace, `blob:${blob.id}`, captured);
        }
        return dataReference({ ...projection, $data: { id: blob.id, bytes: blob.bytes, sha256, kind } });
    }
    function transaction(callback) {
        return repository.transaction(raw => {
            const tx = { ...raw,
                async put(table, row) {
                    if (['tasks', 'jobs', 'aggregates', 'records'].includes(table)) {
                        await unlink(raw, `${table}:${row.id}`);
                        await link(raw, row.namespace, `${table}:${row.id}`, row);
                    }
                    await raw.put(table, row);
                },
                async insert(table, row) {
                    if (['tasks', 'jobs', 'aggregates', 'records'].includes(table)) await link(raw, row.namespace, `${table}:${row.id}`, row);
                    await raw.insert(table, row);
                },
                async remove(table, rowId) {
                    if (['tasks', 'jobs', 'aggregates', 'records'].includes(table)) await unlink(raw, `${table}:${rowId}`);
                    await raw.remove(table, rowId);
                },
            };
            return callback(tx);
        });
    }
    async function maintain(namespace) {
        const candidates = await repository.transaction(async tx => {
            for (const owner of await tx.find('owners', { namespace, expiresAt: { lte: Date.now() } }, { limit: 100, lock: true })) {
                await unlink(tx, owner.id);
                for (const table of ['records', 'frames']) {
                    const rows = await tx.find(table, { namespace, ownerId: owner.id }, { limit: 100 });
                    for (const row of rows) { await unlink(tx, `${table}:${row.id}`); await tx.remove(table, row.id); }
                }
                if (!await tx.count('records', { ownerId: owner.id }) && !await tx.count('frames', { ownerId: owner.id })) await tx.remove('owners', owner.id);
            }
            const cursorId = key(namespace, 'blob-gc');
            const cursor = await tx.get('controls', cursorId);
            let candidates = await tx.find('blobs', { namespace, ...(cursor ? { id: { gt: cursor.cursor } } : {}) }, { limit: 100 });
            if (!candidates.length && cursor) candidates = await tx.find('blobs', { namespace }, { limit: 100 });
            if (candidates.length) await tx.put('controls', { id: cursorId, namespace, cursor: candidates.at(-1).id });
            else await tx.remove('controls', cursorId);
            return candidates;
        });
        // A collector holds one blob lock at a time, preventing inverse lock orders
        // with transactions that link several independent immutable values.
        for (const blob of candidates) await repository.transaction(async tx => {
            if ((await tx.find('refs', { namespace, blobId: blob.id }, { limit: 1 })).length) return;
            await tx.lock(key(namespace, 'blob', blob.sha256));
            const current = await tx.get('blobs', blob.id, true);
            if (!current) return;
            if ((await tx.find('refs', { namespace, blobId: blob.id }, { limit: 1 })).length) return;
            // Partial deletion must never remain eligible for deduplication or new pins.
            if (current.state !== 'collecting') await tx.put('blobs', { ...current, state: 'collecting' });
            for (const chunk of await tx.find('chunks', { namespace, blobId: blob.id }, { limit: 100 })) await tx.remove('chunks', chunk.id);
            if (!(await tx.find('chunks', { namespace, blobId: blob.id }, { limit: 1 })).length) {
                await unlink(tx, `blob:${blob.id}`); await tx.remove('blobs', blob.id);
            }
        });
    }
    return Object.freeze({ stage, transaction, raw: repository, hydrate, read, link, unlink, maintain, putSmall });
}
