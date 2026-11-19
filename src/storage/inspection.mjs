import { referenceNode } from './data-ref.mjs';
import { assert, freeze, id, fingerprint } from '../util.mjs';
import { CHUNK_BYTES } from './values.mjs';

const key = (...parts) => JSON.stringify(parts);
const cursor = value => {
    assert(value === undefined || typeof value === 'string' && /^\d+$/.test(value) && Number.isSafeInteger(Number(value)), 'Invalid inspection cursor.');
    return value === undefined ? -1 : Number(value);
};
export function base64(bytes) {
    const parts = [];
    for (let offset = 0; offset < bytes.length; offset += 8192) parts.push(String.fromCharCode(...bytes.subarray(offset, offset + 8192)));
    return btoa(parts.join(''));
}
export function unbase64(text) {
    assert(typeof text === 'string' && text.length <= 87384 && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(text), 'Invalid transfer chunk.', 'INVALID_TRANSFER');
    const decoded = atob(text);
    const bytes = new Uint8Array(decoded.length);
    for (let index = 0; index < decoded.length; index++) bytes[index] = decoded.charCodeAt(index);
    return bytes;
}

/** Snapshot rows copy metadata only. Immutable value ownership is pinned atomically. */
export function createInspection(data, limits) {
    async function owner(tx, namespace, snapshotId, renew = true) {
        if (renew) await tx.lock(key(namespace, 'transfers'));
        const saved = await tx.get('owners', snapshotId, true);
        assert(saved?.namespace === namespace && saved.kind === 'snapshot', 'Snapshot not found.', 'NOT_FOUND');
        assert(saved.expiresAt > Date.now(), 'Snapshot expired.', 'TRANSFER_EXPIRED');
        if (renew) { saved.expiresAt = Date.now() + limits.transferIdleMs; await tx.put('owners', saved); }
        return saved;
    }
    async function open(namespace, taskId, prepared) {
        return data.transaction(async tx => {
            await tx.lock(key(namespace, 'transfers'));
            assert(await tx.count('owners', { namespace, kind: { in: ['snapshot', 'import', 'import-complete'] }, expiresAt: { gt: Date.now() } }) < limits.maxTransfers,
                'Transfer capacity reached.', 'RESOURCE_LIMIT');
            const task = prepared?.task ?? await tx.get('tasks', taskId, true, namespace);
            assert(task?.namespace === namespace, 'Execution not found.', 'NOT_FOUND');
            const jobs = prepared?.jobs ?? await tx.find('jobs', { namespace, taskId }, { limit: 512 });
            const snapshotId = id('snapshot'); const expiresAt = Date.now() + limits.transferIdleMs;
            await tx.insert('owners', { id: snapshotId, namespace, kind: 'snapshot', taskId, flow: task.flow, expiresAt, catalogued: false });
            let ordinal = 0;
            for (const [kind, row] of [['task', task], ...jobs.sort((a, b) => a.id.localeCompare(b.id)).map(job => ['job', job])]) {
                await tx.insert('records', { id: key(snapshotId, ordinal), namespace, ownerId: snapshotId, ordinal: ordinal++, kind, record: row });
            }
            return freeze({ id: snapshotId, taskId, flow: task.flow, expiresAt, schemaVersion: 3 });
        });
    }
    // Expand only generated index values; application JSON remains opaque and unread.
    async function expand(tx, namespace, value) {
        if (!value || typeof value !== 'object') return value;
        if (referenceNode(value)) {
            if (value.$data.kind !== 'index') return value;
            return expand(tx, namespace, await data.read(tx, namespace, value.$data));
        }
        if (Array.isArray(value)) return Promise.all(value.map(item => expand(tx, namespace, item)));
        return Object.fromEntries(await Promise.all(Object.entries(value).map(async ([name, item]) => [name, await expand(tx, namespace, item)])));
    }
    async function catalogue(namespace, snapshotId) {
        return data.transaction(async tx => {
            const saved = await owner(tx, namespace, snapshotId);
            if (saved.catalogued) return;
            const rows = await tx.find('records', { namespace, ownerId: snapshotId, kind: { in: ['task', 'job'] } }, { limit: 512, order: ['ordinal'] });
            let ordinal = 0;
            for (const row of rows) {
                const expanded = await expand(tx, namespace, row.record);
                // Store the expanded index as small per-part records, not a composed payload.
                async function walk(value, path) {
                    if (!value || typeof value !== 'object') return;
                    if (referenceNode(value)) {
                        const partId = await fingerprint([snapshotId, row.kind, row.record.id, path]);
                        await tx.insert('records', { id: key(snapshotId, 'part', partId), namespace, ownerId: snapshotId, kind: 'part',
                            ordinal: ordinal++, partId, entity: row.kind, entityId: row.record.id, path, ref: value.$data,
                            ...(value.responseUnavailable === true ? { responseUnavailable: true } : {}) });
                        return;
                    }
                    for (const name of Object.keys(value).sort()) await walk(value[name], [...path, name]);
                }
                await walk(expanded, []);
            }
            await tx.put('owners', { ...saved, catalogued: true, expiresAt: Date.now() + limits.transferIdleMs });
        });
    }
    async function list(namespace, snapshotId, { collection = 'jobs', after, limit = 25 } = {}) {
        assert(['jobs', 'data'].includes(collection) && Number.isSafeInteger(limit) && limit > 0 && limit <= 100, 'Invalid snapshot page.');
        const ordinal = cursor(after);
        if (collection === 'data') await catalogue(namespace, snapshotId);
        return data.transaction(async tx => {
            await owner(tx, namespace, snapshotId);
            const rows = await tx.find('records', { namespace, ownerId: snapshotId, kind: collection === 'data' ? 'part' : 'job', ordinal: { gt: ordinal } }, { limit, order: ['ordinal'] });
            const items = rows.map(row => collection === 'data' ? { id: row.partId, entity: row.entity, entityId: row.entityId, path: row.path,
                bytes: row.ref.bytes, sha256: row.ref.sha256, kind: row.ref.kind, ...(row.responseUnavailable ? { responseUnavailable: true } : {}) } : {
                id: row.record.id, kind: row.record.kind, status: row.record.state, required: Boolean(row.record.required),
                connectionId: row.record.plan?.id ?? null, connector: row.record.plan?.connector ?? null,
                attempts: row.record.attempts?.length ?? 0, completion: row.record.completion?.status ?? 'untracked',
                childTaskId: row.record.childTaskId ?? null, nextFlow: row.record.plan?.nextFlow ?? null,
                admissionAttempts: row.record.admissionAttempts ?? 0, nextAttemptAt: row.record.state === 'retry_pending' ? row.record.runAt : null,
                deadlineAt: row.record.deadlineAt ?? null, errorCode: row.record.error?.code ?? null, hasPayload: Object.hasOwn(row.record, 'payload'), hasReceipt: Boolean(row.record.receipt) });
            return freeze({ items, next: rows.length === limit ? String(rows.at(-1).ordinal) : null });
        });
    }
    async function read(namespace, snapshotId, partId, { offset = 0, maxBytes = CHUNK_BYTES } = {}) {
        assert(Number.isSafeInteger(offset) && offset >= 0 && Number.isSafeInteger(maxBytes) && maxBytes > 0 && maxBytes <= CHUNK_BYTES,
            'Invalid data page.');
        await catalogue(namespace, snapshotId);
        return data.transaction(async tx => {
            await owner(tx, namespace, snapshotId);
            const part = await tx.get('records', key(snapshotId, 'part', partId));
            assert(part?.namespace === namespace && part.ownerId === snapshotId, 'Data part not found.', 'NOT_FOUND');
            assert(offset <= part.ref.bytes, 'Data offset exceeds its size.');
            const size = Math.min(maxBytes, part.ref.bytes - offset); const result = new Uint8Array(size); let copied = 0;
            while (copied < size) {
                const position = offset + copied; const index = Math.floor(position / CHUNK_BYTES);
                const chunk = await tx.get('chunks', key(part.ref.id, index));
                assert(chunk?.namespace === namespace && chunk.data?.length > position % CHUNK_BYTES, 'Stored data is incomplete.', 'DATA_UNAVAILABLE');
                const piece = chunk.data.subarray(position % CHUNK_BYTES, Math.min(chunk.data.length, position % CHUNK_BYTES + size - copied));
                result.set(piece, copied); copied += piece.length;
            }
            return { encoding: 'base64', data: base64(result), offset, bytes: part.ref.bytes, sha256: part.ref.sha256,
                nextOffset: offset + size < part.ref.bytes ? offset + size : null };
        });
    }
    async function readValue(namespace, snapshotId, partId) {
        await catalogue(namespace, snapshotId);
        return data.transaction(async tx => {
            await owner(tx, namespace, snapshotId);
            const part = await tx.get('records', key(snapshotId, 'part', partId));
            assert(part?.namespace === namespace && part.ownerId === snapshotId, 'Data part not found.', 'NOT_FOUND');
            return freeze(await data.hydrate(tx, namespace, { $data: part.ref, ...(part.responseUnavailable ? { responseUnavailable: true } : {}) }));
        });
    }
    async function close(namespace, snapshotId) {
        await data.transaction(async tx => {
            const saved = await tx.get('owners', snapshotId, true);
            if (!saved) return;
            assert(saved.namespace === namespace && saved.kind === 'snapshot', 'Snapshot not found.', 'NOT_FOUND');
            await tx.put('owners', { ...saved, expiresAt: 0 });
        });
    }
    async function describe(namespace, snapshotId) {
        return data.transaction(async tx => {
            const saved = await owner(tx, namespace, snapshotId, false);
            return { id: saved.id, taskId: saved.taskId, flow: saved.flow, expiresAt: saved.expiresAt, schemaVersion: 3 };
        });
    }
    async function record(namespace, snapshotId, { metadata = false } = {}) {
        return data.transaction(async tx => {
            await owner(tx, namespace, snapshotId);
            const rows = await tx.find('records', { namespace, ownerId: snapshotId, kind: { in: ['task', 'job'] } }, { limit: 512, order: ['ordinal'] });
            const saved = { task: rows.find(row => row.kind === 'task').record, jobs: rows.filter(row => row.kind === 'job').map(row => row.record) };
            return metadata ? saved : data.hydrate(tx, namespace, saved);
        });
    }
    return Object.freeze({ open, list, read, readValue, close, describe, catalogue, expand, owner, record });
}
