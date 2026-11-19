import { referenceNode } from './data-ref.mjs';
import { assert, fingerprint, freeze, id, snapshot, stableStringify } from '../util.mjs';
import { CHUNK_BYTES, digest } from './values.mjs';
import { base64, unbase64 } from './inspection.mjs';
import { EMPTY_HASH, FRAME_BYTES, chainHash, frameShape, hashPattern, portableMetadata, wireBytes } from './framing.mjs';

const key = (...parts) => JSON.stringify(parts);
const valueLimits = { json: 67108864, plan: 67108864, diagnostic: 65536 };

/** Transfer state references immutable snapshots or isolated unverified chunks. */
export function createTransfers(data, inspection, limits) {
    const raw = data.raw;
    const admissionIdentity = owner => fingerprint([owner.namespace, 'import', owner.flow, `import:${owner.checksum}`]);
    async function importOwner(tx, namespace, transferId, { complete = false } = {}) {
        await tx.lock(key(namespace, 'transfers'));
        const owner = await tx.get('owners', transferId, true);
        assert(owner?.namespace === namespace && (owner.kind === 'import' || complete && owner.kind === 'import-complete'), 'Import not found.', 'NOT_FOUND');
        assert(owner.expiresAt > Date.now(), 'Import expired.', 'TRANSFER_EXPIRED');
        return owner;
    }
    async function openImport(namespace, flow, runtimeRevision) {
        return raw.transaction(async tx => {
            await tx.lock(key(namespace, 'transfers'));
            assert(await tx.count('owners', { namespace, kind: { in: ['snapshot', 'import', 'import-complete'] }, expiresAt: { gt: Date.now() } }) < limits.maxTransfers,
                'Transfer capacity reached.', 'RESOURCE_LIMIT');
            const owner = { id: id('import'), namespace, flow, runtimeRevision, kind: 'import', expiresAt: Date.now() + limits.transferIdleMs,
                seq: 0, hash: EMPTY_HASH, wireBytes: 0, recordCount: 0, currentRecord: null, currentBlob: null,
                lastBlob: '', phase: 'header', verifying: null, verified: false };
            await tx.insert('owners', owner);
            return { id: owner.id, flow, expiresAt: owner.expiresAt, schemaVersion: 3 };
        });
    }
    async function gather(tx, namespace, transferId, kind, rootId, maximum) {
        const rows = []; let after;
        do {
            const page = await tx.find('frames', { namespace, ownerId: transferId, kind, rootId }, { limit: 128, order: ['ordinal'], after });
            rows.push(...page); after = page.length === 128 ? { ordinal: page.at(-1).ordinal } : undefined;
        } while (after);
        const size = rows.reduce((sum, row) => sum + row.data.length, 0);
        assert(size <= maximum && rows.every((row, index) => row.ordinal === index), 'Incomplete or oversized transfer value.', 'INVALID_TRANSFER');
        const bytes = new Uint8Array(size); let offset = 0;
        for (const row of rows) { bytes.set(row.data, offset); offset += row.data.length; }
        return bytes;
    }
    function expectedReferences(value) {
        const refs = [];
        function visit(item) {
            if (!item || typeof item !== 'object') return;
            if (referenceNode(item)) {
                const ref = item.$data;
                assert(ref && Object.keys(ref).length === 3 && hashPattern.test(ref.sha256) && Object.hasOwn(valueLimits, ref.kind) &&
                    Number.isSafeInteger(ref.bytes) && ref.bytes > 0 && ref.bytes <= valueLimits[ref.kind], 'Invalid exported data reference.', 'INVALID_TRANSFER');
                refs.push(ref); return;
            }
            Object.values(item).forEach(visit);
        }
        visit(value); return refs;
    }
    async function verifyPending(namespace, transferId) {
        const work = await raw.transaction(async tx => {
            const owner = await importOwner(tx, namespace, transferId, { complete: true });
            if (!owner.verifying) return null;
            const pending = owner.verifying;
            return { pending, bytes: await gather(tx, namespace, transferId, pending.kind === 'record' ? 'incoming-record' : 'incoming-blob', pending.key,
                pending.kind === 'record' ? 4194304 : pending.bytes) };
        });
        if (!work) return;
        const text = new TextDecoder('utf-8', { fatal: true }).decode(work.bytes);
        let value;
        try { value = JSON.parse(text); } catch { assert(false, 'Invalid transferred JSON value.', 'INVALID_TRANSFER'); }
        if (work.pending.kind === 'blob') {
            assert(work.bytes.length === work.pending.bytes && await digest(work.bytes) === work.pending.key, 'Data checksum differs.', 'INVALID_TRANSFER');
            value = snapshot(value, { maxBytes: work.pending.maximum });
            assert(stableStringify(value) === text, 'Data is not canonical JSON.', 'INVALID_TRANSFER');
        } else {
            value = snapshot(value, { maxBytes: 4194304 });
            assert(value.id === work.pending.recordId && typeof value.namespace === 'string' &&
                (work.pending.entity !== 'task' || value.schemaVersion === 3 && typeof value.flow === 'string'), 'Invalid saved metadata.', 'INVALID_TRANSFER');
        }
        await raw.transaction(async tx => {
            const owner = await importOwner(tx, namespace, transferId, { complete: true });
            if (owner.verifying?.key !== work.pending.key) {
                const done = work.pending.kind === 'record'
                    ? await tx.get('records', key(transferId, 'metadata', work.pending.entity, work.pending.recordId))
                    : await tx.get('records', key(transferId, 'expected', work.pending.key));
                assert(done && (work.pending.kind === 'record' ? done.rootId === work.pending.key : done.state === 'verified'), 'Import verification changed.', 'CONFLICT');
                return;
            }
            if (work.pending.kind === 'record') {
                for (const ref of expectedReferences(value)) {
                    const refId = key(transferId, 'expected', ref.sha256); const previous = await tx.get('records', refId);
                    assert(!previous || previous.bytes === ref.bytes, 'Conflicting value descriptions.', 'INVALID_TRANSFER');
                    await tx.put('records', { id: refId, namespace, ownerId: transferId, kind: 'expected', rootId: ref.sha256,
                        bytes: ref.bytes, maximum: Math.min(previous?.maximum ?? Infinity, valueLimits[ref.kind]), state: 'pending' });
                }
                await tx.insert('records', { id: key(transferId, 'metadata', work.pending.entity, work.pending.recordId), namespace, ownerId: transferId,
                    kind: work.pending.entity === 'task' ? 'source-task' : 'source-job', rootId: work.pending.key });
                owner.currentRecord = null;
            } else {
                const expected = await tx.get('records', key(transferId, 'expected', work.pending.key));
                await tx.put('records', { ...expected, state: 'verified' });
                owner.lastBlob = work.pending.key; owner.currentBlob = null;
            }
            owner.verifying = null; owner.expiresAt = Date.now() + limits.transferIdleMs;
            await tx.put('owners', owner);
        });
    }
    async function appendImport(namespace, transferId, received) {
        const frame = frameShape(snapshot(received, { maxBytes: FRAME_BYTES }));
        const bytes = wireBytes(frame); const bodyHash = await digest(bytes);
        const binary = ['record', 'chunk'].includes(frame.type) ? unbase64(frame.data) : null;
        if (binary) assert(binary.length > 0 && binary.length <= CHUNK_BYTES, 'Invalid chunk size.', 'INVALID_TRANSFER');
        await raw.transaction(async tx => {
            const owner = await importOwner(tx, namespace, transferId, { complete: true });
            if (frame.seq < owner.seq) {
                const saved = await tx.get('frames', key(transferId, 'ledger', frame.seq));
                assert(saved?.hash === bodyHash, 'A frame sequence was reused with different data.', 'CONFLICT');
                await tx.put('owners', { ...owner, expiresAt: Date.now() + limits.transferIdleMs }); return;
            }
            assert(owner.kind === 'import' && !owner.verified && !owner.verifying && frame.seq === owner.seq, 'Import frame is out of order or verification is pending.', 'CONFLICT');
            assert(owner.wireBytes + bytes.length + 1 <= limits.maxTransferBytes, 'Transfer exceeds its byte limit.', 'RESOURCE_LIMIT');
            if (owner.phase === 'header') {
                assert(frame.type === 'header', 'Transfer header is required.', 'INVALID_TRANSFER'); owner.phase = 'records';
            } else if (frame.type === 'record') {
                assert(owner.phase === 'records' && owner.recordCount < 260, 'Invalid metadata order or count.', 'INVALID_TRANSFER');
                if (!owner.currentRecord) {
                    assert(frame.index === 0 && (owner.recordCount === 0 ? frame.entity === 'task' : frame.entity === 'job'), 'Invalid metadata sequence.', 'INVALID_TRANSFER');
                    assert(!await tx.get('records', key(transferId, 'metadata', frame.entity, frame.recordId)), 'Duplicate metadata record.', 'INVALID_TRANSFER');
                    owner.currentRecord = { entity: frame.entity, recordId: frame.recordId, index: 0, bytes: 0, key: String(owner.recordCount++) };
                }
                const current = owner.currentRecord;
                assert(current.entity === frame.entity && current.recordId === frame.recordId && current.index === frame.index &&
                    current.bytes + binary.length <= 4194304, 'Invalid metadata fragment.', 'INVALID_TRANSFER');
                await tx.insert('frames', { id: key(transferId, 'record', current.key, frame.index), namespace, ownerId: transferId,
                    kind: 'incoming-record', rootId: current.key, ordinal: frame.index, data: binary });
                current.index++; current.bytes += binary.length;
                if (frame.final) owner.verifying = { ...current, kind: 'record' };
            } else if (frame.type === 'chunk') {
                assert(!owner.currentRecord && owner.recordCount > 0, 'Metadata is incomplete.', 'INVALID_TRANSFER'); owner.phase = 'blobs';
                const expected = await tx.get('records', key(transferId, 'expected', frame.sha256));
                assert(expected && expected.namespace === namespace && expected.state === 'pending', 'Unexpected data value.', 'INVALID_TRANSFER');
                if (!owner.currentBlob) {
                    assert(frame.index === 0 && frame.sha256 > owner.lastBlob, 'Invalid value order.', 'INVALID_TRANSFER');
                    owner.currentBlob = { key: frame.sha256, index: 0, bytes: 0 };
                }
                const current = owner.currentBlob;
                assert(current.key === frame.sha256 && current.index === frame.index && current.bytes + binary.length <= expected.bytes,
                    'Invalid value fragment.', 'INVALID_TRANSFER');
                await tx.insert('frames', { id: key(transferId, 'blob', frame.sha256, frame.index), namespace, ownerId: transferId,
                    kind: 'incoming-blob', rootId: frame.sha256, ordinal: frame.index, data: binary });
                current.index++; current.bytes += binary.length;
                if (current.bytes === expected.bytes) owner.verifying = { kind: 'blob', key: frame.sha256, bytes: expected.bytes, maximum: expected.maximum };
            } else if (frame.type === 'end') {
                assert(!owner.currentRecord && !owner.currentBlob && owner.recordCount > 0 && frame.checksum === owner.hash &&
                    !await tx.count('records', { ownerId: transferId, kind: 'expected', state: 'pending' }), 'Incomplete transfer or invalid integrity trailer.', 'INVALID_TRANSFER');
                owner.verified = true; owner.checksum = frame.checksum;
                owner.rootId = await admissionIdentity(owner);
            } else assert(false, 'Unexpected transfer header.', 'INVALID_TRANSFER');
            await tx.insert('frames', { id: key(transferId, 'ledger', frame.seq), namespace, ownerId: transferId, kind: 'ledger', ordinal: frame.seq, hash: bodyHash });
            if (frame.type !== 'end') owner.hash = await chainHash(owner.hash, bytes);
            owner.seq++; owner.wireBytes += bytes.length + 1; owner.expiresAt = Date.now() + limits.transferIdleMs;
            await tx.put('owners', owner);
        });
        await verifyPending(namespace, transferId);
        return raw.transaction(async tx => { const owner = await importOwner(tx, namespace, transferId, { complete: true });
            return { nextSequence: owner.seq, verified: owner.verified, expiresAt: owner.expiresAt }; });
    }
    async function importedTask(namespace, transferId) {
        await verifyPending(namespace, transferId);
        return raw.transaction(async tx => {
            const owner = await importOwner(tx, namespace, transferId, { complete: true });
            // Previously verified sessions acquire the identity before their next finish attempt.
            if (owner.verified && !owner.rootId) {
                owner.rootId = await admissionIdentity(owner); await tx.put('owners', owner);
            }
            if (owner.resultTaskId) return { taskId: owner.resultTaskId, flow: owner.flow, runtimeRevision: owner.runtimeRevision };
            assert(owner.verified, 'Import has not passed integrity verification.', 'INVALID_TRANSFER');
            const [source] = await tx.find('records', { ownerId: transferId, kind: 'source-task' }, { limit: 1 });
            assert(source, 'Saved task metadata is missing.', 'INVALID_TRANSFER');
            const metadata = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(await gather(tx, namespace, transferId, 'incoming-record', source.rootId, 4194304)));
            const value = async wrapped => {
                if (wrapped === undefined) return undefined;
                const ref = wrapped.$data;
                assert(ref && ref.kind === 'json', 'Saved evidence requires an application data reference.', 'INVALID_TRANSFER');
                const expected = await tx.get('records', key(transferId, 'expected', ref.sha256));
                assert(expected?.state === 'verified' && expected.bytes === ref.bytes, 'Saved evidence is unavailable.', 'INVALID_TRANSFER');
                return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(await gather(tx, namespace, transferId, 'incoming-blob', ref.sha256, expected.bytes)));
            };
            let requestBytes = 0;
            async function expandRequest(item) {
                if (!item || typeof item !== 'object') return item;
                if (referenceNode(item)) {
                    requestBytes += item.$data.bytes;
                    assert(requestBytes <= limits.maxBytes, 'Saved request exceeds the value limit.', 'RESOURCE_LIMIT');
                    return value(item);
                }
                if (Array.isArray(item)) return Promise.all(item.map(expandRequest));
                return Object.fromEntries(await Promise.all(Object.entries(item)
                    .map(async ([name, child]) => [name, await expandRequest(child)])));
            }
            assert(metadata.result !== undefined, 'Import requires a saved result.', 'INVALID_TRANSFER');
            return { flow: owner.flow, runtimeRevision: owner.runtimeRevision, checksum: owner.checksum,
                evidenceAt: metadata.evidenceAt, result: await value(metadata.result), validation: await value(metadata.input?.validation),
                originalRequest: await (metadata.aggregateRequest === true ? expandRequest(metadata.request) : value(metadata.request)),
                ...(metadata.aggregateRequest === true ? { aggregateRequest: true } : {}) };
        });
    }
    async function completeImport(namespace, transferId, taskId) {
        return raw.transaction(async tx => {
            const owner = await importOwner(tx, namespace, transferId, { complete: true });
            assert(!owner.resultTaskId || owner.resultTaskId === taskId, 'Import was completed with another task.', 'CONFLICT');
            await tx.put('owners', { ...owner, kind: 'import-complete', resultTaskId: taskId });
        });
    }
    async function closeImport(namespace, transferId) {
        return raw.transaction(async tx => {
            const owner = await tx.get('owners', transferId, true);
            if (!owner) return;
            assert(owner.namespace === namespace && ['import', 'import-complete'].includes(owner.kind), 'Import not found.', 'NOT_FOUND');
            await tx.put('owners', { ...owner, expiresAt: 0 });
        });
    }
    async function describeImport(namespace, transferId) {
        return raw.transaction(async tx => {
            const owner = await importOwner(tx, namespace, transferId, { complete: true });
            return { id: owner.id, flow: owner.flow, expiresAt: owner.expiresAt, schemaVersion: 3 };
        });
    }
    async function prepareExport(namespace, snapshotId) {
        await inspection.catalogue(namespace, snapshotId);
        return raw.transaction(async tx => {
            const owner = await inspection.owner(tx, namespace, snapshotId);
            if (owner.exportState?.blobOrderVersion === 1) return;
            let after; const unique = new Map();
            do {
                const rows = await tx.find('records', { namespace, ownerId: snapshotId, kind: 'part' }, { limit: 128, order: ['ordinal'], after });
                for (const row of rows) unique.set(row.ref.sha256, row.ref);
                after = rows.length === 128 ? { ordinal: rows.at(-1).ordinal } : undefined;
            } while (after);
            // Wire order is fixed independently of host and database collations.
            const values = [...unique].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
            for (let ordinal = 0; ordinal < values.length; ordinal++) {
                const [sha256, ref] = values[ordinal];
                await tx.put('records', { id: key(snapshotId, 'export-blob', sha256), namespace, ownerId: snapshotId,
                    kind: 'export-blob', rootId: sha256, ordinal, ref });
            }
            // Earlier sessions keep their saved frames and hash cursor during metadata upgrade.
            const state = owner.exportState ?? { phase: 'header', seq: 0, hash: EMPTY_HASH, record: 0, offset: 0,
                blobAfter: '', currentBlob: null, chunk: 0, wireBytes: 0 };
            await tx.put('owners', { ...owner, exportState: { ...state, blobOrderVersion: 1 } });
        });
    }
    async function publicFrame(tx, namespace, saved) {
        let binary = saved.data;
        if (saved.blobId) binary = (await tx.get('chunks', key(saved.blobId, saved.chunkIndex)))?.data;
        assert(!saved.blobId || binary, 'Snapshot data is missing.', 'DATA_UNAVAILABLE');
        return { ...saved.frame, ...(binary ? { data: base64(binary) } : {}) };
    }
    async function nextFrame(namespace, snapshotId, sequence) {
        for (;;) {
            const captured = await raw.transaction(async tx => {
                const owner = await inspection.owner(tx, namespace, snapshotId);
                const existing = await tx.get('frames', key(snapshotId, sequence));
                if (existing) return { existing: await publicFrame(tx, namespace, existing) };
                assert(owner.exportState.seq === sequence && owner.exportState.phase !== 'done', 'Invalid export cursor.', 'CONFLICT');
                const state = { ...owner.exportState }; let frame; let binary; let blobId; let chunkIndex;
                while (!frame) {
                    if (state.phase === 'header') { frame = { type: 'header', seq: sequence, kind: 'execution-export', schemaVersion: 3 }; state.phase = 'records'; }
                    else if (state.phase === 'records') {
                        const row = await tx.get('records', key(snapshotId, state.record));
                        if (!row) { state.phase = 'blobs'; continue; }
                        const metadata = portableMetadata(await inspection.expand(tx, namespace, row.record));
                        const bytes = wireBytes(metadata);
                        assert(bytes.length <= 4194304, 'Export metadata exceeds its limit.', 'RESOURCE_LIMIT');
                        binary = bytes.slice(state.offset, state.offset + CHUNK_BYTES);
                        frame = { type: 'record', seq: sequence, entity: row.kind, recordId: row.record.id,
                            index: state.offset / CHUNK_BYTES, final: state.offset + binary.length === bytes.length };
                        state.offset += binary.length;
                        if (frame.final) { state.record++; state.offset = 0; }
                    } else if (state.phase === 'blobs') {
                        let rows;
                        if (state.currentBlob) rows = [await tx.get('records', key(snapshotId, 'export-blob', state.currentBlob))];
                        else {
                            const previous = state.blobAfter ? await tx.get('records', key(snapshotId, 'export-blob', state.blobAfter)) : null;
                            assert(!state.blobAfter || previous, 'Export cursor data is missing.', 'DATA_UNAVAILABLE');
                            rows = await tx.find('records', { namespace, ownerId: snapshotId, kind: 'export-blob',
                                ordinal: { gt: previous?.ordinal ?? -1 } }, { limit: 1, order: ['ordinal'] });
                        }
                        if (!rows[0]) { state.phase = 'end'; continue; }
                        const ref = rows[0].ref; blobId = ref.id; chunkIndex = state.chunk;
                        binary = (await tx.get('chunks', key(blobId, chunkIndex)))?.data;
                        assert(binary, 'Snapshot data is missing.', 'DATA_UNAVAILABLE');
                        frame = { type: 'chunk', seq: sequence, sha256: ref.sha256, index: chunkIndex };
                        state.chunk++; state.currentBlob = ref.sha256;
                        if (state.chunk * CHUNK_BYTES >= ref.bytes) { state.blobAfter = ref.sha256; state.currentBlob = null; state.chunk = 0; }
                    } else { frame = { type: 'end', seq: sequence, frameCount: sequence, checksum: state.hash }; state.phase = 'done'; }
                }
                return { state, frame, binary, blobId, chunkIndex };
            });
            if (captured.existing) return captured.existing;
            const frame = { ...captured.frame, ...(captured.binary ? { data: base64(captured.binary) } : {}) };
            const bytes = wireBytes(frame);
            assert(bytes.length <= FRAME_BYTES && captured.state.wireBytes + bytes.length + 1 <= limits.maxTransferBytes, 'Export exceeds its transfer limit.', 'RESOURCE_LIMIT');
            const hash = frame.type === 'end' ? captured.state.hash : await chainHash(captured.state.hash, bytes);
            const saved = await raw.transaction(async tx => {
                const owner = await inspection.owner(tx, namespace, snapshotId);
                if (owner.exportState.seq !== sequence) return false;
                await tx.insert('frames', { id: key(snapshotId, sequence), namespace, ownerId: snapshotId, ordinal: sequence, kind: 'export-frame',
                    frame: captured.frame, ...(captured.blobId ? { blobId: captured.blobId, chunkIndex: captured.chunkIndex } : captured.binary ? { data: captured.binary } : {}) });
                await tx.put('owners', { ...owner, exportState: { ...captured.state, seq: sequence + 1, hash, wireBytes: captured.state.wireBytes + bytes.length + 1 } });
                return true;
            });
            if (saved) return frame;
        }
    }
    async function readExportFrames(namespace, snapshotId, { after, limit = 1 } = {}) {
        assert(after === undefined || typeof after === 'string' && /^\d+$/.test(after) && Number.isSafeInteger(Number(after)), 'Invalid export cursor.');
        assert(Number.isSafeInteger(limit) && limit >= 1 && limit <= 8, 'Export page size must be 1–8.');
        await prepareExport(namespace, snapshotId);
        const frames = []; let sequence = after === undefined ? 0 : Number(after) + 1;
        for (let index = 0; index < limit; index++) {
            const frame = await nextFrame(namespace, snapshotId, sequence++); frames.push(frame);
            if (frame.type === 'end') return freeze({ frames, next: null });
        }
        return freeze({ frames, next: String(sequence - 1) });
    }
    return Object.freeze({ openImport, appendImport, importedTask, completeImport, closeImport, describeImport, readExportFrames });
}
