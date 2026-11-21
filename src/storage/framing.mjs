import { referenceNode } from './data-ref.mjs';
import { assert, bounded, snapshot, stableStringify, validateSignal, abortError } from '../util.mjs';
import { digest } from './values.mjs';

export const FRAME_BYTES = 262144;
export const EMPTY_HASH = '0'.repeat(64);
export const hashPattern = /^[0-9a-f]{64}$/;
export const wireBytes = frame => new TextEncoder().encode(stableStringify(frame));

/** Classify only decoder failures as corrupt input. Iterator and storage errors keep their own disposition. */
export function decodeTransferText(decoder, bytes, options) {
    try { return decoder.decode(bytes, options); }
    catch { assert(false, 'Transfer contains invalid UTF-8.', 'INVALID_TRANSFER'); }
}

export async function chainHash(previous, bytes) {
    assert(hashPattern.test(previous), 'Invalid integrity state.', 'INVALID_TRANSFER');
    const combined = new Uint8Array(32 + bytes.length);
    combined.set(previous.match(/../g).map(value => parseInt(value, 16)));
    combined.set(bytes, 32);
    return digest(combined);
}

/** Independently bounded lines permit incremental verification without a JSON parser dependency. */
export async function* parseFrames(chunks, { signal, maxBytes = 1073741824, idleMs = 300000 } = {}) {
    validateSignal(signal);
    if (signal?.aborted) throw abortError('import');
    const decoder = new TextDecoder('utf-8', { fatal: true });
    const encoder = new TextEncoder();
    let pending = ''; let frameBytes = 0; let size = 0;
    const iterator = chunks?.[Symbol.asyncIterator]?.() ?? chunks?.[Symbol.iterator]?.();
    assert(iterator && typeof iterator.next === 'function', 'Transfer input must be a byte iterable.', 'INVALID_TRANSFER');
    let exhausted = false;
    try { while (true) {
        const { done, value: chunk } = await bounded(() => iterator.next(), { signal, timeoutMs: idleMs, stage: 'import' });
        if (done) { exhausted = true; break; }
        if (signal?.aborted) throw abortError('import');
        assert(chunk instanceof Uint8Array, 'Transfer streams require byte chunks.', 'INVALID_TRANSFER');
        size += chunk.length; assert(size <= maxBytes, 'Transfer exceeds its byte limit.', 'RESOURCE_LIMIT');
        // Even a caller-supplied giant chunk is decoded in bounded slices.
        for (let offset = 0; offset < chunk.length; offset += 65536) {
            const text = decodeTransferText(decoder, chunk.subarray(offset, offset + 65536), { stream: true });
            // The decoder emits complete code points. Scan and charge only newly decoded text.
            let start = 0;
            while (start < text.length) {
                const newline = text.indexOf('\n', start);
                const part = text.slice(start, newline < 0 ? text.length : newline);
                frameBytes += encoder.encode(part).length;
                assert(frameBytes <= FRAME_BYTES, 'Transfer frame exceeds its byte limit.', 'INVALID_TRANSFER');
                pending += part;
                if (newline < 0) break;
                assert(frameBytes > 0, 'Empty transfer frame.', 'INVALID_TRANSFER');
                let frame;
                try { frame = JSON.parse(pending); } catch { assert(false, 'Invalid transfer JSON.', 'INVALID_TRANSFER'); }
                pending = ''; frameBytes = 0; start = newline + 1;
                yield snapshot(frame, { maxBytes: FRAME_BYTES });
                if (signal?.aborted) throw abortError('import');
            }
        }
    } } finally {
        // Do not let a non-cooperative producer extend cancellation or shutdown.
        if (!exhausted) void Promise.resolve().then(() => iterator.return?.()).catch(() => {});
    }
    pending += decodeTransferText(decoder);
    assert(pending.length === 0, 'Transfer ended without a complete frame.', 'INVALID_TRANSFER');
}

/** Public metadata uses content hashes, never source-specific physical blob IDs. */
export function portableMetadata(value) {
    if (!value || typeof value !== 'object') return value;
    if (referenceNode(value)) {
        const { id: _id, ...ref } = value.$data;
        return { ...value, $data: ref };
    }
    if (Array.isArray(value)) return value.map(portableMetadata);
    return Object.fromEntries(Object.entries(value).map(([name, item]) => [name, portableMetadata(item)]));
}

export function frameShape(value) {
    assert(value && typeof value === 'object' && Number.isSafeInteger(value.seq) && value.seq >= 0, 'Invalid frame sequence.', 'INVALID_TRANSFER');
    const fields = {
        header: ['type', 'seq', 'kind', 'schemaVersion'], record: ['type', 'seq', 'entity', 'recordId', 'index', 'final', 'data'],
        chunk: ['type', 'seq', 'sha256', 'index', 'data'], end: ['type', 'seq', 'frameCount', 'checksum'],
    };
    assert(typeof value.type === 'string' && Object.hasOwn(fields, value.type) && Object.keys(value).length === fields[value.type].length &&
        fields[value.type].every(name => Object.hasOwn(value, name)), 'Invalid transfer frame fields.', 'INVALID_TRANSFER');
    if (value.type === 'header') assert(value.seq === 0 && value.kind === 'execution-export' && value.schemaVersion === 3, 'Unsupported transfer schema.', 'UNSUPPORTED_SCHEMA');
    if (['record', 'chunk'].includes(value.type)) assert(Number.isSafeInteger(value.index) && value.index >= 0 && typeof value.data === 'string', 'Invalid data frame.', 'INVALID_TRANSFER');
    if (value.type === 'record') assert(['task', 'job'].includes(value.entity) && typeof value.recordId === 'string' && value.recordId.length > 0 &&
        value.recordId.length <= 256 && typeof value.final === 'boolean', 'Invalid metadata frame.', 'INVALID_TRANSFER');
    if (value.type === 'chunk') assert(typeof value.sha256 === 'string' && hashPattern.test(value.sha256), 'Invalid data checksum.', 'INVALID_TRANSFER');
    if (value.type === 'end') assert(value.frameCount === value.seq && typeof value.checksum === 'string' && hashPattern.test(value.checksum), 'Invalid integrity trailer.', 'INVALID_TRANSFER');
    return value;
}
