import { abortError, assert, validateSignal } from '../util.mjs';
import { parseFrames, wireBytes } from '../storage/framing.mjs';
import { ADMISSION_OWNER } from './admission-owner.mjs';

/** Stream ownership is local to the iterator; explicit sessions use renewable leases. */
export function runnerTransfers({ store, namespace, ready, track, trackAdmission, submit, get, signal: lifetime }) {
    const snapshots = new Set();
    const imports = new Set();
    async function finish(transferId, signal = lifetime) {
        const saved = await store.importedTask(namespace, transferId);
        if (signal.aborted) throw abortError('import');
        if (saved.taskId) return get(saved.taskId);
        const flows = await ready;
        assert(flows[saved.flow]?.runtimeRevision === saved.runtimeRevision, 'Import destination revision changed.', 'REVISION_UNAVAILABLE');
        const admitted = await submit({ flow: saved.flow, kind: 'result', result: saved.result, evidenceAt: saved.evidenceAt,
            ...(saved.validation === undefined ? {} : { validation: saved.validation }), idempotencyKey: `import:${saved.checksum}` },
        { source: 'import', signal, validationRequest: saved.originalRequest, aggregateRequest: saved.aggregateRequest,
            [ADMISSION_OWNER]: { kind: 'import', id: transferId } }, true);
        await store.completeImport(namespace, transferId, admitted.taskId);
        return admitted;
    }
    const methods = {
        async readExportFrames(snapshotId, query) { return track(() => store.readExportFrames(namespace, snapshotId, query)); },
        async openImport({ flow } = {}) {
            return track(async () => {
                const flows = await ready;
                assert(typeof flow === 'string' && Object.hasOwn(flows, flow), 'Import requires an explicitly authorized flow.');
                return store.openImport(namespace, flow, flows[flow].runtimeRevision);
            });
        },
        async appendImport(transferId, frame) { return track(() => store.appendImport(namespace, transferId, frame)); },
        async finishImport(transferId, { signal } = {}) {
            validateSignal(signal);
            return trackAdmission(() => finish(transferId, signal ? AbortSignal.any([signal, lifetime]) : lifetime));
        },
        async closeImport(transferId) { return track(() => store.closeImport(namespace, transferId)); },
        async describeImport(transferId) { return track(() => store.describeImport(namespace, transferId)); },
        async *exportTask(taskId, { signal } = {}) {
            validateSignal(signal);
            const combined = signal ? AbortSignal.any([signal, lifetime]) : lifetime;
            if (combined.aborted) throw abortError('export');
            const lease = await track(() => store.openSnapshot(namespace, taskId));
            snapshots.add(lease.id);
            try {
                let after;
                do {
                    if (combined.aborted) throw abortError('export');
                    const page = await track(() => store.readExportFrames(namespace, lease.id, { after }));
                    for (const frame of page.frames) {
                        const encoded = wireBytes(frame); const line = new Uint8Array(encoded.length + 1);
                        line.set(encoded); line[encoded.length] = 10; yield line;
                    }
                    after = page.next;
                } while (after);
            } finally {
                snapshots.delete(lease.id);
                await store.closeSnapshot(namespace, lease.id).catch(() => {});
            }
        },
        async importTask(chunks, { flow, signal } = {}) {
            validateSignal(signal);
            const combined = signal ? AbortSignal.any([signal, lifetime]) : lifetime;
            if (combined.aborted) throw abortError('import');
            return trackAdmission(async () => {
                const flows = await ready;
                assert(typeof flow === 'string' && Object.hasOwn(flows, flow), 'Import requires an explicitly authorized flow.');
                if (combined.aborted) throw abortError('import');
                const lease = await store.openImport(namespace, flow, flows[flow].runtimeRevision); imports.add(lease.id);
                try {
                    for await (const frame of parseFrames(chunks, { signal: combined, maxBytes: store.limits.maxTransferBytes, idleMs: store.limits.transferIdleMs }))
                        await store.appendImport(namespace, lease.id, frame);
                    if (combined.aborted) throw abortError('import');
                    return await finish(lease.id, combined);
                } finally {
                    imports.delete(lease.id);
                    await store.closeImport(namespace, lease.id).catch(() => {});
                }
            });
        },
    };
    return { methods, async close() {
        await Promise.allSettled([...snapshots].map(snapshotId => store.closeSnapshot(namespace, snapshotId)));
        await Promise.allSettled([...imports].map(transferId => store.closeImport(namespace, transferId)));
    } };
}
