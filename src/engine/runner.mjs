import { abortError, assert, bounded, captureObject, delay, freeze, id, isRecord, keysOnly, publicError, runnerIdentity, snapshot, validateSignal } from '../util.mjs';
import { createMemoryStore } from '../storage/memory.mjs';
import { compileFlows } from './flows.mjs';
import { prepare } from './admission.mjs';
import { executeClaim } from './worker.mjs';
import { planOutputs } from './planning.mjs';
import { report } from './reports.mjs';
import { validateResolution } from './reconciliation.mjs';
import { runnerTransfers } from './transfers.mjs';
import { ADMISSION_OWNER } from './admission-owner.mjs';
import { ADMISSION_LIFETIME, createAdmissionLifetime } from './admission-lifetime.mjs';
import { AGGREGATE_EVENT } from './aggregation-values.mjs';
import { acknowledgementFailure } from './receipts.mjs';

/** One application service powers embedded, command-line and network hosts. */
export function createRunner(options) {
    const identity = runnerIdentity(options);
    const store = options.store ?? createMemoryStore(options.limits);
    const namespace = identity.namespace;
    const worker = identity.workerId ?? id('worker');
    const controller = new AbortController();
    const pending = new Set();
    let closed = false; let closing; let cursor = 0;
    const ready = compileFlows(options).then(flows => {
        assert(store.capabilities.schemaVersion === 3 && store.capabilities.dataParts && store.capabilities.snapshots &&
            ['openSnapshot', 'readSnapshot', 'listSnapshot', 'closeSnapshot', 'readValue'].every(method => typeof store[method] === 'function'),
            'A schema-v3 store with data parts and snapshots is required.');
        if (Object.values(flows).some(flow => flow.connections.some(connection => connection.nextFlow)))
            assert(typeof store.findChildAdmission === 'function', 'Chaining requires store.findChildAdmission().');
        return flows;
    });
    const notify = async event => {
        if (!options.onEvent) return;
        try { await bounded(signal => options.onEvent(freeze(event), { signal }), { timeoutMs: 1000 }); }
        catch { /* Observers cannot alter execution or delivery outcomes. */ }
    };
    // Register before the first await so close cannot miss admission or a claim in progress.
    function track(operation, internal = false) {
        assert(!closed || internal, 'Runner is closed.', 'RUNNER_CLOSED');
        const work = Promise.resolve().then(operation);
        pending.add(work);
        work.then(() => pending.delete(work), () => pending.delete(work));
        return work;
    }
    const admissions = createAdmissionLifetime(track);
    // Preparation can be cancelled independently without cancelling admitted work.
    function prepareWork(signal, operation, trackWork = track) {
        validateSignal(signal);
        const combined = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal;
        return trackWork(() => {
            if (combined.aborted) throw abortError('preparation');
            return operation(combined);
        });
    }
    async function submit(input, context = {}, internal = false) {
        reportOptions(context.report);
        return admissions.track(async () => {
            const signal = context.signal ? AbortSignal.any([context.signal, controller.signal]) : controller.signal;
            const flows = await ready;
            const prepared = await prepare(input, { flows, options, namespace, source: context.source ?? 'sdk', lineage: context.lineage,
                evaluationId: context.evaluationId, validationRequest: context.validationRequest, aggregateRequest: context.aggregateRequest,
                aggregateEvent: context[AGGREGATE_EVENT] === true,
                policyOnly: context.policyOnly, admissionOwner: context[ADMISSION_OWNER], signal, maxBytes: store.limits.maxBytes });
            if (signal.aborted) throw abortError('admission');
            const task = await store.admit(prepared.task, prepared.jobs, prepared.dedupKey);
            await notify({ type: 'task.admitted', taskId: task.id, flow: task.flow });
            return projectReport(task.id, context.report);
        }, internal);
    }
    function reportOptions(requested = options.report ?? {}) {
        keysOnly(requested, ['includeData', 'maxInlineBytes'], 'Report options');
        const { includeData = false, maxInlineBytes = 1048576 } = requested;
        assert(typeof includeData === 'boolean' && Number.isSafeInteger(maxInlineBytes) && maxInlineBytes > 0 && maxInlineBytes <= 67108864, 'Invalid inline report options.');
        return { includeData, maxInlineBytes };
    }
    async function projectReport(taskId, requested, signal) {
        const { includeData, maxInlineBytes } = reportOptions(requested);
        const current = report(await store.get(taskId, namespace, { metadata: true }));
        if (signal?.aborted) throw abortError('wait');
        if (!current || !includeData) return current;
        let lease;
        try {
            lease = await store.openSnapshot(namespace, taskId);
            const entries = []; const omitted = []; let omittedCount = 0; let after; let remaining = maxInlineBytes;
            do {
                if (signal?.aborted) throw abortError('wait');
                const page = await store.listSnapshot(namespace, lease.id, { collection: 'data', after });
                for (const part of page.items) {
                    if (signal?.aborted) throw abortError('wait');
                    const cost = part.bytes + new TextEncoder().encode(JSON.stringify(part)).length + 16;
                    if (cost <= remaining) { entries.push({ ...part, value: await store.readValue(namespace, lease.id, part.id) }); remaining -= cost; }
                    else { omittedCount++; if (omitted.length < 25) omitted.push(part); }
                }
                after = page.next;
            } while (after);
            return freeze({ ...report(await store.snapshotRecord(namespace, lease.id, { metadata: true })), data: entries,
                dataComplete: omittedCount === 0, dataOmitted: omitted, omittedCount });
        } catch (error) { return freeze({ ...current, dataComplete: false, dataError: publicError(error, 'inspection') }); }
        finally { if (lease) await store.closeSnapshot(namespace, lease.id).catch(() => {}); }
    }
    async function schedule({ taskId, claimSignal, workSignal, deadline }) {
        if (claimSignal?.aborted) throw abortError('claim');
        const stopped = () => closed || claimSignal?.aborted || (deadline !== undefined && performance.now() >= deadline);
        return track(async () => {
            const flows = Object.values(await ready);
            const ordered = [...flows.slice(cursor % flows.length), ...flows.slice(0, cursor % flows.length)]; cursor++;
            const work = [];
            for (const flow of ordered) {
                const candidates = [{ kind: 'evaluation' }, ...(flow.connections.some(connection => connection.nextFlow) ? [{ kind: 'chain' }] : []),
                    ...(flow.connections.some(connection => connection.when === 'failure') ? [{ kind: 'failure' }] : []),
                    ...flow.connections.map(connection => ({ kind: 'delivery', destination: connection.destinationKey }))];
                for (const candidate of candidates) {
                    if (stopped()) return work.length;
                    const claim = await store.claim(namespace, candidate.kind, { worker, placements: identity.placements,
                        taskId, flow: flow.id, revision: flow.runtimeRevision, destination: candidate.destination });
                    if (!claim) continue;
                    if (stopped()) {
                        if (!await store.releaseClaim?.(claim.job)) {
                            try { await store.commit(claim.job, { job: { state: 'pending', token: null, leaseUntil: null } }); }
                            catch (error) { if (error?.code !== 'STALE_LEASE') throw error; }
                        }
                        return work.length;
                    }
                    const task = executeClaim(claim, { store, options, signal: workSignal ? AbortSignal.any([workSignal, controller.signal]) : controller.signal, onEvent: notify,
                        chain: (flow, event, parent, owner, signal) => submit({ flow, kind: 'event', event, idempotencyKey: owner.id }, {
                            source: 'chain', signal, [ADMISSION_OWNER]: { kind: 'chain', id: owner.id, token: owner.token },
                            lineage: { ...parent.lineage, depth: parent.lineage.depth + 1, path: [...parent.lineage.path, parent.id] } }, true) })
                        .catch(async error => {
                            const failure = publicError(error);
                            if (failure.code !== 'STALE_LEASE') await notify({ type: 'worker.error', taskId: claim.task.id, error: failure });
                        })
                        .finally(() => pending.delete(task));
                    pending.add(task); work.push(task);
                }
            }
            return work.length;
        });
    }
    const transfers = runnerTransfers({ store, namespace, ready, track, trackAdmission: admissions.track, submit, get: projectReport, signal: controller.signal });
    const runner = {
        ready, store, namespace, placements: identity.placements, capabilities: store.capabilities,
        [ADMISSION_LIFETIME]: admissions,
        async submit(input, context = {}) {
            keysOnly(context, ['source', 'signal', 'report'], 'Submission context');
            validateSignal(context.signal);
            if (context.signal?.aborted) throw abortError('admission');
            return submit(input, context);
        },
        async run(input, { signal, waitMs = 60000, waitFor = 'required', source, report } = {}) {
            validateSignal(signal); assert(Number.isSafeInteger(waitMs) && waitMs > 0 && waitMs <= 2147483647, 'Invalid wait budget.');
            assert(['admitted', 'evaluated', 'required', 'settled'].includes(waitFor), 'Unknown wait milestone.');
            reportOptions(report);
            if (signal?.aborted) throw abortError('admission');
            const admitted = await submit(input, { source, signal });
            return runner.execute(admitted.taskId, { signal, waitMs, waitFor, report });
        },
        async execute(taskId, { signal, waitMs = 60000, waitFor = 'required', report } = {}) {
            validateSignal(signal);
            assert(Number.isSafeInteger(waitMs) && waitMs > 0 && waitMs <= 2147483647, 'Invalid wait budget.');
            assert(['admitted', 'evaluated', 'required', 'settled'].includes(waitFor), 'Unknown wait milestone.');
            const display = reportOptions(report);
            const end = performance.now() + waitMs;
            let current;
            let waiter;
            try {
                return await bounded(async child => {
                    waiter = child;
                    while (!child.aborted) {
                        current = await runner.get(taskId, {});
                        if (child.aborted) throw abortError('wait');
                        assert(current, 'Execution not found.', 'NOT_FOUND');
                        if (waitFor === 'admitted' || waitFor === 'evaluated' && current.evaluation.status === 'succeeded' ||
                            waitFor === 'required' && current.requiredSettled || current.settled || performance.now() >= end) {
                            return display.includeData ? projectReport(taskId, display, child) : current;
                        }
                        await schedule({ taskId, claimSignal: child, deadline: end });
                        await delay(Math.max(0, Math.min(10, end - performance.now())), child);
                    }
                    throw abortError('wait');
                }, { signal, timeoutMs: waitMs, stage: 'wait' });
            } catch (error) {
                if (error?.code !== 'TIMEOUT' || signal?.aborted || !waiter?.aborted || !current) throw error;
                return display.includeData ? freeze({ ...current, dataComplete: false, dataError: publicError(error, 'wait') }) : current;
            }
        },
        async tick({ taskId, signal } = {}) {
            validateSignal(signal);
            return schedule({ taskId, claimSignal: signal, workSignal: signal });
        },
        async get(taskId, reportOptions) { return projectReport(taskId, reportOptions); },
        async openSnapshot(taskId) { return track(() => store.openSnapshot(namespace, taskId)); },
        async listSnapshot(snapshotId, query) { return store.listSnapshot(namespace, snapshotId, query); },
        async readSnapshot(snapshotId, partId, query) { return store.readSnapshot(namespace, snapshotId, partId, query); },
        async readValue(snapshotId, partId) { return store.readValue(namespace, snapshotId, partId); },
        async closeSnapshot(snapshotId) { return store.closeSnapshot(namespace, snapshotId); },
        async describeSnapshot(snapshotId) { return store.describeSnapshot(namespace, snapshotId); },
        async list(query) {
            const page = await store.list(namespace, query);
            const items = await Promise.all(page.items.map(task => runner.get(task.id)));
            // Retirement may remove an execution after the page was selected.
            return { items: items.filter(item => item !== null), next: page.next };
        },
        async preview(input, { signal } = {}) {
            return prepareWork(signal, async signal => {
                const flows = await ready;
                const { task, jobs } = await prepare(input, { flows, options, namespace, source: 'preview', signal, maxBytes: store.limits.maxBytes });
                const planned = task.result === undefined ? null : await planOutputs(task, options, { signal, maxBytes: store.limits.maxBytes });
                let snapshot;
                try {
                    if (signal.aborted) throw abortError('preview');
                    snapshot = await store.openPreview(namespace, { ...task, ...planned?.task }, jobs.map(job =>
                        ({ ...job, ...(planned?.jobs.find(update => update.id === job.id) ?? {}) })));
                    if (signal.aborted) throw abortError('preview');
                    const value = report(await store.snapshotRecord(namespace, snapshot.id, { metadata: true }));
                    if (signal.aborted) throw abortError('preview');
                    return freeze({ ...value, preview: true, snapshot, inference: false, delivery: false });
                } catch (error) {
                    // A caller that received no preview cannot release its lease.
                    if (snapshot) await store.closeSnapshot(namespace, snapshot.id).catch(() => {});
                    throw error;
                }
            });
        },
        async retryDelivery(deliveryId) { return track(async () => runner.get(await store.mutateJob(namespace, deliveryId, 'retry'))); },
        async resolveDelivery(deliveryId, resolution) { return track(async () => runner.get(await store.mutateJob(namespace, deliveryId, 'resolve', resolution))); },
        async resolveEvaluation(taskId, resolution, { signal } = {}) {
            return prepareWork(signal, async signal => {
                const saved = await store.get(taskId, namespace, { jobData: false });
                const checked = await validateResolution(saved, resolution, options, { signal, maxBytes: store.limits.maxBytes });
                if (signal.aborted) throw abortError('reconciliation');
                await store.resolveEvaluation(namespace, taskId, checked);
                return runner.get(taskId);
            });
        },
        async recordCompletion(deliveryId, completion) {
            return track(async () => {
                assert(['completed', 'failed'].includes(completion.status), 'Invalid completion state.');
                return runner.get(await store.mutateJob(namespace, deliveryId, 'completion', completion));
            });
        },
        async cancel(taskId) { return track(async () => { await store.cancel(namespace, taskId); return runner.get(taskId); }); },
        recover: async () => track(() => store.recover(namespace)),
        maintain: async () => track(() => store.maintain(namespace)),
        inspect: () => store.inspect(namespace),
        async pending() { return (await Promise.all(Object.values(await ready).map(flow => store.pending(namespace, flow.runtimeRevision)))).reduce((a, b) => a + b, 0); },
        async reply(taskId, callback, { signal, timeoutMs = 10000 } = {}) {
            validateSignal(signal);
            assert(Number.isSafeInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 2147483647, 'Invalid reply timeout.');
            return track(async () => {
                const attemptId = id('reply');
                const expiresAt = Date.now() + timeoutMs;
                await store.recordReply(namespace, taskId, { status: 'in_flight', attemptId, expiresAt });
                let disposition;
                try {
                    const current = await runner.get(taskId);
                    const remaining = expiresAt - Date.now();
                    assert(remaining > 0, 'Reply deadline elapsed before dispatch.', 'EXPIRED');
                    const receipt = await bounded(child => callback(current, { signal: child }),
                        { signal: signal ? AbortSignal.any([signal, controller.signal]) : controller.signal, timeoutMs: remaining });
                    assert(receipt?.status === undefined || ['acknowledged', 'submitted', 'unknown', 'failed'].includes(receipt.status), 'Invalid reply receipt.', 'INVALID_RECEIPT');
                    disposition = { status: receipt?.status ?? 'submitted', attemptId };
                } catch (error) { disposition = { status: 'unknown', attemptId, error: publicError(error, 'reply') }; }
                await store.recordReply(namespace, taskId, disposition);
                return disposition;
            });
        },
        async acknowledge(taskId, callback, { signal, timeoutMs = 10000 } = {}) {
            validateSignal(signal);
            assert(Number.isSafeInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 2147483647, 'Invalid acknowledgement timeout.');
            return track(async () => {
                assert(store.capabilities.durable, 'Source acknowledgement requires durable admission.');
                const attemptId = id('ack');
                const expiresAt = Date.now() + timeoutMs;
                await store.recordAcknowledgement(namespace, taskId, { status: 'in_flight', attemptId, expiresAt });
                let disposition;
                try {
                    const remaining = expiresAt - Date.now();
                    assert(remaining > 0, 'Acknowledgement deadline elapsed before dispatch.', 'EXPIRED');
                    const received = await bounded(child => callback({ taskId, signal: child }),
                        { signal: signal ? AbortSignal.any([signal, controller.signal]) : controller.signal, timeoutMs: remaining });
                    const receipt = received === undefined ? undefined : isRecord(received) ? captureObject(received, ['receipt'], store.limits.maxBytes) : snapshot(received, { maxBytes: store.limits.maxBytes });
                    assert(receipt?.status === undefined || ['acknowledged', 'submitted', 'unknown', 'failed'].includes(receipt.status), 'Invalid source acknowledgement receipt.', 'INVALID_RECEIPT');
                    disposition = { status: receipt?.status === 'submitted' ? 'unknown' : receipt?.status ?? 'acknowledged', attemptId,
                        ...(receipt?.receipt === undefined ? {} : { receipt: receipt.receipt }) };
                    const failure = acknowledgementFailure({ ...disposition, error: receipt?.error });
                    if (failure) disposition.error = publicError(failure);
                } catch (error) {
                    const failure = publicError(error, 'source-acknowledgement');
                    disposition = { status: failure.ambiguous === false ? 'failed' : 'unknown', attemptId, error: failure };
                }
                await store.recordAcknowledgement(namespace, taskId, disposition);
                return disposition;
            });
        },
        pause: async (type, name, paused = true) => track(() => store.pause(namespace, type, name, paused)),
        async reprocess(taskId, options = {}) {
            keysOnly(options, ['flow', 'idempotencyKey', 'signal'], 'Reprocessing options');
            return prepareWork(options.signal, signal => reprocess(taskId, options, false, signal), admissions.track);
        },
        async redeliver(taskId, options = {}) {
            keysOnly(options, ['flow', 'idempotencyKey', 'signal'], 'Redelivery options');
            assert(options.flow, 'Redelivery requires an explicit destination flow.');
            return prepareWork(options.signal, signal => reprocess(taskId, options, true, signal), admissions.track);
        },
        async reevaluate(taskId, { flow, signal } = {}) {
            return prepareWork(signal, async signal => {
                const saved = await store.get(taskId, namespace, { jobData: false });
                assert(saved?.task.request !== undefined, 'Original evaluation request is unavailable.');
                return submit({ flow: flow ?? saved.task.flow, kind: 'request', request: saved.task.request }, { source: 'reevaluate', signal,
                    aggregateRequest: saved.task.aggregateRequest,
                    lineage: { ...saved.task.lineage, depth: saved.task.lineage.depth + 1, path: [...saved.task.lineage.path, taskId] } }, true);
            }, admissions.track);
        },
        ...transfers.methods,
        async close({ cancel = false } = {}) {
            if (cancel) controller.abort();
            if (closing) return closing;
            closed = true;
            closing = (async () => {
                // Accepted derived operations and active workers can admit work while draining.
                while (pending.size) await Promise.allSettled([...pending]);
                await transfers.close();
                if (!options.store) await store.close();
            })();
            return closing;
        },
    };
    async function reprocess(taskId, { flow, idempotencyKey }, deliver, signal) {
        const saved = await store.get(taskId, namespace, { jobData: false });
        assert(saved?.task.result !== undefined, 'A saved result is required.');
        return submit({ flow: flow ?? saved.task.flow, kind: 'result', result: saved.task.result,
            evidenceAt: saved.task.evidenceAt, ...(saved.task.input?.validation === undefined ? {} : { validation: saved.task.input.validation }),
            ...(idempotencyKey === undefined ? {} : { idempotencyKey }) }, { source: 'reprocess', signal, validationRequest: saved.task.request,
                aggregateRequest: saved.task.aggregateRequest, evaluationId: saved.task.evaluationId, policyOnly: !deliver,
                lineage: { ...saved.task.lineage, depth: saved.task.lineage.depth + 1,
                path: [...saved.task.lineage.path, taskId] } }, true);
    }
    return Object.freeze(runner);
}
