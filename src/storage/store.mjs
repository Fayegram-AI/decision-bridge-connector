import { assert, ConnectorError, fingerprint, freeze, id, identifier, isRecord, placementLabels, snapshot, captureObject } from '../util.mjs';
import { storeLimits } from './limits.mjs';
import { deliveryDisposition, unconfirmedAcceptance } from '../engine/freshness.mjs';
import { admissionKey, admissionPending } from './admission-retention.mjs';
import { acceptedDisposition, validateReceipt } from '../engine/receipts.mjs';
import { captureDeliveryResolution, captureEvaluationResolution } from '../engine/input-capture.mjs';
import { createValueStorage } from './values.mjs';
import { encodeRecord } from './record-codec.mjs';
import { createInspection } from './inspection.mjs';
import { createTransfers } from './transfers.mjs';
import { storeLifetime } from './lifetime.mjs';
import { hasRetryAllowance, needsRevision, recoveryAwareQueries, unsettled } from './revision-work.mjs';
import { restoreUnstartedClaim } from './claim-release.mjs';
import { createFollowUp } from './follow-up.mjs';
import { mergeErrors } from './diagnostics.mjs';
import { scanDueAggregates } from './aggregate-scan.mjs';

const now = () => Date.now();
const scope = (namespace, name) => JSON.stringify([namespace, name]);
const expired = record => Number.isFinite(record?.expiresAt) && record.expiresAt <= now();

/** Domain transactions never execute providers, connectors, or application callbacks. */
export function createExecutionStore(repository, options = {}) {
    const limits = storeLimits(options);
    const data = createValueStorage(repository, limits);
    const inspection = createInspection(data, limits);
    const followUp = createFollowUp(data);
    repository = data;
    const staged = async (namespace, operation) => {
        const stage = await data.stage(namespace);
        try { return await operation(stage); } finally { await stage.close(); }
    };
    async function owned(tx, claim) {
        const job = await tx.get('jobs', claim.id, true);
        assert(job?.state === 'active' && job.token === claim.token && job.leaseUntil > now(), 'Worker lease is no longer valid.', 'STALE_LEASE');
        return job;
    }
    async function childAdmission(tx, job) {
        const key = await fingerprint([job.namespace, 'chain', job.plan.nextFlow, job.id]);
        const duplicate = await tx.get('dedup', key);
        const child = duplicate ? await tx.get('tasks', duplicate.taskId) : null;
        return child?.namespace === job.namespace ? child.id : null;
    }
    async function settle(tx, taskId) {
        const task = await tx.get('tasks', taskId, true);
        const jobs = await tx.find('jobs', { taskId, namespace: task.namespace }, { limit: 512 });
        const state = jobs.some(job => unsettled.includes(job.state)) ? 'active' : 'complete';
        if (state === 'active') delete task.expiresAt;
        else if (task.state !== 'complete' || task.expiresAt === undefined) task.expiresAt = now() + limits.retentionMs;
        task.state = state;
        task.updatedAt = now();
        await tx.put('tasks', task);
    }
    async function recordChannel(namespace, taskId, key, disposition) {
        return staged(namespace, async stage => {
            const value = (await encodeRecord('tasks', { [key]: disposition }, stage))[key];
            assert(isRecord(value) && ['in_flight', 'acknowledged', 'submitted', 'unknown', 'failed'].includes(value.status), 'Invalid channel disposition.');
            identifier(value.attemptId, 'Channel attempt ID');
            if (value.status === 'in_flight') assert(Number.isSafeInteger(value.expiresAt) && value.expiresAt >= 0, 'Invalid channel attempt expiry.');
            return repository.transaction(async tx => {
                const task = await tx.get('tasks', taskId, true);
                assert(task?.namespace === namespace, 'Execution not found.', 'NOT_FOUND');
                const previous = task[key];
                if (value.status === 'in_flight') {
                    assert(previous?.status !== 'in_flight' || previous.expiresAt <= now(), 'A channel attempt is already in flight.', 'CONFLICT');
                    assert(previous?.attemptId !== value.attemptId, 'A replacement requires a new attempt ID.', 'CONFLICT');
                } else assert(previous?.attemptId === value.attemptId, 'Channel attempt has been superseded.', 'STALE_ATTEMPT');
                task[key] = value;
                task.leaseUntil = Math.min(...['reply', 'sourceAcknowledgement'].filter(channel => task[channel]?.status === 'in_flight').map(channel => task[channel].expiresAt));
                if (!Number.isFinite(task.leaseUntil)) task.leaseUntil = null;
                await tx.put('tasks', task);
            });
        });
    }
    const store = storeLifetime({
        ...createTransfers(data, inspection, limits),
        capabilities: Object.freeze({ ...data.raw.capabilities, schemaVersion: 3, atomicDeduplication: true, fencing: true,
            dataParts: true, snapshots: true }),
        limits: Object.freeze(limits),
        async admit(task, jobs, dedupKey) {
            if (dedupKey) task = { ...task, admissionKey: dedupKey };
            return staged(task.namespace, async stage => {
                task = await encodeRecord('tasks', task, stage);
                jobs = await Promise.all(jobs.map(job => encodeRecord('jobs', job, stage)));
                return repository.transaction(async tx => {
                    await tx.lock(scope(task.namespace, 'admission'));
                    if (dedupKey) {
                        const duplicate = await tx.get('dedup', dedupKey);
                        const existing = duplicate ? await tx.get('tasks', duplicate.taskId) : null;
                        const pendingOwner = task.admissionOwner && await admissionPending(tx, task, dedupKey, true);
                        if (duplicate && (duplicate.expiresAt > now() || existing?.state === 'active' || pendingOwner || await admissionPending(tx, existing, dedupKey))) {
                            // Derived work also binds whether it may deliver and which task it reuses.
                            const sameIntent = !existing || Boolean(existing.policyOnly) === Boolean(task.policyOnly) &&
                                (task.source !== 'reprocess' || (existing.lineage?.parentId ?? null) === (task.lineage?.parentId ?? null));
                            assert(duplicate.fingerprint === task.fingerprint && sameIntent, 'Idempotency key has different input or execution intent.', 'IDEMPOTENCY_CONFLICT');
                            assert(existing, 'Execution was retained as a deduplication tombstone.', 'EXECUTION_RETIRED');
                            // Upgrade retained pre-owner records when their producer retries linkage.
                            if (pendingOwner && !existing.admissionOwner) {
                                existing.admissionOwner = task.admissionOwner; existing.admissionKey = dedupKey;
                                await tx.put('tasks', existing);
                            }
                            return freeze(existing);
                        }
                    }
                    // A stale producer may resume after its owner settled and its dedup TTL elapsed.
                    if (task.admissionOwner) assert(await admissionPending(tx, task, dedupKey, true),
                        'Internal admission owner is no longer pending.', 'ADMISSION_OWNER_SETTLED');
                    let chainAuthority;
                    if (task.admissionOwner?.kind === 'chain') {
                        const owner = await owned(tx, task.admissionOwner);
                        assert(owner.namespace === task.namespace && owner.kind === 'chain' && owner.plan.nextFlow === task.flow,
                            'Invalid child admission owner.', 'STALE_LEASE');
                        assert(owner.deadlineAt == null || owner.deadlineAt > now(), 'Child admission deadline elapsed.', 'ADMISSION_EXPIRED');
                        assert(owner.admissionAttempts > 0 && (owner.plan.chainRetry.maxAttempts === undefined ||
                            owner.admissionAttempts <= owner.plan.chainRetry.maxAttempts), 'Child admission attempt budget exhausted.', 'ADMISSION_EXPIRED');
                        chainAuthority = { deadline: owner.deadlineAt ?? Infinity, leaseUntil: owner.leaseUntil };
                    }
                    if (await tx.count('tasks', { namespace: task.namespace, state: 'active' }) >= limits.maxPending)
                        throw new ConnectorError('RESOURCE_LIMIT', 'Admission capacity reached.', { retryable: true, ambiguous: false });
                    const chainKey = scope(task.namespace, `root:${task.rootId}`);
                    const chain = await tx.get('controls', chainKey) ?? { id: chainKey, namespace: task.namespace, sequence: 0 };
                    assert(chain.sequence < (task.lineage?.maxExecutions ?? task.plan.maxExecutions), 'Inherited flow chain execution limit reached.', 'RESOURCE_LIMIT');
                    chain.sequence++; await tx.put('controls', chain);
                    const sequenceId = scope(task.namespace, 'sequence');
                    const counter = await tx.get('controls', sequenceId) ?? { id: sequenceId, namespace: task.namespace, sequence: 0 };
                    task.sequence = ++counter.sequence;
                    await tx.put('controls', counter);
                    if (chainAuthority) {
                        assert(chainAuthority.leaseUntil > now(), 'Worker lease is no longer valid.', 'STALE_LEASE');
                        assert(chainAuthority.deadline > now(), 'Child admission deadline elapsed.', 'ADMISSION_EXPIRED');
                    }
                    await tx.insert('tasks', task);
                    for (const job of jobs) await tx.insert('jobs', { ...job, sequence: task.sequence });
                    if (dedupKey) await tx.put('dedup', { id: dedupKey, namespace: task.namespace, taskId: task.id,
                        fingerprint: task.fingerprint, expiresAt: now() + limits.dedupeMs });
                    return freeze(task);
                });
            });
        },
        async get(taskId, namespace, { metadata = false, jobData = true } = {}) {
            return repository.transaction(async tx => {
                // Settlement and retirement lock this row before changing the execution record.
                const task = await tx.get('tasks', taskId, true, namespace);
                if (!task || task.namespace !== namespace) return null;
                const jobs = await tx.find('jobs', { namespace, taskId }, { limit: 512 });
                return freeze(metadata ? { task, jobs } : { task: await data.hydrate(tx, namespace, task),
                    jobs: jobData ? await data.hydrate(tx, namespace, jobs) : jobs });
            });
        },
        async getJob(namespace, jobId) {
            return repository.transaction(async tx => {
                // Keep this version's value pins alive until all evidence is read.
                const job = await tx.get('jobs', jobId, true, namespace);
                return job ? data.hydrate(tx, namespace, job) : null;
            });
        },
        openSnapshot: (namespace, taskId, prepared) => inspection.open(namespace, taskId, prepared),
        listSnapshot: (namespace, snapshotId, query) => inspection.list(namespace, snapshotId, query),
        readSnapshot: (namespace, snapshotId, partId, query) => inspection.read(namespace, snapshotId, partId, query),
        readValue: (namespace, snapshotId, partId) => inspection.readValue(namespace, snapshotId, partId),
        closeSnapshot: (namespace, snapshotId) => inspection.close(namespace, snapshotId),
        describeSnapshot: (namespace, snapshotId) => inspection.describe(namespace, snapshotId),
        snapshotRecord: (namespace, snapshotId, options) => inspection.record(namespace, snapshotId, options),
        async openPreview(namespace, task, jobs) {
            return staged(namespace, async stage => inspection.open(namespace, task.id, {
                task: await encodeRecord('tasks', task, stage), jobs: await Promise.all(jobs.map(job => encodeRecord('jobs', job, stage))),
            }));
        },
        async findChildAdmission(namespace, claim) {
            return repository.transaction(async tx => {
                await tx.lock(scope(namespace, 'admission'));
                const job = await owned(tx, claim);
                assert(job.namespace === namespace && job.kind === 'chain', 'Invalid chain claim.', 'STALE_LEASE');
                return childAdmission(tx, job);
            });
        },
        async list(namespace, { after, limit = limits.pageSize, state, flow } = {}) {
            assert(Number.isSafeInteger(limit) && limit > 0 && limit <= 100, 'Page size must be 1–100.');
            return repository.transaction(async tx => {
                const rows = await tx.find('tasks', { namespace, ...(after ? { id: { gt: after } } : {}),
                    ...(state ? { state } : {}), ...(flow ? { flow } : {}) }, { limit });
                return freeze({ items: rows, next: rows.length === limit ? rows.at(-1).id : null });
            });
        },
        async claim(namespace, kind, { worker, placements = [], taskId, flow, destination, revision } = {}) {
            placements = placementLabels(placements);
            const cursorId = taskId ? null : scope(namespace, `claim-cursor:${await fingerprint([
                kind, worker ?? null, [...placements].sort(), flow ?? null, destination ?? null, revision ?? null,
            ])}`);
            return repository.transaction(async tx => {
                // Scope locks coordinate quota checks; transactions contain only bounded indexed work.
                await tx.lock(scope(namespace, `claim:${kind}`));
                let timestamp = now();
                const cap = kind === 'evaluation' ? limits.evaluationConcurrency : limits.deliveryConcurrency;
                if (await tx.count('jobs', { namespace, kind, state: 'active', leaseUntil: { gt: timestamp } }) >= cap) return null;
                // Runtime workers already select one flow; gate it before the first
                // row query, avoiding a discovery read on the common claim path.
                if (flow) { await tx.lock(scope(namespace, `claim-flow:${flow}`)); timestamp = now(); }
                const where = { namespace, kind, state: { in: ['pending', 'retry_pending'] }, runAt: { lte: timestamp },
                    ...(taskId ? { taskId } : {}), ...(flow ? { flow } : {}), ...(destination ? { destination } : {}), ...(revision ? { revision } : {}) };
                const cursor = cursorId ? (await tx.get('controls', cursorId))?.cursor : null;
                const page = { limit: 128, order: ['priority', 'runAt', 'id'], lock: Boolean(flow) };
                let candidates = await tx.find('jobs', where, { ...page, after: cursor });
                // A blocked page must not hide later work. Wrap only after reaching the end,
                // keeping every transaction bounded and retaining the priority/runAt/id order.
                if (!candidates.length && cursor) candidates = await tx.find('jobs', where, page);
                // Acquire flow gates before job rows: expiry can settle a failure job
                // whose claimant also needs this gate. Revalidate the bounded page after waiting.
                const pageEnd = candidates.at(-1);
                const flows = flow ? [flow] : [...new Set(candidates.map(job => job.flow).filter(name => typeof name === 'string'))].sort();
                if (!flow) {
                    for (const name of flows) await tx.lock(scope(namespace, `claim-flow:${name}`));
                    timestamp = now();
                    if (candidates.length) candidates = await tx.find('jobs', { ...where, runAt: { lte: timestamp },
                        id: { in: candidates.map(job => job.id) } }, { ...page, lock: true });
                }
                async function advance(job) {
                    if (!cursorId) return;
                    if (!job) { await tx.remove('controls', cursorId); return; }
                    await tx.put('controls', { id: cursorId, namespace, kind: 'claim-cursor',
                        cursor: { priority: job.priority, runAt: job.runAt, id: job.id }, expiresAt: timestamp + 600000 });
                }
                for (const job of candidates) {
                    timestamp = now();
                    if (!flows.includes(job.flow)) continue;
                    if (job.placement && !placements.includes(job.placement)) continue;
                    const paused = await tx.get('controls', scope(namespace, `pause:flow:${job.flow}`));
                    if (paused?.paused) continue;
                    if (typeof job.destination === 'string' &&
                        (await tx.get('controls', scope(namespace, `pause:destination:${job.destination}`)))?.paused) continue;
                    if (job.required === 0 && await tx.count('jobs', { namespace, kind, required: 0, state: 'active', leaseUntil: { gt: timestamp } }) >= limits.optionalConcurrency) continue;
                    let task = await tx.get('tasks', job.taskId);
                    if (await tx.count('jobs', { namespace, flow: job.flow, state: 'active', leaseUntil: { gt: timestamp } }) >= task.plan.concurrency) continue;
                    if (kind === 'delivery') {
                        if (job.plan.when === 'failure' && job.eligibleAt === undefined && !job.attempts.length) {
                            const ttl = task.plan.deliveryTtlMs ?? (await data.hydrate(tx, namespace, task.plan)).deliveryTtlMs;
                            job.eligibleAt = timestamp; job.expiresAt = timestamp + ttl;
                        }
                        const expiry = deliveryDisposition(task, job, timestamp);
                        if (expiry) {
                            job.state = expiry; await tx.put('jobs', job);
                            await followUp(tx, job);
                            await settle(tx, job.taskId); continue;
                        }
                        if (await tx.count('jobs', { namespace, destination: job.destination, state: 'active', leaseUntil: { gt: timestamp } }) >= limits.perDestination) continue;
                        let blocked = false;
                        for (const dependency of job.plan.dependsOn) {
                            const parent = await tx.get('jobs', `${job.taskId}:delivery:${dependency}`);
                            if (parent?.state !== 'acknowledged') blocked = true;
                        }
                        if (blocked) continue;
                        if (job.plan.ordering && await tx.count('jobs', { namespace, orderingKey: job.orderingKey,
                            sequence: { lt: job.sequence }, state: { in: job.plan.ordering.onFailure === 'continue' ? unsettled : [...unsettled, 'failed', 'expired', 'cancelled'] } })) continue;
                    }
                    const rate = job.plan?.rate ?? task.plan.rate;
                    const rateKey = rate ? scope(namespace, `rate:${job.destination ?? job.flow}`) : null;
                    if (rateKey && (await tx.get('controls', rateKey, true))?.runAt > timestamp) continue;
                    if (kind === 'failure' && job.error && !job.triggerRecovered) {
                        // Earlier schema-v3 writers kept the triggering error only on this job.
                        const origins = await tx.find('jobs', { namespace, taskId: job.taskId,
                            kind: { in: ['evaluation', 'delivery', 'chain'] } }, { limit: 512 });
                        const origin = origins.find(item => item.error?.$data?.sha256 &&
                            item.error.$data.sha256 === job.error.$data?.sha256) ?? origins.find(item =>
                            job.error.code === `${item.kind.toUpperCase()}_${item.state.toUpperCase()}`);
                        if (origin) await followUp(tx, origin);
                        task = await tx.get('tasks', job.taskId);
                        job.triggerRecovered = true;
                    }
                    const token = id('lease');
                    if (rateKey) {
                        await tx.put('controls', { id: rateKey, namespace, token, runAt: timestamp + Math.ceil(1000 / rate) });
                    }
                    // Persist the original queue state until a worker commits its first execution write.
                    job.unstartedClaim = { state: job.state, rateKey, ...(job.worker === undefined ? {} : { worker: job.worker }) };
                    job.state = 'active'; job.token = token; job.worker = worker;
                    // Selection and its quota checks do not consume the worker's lease.
                    job.leaseUntil = now() + limits.leaseMs;
                    await tx.put('jobs', job);
                    await advance(job);
                    const selectedTask = kind === 'delivery' ? task : kind === 'chain'
                        ? { ...task, ...(task.lineage === undefined ? {} : { lineage: await data.hydrate(tx, namespace, task.lineage) }) } : await data.hydrate(tx, namespace, task);
                    const selectedJob = { ...job, ...(job.plan ? { plan: await data.hydrate(tx, namespace, job.plan) } : {}),
                        ...(Object.hasOwn(job, 'payload') ? { payload: await data.hydrate(tx, namespace, job.payload) } : {}) };
                    return freeze({ job: selectedJob, task: selectedTask });
                }
                await advance(candidates.at(-1) ?? pageEnd);
                return null;
            });
        },
        async releaseClaim(claim) {
            return repository.transaction(async tx => {
                const job = await tx.get('jobs', claim.id, true, claim.namespace);
                if (job?.state !== 'active' || job.token !== claim.token) return false;
                if (!await restoreUnstartedClaim(tx, job)) return false;
                await tx.put('jobs', job);
                return true;
            });
        },
        async renew(claim) {
            return repository.transaction(async tx => {
                const job = await owned(tx, claim); job.leaseUntil = now() + limits.leaseMs;
                await tx.put('jobs', job);
            });
        },
        async commit(claim, { task: patch, jobs = [], job: disposition = {} } = {}) {
            if (disposition.acceptance !== undefined) validateReceipt(disposition.acceptance.receipt, limits.maxBytes);
            return staged(claim.namespace, async stage => {
                patch = patch ? await encodeRecord('tasks', patch, stage) : undefined;
                jobs = await Promise.all(jobs.map(job => encodeRecord('jobs', job, stage)));
                disposition = await encodeRecord('jobs', disposition, stage);
                return repository.transaction(async tx => {
                    // Serialize terminal linkage with late admission, including a lost admission response.
                    const terminalChain = claim.kind === 'chain' && ['failed', 'expired'].includes(disposition.state);
                    if (terminalChain) await tx.lock(scope(claim.namespace, 'admission'));
                    const current = await owned(tx, claim);
                    if (disposition.acceptance !== undefined) {
                        assert(current.kind === 'delivery' && isRecord(disposition.acceptance) &&
                            typeof disposition.acceptance.attemptId === 'string' && disposition.acceptance.attemptId.length > 0 &&
                            disposition.acceptance.attemptId === current.attempts?.at(-1)?.id,
                            'Acceptance belongs to a different delivery attempt.', 'STALE_ATTEMPT');
                        assert(disposition.acceptance.receipt.status === 'acknowledged',
                            'Acceptance requires an acknowledged receipt.', 'INVALID_RECEIPT');
                    }
                    if (disposition.state && disposition.state !== 'active' && disposition.state !== 'acknowledged') {
                        const accepted = acceptedDisposition(current);
                        if (accepted) disposition = { ...disposition, ...accepted };
                    }
                    if (terminalChain) {
                        const child = await childAdmission(tx, current);
                        if (child) disposition = { ...disposition, state: 'acknowledged', childTaskId: child, error: null };
                    }
                    const task = await tx.get('tasks', current.taskId, true);
                    if (patch) await tx.put('tasks', { ...task, ...patch,
                        ...(patch.errors === undefined ? {} : { errors: await mergeErrors(data, tx, task, patch.errors) }), updatedAt: now() });
                    for (const update of jobs) {
                        const original = await tx.get('jobs', update.id);
                        assert(original && original.taskId === task.id && (original.state === 'blocked' || original.state === 'skipped' && original.plan?.when === 'failure'), 'Delivery plan cannot be replaced after dispatch.');
                        const eligible = original.plan?.when === 'failure' && update.state === 'pending' && original.eligibleAt === undefined;
                        const ttl = eligible ? task.plan.deliveryTtlMs ?? (await data.hydrate(tx, claim.namespace, task.plan)).deliveryTtlMs : 0;
                        const timing = eligible ? { eligibleAt: now(), expiresAt: now() + ttl } : {};
                        await tx.put('jobs', { ...original, ...update, ...timing });
                        if (original.plan?.nextFlow && ['skipped', 'failed', 'expired', 'cancelled'].includes(update.state)) {
                            const chained = await tx.get('jobs', `${task.id}:chain:${original.plan.id}`);
                            if (chained) await tx.put('jobs', { ...chained, state: 'skipped' });
                        }
                        if (['failed', 'expired', 'unknown', 'cancelled'].includes(update.state))
                            await followUp(tx, { ...original, ...update, ...timing });
                    }
                    const updated = { ...current, ...disposition };
                    delete updated.unstartedClaim;
                    await tx.put('jobs', updated);
                    await followUp(tx, { ...updated, error: updated.error ?? jobs.find(job => job.state === 'failed')?.error }, jobs.some(job => job.state === 'failed'));
                    await settle(tx, task.id);
                });
            });
        },
        async recover(namespace) {
            return repository.transaction(async tx => {
                const rows = await tx.find('jobs', { namespace, state: 'active', leaseUntil: { lte: now() } }, { limit: 100, lock: true });
                for (const job of rows) {
                    if (await restoreUnstartedClaim(tx, job)) { await tx.put('jobs', job); continue; }
                    const task = await tx.get('tasks', job.taskId);
                    job.state = job.stage === 'invoked' && !(job.kind === 'evaluation' && task.result !== undefined) ? 'unknown' : 'pending';
                    const accepted = acceptedDisposition(job);
                    if (accepted) Object.assign(job, accepted);
                    if (job.state === 'unknown') job.error = { code: 'RECOVERY_UNKNOWN', stage: job.kind, message: 'Worker lease expired after invocation; acceptance is unconfirmed.' };
                    if (job.state === 'unknown' && job.kind === 'delivery' && job.plan.retry.ambiguous === 'retry' &&
                        job.attempts.length < job.plan.retry.maxAttempts && job.expiresAt > now()) job.state = 'retry_pending';
                    if (job.attempts?.at(-1)?.status === 'in_flight') Object.assign(job.attempts.at(-1), { status: 'unknown', finishedAt: now() });
                    job.runAt = now(); job.token = null; job.leaseUntil = null;
                    await tx.put('jobs', job); await followUp(tx, job); await settle(tx, job.taskId);
                }
                for (const task of await tx.find('tasks', { namespace, leaseUntil: { lte: now() } }, { limit: 100, lock: true })) {
                    for (const key of ['reply', 'sourceAcknowledgement']) if (task[key]?.status === 'in_flight' && task[key].expiresAt <= now())
                        task[key] = { ...task[key], status: 'unknown' };
                    task.leaseUntil = Math.min(...['reply', 'sourceAcknowledgement'].filter(key => task[key]?.status === 'in_flight').map(key => task[key].expiresAt));
                    if (!Number.isFinite(task.leaseUntil)) task.leaseUntil = null;
                    await tx.put('tasks', task);
                }
                return rows.length;
            });
        },
        async mutateJob(namespace, jobId, operation, value) {
            // Operator evidence obeys the same JSON budget as recipient receipts.
            if (operation === 'resolve') value = captureDeliveryResolution(value, limits.maxBytes);
            if (operation === 'completion') value = captureObject(value, ['receipt', 'evidence'], limits.maxBytes);
            return staged(namespace, async stage => {
                const evidence = operation === 'completion' ? (await encodeRecord('jobs', { completion: value }, stage)).completion :
                    operation === 'resolve' ? (await encodeRecord('jobs', { receipt: value }, stage)).receipt : undefined;
                const resolutionError = operation === 'resolve' && value.status === 'failed'
                    ? (await encodeRecord('jobs', { error: { code: 'DELIVERY_RESOLVED_FAILED', stage: 'delivery',
                        message: 'Operator confirmed delivery failure.', ambiguous: false } }, stage)).error : undefined;
                return repository.transaction(async tx => {
                    const job = await tx.get('jobs', jobId, true);
                    assert(job?.namespace === namespace && job.kind === 'delivery', 'Delivery not found.', 'NOT_FOUND');
                    assert(job.state !== 'active', 'Active work cannot be reconciled.', 'CONFLICT');
                    if (operation === 'completion') {
                        assert(job.state === 'acknowledged', 'Completion requires accepted delivery.');
                        job.completion = evidence;
                    } else if (operation === 'resolve') {
                        assert(['unknown', 'held', 'failed'].includes(job.state), 'Delivery does not need reconciliation.');
                        assert(['acknowledged', 'failed'].includes(value.status), 'Invalid resolution.');
                        job.state = value.status; job.receipt = evidence; job.resolvedAt = now();
                        if (job.state === 'acknowledged') job.error = null;
                        else job.error = resolutionError;
                        // Operator evidence covers existing attempts, not later recipient calls.
                        job.resolvedAttempts = job.attempts.length;
                    } else if (operation === 'retry') {
                        assert(['failed', 'retry_pending'].includes(job.state), 'Only definite failures can be retried; reconcile unknown outcomes first.');
                        assert(Object.hasOwn(job, 'payload'), 'Delivery has no prepared payload to retry.', 'CONFLICT');
                        assert(hasRetryAllowance(job, now()), 'Retry allowance or expiry reached.');
                        job.state = 'pending'; job.runAt = now();
                    } else assert(false, 'Unknown delivery operation.');
                    await tx.put('jobs', job);
                    if (operation === 'resolve') await followUp(tx, job);
                    await settle(tx, job.taskId); return job.taskId;
                });
            });
        },
        async cancel(namespace, taskId) {
            return repository.transaction(async tx => {
                await tx.lock(scope(namespace, 'admission'));
                const task = await tx.get('tasks', taskId);
                assert(task?.namespace === namespace, 'Execution not found.', 'NOT_FOUND');
                for (const job of await tx.find('jobs', { namespace, taskId }, { limit: 512, lock: true, skipLocked: false })) {
                    if (!unsettled.includes(job.state)) continue;
                    await restoreUnstartedClaim(tx, job);
                    if (job.kind === 'chain') {
                        const child = await childAdmission(tx, job);
                        Object.assign(job, { state: child ? 'acknowledged' : 'cancelled',
                            ...(child ? { childTaskId: child, error: null } : {}), token: null, leaseUntil: null });
                        await tx.put('jobs', job); continue;
                    }
                    const accepted = acceptedDisposition(job);
                    if (accepted) {
                        Object.assign(job, accepted, { token: null, leaseUntil: null });
                        await tx.put('jobs', job); continue;
                    }
                    job.state = job.state === 'unknown' || job.kind === 'delivery' && unconfirmedAcceptance(job) ||
                        job.state === 'active' && job.stage === 'invoked' ? 'unknown' : 'cancelled';
                    job.token = null; await tx.put('jobs', job);
                }
                await settle(tx, taskId);
            });
        },
        async resolveEvaluation(namespace, taskId, input) {
            const resolution = captureEvaluationResolution(input, limits.maxBytes);
            // Prepare recovered values before acquiring job/task locks. A changed
            // diagnostic history is retried explicitly rather than overwritten.
            const before = await repository.transaction(async tx => {
                const task = await tx.get('tasks', taskId, true, namespace);
                assert(task, 'Execution not found.', 'NOT_FOUND');
                return { task, errors: await data.hydrate(tx, namespace, task.errors) };
            });
            return staged(namespace, async stage => {
                const errors = before.errors.filter(error => error.stage !== 'evaluation');
                if (resolution.status === 'failed') errors.push({ code: 'EVALUATION_RESOLVED_FAILED', message: 'Operator confirmed evaluation failure.', stage: 'evaluation' });
                const taskPatch = await encodeRecord('tasks', { errors,
                    ...(resolution.status === 'succeeded' ? { result: resolution.result, evaluatedAt: now() } : {}) }, stage);
                const jobPatch = await encodeRecord('jobs', { resolution: { ...resolution, previousErrors: before.errors, resolvedAt: now() } }, stage);
                return repository.transaction(async tx => {
                    const job = await tx.get('jobs', `${taskId}:evaluation`, true);
                    assert(job?.namespace === namespace && job.state === 'unknown', 'Evaluation is not awaiting reconciliation.', 'CONFLICT');
                    const failure = await tx.get('jobs', `${taskId}:failure`, true);
                    assert(failure?.state !== 'active', 'Failure planning is active; retry reconciliation after it settles.', 'CONFLICT');
                    if (failure?.state === 'pending') await tx.put('jobs', { ...failure, state: 'skipped' });
                    const task = await tx.get('tasks', taskId, true);
                    assert(task.errors?.$data?.sha256 === before.task.errors?.$data?.sha256, 'Evaluation history changed; retry reconciliation.', 'CONFLICT');
                    Object.assign(job, jobPatch); Object.assign(task, taskPatch);
                    job.state = 'pending'; job.stage = 'reconciled'; job.token = null; job.leaseUntil = null; job.runAt = now();
                    await tx.put('tasks', task); await tx.put('jobs', job); await settle(tx, taskId);
                });
            });
        },
        async pause(namespace, type, name, paused) {
            assert(['flow', 'destination'].includes(type) && typeof paused === 'boolean', 'Invalid pause operation.');
            identifier(name, 'Pause target');
            return repository.transaction(tx => tx.put('controls', { id: scope(namespace, `pause:${type}:${name}`), namespace, paused }));
        },
        async maintain(namespace) {
            const result = await repository.transaction(async tx => {
                // Retirement and admission share task, root-counter and dedup identities.
                await tx.lock(scope(namespace, 'admission'));
                async function page(table, where) {
                    const cursorId = scope(namespace, `retirement-cursor:${table}`);
                    const cursor = await tx.get('controls', cursorId);
                    let rows = await tx.find(table, { ...where, ...(cursor ? { id: { gt: cursor.cursor } } : {}) }, { limit: 100 });
                    if (!rows.length && cursor) rows = await tx.find(table, where, { limit: 100 });
                    // Retained records must not hide later eligible retirement indefinitely.
                    if (rows.length) await tx.put('controls', { id: cursorId, namespace, cursor: rows.at(-1).id });
                    else await tx.remove('controls', cursorId);
                    return rows;
                }
                let removed = 0;
                for (const candidate of await page('tasks', { namespace, state: 'complete', expiresAt: { lte: now() } })) {
                    const task = await tx.get('tasks', candidate.id, true);
                    if (!task || task.state !== 'complete' || !expired(task)) continue;
                    if (task.reply?.status === 'in_flight' || task.sourceAcknowledgement?.status === 'in_flight') continue;
                    if (await admissionPending(tx, task, await admissionKey(task))) continue;
                    const jobs = await tx.find('jobs', { namespace, taskId: task.id }, { limit: 512 });
                    let retained = false;
                    for (const job of jobs) if (job.orderingKey && ['failed', 'expired', 'cancelled'].includes(job.state) && job.plan.ordering.onFailure !== 'continue' &&
                        await tx.count('jobs', { namespace, orderingKey: job.orderingKey, sequence: { gt: job.sequence }, state: { in: unsettled } })) retained = true;
                    if (retained) continue;
                    for (const job of jobs) await tx.remove('jobs', job.id);
                    await tx.remove('tasks', task.id); removed++;
                    if (!await tx.count('tasks', { namespace, rootId: task.rootId })) await tx.remove('controls', scope(namespace, `root:${task.rootId}`));
                }
                for (const candidate of await page('dedup', { namespace, expiresAt: { lte: now() } })) {
                    const record = await tx.get('dedup', candidate.id, true);
                    const task = record ? await tx.get('tasks', record.taskId) : null;
                    if (expired(record) && task?.state !== 'active' && !await admissionPending(tx, task, record.id)) await tx.remove('dedup', record.id);
                }
                for (const candidate of await page('aggregates', { namespace, expiresAt: { lte: now() }, state: { in: ['complete', 'incomplete', 'discarded'] } })) {
                    // Updates use this gate before reading; retirement cannot remove their quota slot mid-update.
                    await tx.lock(candidate.id);
                    const record = await tx.get('aggregates', candidate.id, true);
                    if (expired(record) && (record.emitted || record.state === 'discarded')) await tx.remove('aggregates', record.id);
                }
                for (const candidate of await tx.find('controls', { namespace, kind: { in: ['claim-cursor', 'aggregate-cursor'] }, expiresAt: { lte: now() } }, { limit: 100 })) {
                    const record = await tx.get('controls', candidate.id, true);
                    if (expired(record)) await tx.remove('controls', record.id);
                }
                return { removed };
            });
            await data.maintain(namespace);
            return result;
        },
        async inspect(namespace) {
            return repository.transaction(async tx => {
                const counts = {};
                for (const state of [...unsettled, 'failed', 'acknowledged', 'expired']) counts[state] = await tx.count('jobs', { namespace, state });
                const [oldest] = await tx.find('jobs', { namespace, state: { in: ['pending', 'retry_pending'] } }, { limit: 1, order: ['createdAt'] });
                return { counts, oldestPendingAgeMs: oldest ? now() - oldest.createdAt : 0 };
            });
        },
        async pending(namespace, revision) {
            return repository.transaction(async tx => await tx.count('jobs', { namespace, revision, state: { in: unsettled } }) +
                await tx.count('aggregates', { namespace, revision, required: 1 }));
        },
        pendingJobs(namespace, revision, { includeRecoverable = false } = {}) {
            return repository.transaction(async tx => {
                if (!includeRecoverable) return tx.count('jobs', { namespace, revision, state: { in: unsettled } });
                // Select pending and failed jobs together so an operator retry
                // cannot disappear by moving a job between separate scans.
                const where = { namespace, revision, state: { in: [...unsettled, 'failed'] } };
                const timestamp = now();
                let count = 0; let after;
                while (true) {
                    const jobs = await tx.find('jobs', where, { limit: 100, after });
                    for (const job of jobs) if (await needsRevision(job, id => tx.get('jobs', id), timestamp)) count++;
                    if (jobs.length < 100) return count;
                    after = { id: jobs.at(-1).id };
                }
            });
        },
        pendingAggregates: (namespace, runtimeRevision) => repository.transaction(tx => tx.count('aggregates', { namespace, kind: runtimeRevision, required: 1 })),
        recordReply: (namespace, taskId, reply) => recordChannel(namespace, taskId, 'reply', reply),
        recordAcknowledgement: (namespace, taskId, acknowledgement) => recordChannel(namespace, taskId, 'sourceAcknowledgement', acknowledgement),
        // Durable aggregation uses its own domain operation and serializes only one correlation group.
        async aggregate(namespace, key, update, maxGroups = 1000) {
            assert(Number.isSafeInteger(maxGroups) && maxGroups > 0, 'Invalid aggregation group limit.');
            const recordId = scope(namespace, `aggregate:${key}`);
            for (let attempt = 0; attempt < 20; attempt++) {
                const before = await repository.transaction(async tx => {
                    // Protect evidence from replacement/collection during hydration;
                    // the application callback still runs after this transaction.
                    const raw = await tx.get('aggregates', recordId, true);
                    return { raw, value: raw ? await data.hydrate(tx, namespace, raw) : null };
                });
                const result = update(before.value);
                if (result === null) return null;
                if (result instanceof Promise) void result.catch(() => {});
                assert(isRecord(result), 'Aggregation updates must return a plain record or null synchronously.');
                const outcome = await staged(namespace, async stage => {
                    const encoded = await encodeRecord('aggregates', result, stage);
                    return repository.transaction(async tx => {
                        await tx.lock(recordId);
                        const current = await tx.get('aggregates', recordId, true);
                        if ((current?.version ?? null) !== (before.raw?.version ?? null) || Boolean(current) !== Boolean(before.raw)) return { retry: true };
                        if (!current) {
                            await tx.lock(scope(namespace, 'aggregate-admission'));
                            const ruleId = typeof result.ruleId === 'string' && result.ruleId.length ? result.ruleId : null;
                            assert(await tx.count('aggregates', { namespace, ...(ruleId ? { ruleId } : {}) }) < maxGroups,
                                'Aggregation group capacity reached.', 'RESOURCE_LIMIT');
                        }
                        await tx.put('aggregates', { ...encoded, id: recordId, namespace, version: id('version'),
                            kind: result.runtimeRevision ?? 'unscoped', required: !result.emitted && result.state !== 'discarded' ? 1 : 0 });
                        return { result: freeze(result) };
                    });
                });
                if (!outcome.retry) return outcome.result;
            }
            throw new ConnectorError('CONFLICT', 'Aggregation changed repeatedly; retry the update.', { retryable: true, ambiguous: false });
        },
        async dueAggregates(namespace, runtimeRevision) {
            return repository.transaction(tx => scanDueAggregates(tx, namespace, runtimeRevision));
        },
    }, () => data.raw.close());
    recoveryAwareQueries.add(store.pendingJobs);
    return store;
}
