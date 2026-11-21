import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CloudflareExtractionProvider } from '@fayegram-ai/decision-bridge/extraction';
import { consumeSource, createCallbackConnector, createRunner, extractionEvaluator, routedExtractionEvaluator } from '@fayegram-ai/decision-bridge-connectors';
import { createConfiguredRunner, createSpoolSource } from '@fayegram-ai/decision-bridge-connectors/node';
import { createSQLiteStore } from '@fayegram-ai/decision-bridge-connectors/sqlite';

// Public imports let the installed-package smoke run these same cases in a fresh consumer.
// Both pending records and the completed export came from the Connector running Core 0.1.8.
const { config, records: [valid, invalid] } = JSON.parse(await readFile(new URL('./fixtures/clairic-core-0.1.8-queue.json', import.meta.url)));
const archive = await readFile(new URL('./fixtures/clairic-core-0.1.8.ndjson', import.meta.url));
const env = { CLOUDFLARE_API_TOKEN: 'fixture', CLOUDFLARE_ACCOUNT_ID: 'fixture' };
const flow = { revision: '1', operation: 'extraction', inputs: ['request'], evaluator: 'cf' };
const pdf = { content_type: 'application/pdf', base64: 'JVBERi0xLjQKJSVFT0Y=' };
function native(model) {
    return { model, answers: { name: { type: 'text', value: 'Ada', span: null } }, truncated: false,
        usage: { input_tokens: 1, output_tokens: 0 } };
}
function direct(fetchImpl) {
    return extractionEvaluator(new CloudflareExtractionProvider({ accountId: 'fixture', apiKey: 'fixture', fetchImpl }));
}

test('spool intake quarantines a PDF exceeding the local Core request budget before admission', async t => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-extraction-budget-'));
    const calls = t.mock.method(globalThis, 'fetch', assert.fail);
    const settings = structuredClone(config);
    settings.store = { type: 'sqlite', options: { path: 'state.sqlite' } };
    settings.evaluators.cf.options.provider.maxRequestBytes = 64;
    const controller = new AbortController();
    let runner;
    try {
        runner = await createConfiguredRunner(settings, { baseDir: directory, env });
        const source = createSpoolSource({ directory, flow: 'extract', inputMode: 'request' });
        const input = JSON.stringify({ documents: [pdf], fields: valid.task.request.fields });
        await writeFile(join(directory, 'input.json'), input);
        let failure;
        const summary = await consumeSource(runner, source, { concurrency: 1, acknowledge: 'admitted',
            signal: AbortSignal.any([controller.signal, AbortSignal.timeout(5000)]),
            onOutcome(outcome) { failure = outcome.error; controller.abort(); } });
        assert.equal(summary.admitted, 0);
        assert.equal(summary.rejected, 1);
        assert.equal(failure.code, 'REQUEST_TOO_LARGE');
        assert.equal(failure.source, 'client');
        assert.equal(calls.mock.callCount(), 0);
        const claims = await readdir(join(directory, '.quarantine'));
        assert.equal(claims.length, 1);
        assert.equal(await readFile(join(directory, '.quarantine', claims[0], 'input.json'), 'utf8'), input);
        assert.equal(failure.inputInvalid, true);
    } finally { await runner?.close(); await rm(directory, { recursive: true, force: true }); }
});

test('a configured worker settles old invalid work before dispatch and executes old valid work exactly once', async t => {
    const calls = t.mock.method(globalThis, 'fetch', async () => Response.json({ success: true, result: native('clairic') }));
    const runner = await createConfiguredRunner(config, { env });
    try {
        for (const { task, jobs } of [invalid, valid]) {
            assert.equal((await runner.ready).extract.runtimeRevision, task.plan.runtimeRevision);
            await runner.store.admit(task, jobs, null);
        }
        const failed = await runner.execute(invalid.task.id);
        assert.equal(failed.status, 'failed');
        assert.equal(failed.evaluation.status, 'failed');
        assert.equal((await runner.store.get(failed.taskId, runner.namespace)).task.errors[0].code, 'VALIDATION_ERROR');
        assert.equal(calls.mock.callCount(), 0);
        await assert.rejects(runner.submit({ flow: 'extract', kind: 'request', request: invalid.task.request }), { code: 'VALIDATION_ERROR' });

        const completed = await runner.execute(valid.task.id);
        assert.equal(completed.status, 'completed');
        const { task } = await runner.store.get(completed.taskId, runner.namespace);
        assert.deepEqual(task.request, valid.task.request);
        assert.equal(task.result.meta.requestedModel, '@cf/cloudflare/clairic');
        assert.equal(task.result.meta.resolvedModel, 'clairic');
        assert.equal((await runner.execute(valid.task.id)).status, 'completed');
        assert.equal(calls.mock.callCount(), 1);
    } finally { await runner.close(); }
});

test('ordinary schema-v3 export from Core 0.1.8 imports and reprocesses with its original model and no inference', async t => {
    const calls = t.mock.method(globalThis, 'fetch', () => { throw new Error('Unexpected inference during replay'); });
    const runner = await createConfiguredRunner(config, { env });
    try {
        const imported = await runner.importTask([archive], { flow: 'extract' });
        const completed = await runner.execute(imported.taskId);
        const replay = await runner.reprocess(imported.taskId);
        for (const report of [completed, await runner.execute(replay.taskId)]) {
            assert.equal(report.status, 'completed');
            assert.equal(report.schemaVersion, 3);
            const { task } = await runner.store.get(report.taskId, runner.namespace);
            assert.equal(task.result.meta.requestedModel, '@cf/cloudflare/clairic');
            assert.equal(task.result.meta.resolvedModel, 'clairic');
            assert.equal(task.result.extraction.answers.name.value, 'Ada');
        }
        assert.equal(calls.mock.callCount(), 0);
    } finally { await runner.close(); }
});

test('embedded direct preflight plans failure output and preserves uncertainty after a genuine dispatch', async () => {
    const backing = createSQLiteStore(':memory:');
    let markers = 0; let calls = 0; const notices = [];
    const store = { ...backing, async commit(job, update) {
        await backing.commit(job, update);
        if (job.kind === 'evaluation' && update.job?.stage === 'invoked') markers++;
    } };
    const options = { store, flows: { extract: { ...flow, connections: [
        { id: 'errors', connector: 'alerts', when: 'failure', payload: 'failure' },
    ] } }, connectors: { alerts: createCallbackConnector(payload => { notices.push(payload); return { status: 'acknowledged' }; }) } };
    const producer = createRunner({ ...options, evaluators: { cf: { operation: 'extraction', revision: '1', execute: assert.fail } } });
    const worker = createRunner({ ...options, evaluators: { cf: direct(async () => {
        calls++;
        return calls === 1 ? Response.json({ success: true, result: {} })
            : Response.json({ success: false, errors: [{ code: 3006 }] }, { status: 413 });
    }) } });
    try {
        const { taskId } = await producer.submit({ flow: 'extract', kind: 'request', request: invalid.task.request });
        const failed = await worker.execute(taskId);
        assert.equal(failed.status, 'failed'); assert.equal(failed.deliveryCounts.acknowledged, 1);
        assert.equal(notices.length, 1); assert.equal(notices[0].errors[0].code, 'VALIDATION_ERROR');
        assert.equal(markers, 0); assert.equal(calls, 0);

        const ambiguous = await worker.run({ flow: 'extract', kind: 'request', request: valid.task.request });
        assert.equal(ambiguous.status, 'unknown');
        assert.equal(markers, 1); assert.equal(calls, 1);

        const upstream = await worker.run({ flow: 'extract', kind: 'request', request: valid.task.request });
        assert.equal(upstream.status, 'unknown');
        const [failure] = (await store.get(upstream.taskId, worker.namespace)).task.errors;
        assert.equal(failure.code, 'REQUEST_TOO_LARGE'); assert.equal(failure.inputInvalid, undefined);
        assert.equal(markers, 2); assert.equal(calls, 2);
    } finally { await producer.close(); await worker.close(); await backing.close(); }
});

for (const action of ['cancel', 'expire']) {
    test(`SQLite fences a direct worker when its claim is lost before the invocation marker (${action})`, async t => {
        const backing = createSQLiteStore(':memory:');
        const fenced = Promise.withResolvers();
        let calls = 0; let first = true;
        const store = { ...backing, async commit(job, update) {
            if (first && update.job?.stage === 'invoked') {
                first = false;
                let clock;
                try {
                    if (action === 'cancel') await worker.cancel(job.taskId);
                    else {
                        clock = t.mock.method(Date, 'now', () => job.leaseUntil + 1);
                        await backing.recover(worker.namespace);
                    }
                    await backing.commit(job, update);
                    fenced.resolve('marker committed');
                } catch (error) { fenced.resolve(error.code); throw error; }
                finally { clock?.mock.restore(); }
                return;
            }
            return backing.commit(job, update);
        } };
        const worker = createRunner({ store, flows: { extract: flow }, evaluators: { cf: direct(async () => {
            calls++; return Response.json({ success: true, result: native('clairic') });
        }) } });
        try {
            const { taskId } = await worker.submit({ flow: 'extract', kind: 'request', request: valid.task.request });
            await worker.tick({ taskId });
            assert.equal(await fenced.promise, 'STALE_LEASE');
            assert.equal(calls, 0);
            const { jobs } = await backing.get(taskId, worker.namespace);
            assert.equal(jobs[0].stage, 'prepared');
            assert.equal(jobs[0].state, action === 'cancel' ? 'cancelled' : 'pending');
            if (action === 'expire') {
                assert.equal((await worker.execute(taskId)).status, 'completed');
                assert.equal(calls, 1);
            }
        } finally { await worker.close(); await backing.close(); }
    });
}

test('a direct Workers AI binding preserves the inline PDF, Flash selector and nullable span', async () => {
    const documents = [pdf];
    const calls = [];
    const defaultProfile = direct(assert.fail);
    const provider = new CloudflareExtractionProvider({ transport: 'binding', model: 'clairic-flash', ai: { async run(model, body) {
        calls.push({ model, body }); return Response.json(native('clairic-flash'));
    } } });
    const runner = createRunner({ flows: { extract: { ...flow, inputs: ['request', 'result'] } }, evaluators: { cf: extractionEvaluator(provider) } });
    try {
        for (const model of [undefined, '@cf/cloudflare/clairic-flash']) {
            const request = { fields: valid.task.request.fields, documents, ...(model ? { model } : {}) };
            const report = await runner.run({ flow: 'extract', kind: 'request', request });
            assert.equal(report.status, 'completed');
            const { task } = await runner.store.get(report.taskId, runner.namespace);
            assert.equal(task.result.meta.requestedModel, model ?? 'clairic-flash');
            assert.equal(task.result.meta.resolvedModel, 'clairic-flash');
            assert.equal(task.result.extraction.answers.name.span, null);
            assert.deepEqual(task.request, request);
            assert.equal(calls.at(-1).model, '@cf/cloudflare/clairic-flash');
            assert.deepEqual(calls.at(-1).body.documents, documents);
            if (model === undefined) {
                assert.throws(() => defaultProfile.validateSaved(task.result, request), { code: 'INVALID_SAVED_OUTPUT' });
            }
            const replay = await runner.reprocess(report.taskId);
            assert.equal((await runner.execute(replay.taskId)).status, 'completed');
        }
        assert.equal(calls.length, 2);
    } finally { await runner.close(); }
});

test('configured routing selects its Flash route for an inline PDF data URL', async t => {
    const documents = ['data:application/pdf;base64,' + pdf.base64];
    const calls = t.mock.method(globalThis, 'fetch', async () => Response.json({ success: true, result: native('clairic-flash') }));
    const runner = await createConfiguredRunner({ schemaVersion: 3, flows: { extract: flow },
        evaluators: { cf: { type: 'routing', options: { operation: 'extraction', routing: { defaultRoute: 'primary',
            routes: { primary: { config: { type: 'cloudflare-extraction', model: 'clairic-flash' } } } } } } } }, { env });
    try {
        const report = await runner.run({ flow: 'extract', kind: 'request', request: { documents, fields: valid.task.request.fields } });
        assert.equal(report.status, 'completed');
        const { task } = await runner.store.get(report.taskId, runner.namespace);
        assert.equal(task.result.meta.routing.selectedRoute, 'primary');
        assert.equal(task.result.meta.resolvedModel, 'clairic-flash');
        assert.equal(calls.mock.callCount(), 1);
        assert.deepEqual(JSON.parse(calls.mock.calls[0].arguments[1].body).documents, documents);
    } finally { await runner.close(); }
});

test('preflight cannot classify work for a proxy or a provider decorated after registration', async () => {
    const provider = new CloudflareExtractionProvider({ accountId: 'fixture', apiKey: 'fixture', fetchImpl: assert.fail });
    const proxy = extractionEvaluator(new Proxy(provider, {}));
    const decorated = extractionEvaluator(provider);
    let calls = 0;
    Object.defineProperty(provider, 'extract', { get() {
        calls++;
        return async () => { throw new Error('Unconfirmed custom dispatch'); };
    } });
    const store = createSQLiteStore(':memory:');
    const options = { store, flows: { extract: flow } };
    const producer = createRunner({ ...options, evaluators: { cf: { operation: 'extraction', revision: '1', execute: assert.fail } } });
    try {
        for (const evaluator of [proxy, decorated]) {
            const worker = createRunner({ ...options, evaluators: { cf: evaluator } });
            try {
                const { taskId } = await producer.submit({ flow: 'extract', kind: 'request', request: invalid.task.request });
                assert.equal((await worker.execute(taskId)).status, 'unknown');
            } finally { await worker.close(); }
        }
        assert.equal(calls, 2, 'The custom getter runs once per dispatch, after preflight');
    } finally { await producer.close(); await store.close(); }
});

test('custom and routed extraction adapters retain their own admission and dispatch semantics', async () => {
    let validations = 0; let executions = 0;
    class Custom extends CloudflareExtractionProvider {
        validate(value) { validations++; return value; }
        async extract() { executions++; throw new Error('Unconfirmed provider response'); }
    }
    const router = { inspect() { validations++; }, async extract() { executions++; throw new Error('Unconfirmed route response'); } };
    for (const evaluator of [extractionEvaluator(new Custom({ accountId: 'fixture', apiKey: 'fixture' })), routedExtractionEvaluator(router)]) {
        const runner = createRunner({ flows: { extract: flow }, evaluators: { cf: evaluator } });
        try {
            const report = await runner.run({ flow: 'extract', kind: 'request', request: invalid.task.request });
            assert.equal(report.status, 'unknown');
        } finally { await runner.close(); }
    }
    assert.equal(validations, 2);
    assert.equal(executions, 2);
});
