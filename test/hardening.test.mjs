import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createBridge, createProvider } from '@fayegram-ai/decision-bridge';
import { createCallbackConnector, ConnectorError, decisionEvaluator, createIterableSource, consumeSource } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createFileConnector } from '../src/node/connectors.mjs';

const validator = { revision: '1', validate: value => value };
const configured = evaluator => ({ limits: { maxBytes: 1024 }, evaluators: { e: evaluator }, validators: { v: validator },
    flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v' } } });

test('coded and retention failures after evaluation invocation remain reconcilable', async () => {
    for (const mode of ['coded', 'oversized', 'bridge']) {
        let calls = 0;
        const evaluator = mode === 'bridge' ? decisionEvaluator(createBridge({ provider: createProvider({
            type: 'openai-decisions', apiKey: 'synthetic-only', fetchImpl() { calls++; throw new TypeError('Lost response.'); },
        }) })) : { revision: '1', execute() {
            calls++;
            if (mode === 'coded') throw new ConnectorError('NETWORK_LOST', 'No response was confirmed.');
            return 'x'.repeat(2048);
        } };
        const runner = createRunner(configured(evaluator));
        try {
            const result = await runner.run({ flow: 'a', kind: 'request', request: mode === 'bridge'
                ? { state: 'Synthetic evidence.', questions: { ready: { type: 'noul', instructions: 'Ready?' } } } : 1 });
            assert.equal(result.status, 'unknown'); assert.equal(result.settled, false); assert.equal(calls, 1);
            await runner.resolveEvaluation(result.taskId, { status: 'succeeded', reason: 'Recovered provider evidence.', result: 2 });
            assert.equal((await runner.execute(result.taskId)).status, 'completed'); assert.equal(calls, 1);
        } finally { await runner.close({ cancel: true }); }
    }
});

test('explicit definite rejection overrides timeout and abort code spelling', async () => {
    for (const code of ['TIMEOUT', 'ABORTED', 'DECLINED']) {
        const runner = createRunner(configured({ revision: '1', execute() {
            throw new ConnectorError(code, 'Rejected before provider dispatch.', { ambiguous: false, retryable: false });
        } }));
        try {
            const result = await runner.run({ flow: 'a', kind: 'request', request: 1 });
            assert.equal(result.status, 'failed'); assert.equal(result.settled, true);
        } finally { await runner.close(); }
    }
});

test('planning cannot authorize outputs using a replaced policy or transform revision', async () => {
    for (const replacement of ['policy', 'transform', 'missing-policy']) {
        let sends = 0; let replacementCalls = 0;
        const policy = (revision, status) => ({ revision, execute: () => ({ status, reasons: [], policyRevision: revision }) });
        const options = { validators: { v: validator }, policies: { p: policy('1', replacement === 'policy' ? 'review' : 'accepted') },
            transforms: { t: { revision: '1', execute: value => value } },
            connectors: { sink: createCallbackConnector(() => { sends++; return { status: 'acknowledged' }; }) },
            flows: { a: { revision: '1', operation: 'extraction', inputs: ['result'], validator: 'v', policies: { gate: { type: 'module', name: 'p' } },
                connections: [{ id: 'out', connector: 'sink', policy: 'gate', when: 'decision.accepted', payload: 'final-decision',
                    transforms: [{ type: 'module', name: 't' }] }] } } };
        const runner = createRunner(options);
        try {
            const task = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
            if (replacement === 'policy') options.policies.p = { revision: '2', execute() { replacementCalls++; return policy('2', 'accepted').execute(); } };
            else if (replacement === 'transform') options.transforms.t = { revision: '2', execute() { replacementCalls++; return 'replacement'; } };
            else delete options.policies.p;
            const result = await runner.execute(task.taskId);
            assert.equal(sends, 0); assert.equal(replacementCalls, 0);
            assert.equal(replacement === 'transform' ? result.deliveries[0].error.code : result.policies.gate.error.code, 'REVISION_UNAVAILABLE');
        } finally { await runner.close(); }
    }
});

test('overwrite file destinations cannot enable ambiguous automatic retry', async () => {
    for (const overwrite of [false, true]) {
        const connector = createFileConnector({ directory: 'unused-by-this-test', overwrite });
        assert.equal(connector.capabilities.idempotent, !overwrite);
        const runner = createRunner({ validators: { v: validator }, placements: ['local'], connectors: { sink: connector },
            flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [{ id: 'out', connector: 'sink',
                placement: 'local', when: 'always', payload: 'raw-result', retry: { ambiguous: 'retry' } }] } } });
        try { if (overwrite) await assert.rejects(runner.ready, { code: 'CONFIGURATION_ERROR' }); else await runner.ready; }
        finally { await runner.close(); }
    }
});

test('caller waits finish using monotonic time when wall time stops', async t => {
    const clock = Date.now(); t.mock.method(Date, 'now', () => clock);
    const runner = createRunner(configured({ revision: '1', execute: () => new Promise(() => {}) }));
    try {
        const task = await runner.submit({ flow: 'a', kind: 'request', request: 1 });
        const result = await Promise.race([runner.execute(task.taskId, { waitMs: 20 }), sleep(1000).then(() => { throw Error('Wait did not finish.'); })]);
        assert.equal(result.status, 'running');
    } finally { await runner.close({ cancel: true }); }
});

test('source processing beyond sixty seconds uses its configured wait budget', async t => {
    let clock = 0; t.mock.method(performance, 'now', () => clock);
    const release = Promise.withResolvers(); let rejected = 0;
    const options = configured({ revision: '1', execute() { clock += 60001; return release.promise; } });
    options.flows.a.timeoutMs = 120000;
    const runner = createRunner(options);
    const source = createIterableSource([{ input: { flow: 'a', kind: 'request', request: 1 }, reject() { rejected++; } }], { name: 'long-processing' });
    try {
        const pending = consumeSource(runner, source, { processingTimeoutMs: 120000 });
        await sleep(30); release.resolve(2);
        const result = await pending;
        assert.equal(result.completed, 1); assert.equal(rejected, 0);
    } finally { release.resolve(2); await runner.close({ cancel: true }); }
});
