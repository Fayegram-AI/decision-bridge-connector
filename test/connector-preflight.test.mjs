import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ConnectorError, createMemoryStore } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createFileConnector, createHttpConnector, createProcessConnector } from '../src/node/connectors.mjs';
import { createMcpConnector } from '../src/integrations/mcp-client.mjs';
import { createRedisStreamConnector } from '../src/integrations/redis.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';

const kinds = ['file', 'http', 'process', 'mcp', 'redis'];
const context = { taskId: 'task', eventId: 'event', deliveryId: 'delivery', attemptId: 'attempt', idempotencyKey: 'key' };

// Keep preparation failures separate from any recipient function or filesystem effect.
async function fixture(kind, directory, mode) {
    let calls = 0;
    const failed = () => { calls++; throw new ConnectorError('PAYLOAD_TOO_LARGE', 'Transport-side failure.'); };
    const payload = mode === 'payload' ? kind === 'mcp' ? 42 : { value: 'x'.repeat(32) } : {};
    const options = mode === 'payload' ? { maxPayloadBytes: 8 } : {};
    let connector; let overrides = {}; let code = mode === 'payload' ? 'PAYLOAD_TOO_LARGE' : 'CONFIGURATION_ERROR';
    if (kind === 'file') {
        connector = createFileConnector({ directory: join(directory, 'output'), ...options });
        if (mode === 'destination') overrides.destination = { filename: '../invalid.json' };
    } else if (kind === 'http') {
        connector = createHttpConnector({ url: 'https://example.test/', ...options,
            fetchImpl: mode === 'transport' ? failed : async () => { calls++; return new Response('{}'); } });
        if (mode === 'destination') overrides.destination = { url: 'https://other.test/' };
    } else if (kind === 'process') {
        connector = createProcessConnector({ command: process.execPath, inputMode: 'payload',
            args: ['-e', `require('node:fs').writeFileSync(${JSON.stringify(join(directory, 'spawned'))}, 'invoked'); console.log('{}');`],
            ...(mode === 'payload' ? { maxInputBytes: 8 } : {}) });
        if (mode === 'destination') overrides.destination = {};
    } else if (kind === 'mcp') {
        connector = await createMcpConnector({ tool: 'deliver', idempotent: mode === 'destination',
            ...(mode === 'destination' ? { idempotencyArgument: 'deliveryKey' } : {}),
            client: { callTool: mode === 'transport' ? failed : async () => { calls++; return { content: [] }; } } });
        if (mode === 'destination') overrides.idempotencyKey = '';
        code = 'CONFIGURATION_ERROR';
    } else {
        connector = createRedisStreamConnector({ stream: 'events', ...options,
            client: { sendCommand: mode === 'transport' ? failed : async () => { calls++; return '1-0'; } } });
        if (mode === 'destination') overrides.destination = { stream: '' };
        if (mode === 'payload') code = 'RESOURCE_LIMIT';
    }
    return { connector, payload, overrides, code, calls: () => calls };
}

for (const kind of kinds) for (const mode of ['payload', 'destination'])
test(`${kind}: local ${mode} rejection is definite before recipient dispatch`, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-preflight-'));
    const setup = await fixture(kind, directory, mode);
    try {
        await assert.rejects(setup.connector.send(setup.payload, { ...context, ...setup.overrides }), {
            code: setup.code, ambiguous: false, retryable: false,
        });
        assert.equal(setup.calls(), 0); assert.deepEqual(await readdir(directory), []);
    } finally { await setup.connector.close?.(); await rm(directory, { recursive: true }); }
});

for (const [name, factory] of [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')]])
test(`${name}: built-in preflight failures settle once without operator reconciliation or automatic retry`, async () => {
    const store = await factory(); const directory = await mkdtemp(join(tmpdir(), 'dbc-preflight-engine-'));
    try {
        for (const kind of kinds) {
            const setup = await fixture(kind, directory, 'payload');
            const runner = createRunner({ store, namespace: `preflight-${kind}-${crypto.randomUUID()}`, placements: ['local'],
                validators: { v: { revision: '1', validate: value => value } }, connectors: { sink: setup.connector },
                flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                    { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', placement: 'local',
                        retry: { maxAttempts: 3, delayMs: 1, maxDelayMs: 1 } },
                ] } } });
            try {
                const result = await runner.run({ flow: 'a', kind: 'result', result: setup.payload });
                assert.equal(result.status, 'partial', kind); assert.equal(result.settled, true);
                assert.equal(result.deliveries[0].status, 'failed');
                assert.equal(result.deliveries[0].error.ambiguous, false);
                assert.equal(result.deliveries[0].error.retryable, false);
                assert.equal(result.deliveries[0].attempts.length, 1);
                await runner.recover(); await runner.execute(result.taskId);
                assert.equal((await runner.get(result.taskId)).deliveries[0].attempts.length, 1);
                assert.equal(setup.calls(), 0); assert.deepEqual(await readdir(directory), []);
            } finally { await runner.close(); await setup.connector.close?.(); }
        }
    } finally { await store.close(); await rm(directory, { recursive: true }); }
});

test('closed process, MCP and Redis connectors reject definitely before recipient dispatch', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-preflight-closed-'));
    try {
        for (const kind of ['process', 'mcp', 'redis']) {
            const setup = await fixture(kind, directory, 'closed');
            await setup.connector.close();
            await assert.rejects(setup.connector.send({}, context), { ambiguous: false, retryable: false });
            assert.equal(setup.calls(), 0); assert.deepEqual(await readdir(directory), []);
        }
    } finally { await rm(directory, { recursive: true }); }
});

test('preflight-like error codes after dispatch still require recipient outcome evidence', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-preflight-dispatched-'));
    try {
        for (const kind of ['http', 'mcp', 'redis']) {
            const setup = await fixture(kind, directory, 'transport');
            try {
                await assert.rejects(setup.connector.send({}, context), { ambiguous: true });
                assert.equal(setup.calls(), 1);
            } finally { await setup.connector.close?.(); }
        }
    } finally { await rm(directory, { recursive: true }); }
});

test('invalid or already-cancelled lifetimes reject definitely before built-in dispatch', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-preflight-signal-'));
    try {
        for (const kind of kinds) {
            const setup = await fixture(kind, directory, 'signal');
            try {
                for (const signal of [{ aborted: true }, AbortSignal.abort()]) {
                    await assert.rejects(setup.connector.send({}, { ...context, signal }), {
                        code: signal instanceof AbortSignal ? 'ABORTED' : 'CONFIGURATION_ERROR',
                        ambiguous: false, retryable: false,
                    });
                }
                assert.equal(setup.calls(), 0); assert.deepEqual(await readdir(directory), []);
            } finally { await setup.connector.close?.(); }
        }
    } finally { await rm(directory, { recursive: true }); }
});

test('native HTTP header validation errors are definite without invoking fetch', async () => {
    let calls = 0;
    const connector = createHttpConnector({ url: 'https://example.test/', fetchImpl() { calls++; } });
    await assert.rejects(connector.send({}, { headers: { 'invalid\nname': 'value' } }), {
        code: 'UNEXPECTED_ERROR', ambiguous: false, retryable: false,
    });
    assert.equal(calls, 0);
});
