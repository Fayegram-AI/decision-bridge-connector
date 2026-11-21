import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter, once } from 'node:events';
import { request as httpRequest } from 'node:http';
import { Readable } from 'node:stream';
import { ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createHttpHandler, createHttpServer } from '../src/node/http-service.mjs';

const configuration = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };

test('HTTP reports early input failures while the client is still uploading', { timeout: 10000 }, async () => {
    const runner = createRunner(configuration);
    const server = createHttpServer({ runner, flow: 'a', reply: 'complete', allowAdmin: true, maxBytes: 128 });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const base = 'http://127.0.0.1:' + server.address().port;
    try {
        const original = await runner.run({ flow: 'a', kind: 'result', result: 42 });
        const exported = await fetch(base + '/tasks/' + original.taskId + '/export');
        assert.equal(exported.status, 200);
        const archive = await exported.text();
        for (const [path, prefix, status, code] of [
            ['/tasks/import', archive + archive, 400, 'INVALID_TRANSFER'],
            ['/tasks', ' '.repeat(129), 429, 'RESOURCE_LIMIT'],
        ]) {
            const upload = httpRequest(base + path, { method: 'POST', signal: AbortSignal.timeout(3000) });
            try {
                const received = once(upload, 'response');
                upload.write(prefix);
                const [response] = await received;
                const chunks = [];
                for await (const chunk of response) chunks.push(chunk);
                assert.equal(response.statusCode, status);
                assert.equal(JSON.parse(Buffer.concat(chunks)).error.code, code);
                assert.equal(response.headers.connection, 'close');
            } finally { upload.destroy(); }
        }
        assert.equal((await runner.list()).items.length, 1);
        const accepted = await fetch(base + '/tasks/import', { method: 'POST', body: archive });
        assert.equal(accepted.status, 202);
        assert.ok((await accepted.json()).value.taskId);
    } finally { await server.drain(); await runner.close(); }
});

test('HTTP reports export failures as JSON until the first archive frame is sent', async () => {
    const runner = createRunner({ ...configuration, limits: { maxTransfers: 1 } });
    const server = createHttpServer({ runner, flow: 'a', reply: 'complete' });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    try {
        const { taskId } = await runner.run({ flow: 'a', kind: 'result', result: 42 });
        const url = 'http://127.0.0.1:' + server.address().port + '/tasks/' + taskId + '/export';
        const lease = await runner.openSnapshot(taskId);
        try {
            const refused = await fetch(url);
            assert.equal(refused.status, 429);
            assert.equal(refused.headers.get('content-type'), 'application/json');
            assert.equal((await refused.json()).error.code, 'RESOURCE_LIMIT');
        } finally { await runner.closeSnapshot(lease.id); }
        const exported = await fetch(url);
        assert.equal(exported.status, 200);
        assert.equal(exported.headers.get('content-type'), 'application/x-ndjson');
        assert.equal(JSON.parse((await exported.text()).split('\n')[0]).type, 'header');
    } finally { await server.drain(); await runner.close(); }
});

test('HTTP rejects malformed UTF-8 before admission and preserves valid Unicode', async () => {
    const runner = createRunner(configuration);
    const server = createHttpServer({ runner, flow: 'a', reply: 'complete', maxBytes: 128 });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const url = `http://127.0.0.1:${server.address().port}/tasks`;
    const post = body => fetch(url, { method: 'POST', body, headers: { 'content-type': 'application/json' } });
    try {
        const invalid = Buffer.concat([Buffer.from('{"kind":"result","result":"'), Buffer.from([0xff]), Buffer.from('"}')]);
        const rejected = await post(invalid);
        assert.equal(rejected.status, 400); assert.equal((await rejected.json()).error.code, 'INVALID_JSON');
        assert.equal((await runner.list()).items.length, 0);
        const accepted = await post(Buffer.from(JSON.stringify({ kind: 'result', result: 'é猫🌉' })));
        assert.equal(accepted.status, 200); assert.equal((await accepted.json()).value.evaluation.result, 'é猫🌉');
        assert.equal((await runner.list()).items.length, 1);
    } finally { await server.drain(); await runner.close(); }
});

/** A blocked end callback models socket backpressure without platform buffer-size assumptions. */
function blockedResponse() {
    const response = new EventEmitter();
    Object.assign(response, { headersSent: false, writableEnded: false, writableFinished: false, destroyed: false, ends: 0,
        setHeader() {},
        end(text, callback) {
            this.ends++; this.headersSent = true; this.writableEnded = true; this.body = text; this.callback = callback;
        },
        destroy() { this.destroyed = true; },
    });
    return response;
}

for (const mode of ['task', 'read', 'error']) test(`HTTP ${mode} responses close stalled writes within the reply budget`, { timeout: 5000 }, async () => {
    const runner = createRunner(configuration); const response = blockedResponse();
    const request = Readable.from(mode === 'task' ? [Buffer.from('{"kind":"result","result":1}')] : []);
    Object.assign(request, { method: mode === 'task' ? 'POST' : 'GET', url: mode === 'task' ? '/tasks' : '/health', headers: { host: 'localhost' } });
    const handler = createHttpHandler({ runner, flow: 'a', reply: 'complete', replyTimeoutMs: 25,
        ...(mode === 'error' ? { authorizationTimeoutMs: 100, authorize() { throw null; } } : {}) });
    try {
        await handler(request, response);
        assert.equal(response.ends, 1); assert.equal(response.destroyed, true);
        if (mode === 'error') assert.equal(JSON.parse(response.body).error.code, 'UNEXPECTED_ERROR');
        if (mode === 'task') {
            const [task] = (await runner.list()).items;
            assert.equal(task.status, 'completed'); assert.equal(task.reply.status, 'unknown');
        }
        // Late write failure remains observed after cancellation.
        response.callback(new Error('late pipe error')); response.emit('error', new Error('late pipe error'));
        await new Promise(resolve => setImmediate(resolve));
    } finally { response.emit('close'); await runner.close(); }
});

test('HTTP closes a reply that expires before its send callback starts', async () => {
    const runner = createRunner(configuration); const response = blockedResponse();
    const request = Readable.from([Buffer.from('{"kind":"result","result":1}')]);
    Object.assign(request, { method: 'POST', url: '/tasks', headers: { host: 'localhost' } });
    try {
        assert.throws(() => createHttpHandler({ runner, flow: 'a', reply: 'complete', replyTimeoutMs: 0 }), { code: 'CONFIGURATION_ERROR' });
        const handler = createHttpHandler({ runner: { ...runner, reply: async () => ({ status: 'unknown' }) }, flow: 'a', reply: 'complete' });
        await handler(request, response);
        assert.equal(response.ends, 0); assert.equal(response.destroyed, true);
    } finally { await runner.close(); }
});

test('HTTP normalizes unreadable authorization errors and enforces the execution restriction', async () => {
    const runner = createRunner(configuration);
    let error = Object.defineProperty(new ConnectorError('BROKEN', 'Untrusted detail'), 'code', { get() { throw new Error('Unreadable'); } });
    const server = createHttpServer({ runner, flow: 'a', reply: 'complete', allowRun: false,
        authorizationTimeoutMs: 100, authorize() { throw error; } });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const url = `http://127.0.0.1:${server.address().port}`;
    try {
        const rejected = await fetch(`${url}/health`);
        assert.equal(rejected.status, 400); assert.equal((await rejected.json()).error.code, 'UNEXPECTED_ERROR');
        error = new ConnectorError('toString', 'Configured authorization failure.');
        const inherited = await fetch(`${url}/health`);
        assert.equal(inherited.status, 400); assert.equal((await inherited.json()).error.code, 'toString');
        const disabled = await fetch(`${url}/operations/run`, { method: 'POST', body: JSON.stringify({ input: { kind: 'result', result: 1 } }) });
        assert.equal(disabled.status, 403); assert.equal((await disabled.json()).error.code, 'FORBIDDEN');
        assert.equal((await runner.list()).items.length, 0);
    } finally { await server.drain(); await runner.close(); }
});
