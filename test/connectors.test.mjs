import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createFileConnector, createProcessConnector, createHttpConnector } from '../src/node/connectors.mjs';

const context = { deliveryId: 'delivery-1', attemptId: 'attempt-1', taskId: 'task-1', eventId: 'event-1', idempotencyKey: 'idem-1' };

test('outgoing connectors reject unknown configuration fields before creating effects', async () => {
  assert.throws(() => createFileConnector({ directory: 'unused', overwite: true }), { code: 'CONFIGURATION_ERROR' });
  assert.throws(() => createHttpConnector({ url: 'https://example.invalid', timeoutMS: 10 }), { code: 'CONFIGURATION_ERROR' });
  assert.throws(() => createProcessConnector({ command: 'unused', shell: true }), { code: 'CONFIGURATION_ERROR' });
  await assert.rejects(createFileConnector({ directory: 'unused' }).send({}, { ...context, destination: { path: 'ignored' } }), { code: 'CONFIGURATION_ERROR' });
  await assert.rejects(createHttpConnector({ url: 'https://example.invalid' }).send({}, { ...context, destination: { endpoint: 'ignored' } }), { code: 'CONFIGURATION_ERROR' });
  await assert.rejects(createProcessConnector({ command: 'unused' }).send({}, { ...context, destination: { command: 'ignored' } }), { code: 'CONFIGURATION_ERROR' });
});

test('file delivery commits atomically, recognizes replay, and rejects conflicting output and path escape', async t => {
  const directory = await mkdtemp(join(process.cwd(), 'decision-connectors-files-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const connector = createFileConnector({ directory });
  const first = await connector.send({ answer: 42 }, context);
  assert.equal(first.status, 'acknowledged');
  assert.equal(first.receipt.duplicate, false);
  assert.deepEqual(JSON.parse(await readFile(first.receipt.path, 'utf8')), { answer: 42 });
  const duplicate = await connector.send({ answer: 42 }, context);
  assert.equal(duplicate.receipt.duplicate, true);
  await assert.rejects(connector.send({ answer: 99 }, context), { code: 'DELIVERY_CONFLICT', ambiguous: false });
  await assert.rejects(connector.send({}, { ...context, destination: { filename: '../escape.json' } }), { code: 'CONFIGURATION_ERROR' });
  assert.deepEqual(await readdir(directory), ['delivery-1.json']);
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(connector.send({}, { ...context, deliveryId: 'cancelled', signal: controller.signal }), { code: 'ABORTED', ambiguous: false });
  assert.deepEqual(await readdir(directory), ['delivery-1.json']);
});

test('concurrent identical file attempts converge on one committed file', async t => {
  const directory = await mkdtemp(join(process.cwd(), 'decision-connectors-files-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const connector = createFileConnector({ directory });
  const reports = await Promise.all(Array.from({ length: 5 }, () => connector.send({ answer: 'same' }, context)));
  assert.equal(reports.every(report => report.status === 'acknowledged'), true);
  assert.equal(reports.filter(report => !report.receipt.duplicate).length, 1);
  assert.deepEqual(await readdir(directory), ['delivery-1.json']);
});

test('file connector creates configured nested directories only when sending', async t => {
  const directory = await mkdtemp(join(process.cwd(), 'decision-connectors-nested-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const destination = join(directory, 'new', 'outputs');
  const connector = createFileConnector({ directory: destination });
  assert.deepEqual(await readdir(directory), []);
  const receipt = await connector.send({ answer: 'saved' }, context);
  assert.equal(receipt.status, 'acknowledged');
  assert.deepEqual(JSON.parse(await readFile(join(destination, 'delivery-1.json'), 'utf8')), { answer: 'saved' });
});

test('HTTP sends JSON and stable idempotency metadata; acceptance is not downstream completion', async t => {
  let received;
  const server = createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    received = { payload: JSON.parse(Buffer.concat(chunks)), key: req.headers['idempotency-key'], delivery: req.headers['x-decision-delivery-id'] };
    res.writeHead(202, { 'x-request-id': 'receipt-1', 'content-type': 'application/json' });
    res.end('{"queued":true}');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolvePromise => server.close(resolvePromise)));
  const connector = createHttpConnector({ url: `http://127.0.0.1:${server.address().port}/events`, captureResponse: true });
  const receipt = await connector.send({ accepted: true }, context);
  assert.deepEqual(received, { payload: { accepted: true }, key: 'idem-1', delivery: 'delivery-1' });
  assert.equal(receipt.status, 'acknowledged');
  assert.equal(receipt.completion, undefined);
  assert.deepEqual(receipt.receipt, { httpStatus: 202, requestId: 'receipt-1', response: { queued: true } });
});

test('HTTP distinguishes rejection, uncertain send, cancellation, deadline, and accepted unreadable receipt', async () => {
  const url = 'https://example.invalid/events';
  const limited = createHttpConnector({ url, fetchImpl: async () => new Response('busy', { status: 429, headers: { 'retry-after': '2' } }) });
  await assert.rejects(limited.send({}, context), { code: 'HTTP_DELIVERY_ERROR', status: 429, retryable: true, ambiguous: false, retryAfterMs: 2000 });
  const unavailable = createHttpConnector({ url, fetchImpl: async () => new Response('failed', { status: 503 }) });
  await assert.rejects(unavailable.send({}, context), { code: 'HTTP_DELIVERY_ERROR', ambiguous: true });
  const network = createHttpConnector({ url, fetchImpl: async () => { throw new Error('private details'); } });
  await assert.rejects(network.send({}, context), error => error.code === 'NETWORK_ERROR' && error.ambiguous && !error.message.includes('private'));
  let dispatched = false;
  const abort = new AbortController();
  abort.abort();
  const never = createHttpConnector({ url, timeoutMs: 15, fetchImpl: async () => { dispatched = true; return new Promise(() => {}); } });
  await assert.rejects(never.send({}, { ...context, signal: abort.signal }), { code: 'ABORTED', ambiguous: false });
  assert.equal(dispatched, false);
  await assert.rejects(never.send({}, context), { code: 'TIMEOUT', ambiguous: true });
  const oversized = createHttpConnector({ url, maxResponseBytes: 4, captureResponse: true, fetchImpl: async () => new Response('too large', { status: 200 }) });
  assert.deepEqual(await oversized.send({}, context), { status: 'acknowledged', receipt: { httpStatus: 200, responseUnavailable: true } });
  await assert.rejects(network.send({}, { ...context, destination: { url: 'https://other.invalid/' } }), { code: 'CONFIGURATION_ERROR' });
});

test('process connector exchanges JSON with delivery identity and literal arguments without a shell', async t => {
  const script = `let input=''; process.stdin.setEncoding('utf8'); process.stdin.on('data',c=>input+=c); process.stdin.on('end',()=>{
    const event=JSON.parse(input); console.log(JSON.stringify({payload:event.payload,delivery:event.deliveryId,arg:process.argv[1]})); });`;
  const connector = createProcessConnector({ command: process.execPath, args: ['-e', script, '$(not-a-shell);`literal`'], timeoutMs: 10000 });
  t.after(() => connector.close());
  const result = await connector.send({ choice: false }, context);
  assert.deepEqual(result.receipt.output, { payload: { choice: false }, delivery: 'delivery-1', arg: '$(not-a-shell);`literal`' });
  assert.equal(result.completion.status, 'completed');
  const submitted = createProcessConnector({ command: process.execPath, args: ['-e', `process.stdin.resume(); process.stdin.on('end',()=>console.log(JSON.stringify({status:'submitted',receipt:{id:'later'}})))`] });
  t.after(() => submitted.close());
  assert.deepEqual(await submitted.send({}, context), { status: 'submitted', receipt: { id: 'later' } });
});

test('process failures distinguish not-started from ambiguous execution and bound output/time', async t => {
  const directory = await mkdtemp(join(process.cwd(), 'decision-connectors-process-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const missing = createProcessConnector({ command: join(directory, 'missing-executable'), timeoutMs: 500 });
  await assert.rejects(missing.send({}, context), { code: 'PROCESS_START_ERROR', ambiguous: false });
  const cases = [
    { script: `process.stdin.resume(); process.stdin.on('end',()=>process.exit(7));`, code: 'PROCESS_EXIT_ERROR' },
    { script: `process.stdin.resume(); process.stdin.on('end',()=>console.log('not JSON'));`, code: 'INVALID_RESPONSE' },
    { script: `process.stdin.resume(); process.stdin.on('end',()=>console.log(JSON.stringify({status:'acknowledged',unexpected:'dropped data'})));`, code: 'INVALID_RESPONSE' },
    { script: `process.stdout.write('x'.repeat(1000));setInterval(()=>{},100);`, code: 'OUTPUT_TOO_LARGE', options: { maxOutputBytes: 20 } },
    { script: `setInterval(()=>{},100);`, code: 'TIMEOUT', options: { timeoutMs: 40 } },
  ];
  for (const { script, code, options } of cases) {
    const connector = createProcessConnector({ command: process.execPath, args: ['-e', script], timeoutMs: 5000, killGraceMs: 10, ...options });
    t.after(() => connector.close());
    await assert.rejects(connector.send({}, context), { code, ambiguous: true });
  }
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(missing.send({}, { ...context, signal: controller.signal }), { code: 'ABORTED', ambiguous: false });
});
