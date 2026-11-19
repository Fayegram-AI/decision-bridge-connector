import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { Readable, Writable } from 'node:stream';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createOutput, jsonLines, orderedMap, processSource, readJson } from '../src/node/io.mjs';

const nextTurn = () => new Promise(resolve => setImmediate(resolve));
async function boundedTest(promise, ms = 1000) {
  let timer;
  try { return await Promise.race([promise, new Promise((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error('I/O did not settle within the test bound.')), ms);
  })]); } finally { clearTimeout(timer); }
}

test('ordered streaming emits one completed task while input stays open and cancels an idle producer', async () => {
  const controller = new AbortController();
  let reads = 0; let returned = false;
  const source = { [Symbol.asyncIterator]() { return this; },
    next() { return ++reads === 1 ? Promise.resolve({ done: false, value: 'one' }) : new Promise(() => {}); },
    return() { returned = true; return new Promise(() => {}); } };
  const output = orderedMap(source, value => `${value}-complete`, 4, { signal: controller.signal });
  assert.deepEqual(await boundedTest(output.next()), { done: false, value: 'one-complete' });
  const pending = output.next();
  const cancelled = assert.rejects(boundedTest(pending), { code: 'ABORTED' });
  controller.abort();
  await cancelled;
  await nextTurn();
  assert.equal(returned, true);
});

test('ordered streaming bounds the admission window and retains input order', async () => {
  let reads = 0; let release;
  const first = new Promise(resolve => { release = resolve; });
  const source = { [Symbol.asyncIterator]() { return this; }, next() {
    return Promise.resolve(reads < 4 ? { done: false, value: reads++ } : { done: true });
  } };
  const output = orderedMap(source, async value => { if (value === 0) await first; return value; }, 2);
  const pending = output.next();
  await nextTurn();
  assert.equal(reads, 2);
  release();
  const values = [(await pending).value];
  for await (const value of output) values.push(value);
  assert.deepEqual(values, [0, 1, 2, 3]);
});

test('ordered cancellation does not await an uncooperative transform and observes late rejection', async () => {
  const controller = new AbortController(); let rejectLater;
  const output = orderedMap([1, 2], value => value === 1 ? new Promise(() => {}) :
    new Promise((_resolve, reject) => { rejectLater = reject; }), 2, { signal: controller.signal });
  const pending = output.next();
  await nextTurn();
  const cancelled = assert.rejects(boundedTest(pending), { code: 'ABORTED' });
  controller.abort();
  await cancelled;
  rejectLater(new Error('late transform rejection'));
  await nextTurn();
});

test('one-shot JSON cancellation bounds an uncooperative input and prevents pre-aborted reads', async () => {
  const controller = new AbortController(); let reads = 0; let destroyed = 0;
  const input = { [Symbol.asyncIterator]() { return this; }, next() { reads++; return new Promise(() => {}); },
    return() { return new Promise(() => {}); }, destroy() { destroyed++; } };
  const pending = readJson('-', { stdin: input, signal: controller.signal });
  await nextTurn();
  const cancelled = assert.rejects(boundedTest(pending), { code: 'ABORTED' });
  controller.abort();
  await cancelled;
  assert.equal(reads, 1);
  assert(destroyed > 0);
  await assert.rejects(readJson('-', { stdin: input, signal: controller.signal }), { code: 'ABORTED' });
  assert.equal(reads, 1);
});

test('blocked stdout cancels promptly and its later pipe error is handled', async () => {
  const controller = new AbortController();
  const stream = new EventEmitter(); let callback; let destroyed = false;
  stream.write = (_text, done) => { callback = done; return false; };
  stream.destroy = () => { destroyed = true; };
  const writer = await createOutput('-', { stdout: stream, signal: controller.signal });
  const pending = writer.write({ report: true });
  const cancelled = assert.rejects(boundedTest(pending), { code: 'ABORTED', ambiguous: true });
  controller.abort();
  await cancelled;
  assert.equal(destroyed, true);
  const failure = new Error('late EPIPE');
  callback(failure); stream.emit('error', failure);
  await nextTurn();
});

test('real Writable errors become explicit output failures without unhandled errors', async () => {
  const stream = new Writable({ write(_chunk, _encoding, done) { queueMicrotask(() => done(new Error('broken pipe'))); } });
  const writer = await createOutput('-', { stdout: stream });
  await assert.rejects(writer.write({ result: 1 }), { code: 'OUTPUT_ERROR', ambiguous: true });
  await nextTurn();
});

test('file output checks cancellation before creation and publication, retaining prior output', async t => {
  const directory = await mkdtemp(join(process.cwd(), 'decision-connectors-io-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const controller = new AbortController(); controller.abort();
  await assert.rejects(createOutput(join(directory, 'not-created', 'output.json'), { signal: controller.signal }), { code: 'ABORTED' });
  assert.deepEqual(await readdir(directory), []);
  const existing = join(directory, 'existing.json'); await writeFile(existing, 'keep');
  await assert.rejects(createOutput(existing), { code: 'OUTPUT_ERROR' });
  assert.equal(await readFile(existing, 'utf8'), 'keep');
  const live = new AbortController();
  const writer = await createOutput(join(directory, 'pending.json'), { signal: live.signal });
  await writer.write({ result: 1 }); live.abort();
  await assert.rejects(writer.finish(), { code: 'ABORTED' });
  await writer.abort();
  assert.deepEqual(await readdir(directory), ['existing.json']);
});

test('JSONL framing handles split CRLF, per-row errors, and final byte/UTF-8 bounds', async () => {
  const rows = [];
  for await (const row of jsonLines(Readable.from([Buffer.from('{"a":1}\r'), Buffer.from('\nnope\n{"b":'), Buffer.from('2}')]), { maxBytes: 7 })) rows.push(row);
  assert.deepEqual(rows.map(row => [row.line, row.error?.code ?? row.value]), [[1, { a: 1 }], [2, 'INVALID_JSON'], [3, { b: 2 }]]);
  await assert.rejects(async () => { for await (const _row of jsonLines(Readable.from(['{"a":1}\r']), { maxBytes: 7 })) {} }, { code: 'INPUT_TOO_LARGE' });
  await assert.rejects(async () => { for await (const _row of jsonLines(Readable.from([Buffer.from([0xc3])]))) {} }, { code: 'INVALID_UTF8' });
});

test('incoming processes reject pre-aborted dispatch and invalid output limits', () => {
  const controller = new AbortController(); controller.abort();
  assert.throws(() => processSource({ command: 'unused' }, controller.signal), { code: 'ABORTED' });
  assert.throws(() => processSource({ command: 'unused', maxErrorBytes: 0 }), { code: 'CONFIGURATION_ERROR' });
  assert.throws(() => processSource({ command: 'unused', maxOutputBytes: NaN }), { code: 'CONFIGURATION_ERROR' });
});

test('incoming process output is retained until consumed and excessive output is bounded', async t => {
  const source = processSource({ command: process.execPath, args: ['-e', 'console.log(JSON.stringify({id:1}));'], timeoutMs: 5000 });
  t.after(() => source.close());
  await source.done;
  const rows = [];
  for await (const row of jsonLines(source.stream)) rows.push(row);
  assert.deepEqual(rows, [{ line: 1, value: { id: 1 } }]);
  const oversized = processSource({ command: process.execPath, args: ['-e', 'process.stdout.write("x".repeat(1000));'], maxOutputBytes: 20, timeoutMs: 5000 });
  t.after(() => oversized.close());
  await Promise.all([
    assert.rejects(oversized.done, { code: 'OUTPUT_LIMIT' }),
    assert.rejects(async () => { for await (const _chunk of oversized.stream) {} }, { code: 'OUTPUT_LIMIT' }),
  ]);
});
