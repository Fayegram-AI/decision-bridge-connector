import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner } from '../src/index.mjs';
import { createProcessConnector } from '../src/node/connectors.mjs';

function nested() { let value = 1; for (let depth = 0; depth < 100; depth++) value = { value }; return value; }
const onInput = script => `process.stdin.resume();process.stdin.on('end',()=>{${script}});`;

test('a successful process reply survives early stdin closure before a large body drains', async () => {
    const connector = createProcessConnector({ command: process.execPath, inputMode: 'payload',
        args: ['-e', 'process.stdout.write(JSON.stringify({status:"acknowledged",receipt:{accepted:true}}))'] });
    const accepted = [];
    try {
        const result = await connector.send('x'.repeat(4 * 1024 * 1024), { onAcceptance: value => accepted.push(value) });
        assert.deepEqual(result, { status: 'acknowledged', receipt: { accepted: true } });
        assert.deepEqual(accepted, [{ status: 'acknowledged' }]);
    } finally { await connector.close(); }
});

for (const [reply, code] of [['not-json', 0], ['{"status":"acknowledged"}', 1]])
    test(`early stdin closure cannot establish acceptance for ${code ? 'failed exits' : 'invalid replies'}`, async () => {
        const connector = createProcessConnector({ command: process.execPath, inputMode: 'payload',
            args: ['-e', `process.stdout.write(${JSON.stringify(reply)});process.exitCode=${code};`] });
        let accepted = false;
        try {
            await assert.rejects(connector.send('x'.repeat(4 * 1024 * 1024), { onAcceptance() { accepted = true; } }),
                error => error.ambiguous === true);
            assert.equal(accepted, false);
        } finally { await connector.close(); }
    });

test('a stalled process acceptance hook cannot turn a validated reply into an ambiguous timeout', async () => {
    const script = onInput('process.stdout.write(JSON.stringify({answer:1}));');
    const connector = createProcessConnector({ command: process.execPath, args: ['-e', script], timeoutMs: 1500 });
    let checkpoint;
    try {
        const receipt = await connector.send(1, { onAcceptance(value) {
            checkpoint = value;
            return new Promise(() => {});
        } });
        assert.equal(checkpoint?.status, 'acknowledged');
        assert.deepEqual(receipt, { status: 'acknowledged', completion: { status: 'completed', scope: 'process' },
            receipt: { responseUnavailable: true } });
    } finally { await connector.close(); }
});

test('default process envelopes preserve a boundary-depth application payload', async () => {
    const script = `let input='';process.stdin.on('data',c=>input+=c);process.stdin.on('end',()=>{let v=JSON.parse(input).payload,n=0;while(v&&typeof v==='object'){v=v.value;n++;}process.stdout.write(JSON.stringify(n));});`;
    const connector = createProcessConnector({ command: process.execPath, args: ['-e', script] });
    try {
        const receipt = await connector.send(nested());
        assert.equal(receipt.status, 'acknowledged'); assert.equal(receipt.receipt.response, 100);
    } finally { await connector.close(); }
});

test('structured process receipts preserve independent receipt-body depth', async () => {
    const script = onInput(`let value=1;for(let i=0;i<100;i++)value={value};process.stdout.write(JSON.stringify({status:'acknowledged',receipt:value}));`);
    const connector = createProcessConnector({ command: process.execPath, args: ['-e', script] });
    try { assert.deepEqual((await connector.send(1)).receipt, nested()); }
    finally { await connector.close(); }
});

async function runProcess(script) {
    const connector = createProcessConnector({ command: process.execPath, args: ['-e', onInput(script)] });
    const runner = createRunner({ placements: ['local'], validators: { v: { revision: '1', validate: value => value } },
        connectors: { p: connector }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'p', placement: 'local', when: 'always', payload: 'raw-result' },
        ] } } });
    try {
        const report = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        const job = await runner.store.getJob(runner.namespace, `${report.taskId}:delivery:out`);
        assert.equal((await runner.execute(report.taskId)).taskId, report.taskId);
        return { report, job };
    } finally { await runner.close(); await connector.close(); }
}

test('confirmed process completion survives a receipt that exceeds the store capture budget', async () => {
    const { report, job } = await runProcess(`process.stdout.write(JSON.stringify('x'.repeat(600000)));`);
    assert.equal(report.status, 'completed'); assert.equal(job.state, 'acknowledged');
    assert.equal(job.receipt.receipt.responseUnavailable, true); assert.equal(job.completion.status, 'completed');
    assert.equal(job.attempts.length, 1);
});

test('process checkpointing never promotes unknown or malformed protocol replies', async () => {
    for (const output of ['{"status":"unknown"}', '{invalid']) {
        const { report, job } = await runProcess(`process.stdout.write(${JSON.stringify(output)});`);
        assert.equal(report.status, 'unknown'); assert.equal(job.state, 'unknown'); assert.equal(job.attempts.length, 1);
    }
});
