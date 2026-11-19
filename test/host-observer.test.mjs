import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

test('rejected async host error observers cannot terminate the worker process', { timeout: 10000 }, async () => {
    const hostModule = new URL('../src/node/host.mjs', import.meta.url).href;
    const script = `
        import { createHost } from ${JSON.stringify(hostModule)};
        import { setTimeout as sleep } from 'node:timers/promises';
        let ticks = 0;
        const runner = { ready: Promise.resolve({}), async recover() {}, async maintain() {}, async close() {},
            async inspect() { return { ticks }; }, async tick() { if (++ticks < 3) throw new Error('Recoverable worker error'); } };
        const host = await createHost({ runner, pollMs: 1, onError: async () => { throw new Error('Observer failed'); } });
        try { while (ticks < 3) await sleep(5); console.log(JSON.stringify(await host.health())); }
        finally { await host.close(); }
    `;
    const result = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', script], { encoding: 'utf8', timeout: 5000 });
    const health = JSON.parse(result.stdout);
    assert(health.ticks >= 3); assert.equal(health.errors.length, 2);
    assert.equal(result.stderr.includes('Observer failed'), false);
});
