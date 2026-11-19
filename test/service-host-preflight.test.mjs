import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createService } from '../src/node/service.mjs';

test('reload rejects invalid host settings and source contracts before stopping healthy ingress', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-host-preflight-'));
    const key = `host-preflight-${crypto.randomUUID()}`;
    const state = { active: 0, starts: 0, closes: 0 };
    globalThis[key] = state;
    let service;
    try {
        await writeFile(join(directory, 'source.mjs'), `const state = globalThis[${JSON.stringify(key)}];
export default { apiVersion: 1, revision: '1', schema: { type: 'object' }, create(options) {
    if (options.invalid) return {};
    let active = false;
    return { start() { active = true; state.active++; state.starts++; },
        close() { if (active) { active = false; state.active--; state.closes++; } } };
} };`);
        const config = { schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } },
            host: { role: 'ingress' }, extensions: [{ kind: 'incoming', name: 'tracked', module: './source.mjs' }],
            incoming: { source: { type: 'tracked' } }, validators: { v: { type: 'schema', options: { schema: {} } } },
            flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
        service = await createService(config, { baseDir: directory });
        const runner = service.runner;
        for (const host of [{ role: 'invalid' }, { pollMs: 0 }, { maintenanceMs: -1 }, { drainMs: 2147483648 }]) {
            await assert.rejects(service.reload({ ...config, host: { ...config.host, ...host } }), { code: 'CONFIGURATION_ERROR' });
            assert.equal(service.runner, runner);
            assert.equal((await service.health()).ready, true);
            assert.deepEqual(state, { active: 1, starts: 1, closes: 0 });
        }
        await assert.rejects(service.reload({ ...config, incoming: { source: { type: 'tracked', options: { invalid: true } } } }),
            { code: 'CONFIGURATION_ERROR' });
        assert.equal(service.runner, runner);
        assert.equal((await service.health()).ready, true);
        assert.deepEqual(state, { active: 1, starts: 1, closes: 0 });
        // Rejection must leave the service able to perform a later valid cutover.
        await service.reload(config);
        assert.notEqual(service.runner, runner);
        assert.equal((await service.health()).ready, true);
        assert.deepEqual(state, { active: 1, starts: 2, closes: 1 });
    } finally {
        await service?.close();
        delete globalThis[key];
        await rm(directory, { recursive: true, force: true });
    }
    assert.deepEqual(state, { active: 0, starts: 2, closes: 2 });
});
