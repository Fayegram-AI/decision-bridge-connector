import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createRegistry } from '../src/engine/registry.mjs';
import { createConfiguredRunner } from '../src/node/configuration.mjs';

// Factory metadata and creation methods can live on the prototype and use private state.
const definition = `class Factory {
    #created = 0;
    get apiVersion() { return 1; }
    get revision() { return 'class-v1'; }
    get runtime() { return 'node'; }
    get schema() { return { type: 'object' }; }
    create() {
        const created = ++this.#created;
        return { validate: value => ({ ...value, created }) };
    }
}`;
const config = { schemaVersion: 3, validators: { v: { type: 'class-validator' } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };

for (const mode of ['programmatic', 'module']) test(`${mode} factories preserve prototype metadata and their original receiver`, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-factory-'));
    let runner;
    try {
        const registry = createRegistry();
        let input = config;
        if (mode === 'programmatic') {
            const factory = new (Function(`${definition}; return Factory;`)())();
            registry.register('validator', 'class-validator', factory);
            assert.equal(Object.isFrozen(factory), false);
        } else {
            await writeFile(join(directory, 'factory.mjs'), `${definition}\nexport default new Factory();`);
            input = { ...config, extensions: [{ kind: 'validator', name: 'class-validator', module: './factory.mjs' }] };
        }
        runner = await createConfiguredRunner(input, { registry, baseDir: directory });
        const entry = registry.get('validator', 'class-validator');
        assert.equal(entry.apiVersion, 1);
        assert.equal(entry.runtime, 'node');
        assert.deepEqual(entry.schema, { type: 'object' });
        assert(mode === 'module' ? entry.revision.startsWith('class-v1:') : entry.revision === 'class-v1');
        const report = await runner.run({ flow: 'a', kind: 'result', result: { value: 42 } });
        assert.equal(report.status, 'completed');
        const { task } = await runner.store.get(report.taskId, runner.namespace);
        assert.deepEqual(task.result, { value: 42, created: 1 });
        // A second invocation must still use the factory's own mutable private state.
        assert.deepEqual(entry.create({}).validate({}), { created: 2 });
    } finally { await runner?.close(); await rm(directory, { recursive: true, force: true }); }
});
