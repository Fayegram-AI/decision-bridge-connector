import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, realpath, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { pathToFileURL } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createExtensionLoader } from '../src/node/extension-loader.mjs';
import { createConfiguredRunner } from '../src/node/configuration.mjs';
import { createService } from '../src/node/service.mjs';

async function fixture(operation) {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-graph-'));
    // Loader maps use real paths, including resolved temp aliases and Windows casing.
    try { await operation(await realpath(directory)); } finally { await rm(directory, { recursive: true, force: true }); }
}

test('helper-only changes isolate code, JSON and lazy imports while preserving original file metadata', () => fixture(async directory => {
    const root = pathToFileURL(join(directory, 'entry.mjs')).href;
    await writeFile(join(directory, 'entry.mjs'), `import {value} from './helper.mjs';
import config from './config.json' with {type:'json'};
export {other} from './other.mjs';
export const current=()=>[value,config.version];export const lazy=()=>import('./lazy.mjs');export const location=import.meta.url;`);
    await writeFile(join(directory, 'helper.mjs'), 'export const value=1;');
    await writeFile(join(directory, 'config.json'), '{"version":1}');
    await writeFile(join(directory, 'other.mjs'), "export {value as other} from './helper.mjs';");
    await writeFile(join(directory, 'lazy.mjs'), 'export const value=1;');
    const owner = createExtensionLoader(); const first = await owner.prepare([root]);
    let second;
    try {
        await writeFile(join(directory, 'helper.mjs'), 'export const value=2;');
        await writeFile(join(directory, 'config.json'), '{"version":2}');
        await writeFile(join(directory, 'lazy.mjs'), 'export const value=2;');
        second = await owner.prepare([root]);
        const a = first.modules.get(root); const b = second.modules.get(root);
        assert.notEqual(a.digest, b.digest); assert.deepEqual(a.module.current(), [1, 1]); assert.deepEqual(b.module.current(), [2, 2]);
        assert.equal(a.module.other, 1); assert.equal(b.module.other, 2);
        assert.equal((await a.module.lazy()).value, 1); assert.equal((await b.module.lazy()).value, 2);
        assert.equal(a.module.location, root);
        await assert.rejects(first.verify(), { code: 'EXTENSION_GRAPH_CHANGED' });
    } finally { first.release(); second?.release(); owner.close(); }
}));

test('unchanged graphs reuse modules, cycles resolve and per-root identity excludes unrelated extensions', () => fixture(async directory => {
    const root = pathToFileURL(join(directory, 'entry.mjs')).href; const other = pathToFileURL(join(directory, 'other.mjs')).href;
    await writeFile(join(directory, 'entry.mjs'), "import {helper} from './helper.mjs';export const value=()=>helper();export const base=1;");
    await writeFile(join(directory, 'helper.mjs'), "import {base} from './entry.mjs';export const helper=()=>base;");
    await writeFile(join(directory, 'other.mjs'), 'export const value=1;');
    const owner = createExtensionLoader({ maxExtensionGraphs: 2 }); const a = await owner.prepare([root, other]);
    const b = await owner.prepare([other, root]); let c;
    try {
        assert.equal(a.modules.get(root).module, b.modules.get(root).module); assert.equal(a.modules.get(root).module.value(), 1);
        await writeFile(join(directory, 'other.mjs'), 'export const value=2;'); c = await owner.prepare([root, other]);
        assert.equal(a.modules.get(root).digest, c.modules.get(root).digest);
        await writeFile(join(directory, 'other.mjs'), 'export const value=3;');
        await assert.rejects(owner.prepare([root, other]), { code: 'RESOURCE_LIMIT' });
        assert.equal(a.modules.get(root).module.value(), 1);
    } finally { a.release(); b.release(); c?.release(); owner.close(); }
}));

test('workers with different locale collations share extension identity and admitted work', () => fixture(async directory => {
    await writeFile(join(directory, 'z.mjs'), 'export const z=1;');
    await writeFile(join(directory, 'ä.mjs'), 'export const a=2;');
    await writeFile(join(directory, 'entry.mjs'), `import {z} from './z.mjs';import {a} from './ä.mjs';
export default {apiVersion:1,revision:'1',schema:{},create(){return{validate:x=>x+z+a}}};`);
    const configuration = new URL('../src/node/configuration.mjs', import.meta.url).href;
    // Windows ignores LANG for the default Intl locale. Isolate real collators in
    // child processes so this worker compatibility boundary is covered there too.
    const script = `
        const original = String.prototype.localeCompare;
        const compare = new Intl.Collator(process.env.DBC_COLLATION).compare;
        String.prototype.localeCompare = function(other, locales, options) {
            return locales === undefined && options === undefined ? compare(String(this), String(other))
                : original.call(this, other, locales, options);
        };
        const { createConfiguredRunner } = await import(${JSON.stringify(configuration)});
        const config = { schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } },
            extensions: [{ kind: 'validator', name: 'v', module: './entry.mjs' }],
            validators: { v: { type: 'v' } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
        const runner = await createConfiguredRunner(config, { baseDir: process.env.DBC_FIXTURE });
        try {
            const revision = (await runner.ready).a.runtimeRevision;
            const report = process.env.DBC_TASK_ID
                ? await runner.execute(process.env.DBC_TASK_ID, { waitMs: 2000 })
                : await runner.submit({ flow: 'a', kind: 'result', result: 1 });
            console.log(JSON.stringify({ revision, status: report.status, taskId: report.taskId }));
        } finally { await runner.close(); }
    `;
    async function worker(locale, taskId = '') {
        const { stdout } = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', script], {
            env: { ...process.env, DBC_COLLATION: locale, DBC_FIXTURE: directory, DBC_TASK_ID: taskId }, timeout: 20000,
        });
        return JSON.parse(stdout);
    }
    const admitted = await worker('en-US');
    assert.equal(admitted.status, 'running');
    const completed = await worker('sv-SE', admitted.taskId);
    assert.equal(completed.revision, admitted.revision);
    assert.equal(completed.status, 'completed');
}));

test('BOM and hashbang modules retain import.meta identity and original file URLs', () => fixture(async directory => {
    const root = pathToFileURL(join(directory, 'entry.mjs')).href;
    await writeFile(join(directory, 'entry.mjs'), '\uFEFF#!/usr/bin/env node\nimport config from "./config.json" with {type:"json"};\nconst Object=null;import.meta.marker=42;export const same=import.meta===import.meta;export const marker=import.meta.marker;export const value=config.value;');
    await writeFile(join(directory, 'config.json'), '\uFEFF{"value":42}');
    const owner = createExtensionLoader(); let captured;
    try {
        captured = await owner.prepare([root]); const module = captured.modules.get(root).module;
        assert.equal(module.same, true); assert.equal(module.marker, 42); assert.equal(module.value, 42);
    } finally { captured?.release(); owner.close(); }
}));

test('unsupported dependency graphs fail before an extension factory or listener starts', () => fixture(async directory => {
    const key = `graph_${crypto.randomUUID()}`; globalThis[key] = 0;
    const config = { schemaVersion: 3, extensions: [{ kind: 'validator', name: 'v', module: './entry.mjs' }],
        validators: { v: { type: 'v' } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
    try {
        await writeFile(join(directory, 'entry.mjs'), `globalThis[${JSON.stringify(key)}]++;export default {apiVersion:1,revision:'1',schema:{},create(){}};
export const lazy=name=>import(name);`);
        await assert.rejects(createConfiguredRunner(config, { baseDir: directory }), { code: 'EXTENSION_MODULE_UNSUPPORTED' });
        assert.equal(globalThis[key], 0);
    } finally { delete globalThis[key]; }
}));

test('released preparations fence verification without releasing another owner of the same graph', () => fixture(async directory => {
    const root = pathToFileURL(join(directory, 'entry.mjs')).href;
    await writeFile(join(directory, 'entry.mjs'), "export const lazy=()=>import('./helper.mjs');");
    await writeFile(join(directory, 'helper.mjs'), 'export const value=42;');
    const loader = createExtensionLoader(); const first = await loader.prepare([root]); const second = await loader.prepare([root]);
    try {
        first.release(); await assert.rejects(async () => first.verify(), { code: 'EXTENSION_GENERATION_CLOSED' });
        assert.equal((await second.modules.get(root).module.lazy()).value, 42); await second.verify();
    } finally { first.release(); second.release(); loader.close(); }
}));

test('configured extensions preserve valid long revisions and reject invalid declarations', () => fixture(async directory => {
    const config = { schemaVersion: 3, extensions: [{ kind: 'validator', name: 'v', module: './entry.mjs' }],
        validators: { v: { type: 'v' } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
    for (const length of [191, 192, 256, 257]) {
        await writeFile(join(directory, 'entry.mjs'), `export default {apiVersion:1,revision:'${'r'.repeat(length)}',schema:{},create(){return{validate:x=>x};}};`);
        if (length > 256) { await assert.rejects(createConfiguredRunner(config, { baseDir: directory }), { code: 'CONFIGURATION_ERROR' }); continue; }
        const runner = await createConfiguredRunner(config, { baseDir: directory });
        try {
            const revision = runner.registry.get('validator', 'v').revision;
            assert(revision.length <= 256); assert.equal((await runner.run({ flow: 'a', kind: 'result', result: 1 })).status, 'completed');
            if (length === 191) assert(revision.startsWith('r'.repeat(length) + ':'));
        } finally { await runner.close(); }
    }
}));

test('service helper reload retains old worker code and a failed graph leaves current admission available', () => fixture(async directory => {
    await writeFile(join(directory, 'entry.mjs'), `import {value} from './helper.mjs';export default {apiVersion:1,revision:'1',schema:{},
create(){return {execute:()=>value}}};`);
    await writeFile(join(directory, 'helper.mjs'), 'export const value=1;');
    const config = { schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } },
        extensions: [{ kind: 'evaluator', name: 'local', module: './entry.mjs' }], evaluators: { e: { type: 'local' } },
        flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e' } }, host: { role: 'ingress' } };
    const service = await createService(config, { baseDir: directory });
    try {
        const old = service.runner; const admitted = await old.submit({ flow: 'a', kind: 'request', request: 0 });
        await writeFile(join(directory, 'helper.mjs'), 'export const value=2;');
        await service.reload(config);
        // Ingress-only old generations remain usable until health explicitly reaps them.
        await old.execute(admitted.taskId);
        const prior = (await old.store.get(admitted.taskId, old.namespace)).task.result; assert.equal(prior, 1);
        const next = await service.runner.run({ flow: 'a', kind: 'request', request: 0 });
        assert.equal((await service.runner.store.get(next.taskId, service.runner.namespace)).task.result, 2);
        await writeFile(join(directory, 'helper.mjs'), "export const value=import(unknownName);");
        await assert.rejects(service.reload(config), { code: 'EXTENSION_MODULE_UNSUPPORTED' });
        const retained = await service.runner.run({ flow: 'a', kind: 'request', request: 0 });
        assert.equal((await service.runner.store.get(retained.taskId, service.runner.namespace)).task.result, 2);
    } finally { await service.close(); }
}));
