import { mkdtemp, readFile, writeFile, mkdir, cp, rm, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { spawnSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { npmCommand } from './npm-command.mjs';
import { coreScenario } from './package-core-scenario.mjs';

const root = process.cwd();
const temporary = await mkdtemp(join(tmpdir(), 'dbc-package-'));
const original = await Promise.all(['package.json', 'package-lock.json'].map(file => readFile(file, 'utf8')));
const expected = JSON.parse(original[0]);
const npm = 'npm';
function command(file, args, cwd = root) {
    const invocation = file === npm ? npmCommand(args) : { command: file, args };
    const result = spawnSync(invocation.command, invocation.args, { cwd, encoding: 'utf8', timeout: 120000, maxBuffer: 16 * 1024 * 1024,
        env: { ...process.env, 'npm_config_@fayegram-ai:registry': 'https://registry.npmjs.org/' } });
    assert.equal(result.status, 0, `${file}: ${result.stdout}\n${result.stderr}`); return result.stdout;
}
async function inventory(directory, prefix = '') {
    const files = [];
    for (const entry of await readdir(directory, { withFileTypes: true })) {
        const name = `${prefix}${entry.name}`;
        if (entry.isDirectory()) files.push(...await inventory(join(directory, entry.name), `${name}/`));
        else files.push(name);
    }
    return files.sort();
}
try {
    const explicit = process.argv.indexOf('--tarball'); let archive;
    if (explicit >= 0) archive = resolve(process.argv[explicit + 1]);
    else {
        const parsed = JSON.parse(command(npm, ['pack', '--ignore-scripts', '--json', '--pack-destination', temporary]));
        const packages = Array.isArray(parsed) ? parsed : Object.values(parsed);
        assert.equal(packages.length, 1);
        for (const file of packages[0].files) assert(!/^(test|scripts|node_modules|release|verification)\/|REVIEW\.md|VERIFICATION\.md|AGENTS\.md/.test(file.path), `Unexpected package file: ${file.path}`);
        archive = join(temporary, packages[0].filename);
    }
    const consumer = join(temporary, 'consumer'); await mkdir(consumer);
    await writeFile(join(consumer, 'package.json'), JSON.stringify({ name: 'connector-consumer', private: true, type: 'module' }));
    const coreArgument = process.argv.indexOf('--core-tarball');
    const coreArchive = coreArgument >= 0 ? resolve(process.argv[coreArgument + 1]) : undefined;
    const cacheMode = process.argv.includes('--offline') ? '--offline' : '--prefer-offline';
    command(npm, ['install', cacheMode, '--ignore-scripts', '--omit=dev', '--omit=optional', '--no-audit', '--no-fund',
        ...(coreArchive ? [coreArchive] : []), archive], consumer);
    const installed = join(consumer, 'node_modules', expected.name);
    const manifest = JSON.parse(await readFile(join(installed, 'package.json'), 'utf8'));
    assert.equal(manifest.name, expected.name, 'Installed package name differs from the checkout.');
    assert.equal(manifest.version, expected.version, 'Installed package version differs from the checkout.');
    const installedCore = JSON.parse(await readFile(join(consumer, 'node_modules/@fayegram-ai/decision-bridge/package.json'), 'utf8'));
    assert.equal(installedCore.version, expected.dependencies['@fayegram-ai/decision-bridge'], 'Incorrect core dependency installed.');
    await writeFile(join(consumer, 'core-boundary.mjs'), coreScenario);
    console.log(command(process.execPath, ['core-boundary.mjs'], consumer).trim());
    // Resolve public imports from the fresh installation. Old records stay outside the package.
    const upgradeTest = 'cloudflare-extraction-upgrade.test.mjs';
    await cp(join(root, 'test', upgradeTest), join(consumer, upgradeTest));
    await mkdir(join(consumer, 'fixtures'));
    for (const file of ['clairic-core-0.1.8-queue.json', 'clairic-core-0.1.8.ndjson']) {
        await cp(join(root, 'test', 'fixtures', file), join(consumer, 'fixtures', file));
    }
    console.log(command(process.execPath, ['--test', '--test-reporter=spec', upgradeTest], consumer).trim());
    // Inspect installed bytes even when an explicit archive bypasses the initial pack step.
    const shipped = await inventory(installed);
    const examplesAllowed = new Set(manifest.files.filter(file => file.startsWith('examples/')));
    for (const file of shipped) assert(/^(src\/.*\.(mjs|d\.mts)|bin\/[^/]+\.mjs|schemas\/[^/]+\.json|docs\/[^/]+\.md)$/.test(file) ||
        examplesAllowed.has(file) || ['README.md', 'LICENSE', 'CHANGELOG.md', 'package.json'].includes(file), `Unexpected package file: ${file}`);
    for (const entry of Object.keys(manifest.exports).filter(key => !key.endsWith('.json'))) {
        const specifier = manifest.name + (entry === '.' ? '' : entry.slice(1));
        command(process.execPath, ['--input-type=module', '-e', `await import(${JSON.stringify(specifier)});`], consumer);
    }
    const examples = join(consumer, 'examples'); await cp(join(installed, 'examples'), examples, { recursive: true });
    const bin = join(installed, 'bin/decision-bridge-connectors.mjs'); const config = join(examples, 'offline.config.json');
    assert.equal(command(process.execPath, [bin, '--version'], consumer).trim(), manifest.version);
    command(process.execPath, [bin, 'config', 'inspect', '--config', config], consumer);
    command(process.execPath, [bin, 'config', 'inspect', '--config', join(examples, 'gemini.config.json')], consumer);
    command(process.execPath, [bin, 'preview', '--input', join(examples, 'task.json'), '--config', config], consumer);
    const report = JSON.parse(command(process.execPath, [bin, 'run', '--input', join(examples, 'task.json'), '--config', config], consumer));
    assert.equal(report.status, 'completed');
    const finalArgs = ['--reply', 'final', '--policy', 'action'];
    const review = JSON.parse(command(process.execPath, [bin, 'status', report.taskId, ...finalArgs, '--config', config], consumer));
    assert.equal(review.status, 'review');
    const existing = JSON.parse(command(process.execPath, [bin, 'run', '--input', join(examples, 'decision-task.json'), '--config', config], consumer));
    const accepted = JSON.parse(command(process.execPath, [bin, 'status', existing.taskId, ...finalArgs, '--config', config], consumer));
    assert.equal(accepted.status, 'accepted');
    const batch = command(process.execPath, [bin, 'batch', '--input', join(examples, 'events.jsonl'), '--config', config], consumer);
    assert.equal(batch.trim().split('\n').length, 2);
    const submitInput = join(consumer, 'submit.json');
    const submission = JSON.parse(await readFile(join(examples, 'task.json'), 'utf8'));
    await writeFile(submitInput, JSON.stringify({ ...submission, idempotencyKey: 'installed-submit' }));
    const admitted = JSON.parse(command(process.execPath, [bin, 'submit', '--input', submitInput, '--config', config], consumer));
    assert.equal(admitted.status, 'running');
    const saved = JSON.parse(command(process.execPath, [bin, 'status', admitted.taskId, '--config', config], consumer));
    assert.equal(saved.taskId, admitted.taskId); assert.equal(saved.status, 'running');
    // Repack a used example tree, including local settings that must never ship.
    await cp(examples, join(installed, 'examples'), { recursive: true });
    await writeFile(join(installed, 'examples/.env'), 'DBC_PACKAGE_FIXTURE=local-only\n');
    await writeFile(join(installed, 'examples/local-settings.json'), '{"fixture":"local-only"}');
    // Explicit package directories must still exclude private environment variants.
    for (const name of ['.env.local', 'src/.env.local', 'src/node/.env.production',
        'bin/.env.test.mjs', 'docs/.env.local.md', 'schemas/.env.local.json'])
        await writeFile(join(installed, name), 'DBC_PACKAGE_FIXTURE=local-only\n');
    const repacked = JSON.parse(command(npm, ['pack', '--dry-run', '--ignore-scripts', '--json'], installed));
    const repack = (Array.isArray(repacked) ? repacked : Object.values(repacked))[0];
    assert.deepEqual(repack.files.map(file => file.path).sort(), shipped, 'Example execution or local settings changed package contents.');
    console.log('Installed package: exports, optional isolation, CLI submit persistence, existing results, policies, files, JSONL and used-checkout package boundaries passed.');
} finally {
    await rm(temporary, { recursive: true, force: true });
    for (const [index, file] of ['package.json', 'package-lock.json'].entries()) assert.equal(await readFile(file, 'utf8'), original[index]);
}
