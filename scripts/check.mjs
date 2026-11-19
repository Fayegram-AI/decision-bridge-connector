import { readdir, readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { VERSION } from '../src/contracts.mjs';

async function files(directory) {
  const entries = await readdir(directory, { withFileTypes: true }); const result = [];
  for (const entry of entries) {
    const path = `${directory}/${entry.name}`;
    if (entry.isDirectory()) result.push(...await files(path)); else result.push(path);
  }
  return result;
}
const manifest = JSON.parse(await readFile('package.json', 'utf8'));
assert.equal(manifest.version, VERSION);
const lock = JSON.parse(await readFile('package-lock.json', 'utf8'));
assert.equal(lock.version, VERSION);
assert.equal(lock.packages[''].version, VERSION);
assert.equal(lock.packages[''].engines.node, manifest.engines.node, 'Node runtime requirements differ from the lockfile.');
const coreVersion = manifest.dependencies['@fayegram-ai/decision-bridge'];
assert.equal(coreVersion, '0.1.8', 'Connector requires the qualified Core SDK version.');
assert.equal(lock.packages[''].dependencies['@fayegram-ai/decision-bridge'], coreVersion);
assert.equal(lock.packages['node_modules/@fayegram-ai/decision-bridge'].version, coreVersion);
assert.equal(JSON.parse(await readFile('node_modules/@fayegram-ai/decision-bridge/package.json', 'utf8')).version, coreVersion);
const declarations = await readFile('src/index.d.mts', 'utf8');
assert(declarations.includes(`export const VERSION: '${VERSION}';`), 'Public version declaration differs from the release.');
for (const file of (await Promise.all(['src', 'bin', 'scripts', 'test', 'examples', 'schemas'].map(files))).flat()) {
  if (file.endsWith('.mjs')) {
    const check = spawnSync(process.execPath, ['--check', file], { encoding: 'utf8', timeout: 10000 });
    assert.equal(check.status, 0, `${file}: ${check.stderr}`);
    const source = await readFile(file, 'utf8');
    assert(!/@fayegram-ai\/decision-bridge\/(src|bin)\//.test(source), `Private Bridge import: ${file}`);
  } else if (file.endsWith('.json')) JSON.parse(await readFile(file, 'utf8'));
}
console.log('Syntax, JSON, version and public-import checks passed.');
