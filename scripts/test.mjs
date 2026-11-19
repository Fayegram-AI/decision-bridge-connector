import { readdir } from 'node:fs/promises';
// Import actual cases into one Node test context so the full case count is visible.
for (const name of (await readdir(new URL('../test/', import.meta.url))).filter(name => name.endsWith('.test.mjs')).sort()) {
  await import(new URL(`../test/${name}`, import.meta.url));
}
