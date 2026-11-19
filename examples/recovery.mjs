// Inspect and recover expired leases using the same deployment configuration.
import { loadConfig, createConfiguredRunner } from '@fayegram-ai/decision-bridge-connectors/node';

const path = process.argv[2];
if (!path) throw new Error('Usage: node recovery.mjs configuration.json');
const { config, baseDir } = await loadConfig(path);
const runner = await createConfiguredRunner(config, { baseDir });
try {
    const recovered = await runner.recover();
    console.log(JSON.stringify({ recovered, queue: await runner.inspect(), active: await runner.list({ state: 'active', limit: 25 }) }, null, 4));
} finally { await runner.close(); }
