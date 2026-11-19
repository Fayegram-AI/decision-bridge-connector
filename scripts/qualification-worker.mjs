import { qualificationRunner } from './qualification-fixture.mjs';
import { setTimeout as sleep } from 'node:timers/promises';

const [backend, path, namespace, crash = 'false'] = process.argv.slice(2);
const { runner, store } = await qualificationRunner({ backend, path, namespace,
    send: value => process.send?.(value),
    evaluate: async value => {
        process.send?.({ type: 'evaluation', value });
        if (crash === 'true') await new Promise(() => {});
    } });
let stop = false;
process.on('message', value => { if (value === 'stop') stop = true; });
process.send?.({ type: 'ready' });
try {
    while (!stop) { await runner.tick(); await sleep(5); }
    await runner.close();
    process.send?.({ type: 'memory', rss: process.memoryUsage().rss });
} finally { await store.close(); process.disconnect?.(); }
