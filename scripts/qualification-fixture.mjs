import { createRunner, createCallbackConnector } from '../src/index.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { setTimeout as sleep } from 'node:timers/promises';

export async function qualificationRunner({ backend, path, namespace, send = async () => {}, evaluate = async () => {} }) {
    const limits = { evaluationConcurrency: 4, deliveryConcurrency: 8, optionalConcurrency: 2, perDestination: 4, leaseMs: 5000 };
    const store = backend === 'postgres' ? await createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...limits }) : createSQLiteStore(path, limits);
    const runner = createRunner({ store, namespace, onEvent: event => event.type === 'worker.error' ? send(event) : undefined,
        evaluators: { e: { revision: 'qualification-1', async execute(request) { await evaluate(request); await sleep(5); return request; } } },
        connectors: { out: createCallbackConnector(async (value, context) => { await send({ type: 'delivery', id: context.deliveryId }); await sleep(5); return { status: 'acknowledged' }; }) },
        flows: { load: { revision: '1', evaluator: 'e', inputs: ['request'], concurrency: 4,
            connections: [{ id: 'out', connector: 'out', payload: 'raw-result', when: 'evaluation.succeeded' }] } } });
    await runner.ready;
    return { runner, store };
}
