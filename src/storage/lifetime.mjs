import { ConnectorError } from '../util.mjs';

/** A domain operation may span several short transactions and staged values. */
export function storeLifetime(methods, closeRepository) {
    const pending = new Set(); let closed = false; let closing;
    const wrapped = Object.fromEntries(Object.entries(methods).map(([name, value]) => [name, typeof value !== 'function' ? value : (...args) => {
        if (closed) return Promise.reject(new ConnectorError('STORE_CLOSED', 'Store is closed.'));
        const work = Promise.resolve().then(() => value(...args)); pending.add(work);
        work.then(() => pending.delete(work), () => pending.delete(work)); return work;
    }]));
    return Object.freeze({ ...wrapped, close() {
        if (closing) return closing;
        closed = true;
        closing = (async () => { while (pending.size) await Promise.allSettled([...pending]); await closeRepository(); })();
        return closing;
    } });
}
