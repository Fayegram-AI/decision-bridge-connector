import { abortError, assert } from '../util.mjs';

export const SOURCE_ACTIVE = Symbol('source-active');

/** A hosted source may bind during startup, while its runner is fenced until activation. */
export function sourceActivation(runner, signal) {
    const { promise: whenActive, resolve, reject } = Promise.withResolvers();
    void whenActive.catch(() => {});
    let activated = false;
    const active = () => activated && !signal.aborted;
    signal.addEventListener('abort', () => reject(abortError('source-start')), { once: true });
    const guarded = new WeakMap();
    function protect(object, readable = [], overrides = {}, releases = []) {
        if (!guarded.has(object)) guarded.set(object, new Proxy({ ...object, ...overrides }, {
            get(_target, key) {
                const value = Object.hasOwn(overrides, key) ? overrides[key] : object[key];
                if (typeof value !== 'function') return value;
                return (...args) => {
                    assert(active() || readable.includes(key) || activated && signal.aborted && releases.includes(key),
                        'Host is not accepting work.', 'HOST_NOT_ACTIVE');
                    return Reflect.apply(value, object, args);
                };
            },
        }));
        return guarded.get(object);
    }
    const releases = ['closeSnapshot', 'closeImport'];
    const overrides = { ...(runner.store ? { store: protect(runner.store, ['get', 'list', 'inspect', 'getJob'], {}, releases) } : {}),
        ...(runner.aggregators ? { aggregators: Object.freeze(Object.fromEntries(Object.entries(runner.aggregators)
            .map(([name, aggregator]) => [name, protect(aggregator)]))) } : {}) };
    return { active, whenActive, runner: protect(runner, ['get', 'list', 'inspect'], overrides, releases),
        activate() { activated = true; resolve(); } };
}
