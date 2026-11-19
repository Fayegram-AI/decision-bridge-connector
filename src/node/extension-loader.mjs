import { registerHooks } from 'node:module';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { abortError, assert, bounded, id, validateSignal } from '../util.mjs';
import { captureExtensionGraph } from './extension-graph.mjs';

export const EXTENSION_LOADER = Symbol('extension-loader');
const graphs = new Map(); let hooks;
const prefix = 'dbc-extension:';

function sourceFor(node) {
    if (node.format === 'json') return node.source;
    const source = node.source.replace(/^\uFEFF/, '');
    if (!node.imports.some(item => item.type === 'import-meta')) return source;
    // Preserve import.meta object identity and keep a leading BOM/hashbang valid.
    const header = source.match(/^(?:#![^\n]*(?:\n|$))?/)?.[0] ?? '';
    const metadata = { url: node.url, filename: fileURLToPath(node.url), dirname: dirname(fileURLToPath(node.url)) };
    const initialize = Object.entries(metadata).map(([key, value]) => `import.meta.${key}=${JSON.stringify(value)};`).join('');
    return `${header}${initialize}\n${source.slice(header.length)}`;
}

function moduleURL(graph, original) { return `${prefix}${graph.id}/${encodeURIComponent(original)}`; }
function selected(url) {
    if (!url?.startsWith(prefix)) return null;
    const slash = url.indexOf('/', prefix.length); const graph = graphs.get(url.slice(prefix.length, slash));
    assert(graph, 'Extension generation is closed.', 'EXTENSION_GENERATION_CLOSED');
    const node = graph.nodes.get(decodeURIComponent(url.slice(slash + 1)));
    assert(node, 'Extension module is outside its captured graph.', 'EXTENSION_MODULE_UNSUPPORTED');
    return { graph, node };
}

function register(graph) {
    graphs.set(graph.id, graph);
    hooks ??= registerHooks({
        resolve(specifier, context, nextResolve) {
            if (specifier.startsWith(prefix)) { const value = selected(specifier); return { url: specifier, format: value.node.format, shortCircuit: true }; }
            const parent = selected(context.parentURL);
            if (!parent) return nextResolve(specifier, context);
            const edge = parent.node.edges.get(specifier);
            assert(edge, 'Import is outside the captured extension graph.', 'EXTENSION_MODULE_UNSUPPORTED');
            return edge.external ? nextResolve(edge.url, { ...context, parentURL: parent.node.url })
                : { url: moduleURL(parent.graph, edge.url), format: parent.graph.nodes.get(edge.url).format, shortCircuit: true };
        },
        load(url, context, nextLoad) {
            const value = selected(url);
            return value ? { format: value.node.format, source: sourceFor(value.node), shortCircuit: true }
                : nextLoad(url, context);
        },
    });
}

function unregister(graph) {
    graphs.delete(graph.id);
    if (!graphs.size) { hooks?.deregister(); hooks = undefined; }
}

/** An owner reuses unchanged code and bounds distinct generations over its entire life. */
export function createExtensionLoader({ maxExtensionGraphs = 64 } = {}) {
    assert(Number.isSafeInteger(maxExtensionGraphs) && maxExtensionGraphs > 0, 'Invalid maxExtensionGraphs.');
    const versions = new Map(); let closed = false;
    return {
        async prepare(filenames, { signal } = {}) {
            validateSignal(signal);
            if (signal?.aborted) throw abortError('extension-graph');
            assert(!closed, 'Extension loader is closed.', 'RUNNER_CLOSED');
            if (!filenames.length) return { modules: new Map(), async verify() {}, release() {} };
            let abandon;
            try { return await bounded(async signal => {
                let captured = await captureExtensionGraph(filenames, signal);
                signal.throwIfAborted();
                const roots = [...captured.roots]; const digests = new Map(captured.digests);
                assert(!closed, 'Extension loader is closed.', 'RUNNER_CLOSED');
                const key = JSON.stringify([...captured.digests].sort());
                let version = versions.get(key);
                assert(version || versions.size < maxExtensionGraphs, 'Extension graph generation limit reached; restart the owner.', 'RESOURCE_LIMIT');
                if (!version) { version = { id: id('graph'), references: 0, nodes: null }; versions.set(key, version); }
                if (!version.references) { version.nodes = captured.nodes; register(version); }
                version.references++;
                let released = false;
                const release = () => {
                    if (released) return; released = true;
                    captured = undefined;
                    if (!--version.references) { unregister(version); version.nodes = null; }
                };
                const verify = () => {
                    assert(!released, 'Extension generation is closed.', 'EXTENSION_GENERATION_CLOSED');
                    return captured.verify();
                };
                abandon = release;
                try {
                    await verify();
                    const modules = new Map();
                    for (const root of roots) {
                        signal.throwIfAborted();
                        const module = await import(moduleURL(version, root)); signal.throwIfAborted();
                        modules.set(root, { module, digest: digests.get(root) });
                    }
                    signal.throwIfAborted();
                    await verify();
                    return { modules, verify, release };
                } catch (error) { release(); throw error; }
            }, { signal, timeoutMs: 60000, stage: 'extension-graph' }); }
            catch (error) { abandon?.(); throw error; }
        },
        close() { closed = true; },
    };
}
