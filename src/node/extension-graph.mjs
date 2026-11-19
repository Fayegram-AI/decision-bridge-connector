import { createReadStream } from 'node:fs';
import { createHash } from 'node:crypto';
import { relative, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse } from 'es-module-lexer/js';
import { assert, stableStringify } from '../util.mjs';
import { canonicalModule, externalModule, resolveExtensionImport } from './extension-resolution.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');

async function readModule(url, maximum, signal) {
    const chunks = []; let bytes = 0;
    for await (const chunk of createReadStream(new URL(url), { signal })) {
        bytes += chunk.length; assert(bytes <= maximum, 'Extension graph exceeds its source budget.', 'RESOURCE_LIMIT');
        chunks.push(chunk);
    }
    return Buffer.concat(chunks);
}

/** Capture every supported local dependency before any extension factory has effects. */
export async function captureExtensionGraph(filenames, signal) {
    const roots = new Set(await Promise.all(filenames.map(canonicalModule)));
    const nodes = new Map(); let bytes = 0;
    async function visit(url, hint) {
        if (nodes.has(url)) return;
        signal?.throwIfAborted();
        assert(nodes.size < 1024, 'Extension graph exceeds 1024 modules.', 'RESOURCE_LIMIT');
        const path = fileURLToPath(url);
        assert(!path.endsWith('.cjs') && !path.endsWith('.node') && hint !== 'commonjs',
            'Local CommonJS and native extension modules require deployment restart.', 'EXTENSION_MODULE_UNSUPPORTED');
        const content = await readModule(url, 67108864 - bytes, signal); bytes += content.length;
        assert(bytes <= 67108864, 'Extension graph exceeds 64 MiB.', 'RESOURCE_LIMIT');
        const format = path.endsWith('.json') ? 'json' : 'module';
        const source = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(content);
        const node = { url, source, digest: hash(content), format, imports: [], edges: new Map() };
        nodes.set(url, node);
        if (format === 'json') { JSON.parse(source.replace(/^\uFEFF/, '')); return; }
        const [imports] = parse(source); node.imports = imports;
        for (const item of imports) {
            if (item.type === 'import-meta') continue;
            assert(!item.phase && !item.typeOnly && item.specifier !== undefined && !item.glob,
                'Extension graphs require runtime ESM imports with literal specifiers.', 'EXTENSION_MODULE_UNSUPPORTED');
            assert(!['node:module', 'module'].includes(item.specifier),
                'Extension graphs do not support custom module loaders or createRequire().', 'EXTENSION_MODULE_UNSUPPORTED');
            const resolved = resolveExtensionImport(item.specifier, url);
            const external = externalModule(resolved.url, roots);
            const target = external ? resolved.url : await canonicalModule(resolved.url);
            node.edges.set(item.specifier, { url: target, external });
            if (!external) await visit(target, resolved.format);
        }
    }
    for (const root of roots) await visit(root, resolveExtensionImport(root, import.meta.url).format);
    const digests = new Map();
    for (const root of roots) {
        const reachable = new Set(); const queue = [root];
        while (queue.length) {
            const next = queue.pop(); if (reachable.has(next)) continue;
            reachable.add(next);
            for (const edge of nodes.get(next).edges.values()) if (!edge.external) queue.push(edge.url);
        }
        const base = dirname(fileURLToPath(root));
        const logical = url => {
            const parsed = new URL(url);
            return relative(base, fileURLToPath(parsed)).replaceAll('\\', '/') + parsed.search + parsed.hash;
        };
        // Preserve dependency-free identities; graph order must not depend on host locale.
        const digest = reachable.size === 1 ? nodes.get(root).digest : hash(stableStringify([...reachable].map(url => {
            const node = nodes.get(url);
            return { path: logical(url), digest: node.digest, format: node.format,
                edges: [...node.edges].map(([specifier, edge]) => [specifier, edge.external ? specifier : logical(edge.url)]).sort() };
        }).sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0)));
        digests.set(root, digest);
    }
    return { roots, nodes, digests,
        async verify() {
            for (const node of nodes.values()) {
                signal?.throwIfAborted();
                assert(hash(await readModule(node.url, Buffer.byteLength(node.source) + 1, signal)) === node.digest,
                    'Extension files changed during preparation; retry with stable files.', 'EXTENSION_GRAPH_CHANGED');
            }
        } };
}
