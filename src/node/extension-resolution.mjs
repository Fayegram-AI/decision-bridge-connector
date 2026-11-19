import { registerHooks } from 'node:module';
import { realpath } from 'node:fs/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { assert, id } from '../util.mjs';

const runtimeRoot = new URL('../', import.meta.url).href;

/** Ask Node's import resolver with the actual parent, without evaluating a module. */
export function resolveExtensionImport(specifier, parentURL) {
    const probe = `dbc-resolve:${id('probe')}`;
    let result;
    const hooks = registerHooks({ resolve(request, context, nextResolve) {
        if (request !== probe) return nextResolve(request, context);
        result = nextResolve(specifier, { ...context, parentURL }); return result;
    } });
    try { import.meta.resolve(probe); return result; } finally { hooks.deregister(); }
}

export async function canonicalModule(url) {
    const parsed = new URL(url);
    assert(parsed.protocol === 'file:', 'Extensions require local ESM or JSON modules.', 'EXTENSION_MODULE_UNSUPPORTED');
    const path = await realpath(fileURLToPath(parsed));
    const canonical = pathToFileURL(path); canonical.search = parsed.search; canonical.hash = parsed.hash;
    return canonical.href;
}

export function externalModule(url, roots) {
    return url.startsWith('node:') || url.startsWith('file:') && !roots.has(url) &&
        (url.includes('/node_modules/') || url.startsWith(runtimeRoot));
}
