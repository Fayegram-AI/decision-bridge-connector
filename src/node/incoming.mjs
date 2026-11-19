import { createReadStream } from 'node:fs';
import { resolve } from 'node:path';
import { createServer } from 'node:http';
import { abortError, assert, fingerprintCaptured, snapshot } from '../util.mjs';
import { jsonLines, processSource, readJson } from './io.mjs';
import { authorized, createHttpServer, validateHttpHandler } from './http-service.mjs';
import { createRequestPolicy, listenerAddress, validateBearerToken, validateListenerAuthentication } from './http-security.mjs';
import { createSpoolSource } from './spool.mjs';
import { SOURCE_ACTIVE } from './host-activation.mjs';
import { incomingMetadata } from '../source-binding.mjs';
import { bindInput, captureIncomingValue, PREPARED_LISTENER, SOURCE_BINDING, SOURCE_PREFLIGHT, SUSPEND_LISTENER, sourceBinding } from './source-recovery.mjs';

export { bindInput, createSpoolSource };

export async function createIncoming(type, settings, { baseDir = process.cwd() } = {}) {
    // Preflight and eventual consumption must use the same routing settings.
    settings = { ...settings };
    const binding = ['http', 'file', 'stdin', 'process'].includes(type) ? sourceBinding(settings) : undefined;
    if (type === 'http') {
        let server;
        async function stop() { const closing = server; server = undefined; await closing?.drain(); }
        const host = settings.host ?? '127.0.0.1';
        const preflight = async ({ runner, role }) => {
            assert(role !== 'ingress' || (settings.reply ?? 'admitted') === 'admitted', 'Ingress-only HTTP hosts require admitted replies.');
            validateListenerAuthentication({ ...settings, host });
            validateHttpHandler({ ...settings, runner });
            const address = await listenerAddress(host, settings.port ?? 8787);
            validateListenerAuthentication({ ...settings, host: address.host });
            return address;
        };
        return { [SOURCE_BINDING]: binding, [SOURCE_PREFLIGHT]: preflight, [SUSPEND_LISTENER]: stop,
            async start({ runner, name, role, signal, [PREPARED_LISTENER]: prepared, [SOURCE_ACTIVE]: active }) {
            const { port, bindHost } = prepared ?? await preflight({ runner, role });
            if (signal?.aborted) throw abortError('source-start');
            server = createHttpServer({ ...settings, runner, source: name, allowRun: role !== 'ingress', signal, [SOURCE_ACTIVE]: active });
            await new Promise((resolveStart, reject) => { server.once('error', reject); server.listen(port, bindHost, resolveStart); });
        }, close: stop };
    }
    if (type === 'mcp') {
        let server; let controller;
        async function stop() {
            const closing = server; server = undefined;
            controller?.abort();
            if (closing?.closeIdleConnections) await new Promise(done => { closing.close(done); closing.closeIdleConnections(); });
            else await closing?.close();
        }
        const transport = settings.transport ?? 'stdio';
        const host = settings.host ?? '127.0.0.1';
        const api = await import('../integrations/mcp-service.mjs');
        const mcpOptions = ({ runner, role, signal, [SOURCE_ACTIVE]: active }) => ({ ...settings, flows: settings.flows ?? [settings.flow],
            runner, allowRun: role !== 'ingress', signal, [SOURCE_ACTIVE]: active });
        async function preflight(context) {
            assert(['stdio', 'http'].includes(transport), 'Unsupported MCP transport.');
            validateBearerToken(settings.authToken);
            let address;
            if (transport === 'http') {
                validateListenerAuthentication({ host, authToken: settings.authToken });
                address = await listenerAddress(host, settings.port ?? 8788);
                validateListenerAuthentication({ host: address.host, authToken: settings.authToken });
                createRequestPolicy(settings);
            }
            await api.validateMcpOptions(mcpOptions(context));
            return address ?? { stdio: { input: settings.stdin ?? process.stdin, output: settings.stdout ?? process.stdout } };
        }
        return { [SOURCE_PREFLIGHT]: preflight, [SUSPEND_LISTENER]: stop, async start(context) {
            const address = context[PREPARED_LISTENER] ?? await preflight(context);
            const options = mcpOptions(context);
            controller = new AbortController();
            options.signal = options.signal ? AbortSignal.any([options.signal, controller.signal]) : controller.signal;
            if (options.signal.aborted) throw abortError('source-start');
            if (transport === 'stdio') {
                const opened = await api.serveMcpStdio(options);
                if (options.signal.aborted) { await opened.close(); throw abortError('source-start'); }
                server = opened;
            }
            else {
                const handler = await api.createMcpHttpHandler(options);
                if (options.signal.aborted) throw abortError('source-start');
                server = createServer((req, res) => { void (async () => {
                    if (!authorized(req.headers.authorization, settings.authToken)) { res.writeHead(401).end(); return; }
                    if (context[SOURCE_ACTIVE]?.() === false) { res.writeHead(503).end(); return; }
                    await handler(req, res);
                })().catch(() => { if (!res.headersSent && !res.destroyed) res.writeHead(500).end(); else res.destroy(); }); });
                server.requestTimeout = 30000; server.headersTimeout = 10000;
                await new Promise((resolveStart, reject) => { server.once('error', reject); server.listen(address.port, address.bindHost, resolveStart); });
            }
        }, close: stop };
    }
    if (type === 'redis') {
        const { createRedisSource } = await import('../integrations/redis-source.mjs');
        return createRedisSource(settings);
    }
    if (type === 'spool') return createSpoolSource({ ...settings, directory: resolve(baseDir, settings.directory) });
    assert(['file', 'stdin', 'process'].includes(type), 'Unknown incoming connector.');
    const processOptions = type === 'process' ? snapshot(settings.process) : undefined;
    if (processOptions) {
        assert(processOptions.cwd === undefined || typeof processOptions.cwd === 'string' && processOptions.cwd.length > 0,
            'Invalid incoming process working directory.');
        processOptions.cwd = resolve(baseDir, processOptions.cwd ?? '.');
    }
    return {
        ...incomingMetadata(settings.name ?? type, settings.revision, 'none', type === 'file'),
        [SOURCE_BINDING]: binding,
        async *open({ signal }) {
            let child;
            const stream = type === 'stdin' ? process.stdin : type === 'process'
                ? (child = processSource(processOptions, signal)).stream : createReadStream(resolve(baseDir, settings.path));
            let index = 0;
            try {
                if (settings.format === 'json') {
                    const value = captureIncomingValue(await readJson('-', { stdin: stream, signal, maxBytes: settings.maxBytes ?? 1048576 }), settings);
                    yield { input: bindInput(value, settings, type === 'file' ? await fingerprintCaptured([settings.path, value]) : undefined) };
                    if (child) await child.done;
                    return;
                }
                assert(settings.format === undefined || settings.format === 'jsonl', 'Incoming format must be json or jsonl.');
                for await (const item of jsonLines(stream, { signal, maxBytes: settings.maxBytes ?? 1048576 })) {
                    assert(!item.error, 'Incoming JSONL contains an invalid record.', 'INVALID_JSON');
                    const value = captureIncomingValue(item.value, settings);
                    const key = type === 'file' ? await fingerprintCaptured([settings.path, index++, value]) : undefined;
                    yield { input: bindInput(value, settings, key) };
                }
                if (child) await child.done;
            } finally { child?.close(); if (type === 'file') stream.destroy(); }
        },
    };
}
