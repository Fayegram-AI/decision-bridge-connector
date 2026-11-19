import { assert, bounded, identifier, publicError, snapshot } from '../util.mjs';
import { invoke, operationTaskId, operationFlow } from '../node/http-service.mjs';
import { toolSchema } from './mcp-schemas.mjs';
import { readJson } from '../node/io.mjs';
import { authorizeOperation, requestLifetime, validateAuthorization } from '../node/authorization.mjs';
import { createRequestPolicy } from '../node/http-security.mjs';
import { VERSION } from '../contracts.mjs';
import { captureOperationArguments, requireWireBudget } from '../engine/input-capture.mjs';
import { captureMcpBody } from './mcp-capture.mjs';
import { SOURCE_ACTIVE } from '../node/host-activation.mjs';

const reads = ['get', 'list', 'inspect', 'preview', 'openSnapshot', 'listSnapshot', 'readSnapshot', 'closeSnapshot', 'readExportFrames'];
const writes = ['submit', 'run'];
const administrative = ['retryDelivery', 'resolveDelivery', 'resolveEvaluation', 'recordCompletion', 'cancel', 'pause', 'reprocess', 'redeliver', 'reevaluate', 'openImport', 'appendImport', 'finishImport', 'closeImport'];

/** Host authority is fixed at construction; borrow the runner, signal and explicit callbacks. */
function captureOptions(options = {}) {
    if (options.flows === undefined) return Object.freeze({ ...options });
    assert(Array.isArray(options.flows), 'MCP requires a permitted flow list.');
    return Object.freeze({ ...options, flows: Object.freeze(options.flows.map(value => identifier(value, 'MCP flow'))) });
}

/** JSON.parse permits numeric overflow. Validate without changing the wire-byte budget. */
function validateWireNumbers(value) {
    const levels = [[value].values()];
    while (levels.length) {
        const next = levels.at(-1).next();
        if (next.done) { levels.pop(); continue; }
        const current = next.value;
        if (typeof current === 'number') assert(Number.isFinite(current), 'JSON numbers must be finite.', 'VALIDATION_ERROR');
        else if (current !== null && typeof current === 'object')
            levels.push(Array.isArray(current) ? current.values() : Object.values(current).values());
    }
}

export async function validateMcpOptions(options = {}) {
    const { runner, flows } = options;
    validateAuthorization(options);
    for (const key of ['allowAdmin', 'allowRun']) assert(options[key] === undefined || typeof options[key] === 'boolean',
        'MCP permission flags must be boolean.');
    assert(options.maxBytes === undefined || Number.isSafeInteger(options.maxBytes) && options.maxBytes > 0 && options.maxBytes <= 67108864,
        'MCP maxBytes must be an integer from 1 through 64 MiB.');
    assert(runner && Array.isArray(flows) && flows.length > 0, 'MCP requires a runner and permitted flows.');
    assert(new Set(flows).size === flows.length, 'MCP permitted flows must be unique.');
    const available = await runner.ready;
    assert(available && typeof runner.get === 'function', 'MCP requires a ready runner.');
    for (const flow of flows) { identifier(flow, 'MCP flow'); assert(Object.hasOwn(available, flow), 'MCP permitted flow is unavailable.'); }
}

export async function createMcpServer(options = {}) {
    options = captureOptions(options);
    await validateMcpOptions(options);
    const { runner, flows, allowAdmin = false, allowRun = true, name = 'decision-bridge-connectors' } = options;
    const [{ Server }, { ListToolsRequestSchema, CallToolRequestSchema }] = await Promise.all([
        import('@modelcontextprotocol/sdk/server/index.js'), import('@modelcontextprotocol/sdk/types.js'),
    ]);
    const operations = [...reads, ...writes.filter(action => allowRun || action !== 'run'), ...(allowAdmin ? administrative : [])];
    const server = new Server({ name, version: VERSION }, { capabilities: { tools: {} } });
    server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: operations.map(action => ({
        name: `connectors_${action}`, description: `${action} a configured connector execution. Permitted flows: ${flows.join(', ')}.`,
        inputSchema: toolSchema(action, flows),
        annotations: { readOnlyHint: reads.includes(action), destructiveHint: !reads.includes(action), openWorldHint: !reads.includes(action) },
    })) }));
    server.setRequestHandler(CallToolRequestSchema, async (request, context) => {
        let value; let isError = false;
        try {
            assert(options[SOURCE_ACTIVE]?.() !== false, 'Host is not accepting work.', 'HOST_NOT_ACTIVE');
            const action = request.params.name.replace(/^connectors_/, '');
            assert(operations.includes(action), 'MCP operation is not permitted.', 'FORBIDDEN');
            const maxBytes = options.maxBytes ?? 1048576;
            const args = requireWireBudget(captureOperationArguments(action, request.params.arguments ?? {}, maxBytes), maxBytes);
            if (args.input) assert(flows.includes(args.input.flow), 'Flow is not permitted.', 'FORBIDDEN');
            if (args.options?.flow) assert(flows.includes(args.options.flow), 'Destination flow is not permitted.', 'FORBIDDEN');
            const resourceFlow = await operationFlow(runner, action, args);
            if (resourceFlow !== undefined) assert(flows.includes(resourceFlow), 'Transfer not found.', 'NOT_FOUND');
            const taskId = operationTaskId(action, args);
            if (taskId) assert(flows.includes((await runner.get(taskId, {}))?.flow), 'Execution not found.', 'NOT_FOUND');
            if (action === 'list') { assert(args.flow && flows.includes(args.flow), 'List requires a permitted flow.'); }
            if (action === 'pause') assert(args.type === 'flow' && flows.includes(args.name), 'Pause requires a permitted flow.', 'FORBIDDEN');
            const signal = options.signal ? AbortSignal.any([options.signal, context.signal]) : context.signal;
            await authorizeOperation(options, { action, args, namespace: runner.namespace }, signal);
            assert(!signal.aborted, 'Operation cancelled.', 'ABORTED');
            value = { ok: true, value: await invoke(runner, action, args, { source: 'mcp', signal }) };
        } catch (error) { isError = true; value = { ok: false, error: publicError(error) }; }
        return { isError, structuredContent: value, content: [{ type: 'text', text: JSON.stringify(value) }] };
    });
    return server;
}

export async function serveMcpStdio(options) {
    const { StdioServerTransport } = await import('@modelcontextprotocol/sdk/server/stdio.js');
    const server = await createMcpServer(options);
    try { await server.connect(new StdioServerTransport(options.stdin, options.stdout)); }
    catch (error) { await bounded(() => server.close(), { timeoutMs: 1000 }).catch(() => {}); throw error; }
    return server;
}

export async function createMcpHttpHandler(options) {
    options = captureOptions(options);
    const permittedRequest = createRequestPolicy(options);
    await validateMcpOptions(options);
    const { StreamableHTTPServerTransport } = await import('@modelcontextprotocol/sdk/server/streamableHttp.js');
    return async (request, response, body) => {
        const lifetime = requestLifetime(request, response, options.signal);
        let server; let transport; let closing; let connecting = false; let readingInput = false;
        const close = () => closing ??= (async () => {
            lifetime.close();
            response.removeListener('close', onClose); lifetime.signal.removeEventListener('abort', onAbort);
            await bounded(async () => {
                if (!connecting) await transport?.close();
                await server?.close();
            }, { timeoutMs: 1000 }).catch(() => {});
        })();
        const onClose = () => { void close(); };
        const onAbort = () => { response.destroy(); void close(); };
        response.once('close', onClose);
        lifetime.signal.addEventListener('abort', onAbort, { once: true });
        try {
            if (!permittedRequest(request)) { response.statusCode = 403; response.end(); return; }
            server = await createMcpServer({ ...options, signal: lifetime.signal });
            if (lifetime.signal.aborted) { await bounded(() => server.close(), { timeoutMs: 1000 }).catch(() => {}); response.destroy(); return; }
            transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
            readingInput = true;
            if (request.method === 'POST' && body === undefined) {
                body = await readJson('-', { stdin: request, maxBytes: options.maxBytes ?? 1048576, signal: AbortSignal.any([lifetime.signal, AbortSignal.timeout(30000)]) });
                validateWireNumbers(body);
            } else if (body !== undefined) body = captureMcpBody(body, options.maxBytes ?? 1048576);
            readingInput = false;
            connecting = true;
            await server.connect(transport); await transport.handleRequest(request, response, body);
        }
        catch (error) {
            const code = publicError(error).code;
            const status = !readingInput ? 500 : ['INPUT_TOO_LARGE', 'RESOURCE_LIMIT'].includes(code) ? 413 : ['INVALID_JSON', 'VALIDATION_ERROR'].includes(code) ? 400 : 500;
            if (!response.headersSent && !response.destroyed && !response.writableEnded) { response.statusCode = status; response.end(); }
            else response.destroy();
            await close();
        } finally {
            if (response.writableEnded || response.destroyed) {
                response.removeListener('close', onClose); lifetime.signal.removeEventListener('abort', onAbort); await close();
            }
        }
    };
}
