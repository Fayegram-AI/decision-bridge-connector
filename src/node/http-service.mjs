import { createServer } from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import { assert, bounded, publicError, snapshot } from '../util.mjs';
import { writeStream } from './io.mjs';
import { authorizeOperation, requestLifetime, validateAuthorization } from './authorization.mjs';
import { createRequestPolicy, validateBearerToken, validateListenerAuthentication, validateListenerPort } from './http-security.mjs';
import { captureOperationArguments, captureSubmission } from '../engine/input-capture.mjs';
import { SOURCE_ACTIVE } from './host-activation.mjs';

const admin = new Set(['retryDelivery', 'resolveDelivery', 'resolveEvaluation', 'recordCompletion', 'cancel', 'pause', 'reprocess', 'redeliver', 'reevaluate', 'openImport', 'appendImport', 'finishImport', 'closeImport']);
const actions = new Set(['submit', 'run', 'preview', 'get', 'list', 'inspect', 'openSnapshot', 'listSnapshot', 'readSnapshot', 'closeSnapshot', 'readExportFrames', ...admin]);
const deliveryActions = new Set(['retryDelivery', 'resolveDelivery', 'recordCompletion']);
const errorStatus = new Map(Object.entries({ AUTHORIZATION_TIMEOUT: 503, HOST_NOT_ACTIVE: 503, UNAUTHORIZED: 401, FORBIDDEN: 403, NOT_FOUND: 404, RESOURCE_LIMIT: 429, CONFLICT: 409, IDEMPOTENCY_CONFLICT: 409 }));

/** Authorization must inspect the same resource identifier that invocation consumes. */
export function operationTaskId(action, args) {
    if (!deliveryActions.has(action)) return args.taskId;
    assert(typeof args.deliveryId === 'string' && args.deliveryId.includes(':delivery:'), 'Delivery not found.', 'NOT_FOUND');
    return args.deliveryId.split(':delivery:')[0];
}

/** Opaque leases carry the same flow authority as their captured execution. */
export async function operationFlow(runner, action, args) {
    if (['listSnapshot', 'readSnapshot', 'closeSnapshot', 'readExportFrames'].includes(action))
        return (await runner.describeSnapshot(args.snapshotId)).flow;
    if (['appendImport', 'finishImport', 'closeImport'].includes(action))
        return (await runner.describeImport(args.transferId)).flow;
    return action === 'openImport' ? args.options?.flow : undefined;
}

export async function invoke(runner, action, args, context = {}) {
    assert(actions.has(action), 'Unknown operation.', 'NOT_FOUND');
    if (action === 'submit' || action === 'run') return runner[action](args.input, {
        ...args.options, source: context.source ?? 'api',
        ...(context.signal === undefined ? {} : { signal: context.signal }),
    });
    if (action === 'preview') return runner.preview(args.input, { signal: context.signal });
    if (action === 'get') return runner.get(args.taskId, args.options);
    if (action === 'cancel' || action === 'openSnapshot') return runner[action](args.taskId);
    if (action === 'listSnapshot' || action === 'readExportFrames') return runner[action](args.snapshotId, args.options);
    if (action === 'readSnapshot') return runner.readSnapshot(args.snapshotId, args.partId, args.options);
    if (action === 'closeSnapshot') return runner.closeSnapshot(args.snapshotId);
    if (action === 'openImport') return runner.openImport(args.options);
    if (action === 'appendImport') return runner.appendImport(args.transferId, args.frame);
    if (action === 'finishImport') return runner.finishImport(args.transferId, { signal: context.signal });
    if (action === 'closeImport') return runner.closeImport(args.transferId);
    if (action === 'list') return runner.list(args);
    if (action === 'inspect') return runner.inspect();
    if (action === 'retryDelivery') return runner.retryDelivery(args.deliveryId);
    if (action === 'resolveDelivery') return runner.resolveDelivery(args.deliveryId, args.resolution);
    if (action === 'resolveEvaluation') return runner.resolveEvaluation(args.taskId, args.resolution, { signal: context.signal });
    if (action === 'recordCompletion') return runner.recordCompletion(args.deliveryId, args.completion);
    if (action === 'pause') return runner.pause(args.type, args.name, args.paused);
    return runner[action](args.taskId, { ...args.options, ...(context.signal === undefined ? {} : { signal: context.signal }) });
}

export function authorized(header, token) {
    if (token === undefined) return true;
    if (typeof token !== 'string' || !token.trim()) return false;
    const expected = Buffer.from(`Bearer ${token}`); const actual = Buffer.from(header ?? '');
    return expected.length === actual.length && timingSafeEqual(expected, actual);
}

export function validateHttpHandler({ runner, flow, authToken, authorize, allowAdmin = false, inputMode = 'submission',
    reply = 'admitted', maxBytes = 1048576, replyTimeoutMs = 10000, allowRun = true,
    authorizationTimeoutMs, signal, allowedHosts, allowedOrigin } = {}) {
    const permittedRequest = createRequestPolicy({ allowedHosts, allowedOrigin });
    validateAuthorization({ authorize, authorizationTimeoutMs, signal });
    validateBearerToken(authToken);
    assert(typeof allowAdmin === 'boolean' && typeof allowRun === 'boolean', 'HTTP permission flags must be boolean.');
    assert(runner && flow, 'HTTP ingress requires a runner and a flow.');
    assert(['submission', 'request', 'event', 'result'].includes(inputMode), 'Invalid HTTP input mode.');
    assert(['admitted', 'complete'].includes(reply), 'HTTP reply must be admitted or complete.');
    assert(Number.isSafeInteger(maxBytes) && maxBytes > 0 && maxBytes <= 67108864, 'Invalid request byte limit.');
    assert(Number.isSafeInteger(replyTimeoutMs) && replyTimeoutMs > 0 && replyTimeoutMs <= 2147483647, 'Invalid HTTP reply timeout.');
    if (reply === 'admitted') assert(runner.capabilities.durable, 'Admission acknowledgement requires durable storage.');
    return permittedRequest;
}

/** Flow selection and administration permissions are host-owned configuration. */
export function createHttpHandler(options = {}) {
    const permittedRequest = validateHttpHandler(options);
    const { runner, flow, authToken, authorize, allowAdmin = false, inputMode = 'submission',
        reply = 'admitted', maxBytes = 1048576, replyTimeoutMs = 10000, source = 'http', allowRun = true,
        authorizationTimeoutMs, signal: hostSignal } = options;
    return async (request, response) => {
        const lifetime = requestLifetime(request, response, hostSignal);
        const abort = () => response.destroy();
        lifetime.signal.addEventListener('abort', abort, { once: true });
        try {
            if (lifetime.signal.aborted) { response.destroy(); return; }
            response.setHeader('content-type', 'application/json');
            assert(permittedRequest(request), 'Request host or origin is not permitted.', 'FORBIDDEN');
            assert(authorized(request.headers.authorization, authToken), 'Authentication required.', 'UNAUTHORIZED');
            assert(options[SOURCE_ACTIVE]?.() !== false, 'Host is not accepting work.', 'HOST_NOT_ACTIVE');
            const url = new URL(request.url, 'http://localhost');
            const exporting = request.method === 'GET' && /^\/tasks\/[^/]+\/export$/.test(url.pathname);
            const importing = request.method === 'POST' && url.pathname === '/tasks/import';
            if (exporting || importing) {
                const action = exporting ? 'exportTask' : 'importTask';
                const taskId = exporting ? decodeURIComponent(url.pathname.slice(7, -7)) : undefined;
                assert(!importing || allowAdmin, 'Administration is disabled for this endpoint.', 'FORBIDDEN');
                if (taskId) assert((await runner.get(taskId, {}))?.flow === flow, 'Execution not found.', 'NOT_FOUND');
                await authorizeOperation({ authorize, authorizationTimeoutMs }, { action, args: exporting ? { taskId } : { options: { flow } },
                    flow, namespace: runner.namespace, request }, lifetime.signal);
                assert(!lifetime.signal.aborted, 'Operation cancelled.', 'ABORTED');
                // The transfer quota is independent of the ordinary JSON body limit.
                await bounded(async signal => {
                    if (exporting) {
                        response.setHeader('content-type', 'application/x-ndjson');
                        for await (const bytes of runner.exportTask(taskId, { signal })) await writeStream(response, bytes, signal);
                        await writeStream(response, '', signal, { end: true });
                    } else {
                        const value = await runner.importTask(request, { flow, signal });
                        response.statusCode = 202;
                        await writeStream(response, JSON.stringify({ ok: true, value }), signal, { end: true });
                    }
                }, { signal: lifetime.signal, timeoutMs: replyTimeoutMs, stage: 'transfer' });
                return;
            }
            let action; let args;
            if (request.method === 'GET' && url.pathname === '/health') { action = 'inspect'; args = {}; }
            else if (request.method === 'GET' && url.pathname.startsWith('/tasks/')) { action = 'get'; args = { taskId: decodeURIComponent(url.pathname.slice(7)) }; }
            else {
                assert(request.method === 'POST', 'Method not supported.', 'NOT_FOUND');
                let bytes = 0; const chunks = [];
                for await (const chunk of request) { bytes += chunk.length; assert(bytes <= maxBytes, 'Request exceeds its byte limit.', 'RESOURCE_LIMIT'); chunks.push(chunk); }
                let body;
                try { body = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks))); }
                catch { assert(false, 'Request must contain valid JSON.', 'INVALID_JSON'); }
                if (url.pathname === '/tasks') {
                    const input = inputMode === 'submission' ? captureSubmission(body, maxBytes)
                        : { kind: inputMode, [inputMode]: snapshot(body, { maxBytes }) };
                    assert(input.flow === undefined || input.flow === flow, 'Flow is not permitted.', 'FORBIDDEN');
                    action = reply === 'admitted' ? 'submit' : 'run'; args = { input: { ...input, flow } };
                } else {
                    assert(url.pathname.startsWith('/operations/'), 'Endpoint not found.', 'NOT_FOUND');
                    action = url.pathname.slice(12); args = captureOperationArguments(action, body, maxBytes);
                    if (args.input) { assert(!args.input.flow || args.input.flow === flow, 'Flow is not permitted.', 'FORBIDDEN'); args.input.flow = flow; }
                    assert(!admin.has(action) || allowAdmin, 'Administration is disabled for this endpoint.', 'FORBIDDEN');
                }
            }
            if (action === 'list') args.flow = flow;
            assert(allowRun || action !== 'run', 'Execution is disabled on this ingress host.', 'FORBIDDEN');
            if (action === 'pause') assert(args.type === 'flow' && args.name === flow, 'Pause requires the endpoint flow.', 'FORBIDDEN');
            if (args.options?.flow) assert(args.options.flow === flow, 'Destination flow is not permitted.', 'FORBIDDEN');
            const resourceFlow = await operationFlow(runner, action, args);
            if (resourceFlow !== undefined) assert(resourceFlow === flow, 'Transfer not found.', 'NOT_FOUND');
            const taskId = operationTaskId(action, args);
            if (taskId) {
                const existing = await runner.get(taskId, {});
                assert(existing?.flow === flow, 'Execution not found.', 'NOT_FOUND');
            }
            await authorizeOperation({ authorize, authorizationTimeoutMs }, { action, args, flow, namespace: runner.namespace, request }, lifetime.signal);
            assert(!lifetime.signal.aborted, 'Operation cancelled.', 'ABORTED');
            const value = await invoke(runner, action, args, { source, signal: lifetime.signal });
            response.statusCode = action === 'submit' ? 202 : 200;
            const send = async (_report, { signal } = {}) => {
                await writeStream(response, JSON.stringify({ ok: true, value }), signal, { end: true });
                return { status: 'submitted' };
            };
            if (value?.taskId && ['submit', 'run'].includes(action)) {
                await runner.reply(value.taskId, send, { timeoutMs: replyTimeoutMs, signal: lifetime.signal });
                // The reservation/read may use the entire budget before send is called.
                if (!response.writableEnded && !response.destroyed) response.destroy();
            } else await bounded(signal => send(undefined, { signal }), { signal: lifetime.signal, timeoutMs: replyTimeoutMs, stage: 'reply' });
        } catch (error) {
            if (lifetime.signal.aborted) { response.destroy(); return; }
            if (response.headersSent || response.writableEnded || response.destroyed) { response.destroy(); return; }
            const failure = publicError(error);
            response.statusCode = errorStatus.get(failure.code) ?? 400;
            try {
                await bounded(signal => writeStream(response, JSON.stringify({ ok: false, error: failure }), signal, { end: true }),
                    { timeoutMs: replyTimeoutMs, stage: 'reply' });
            } catch { response.destroy(); }
        } finally { lifetime.signal.removeEventListener('abort', abort); lifetime.close(); }
    };
}

export function createHttpServer(options) {
    const host = options.host ?? '127.0.0.1';
    const { authToken, authorize } = options;
    const validateBind = binding => validateListenerAuthentication({ host: binding, authToken, authorize });
    validateBind(host);
    const controller = new AbortController();
    const signal = options.signal ? AbortSignal.any([options.signal, controller.signal]) : controller.signal;
    const handler = createHttpHandler({ ...options, signal });
    const server = createServer((request, response) => { void handler(request, response).catch(() => response.destroy()); });
    const listen = server.listen;
    server.listen = function (...args) {
        const [target, address] = args;
        const binding = target && typeof target === 'object' && 'port' in target
            ? { ...target, port: target.port ?? 0, host: target.host ?? host } : null;
        // Node accepts both TCP overloads and pre-bound handles. Apply the policy
        // to the actual TCP host, and require authentication for opaque bindings.
        if (binding && !binding.path && !binding.handle && !binding._handle && binding.fd === undefined) {
            validateBind(binding.host);
            binding.port = validateListenerPort(binding.port);
            args[0] = binding;
        } else if (target == null || typeof target === 'function' || typeof target === 'number' ||
            typeof target === 'string' && target.trim() && Number(target) >= 0) {
            const port = validateListenerPort(typeof target === 'function' ? 0 : target ?? 0);
            if (typeof address === 'string') {
                validateBind(address);
                args[0] = port;
            } else {
                const remaining = typeof target === 'function' ? args : args.slice(address === undefined ? 2 : 1);
                args = [port, host, ...remaining];
            }
        } else assert(authToken !== undefined || typeof authorize === 'function',
            'Non-TCP listeners require authentication.');
        return listen.apply(this, args);
    };
    server.requestTimeout = 30000; server.headersTimeout = 10000;
    server.drain = () => {
        controller.abort();
        return new Promise(resolve => { server.close(resolve); server.closeIdleConnections(); });
    };
    return server;
}
