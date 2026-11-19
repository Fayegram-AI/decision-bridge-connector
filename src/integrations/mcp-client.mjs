import { abortError, assert, bounded, ConnectorError, deliveryPreflight, isRecord, identifier, keysOnly, snapshot, validateSignal } from '../util.mjs';
import { incompleteReceipt } from '../engine/receipts.mjs';
import { VERSION } from '../contracts.mjs';

/** MCP acceptance and tool completion are distinct; a tool error is not a transport retry. */
export async function createMcpConnector(options = {}, { signal: startupSignal } = {}) {
    validateSignal(startupSignal);
    if (startupSignal?.aborted) throw abortError('mcp-startup');
    keysOnly(options, ['name', 'revision', 'transport', 'url', 'command', 'args', 'headers', 'tool', 'idempotent',
        'idempotencyArgument', 'timeoutMs', 'client'], 'MCP connector options');
    const { name = 'mcp', revision = '1', transport = 'http', url, command, args = [],
        headers = {}, tool, idempotent = false, idempotencyArgument, timeoutMs = 30000, client: suppliedClient } = options;
    identifier(name, 'Connector name'); identifier(revision, 'Connector revision'); identifier(tool, 'MCP tool');
    assert(Number.isSafeInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 2147483647, 'MCP timeoutMs must be a positive 32-bit integer.');
    assert(['http', 'stdio'].includes(transport), 'Unsupported MCP transport.');
    assert(suppliedClient === undefined || typeof suppliedClient?.callTool === 'function', 'An MCP client requires callTool().');
    if (suppliedClient === undefined && transport === 'stdio') {
        assert(typeof command === 'string' && command.trim().length > 0 && !command.includes('\0'), 'An MCP executable is required.');
        assert(Array.isArray(args) && args.every(value => typeof value === 'string' && !value.includes('\0')), 'MCP args must be strings.');
    }
    assert(typeof idempotent === 'boolean', 'MCP idempotent must be boolean.');
    if (idempotent) identifier(idempotencyArgument, 'MCP idempotency argument');
    const { Client } = await import('@modelcontextprotocol/sdk/client/index.js');
    if (startupSignal?.aborted) throw abortError('mcp-startup');
    const client = suppliedClient ?? new Client({ name: 'decision-bridge-connectors', version: VERSION });
    if (!suppliedClient) {
        let connection;
        if (transport === 'stdio') {
            const { StdioClientTransport } = await import('@modelcontextprotocol/sdk/client/stdio.js');
            connection = new StdioClientTransport({ command, args, stderr: 'pipe' });
            connection.stderr?.resume();
        } else {
            const endpoint = new URL(url);
            assert(endpoint.protocol === 'https:' || endpoint.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(endpoint.hostname), 'MCP requires HTTPS outside loopback.');
            const { StreamableHTTPClientTransport } = await import('@modelcontextprotocol/sdk/client/streamableHttp.js');
            connection = new StreamableHTTPClientTransport(endpoint, { requestInit: { headers } });
        }
        const stopStartingProcess = () => {
            // The public transport PID belongs to this constructor's uninitialized
            // stdio server. Abort must not leave it running until the handshake timeout.
            const pid = transport === 'stdio' ? connection.pid : null;
            if (Number.isSafeInteger(pid) && pid > 0) {
                try { process.kill(pid, 'SIGKILL'); } catch { /* The owned child may already have exited. */ }
            }
        };
        startupSignal?.addEventListener('abort', stopStartingProcess, { once: true });
        try {
            if (startupSignal?.aborted) throw abortError('mcp-startup');
            await bounded(signal => client.connect(connection, { signal, timeout: timeoutMs }), { signal: startupSignal, timeoutMs });
        }
        catch (error) {
            stopStartingProcess();
            // Partial initialization must retain its failure even if transport cleanup stalls.
            await bounded(() => client.close(), { timeoutMs: 1000 }).catch(() => {});
            await bounded(() => connection.close(), { timeoutMs: 1000 }).catch(() => {});
            throw error;
        } finally { startupSignal?.removeEventListener('abort', stopStartingProcess); }
    }
    const active = new Set(); let closed = false; let closing;
    return Object.freeze({ name, revision, capabilities: { direction: 'outgoing', runtime: 'node', acknowledgement: 'completed', idempotent, local: transport === 'stdio' },
        async send(payload, context = {}) {
            const args = deliveryPreflight(() => {
                if (closed) throw new ConnectorError('CONNECTOR_CLOSED', 'MCP connector is closed.', { ambiguous: false, retryable: false });
                validateSignal(context.signal);
                if (context.signal?.aborted) throw abortError('delivery');
                assert(isRecord(payload), 'MCP tool arguments must be an object.');
                const args = snapshot(payload);
                if (idempotent) Object.defineProperty(args, idempotencyArgument, { value: identifier(context.idempotencyKey), enumerable: true, configurable: true });
                return args;
            });
            let dispatched = false; let acceptance;
            const pending = bounded(async signal => {
                dispatched = true;
                const response = await client.callTool({ name: tool, arguments: args }, undefined, { signal, timeout: timeoutMs });
                assert(isRecord(response), 'MCP did not return a tool result.', 'INVALID_RESPONSE');
                const completion = Object.getOwnPropertyDescriptor(response, 'isError');
                assert(completion === undefined || 'value' in completion && typeof completion.value === 'boolean',
                    'MCP returned an invalid tool completion flag.', 'INVALID_RESPONSE');
                assert(!signal.aborted, 'MCP response arrived after cancellation.', 'ABORTED');
                // A tool result confirms acceptance before optional response retention can fail.
                acceptance = Object.freeze({ status: 'acknowledged', completion: Object.freeze({
                    status: completion?.value ? 'failed' : 'completed', scope: 'mcp-tool' }) });
                await context.onAcceptance?.(acceptance);
                const value = snapshot(response, { maxBytes: 1048576 });
                return { ...acceptance, receipt: { response: value } };
            }, { signal: context.signal, timeoutMs });
            active.add(pending);
            try {
                return await pending;
            } catch {
                if (acceptance) return incompleteReceipt(acceptance);
                throw new ConnectorError('MCP_DELIVERY', 'MCP invocation ended without a confirmed tool response.', { ambiguous: dispatched, retryable: true });
            }
            finally { active.delete(pending); }
        },
        close() {
            closed = true;
            // Calls admitted before close drain their existing budgets; borrowed clients stay open.
            closing ??= Promise.allSettled([...active]).then(() => suppliedClient ? undefined
                : bounded(() => client.close(), { timeoutMs, stage: 'mcp-close' }));
            return closing;
        },
    });
}
