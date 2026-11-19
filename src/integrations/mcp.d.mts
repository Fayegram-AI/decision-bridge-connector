import type { Runner, OutgoingConnector } from '../index.mjs';
import type { Readable, Writable } from 'node:stream';
import type { IncomingMessage, ServerResponse } from 'node:http';
export interface McpOptions { authorizationTimeoutMs?: number;
    /** Host lifetime also cancels run admission validation and caller waits. Admitted work remains independent. */
    signal?: AbortSignal; runner: Runner; flows: string[]; allowAdmin?: boolean; authorize?: (context: { action: string; args: unknown; namespace: string; signal: AbortSignal }) => boolean | Promise<boolean>; name?: string; }
export interface McpServer { connect(transport: unknown): Promise<void>; close(): Promise<void>; }
export interface McpOptions {
    allowRun?: boolean;
    /** HTTP hostnames without ports; defaults to localhost, 127.0.0.1 and [::1]. Captured at construction. */
    allowedHosts?: string[];
    /** Exact HTTP(S) origin; HTTP requests containing Origin are rejected unless it matches. */
    allowedOrigin?: string;
    /** Tool argument budget and complete HTTP request budget: 1 byte through 64 MiB; default 1 MiB. */
    maxBytes?: number;
}
export function createMcpServer(options: McpOptions): Promise<McpServer>;
export function serveMcpStdio(options: McpOptions & { stdin?: Readable; stdout?: Writable }): Promise<McpServer>;
export function createMcpHttpHandler(options: McpOptions): Promise<(request: IncomingMessage, response: ServerResponse, body?: unknown) => Promise<void>>;
/** Validates timeoutMs (positive 32-bit milliseconds, default 30000) before connecting.
 * Checkpoints acceptance before bounded response capture; unavailable responses retain tool completion.
 * close() fences new sends and drains admitted calls; supplied clients remain application-owned. */
export function createMcpConnector(options: { name?: string; revision?: string; transport?: 'http' | 'stdio'; url?: string; command?: string; args?: string[]; headers?: Record<string, string>; tool: string; idempotent?: boolean; idempotencyArgument?: string; timeoutMs?: number; client?: unknown }, startup?: { signal?: AbortSignal }): Promise<OutgoingConnector>;
