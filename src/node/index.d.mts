import type { IncomingMessage as HttpRequest, Server, ServerResponse } from 'node:http';
import type { Connection, ConsumableIncomingConnector, ExecutionStore, Flow, IncomingConnector, OutgoingConnector, Registry, ResourceLimits, Runner, RunnerOptions } from '../index.mjs';
export interface HttpConnectorOptions { name?: string; revision?: string; url: string; headers?: Record<string, string>; fetchImpl?: typeof fetch; timeoutMs?: number; idempotent?: boolean; idempotencyHeader?: string; maxPayloadBytes?: number; maxResponseBytes?: number; captureResponse?: boolean; method?: 'GET' | 'HEAD' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'; }
export interface FileConnectorOptions { name?: string; revision?: string; directory: string; overwrite?: boolean; pretty?: boolean; maxPayloadBytes?: number; }
export interface ProcessConnectorOptions { name?: string; revision?: string; command: string; args?: string[]; cwd?: string; env?: Record<string, string>; timeoutMs?: number; killGraceMs?: number; maxInputBytes?: number; maxOutputBytes?: number; maxErrorBytes?: number; idempotent?: boolean; inputMode?: 'payload' | 'envelope'; }
export function createHttpConnector(options: HttpConnectorOptions): OutgoingConnector;
export function createFileConnector(options: FileConnectorOptions): OutgoingConnector;
export function createProcessConnector(options: ProcessConnectorOptions): OutgoingConnector;
export interface HttpOptions { authorizationTimeoutMs?: number; signal?: AbortSignal; runner: Runner; flow: string; authToken?: string; authorize?: (context: { action: string; args: unknown; flow: string; namespace: string; request: HttpRequest; signal: AbortSignal }) => boolean | Promise<boolean>; allowAdmin?: boolean; allowRun?: boolean; inputMode?: 'submission' | 'request' | 'event' | 'result'; reply?: 'admitted' | 'complete'; maxBytes?: number; replyTimeoutMs?: number; source?: string; host?: string; }
export interface HttpOptions {
    /** Hostnames without ports; defaults to localhost, 127.0.0.1 and [::1]. Captured at construction. */
    allowedHosts?: string[];
    /** Exact HTTP(S) origin; requests containing Origin are rejected unless it matches. */
    allowedOrigin?: string;
}
export function createHttpHandler(options: HttpOptions): (request: HttpRequest, response: ServerResponse) => Promise<void>;
export function createHttpServer(options: HttpOptions): Server & { drain(): Promise<void> };
export function invoke(runner: Runner, action: string, args: Record<string, unknown>, context?: { source?: string; signal?: AbortSignal }): Promise<unknown>;
/** Explicit eventIdPath takes precedence over transport-generated idempotency keys. */
export interface IncomingAggregationBinding { rule: string; member?: string; correlationPath: string; eventIdPath?: string; }
export interface HostedIncomingConnector extends IncomingConnector { aggregation?: IncomingAggregationBinding; }
export interface ExtensionInstance { type: string; options?: Record<string, unknown>; aggregation?: IncomingAggregationBinding; }
export interface ConnectorConfiguration { schemaVersion: 3; namespace?: string; workerId?: string; placements?: string[]; limits?: ResourceLimits; store?: ExtensionInstance; extensions?: { kind: import('../index.mjs').ExtensionKind; name: string; module: string; export?: string }[]; evaluators?: Record<string, ExtensionInstance>; validators?: Record<string, ExtensionInstance>; policies?: Record<string, ExtensionInstance>; transforms?: Record<string, ExtensionInstance>; connectors?: Record<string, ExtensionInstance>; flows: Record<string, Flow>; incoming?: Record<string, ExtensionInstance>; host?: Record<string, unknown>; }
export interface ConfiguredRunner extends Runner { config: ConnectorConfiguration; sources: Record<string, HostedIncomingConnector>; registry: Registry; }
export interface ConnectorConfiguration { aggregations?: Record<string, Omit<import('../index.mjs').DurableAggregationRule, 'id'>>; }
export interface ConfiguredRunner { aggregators: Record<string, ReturnType<typeof import('../index.mjs').createDurableAggregator>>; }
export function validateConfig(input: unknown): Readonly<ConnectorConfiguration>;
export function loadConfig(path: string, options?: { signal?: AbortSignal }): Promise<{ config: ConnectorConfiguration; baseDir: string; path: string }>;
export function inspectConfiguration(input: unknown): unknown;
export function createConfiguredRunner(input: unknown, options?: { baseDir?: string; env?: Record<string, string | undefined>; registry?: Registry; onEvent?: RunnerOptions['onEvent']; maxExtensionGraphs?: number; signal?: AbortSignal }): Promise<ConfiguredRunner>;
export interface HostOptions { runner: Runner; sources?: Record<string, HostedIncomingConnector>; role?: 'all' | 'ingress' | 'worker'; pollMs?: number; maintenanceMs?: number; drainMs?: number; onError?: (error: unknown) => void | Promise<void>;
    /** Cancels preparation/startup only. Close the returned host/service to end its lifetime. */
    signal?: AbortSignal;
}
export interface Host { health(): Promise<unknown>; stopIngress(): Promise<void>; close(): Promise<void>; }
export interface HostOptions { aggregators?: ConfiguredRunner['aggregators']; }
export function createHost(options: HostOptions): Promise<Host>;
export function createService(configuration: unknown, options?: Omit<HostOptions, 'runner' | 'sources'> & { baseDir?: string; env?: Record<string, string | undefined>; maxGenerations?: number; maxExtensionGraphs?: number }): Promise<{ readonly runner: ConfiguredRunner; health(): Promise<unknown>; reload(configuration: unknown): Promise<void>; close(): Promise<void> }>;
export function createIncoming(type: 'file' | 'stdin' | 'process' | 'spool' | 'redis', settings: Record<string, unknown>, context?: { baseDir?: string }): Promise<ConsumableIncomingConnector>;
export function createIncoming(type: string, settings: Record<string, unknown>, context?: { baseDir?: string }): Promise<IncomingConnector>;
/** Node hosts validate routing before consumption and require durable admission for spool ACKs. */
export function createSpoolSource(settings: { name?: string; revision?: string; directory: string; flow: string; inputMode?: 'submission' | 'request' | 'event' | 'result'; pollMs?: number; leaseMs?: number }): ConsumableIncomingConnector;
