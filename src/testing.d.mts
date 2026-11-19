import type { OutgoingConnector, ExecutionStore, JsonValue } from './index.mjs';
export function checkConnector(connector: OutgoingConnector, options?: { payload?: JsonValue; timeoutMs?: number }): Promise<{ passed: true; status: string; capabilities: JsonValue }>;
export function checkStore(store: ExecutionStore): Promise<{ passed: true; capabilities: ExecutionStore['capabilities'] }>;
