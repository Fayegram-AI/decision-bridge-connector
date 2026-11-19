import type { ExecutionStore, ResourceLimits } from '../index.mjs';
export function createPostgresStore(options?: ResourceLimits & { connectionString?: string; pool?: unknown; max?: number }): Promise<ExecutionStore>;
