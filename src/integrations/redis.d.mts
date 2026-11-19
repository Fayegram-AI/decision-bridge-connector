import type { OutgoingConnector, ConsumableIncomingConnector } from '../index.mjs';
export interface RedisCommandClient { sendCommand(args: string[], options?: unknown): Promise<unknown>; isOpen?: boolean; close?(): Promise<void> | void; destroy?(): void; quit?(): Promise<unknown>; }
export function createRedisStreamConnector(options: { client: RedisCommandClient; stream: string; name?: string; revision?: string; field?: string; timeoutMs?: number; maxPayloadBytes?: number; ownsClient?: boolean }): OutgoingConnector;
/** One consumer per instance; close or open-signal cancellation stops reads while supplied clients stay open.
 * Owned clients drain yielded settlements during close (default 10 seconds); expiry rejects close and fences later callbacks.
 * The quarantine destination must differ from the input stream. */
export function createRedisSource(options: { name?: string; revision?: string; url?: string; client?: RedisCommandClient; stream: string; group: string; consumer?: string; field?: string; flow: string; inputMode?: 'submission' | 'request' | 'event' | 'result'; pollMs?: number; claimIdleMs?: number; quarantine?: string }): ConsumableIncomingConnector;
