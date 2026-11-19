import type { Runner, Evaluator } from '../index.mjs';
import type { IncomingConnector } from '../index.mjs';
import type { LiveDecisionsController } from '@fayegram-ai/decision-bridge/live';
export function validateLiveDecisionNotification(input: unknown): Readonly<Record<string, unknown>>;
export function liveNotificationEvaluator(options?: { revision?: string }): Evaluator;
export function createLiveSource(options: Omit<Parameters<typeof attachLiveController>[0], 'runner' | 'controller' | 'transport' | 'ownsConnection' | 'closeConnection' | 'processing'> & {
    sessionId: string;
    connect(context: { signal: AbortSignal; attempt: number }): Promise<{ controller: LiveDecisionsController; closed: Promise<unknown>; close(): void | Promise<void> }>;
    reconnect?: { maxAttempts?: number; delayMs?: number; maxDelayMs?: number; timeoutMs?: number };
    onError?: (error: unknown) => void | Promise<void>;
}): IncomingConnector & { health(): { ready: boolean }; status(): { state: string; reconnects: number; attachment: unknown } };
export interface LiveAttachment { controller: LiveDecisionsController; sessionId: string; capabilities: Readonly<Record<string, unknown>>; status(): unknown; drain(): Promise<void>; requestDecision(options?: unknown): unknown; appendCommentary(content: string, options?: unknown): unknown; stop(options?: { drain?: boolean }): Promise<void>; }
export function attachLiveController(options: { controller?: LiveDecisionsController; transport?: unknown; configuration?: unknown; controllerOptions?: Record<string, unknown>; runner: Runner; flow: string; taskDefaults?: Record<string, unknown>; source?: string; sessionId?: string; concurrency?: number; maxPending?: number; processing?: 'run' | 'admit'; processingTimeoutMs?: number; observerTimeoutMs?: number; onNotification?: (value: unknown) => void | Promise<void>; onOutcome?: (value: unknown) => void | Promise<void>; ownsController?: boolean; ownsConnection?: boolean; closeConnection?: () => void | Promise<void> }): LiveAttachment;
