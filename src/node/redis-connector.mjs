import { abortError, validateSignal } from '../util.mjs';
import { createRedisStreamConnector } from '../integrations/redis.mjs';

/** A client is transferred to its connector only after successful construction. */
export async function createConfiguredRedisConnector(settings, { signal } = {}, clientFactory) {
    validateSignal(signal);
    if (signal?.aborted) throw abortError('startup');
    const createClient = clientFactory ?? (await import('redis')).createClient;
    const client = createClient({ url: settings.url });
    let destroyed = false;
    const destroy = () => {
        if (destroyed) return;
        destroyed = true;
        try { client.destroy(); } catch { /* A failed connection can already be closed. */ }
    };
    try {
        client.on('error', () => {});
        signal?.addEventListener('abort', destroy, { once: true });
        if (signal?.aborted) throw abortError('startup');
        await client.connect();
        if (signal?.aborted) throw abortError('startup');
        const { url, ...rest } = settings;
        return createRedisStreamConnector({ ...rest, client, ownsClient: true });
    } catch (error) { destroy(); throw error; }
    finally { signal?.removeEventListener('abort', destroy); }
}
