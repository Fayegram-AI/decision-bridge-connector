import { abortError, assert, bounded, ConnectorError, validateSignal } from '../util.mjs';

/** A host supplies its own authorization budget; callbacks receive cancellation. */
export function validateAuthorization({ authorize, authorizationTimeoutMs, signal }) {
    validateSignal(signal);
    assert(authorize === undefined || typeof authorize === 'function', 'authorize must be callable.');
    if (authorize !== undefined || authorizationTimeoutMs !== undefined)
        assert(Number.isSafeInteger(authorizationTimeoutMs) && authorizationTimeoutMs > 0 && authorizationTimeoutMs <= 2147483647,
            'authorize requires an explicit positive 32-bit authorizationTimeoutMs.');
}

export async function authorizeOperation(options, context, signal = options.signal) {
    if (signal?.aborted) throw abortError('authorization');
    if (options.authorize) {
        let permitted;
        try {
            permitted = await bounded(child => options.authorize({ ...context, signal: child }), {
                signal, timeoutMs: options.authorizationTimeoutMs, stage: 'authorization',
            });
        } catch (error) {
            if (error?.code === 'TIMEOUT') throw new ConnectorError('AUTHORIZATION_TIMEOUT', 'Authorization deadline exceeded.', { stage: 'authorization' });
            throw error;
        }
        assert(permitted === true, 'Operation is not permitted.', 'FORBIDDEN');
    }
    if (signal?.aborted) throw abortError('authorization');
}

/** Normal request completion is not a disconnect; only unfinished responses cancel. */
export function requestLifetime(request, response, hostSignal) {
    const controller = new AbortController();
    const abort = () => controller.abort();
    const closed = () => { if (!response.writableFinished) abort(); };
    request.once('aborted', abort);
    response.once('close', closed);
    hostSignal?.addEventListener('abort', abort, { once: true });
    if (request.aborted || hostSignal?.aborted) abort();
    return { signal: controller.signal, close() {
        request.removeListener('aborted', abort); response.removeListener('close', closed);
        hostSignal?.removeEventListener('abort', abort);
    } };
}
