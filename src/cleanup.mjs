import { ConnectorError, publicError } from './util.mjs';

/** Keep SDK causes while exposing only sanitized lifecycle diagnostics on the wire. */
export function cleanupError(errors, message = 'One or more owned resources failed to close.') {
    const failure = new AggregateError(errors, message);
    failure.code = 'RESOURCE_CLOSE_FAILED';
    return failure;
}

export function cleanupDiagnostic(error) {
    try {
        if (error instanceof AggregateError && error.code === 'RESOURCE_CLOSE_FAILED')
            return new ConnectorError('RESOURCE_CLOSE_FAILED', 'One or more owned resources failed to close.', { stage: 'resource-close' });
    } catch { /* publicError handles opaque thrown values without exposing details. */ }
    return error;
}

export function startupCleanupError(primary, cleanup) {
    const failure = new AggregateError([primary, cleanup], 'Startup failed and resource cleanup failed.', { cause: primary });
    failure.code = publicError(primary).code;
    return failure;
}

export async function closeEvery(operations, { sequential = false } = {}) {
    const invoke = operation => {
        try { return operation(); } catch (error) { return Promise.reject(error); }
    };
    const results = [];
    if (sequential) for (const operation of operations) results.push(...await Promise.allSettled([invoke(operation)]));
    else results.push(...await Promise.allSettled(operations.map(invoke)));
    const errors = results.filter(result => result.status === 'rejected').map(result => result.reason);
    if (errors.length) throw cleanupError(errors);
}
