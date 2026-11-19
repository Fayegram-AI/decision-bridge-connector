import { assert, ConnectorError, isRecord } from '../util.mjs';
import { captureCompletion, captureEnvelope, captureReceiptBody } from './input-capture.mjs';

/** Runner.reply records callback failures as dispositions rather than rejecting. */
export function replyFailure(disposition) {
    if (!isRecord(disposition) || !['unknown', 'failed'].includes(disposition.status)) return null;
    const unknown = disposition.status === 'unknown';
    const error = isRecord(disposition.error) ? disposition.error : {};
    return new ConnectorError(typeof error.code === 'string' && error.code.length ? error.code : unknown ? 'REPLY_UNKNOWN' : 'REPLY_FAILED',
        typeof error.message === 'string' ? error.message : unknown ? 'Caller reply has no confirmed outcome.' : 'Caller reply failed.',
        { ...error, stage: 'reply', ambiguous: unknown });
}

/** An unsuccessful acknowledgement need not carry transport-specific error details. */
export function acknowledgementFailure(disposition) {
    if (!isRecord(disposition) || !['unknown', 'failed'].includes(disposition.status)) return null;
    const unknown = disposition.status === 'unknown';
    const error = isRecord(disposition.error) ? disposition.error : {};
    return new ConnectorError(typeof error.code === 'string' && error.code.length ? error.code : unknown ? 'ACK_UNKNOWN' : 'ACK_FAILED',
        typeof error.message === 'string' ? error.message : unknown ? 'Source acknowledgement has no confirmed outcome.' : 'Source acknowledgement failed.',
        { ...error, stage: 'source-acknowledgement', ambiguous: unknown });
}

/** Snapshot recipient evidence before it participates in durable acceptance. */
export function validateReceipt(value, maxBytes) {
    const receipt = captureEnvelope(value, {
        receipt: body => captureReceiptBody(body, maxBytes),
        completion: completion => captureCompletion(completion, maxBytes),
    });
    assert(isRecord(receipt) && ['acknowledged', 'submitted', 'unknown'].includes(receipt.status), 'Connector returned an invalid receipt.', 'INVALID_RECEIPT');
    assert(receipt.completion === undefined || isRecord(receipt.completion) &&
        ['untracked', 'pending', 'completed', 'failed', 'unknown'].includes(receipt.completion.status),
    'Connector returned invalid downstream completion metadata.', 'INVALID_RECEIPT');
    return receipt;
}

export function incompleteReceipt(receipt) {
    if (isRecord(receipt.receipt) && (Object.hasOwn(receipt.receipt, 'response') || receipt.receipt.$data && receipt.receipt.hasResponse === true)) return { ...receipt };
    return { ...receipt, ...(receipt.receipt === undefined || isRecord(receipt.receipt)
        ? { receipt: { ...receipt.receipt, responseUnavailable: true } } : {}) };
}

/** Only a checkpoint belonging to this attempt can settle interrupted capture. */
export function acceptedDisposition(job) {
    const checkpoint = job.acceptance;
    if (job.kind !== 'delivery' || !checkpoint || typeof checkpoint.attemptId !== 'string' || !checkpoint.attemptId.length || checkpoint.receipt?.status !== 'acknowledged' ||
        checkpoint.attemptId !== job.attempts?.at(-1)?.id) return null;
    const receipt = incompleteReceipt(checkpoint.receipt);
    return { state: 'acknowledged', stage: 'settled', receipt, error: null,
        completion: receipt.completion ?? { status: 'untracked' },
        attempts: job.attempts.map(attempt => attempt.id === checkpoint.attemptId
            ? { ...attempt, status: 'acknowledged', finishedAt: attempt.finishedAt ?? Date.now() } : attempt) };
}
