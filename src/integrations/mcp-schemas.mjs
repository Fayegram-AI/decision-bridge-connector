/** Tool arguments mirror the shared application service, with explicit authority. */
export function toolSchema(action, flows) {
    const string = { type: 'string', minLength: 1 };
    const flow = { type: 'string', enum: flows };
    const input = { type: 'object', required: ['flow', 'kind'], additionalProperties: false,
        oneOf: ['request', 'event', 'result'].map(kind => ({ required: [kind], properties: {
            kind: { const: kind }, ...Object.fromEntries(['request', 'event', 'result'].map(form => [form, form === kind ? {} : false])),
        } })),
        properties: { flow, kind: { enum: ['request', 'event', 'result'] }, request: {}, event: {}, result: {},
            evidenceAt: string, validation: {}, idempotencyKey: string, context: {},
            correlationId: string, causationId: string, questionRevision: string, deadlineAt: string } };
    const sequence = { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER };
    const hash = { type: 'string', pattern: '^[0-9a-f]{64}$' };
    const fragment = { type: 'string', minLength: 4, maxLength: 87384,
        pattern: '^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$' };
    const frame = { oneOf: [
        { type: { const: 'header' }, seq: { const: 0 }, kind: { const: 'execution-export' }, schemaVersion: { const: 3 } },
        { type: { const: 'record' }, seq: sequence, entity: { enum: ['task', 'job'] }, recordId: { ...string, maxLength: 256 }, index: sequence, final: { type: 'boolean' }, data: fragment },
        { type: { const: 'chunk' }, seq: sequence, sha256: hash, index: sequence, data: fragment },
        { type: { const: 'end' }, seq: sequence, frameCount: sequence, checksum: hash },
    ].map(properties => ({ type: 'object', required: Object.keys(properties), additionalProperties: false, properties })) };
    const options = properties => ({ type: 'object', additionalProperties: false, properties });
    const destination = options({ flow });
    const report = options({ includeData: { type: 'boolean' }, maxInlineBytes: { type: 'integer', minimum: 1, maximum: 67108864 } });
    let properties = {}; let required = [];
    if (['submit', 'run', 'preview'].includes(action)) {
        properties = { input }; required = ['input'];
        if (action === 'submit') properties.options = options({ report });
        if (action === 'run') properties.options = options({ waitMs: { type: 'integer', minimum: 1, maximum: 2147483647 },
            waitFor: { enum: ['admitted', 'evaluated', 'required', 'settled'] }, report });
    }
    else if (['get', 'openSnapshot', 'cancel', 'reprocess', 'redeliver', 'reevaluate'].includes(action)) {
        properties = { taskId: string }; required = ['taskId'];
        if (action === 'get') properties.options = report;
        if (['reprocess', 'redeliver'].includes(action)) properties.options = {
            ...options({ flow, idempotencyKey: string }), ...(action === 'redeliver' ? { required: ['flow'] } : {}) };
        if (action === 'redeliver') required.push('options');
        if (action === 'reevaluate') properties.options = destination;
    } else if (action === 'list') {
        properties = { flow, after: string, limit: { type: 'integer', minimum: 1, maximum: 100 }, state: { enum: ['active', 'complete'] } }; required = ['flow'];
    } else if (action === 'pause') {
        properties = { type: { const: 'flow' }, name: flow, paused: { type: 'boolean' } }; required = ['type', 'name'];
    } else if (['listSnapshot', 'readSnapshot', 'closeSnapshot', 'readExportFrames'].includes(action)) {
        properties = { snapshotId: string }; required = ['snapshotId'];
        if (action === 'listSnapshot') properties.options = options({ collection: { enum: ['jobs', 'data'] }, after: string, limit: { type: 'integer', minimum: 1, maximum: 100 } });
        if (action === 'readExportFrames') properties.options = options({ after: string, limit: { type: 'integer', minimum: 1, maximum: 8 } });
        if (action === 'readSnapshot') { properties.partId = string; required.push('partId'); properties.options = options({ offset: { type: 'integer', minimum: 0 }, maxBytes: { type: 'integer', minimum: 1, maximum: 65536 } }); }
    } else if (action === 'openImport') { properties = { options: { ...destination, required: ['flow'] } }; required = ['options']; }
    else if (['appendImport', 'finishImport', 'closeImport'].includes(action)) {
        properties = { transferId: string }; required = ['transferId'];
        if (action === 'appendImport') { properties.frame = frame; required.push('frame'); }
    }
    else if (action === 'resolveEvaluation') {
        properties = { taskId: string, resolution: { type: 'object', required: ['status', 'reason'],
            properties: { status: { enum: ['succeeded', 'failed'] }, reason: { ...string, pattern: '\\S' }, result: {} },
            if: { properties: { status: { const: 'succeeded' } } }, then: { properties: { result: {} }, required: ['result'] } } };
        required = ['taskId', 'resolution'];
    } else if (action !== 'inspect') {
        properties = { deliveryId: string }; required = ['deliveryId'];
        if (action === 'resolveDelivery') { properties.resolution = { type: 'object', required: ['status'], properties: { status: { enum: ['acknowledged', 'failed'] }, receipt: {}, reason: string } }; required.push('resolution'); }
        if (action === 'recordCompletion') { properties.completion = { type: 'object', required: ['status'], properties: { status: { enum: ['completed', 'failed'] }, evidence: {} } }; required.push('completion'); }
    }
    return { type: 'object', properties, required, additionalProperties: false };
}
