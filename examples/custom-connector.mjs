// A custom extension wraps the built-in HTTP transport while keeping recipient policy local.
import { createHttpConnector } from '@fayegram-ai/decision-bridge-connectors/node';

export default {
    apiVersion: 1, revision: '1',
    schema: { type: 'object', required: ['url'], additionalProperties: false, properties: { url: { type: 'string' } } },
    create(options) {
        const http = createHttpConnector({ url: options.url, name: 'workflow', captureResponse: true });
        return { ...http, async send(payload, context) {
            return http.send({ event: 'decision.ready', deliveryId: context.deliveryId, evidence: payload }, context);
        } };
    },
};
