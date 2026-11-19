/** Deterministic example for learning the package without a provider account. */
export default {
    apiVersion: 1,
    revision: '1',
    schema: { type: 'object', additionalProperties: false },
    create() {
        return {
            revision: '1', operation: 'decision',
            validate(request) {
                if (typeof request.probability !== 'number' || request.probability < 0 || request.probability > 1)
                    throw new Error('Provide probability between zero and one.');
                return request;
            },
            async execute(request) {
                return { model: 'offline-example', answers: { ready: { type: 'noul', noul: request.probability } },
                    meta: { provider: 'offline-example', providerKind: 'decision', probabilitySource: 'native' } };
            },
        };
    },
};
