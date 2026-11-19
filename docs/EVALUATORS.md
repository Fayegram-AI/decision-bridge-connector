# Evaluators and provider routing

An evaluator connects a flow to a decision service, rules engine, or other application
operation. It is separate from incoming/outgoing transport connectors and from the
application policies that decide whether an output should be delivered.

## Selection boundaries

| Selection | Authority |
| --- | --- |
| Flow | The SDK caller names `flow`; an incoming endpoint/source binds its permitted flow. |
| Evaluator | The flow's `evaluator` names one registered instance. |
| Provider/model inside an evaluator | The evaluator's configuration or an explicitly configured router. |
| Output recipients | The flow's connections and their conditions, policies, and projections. |

`kind: 'request'` validates and evaluates the supplied request. `kind: 'event'` first maps
the event through the flow's request projection. `kind: 'result'` validates existing evidence
and skips evaluation. Registering multiple evaluators does not automatically choose between
them, and application decision policies do not select the evaluator for the current task.

## Built-in Bridge adapters

The embedded SDK exports these adapters:

| Adapter | Supplied object |
| --- | --- |
| `decisionEvaluator(bridge)` | A public Bridge instance. |
| `extractionEvaluator(provider)` | A public extraction provider. |
| `routedDecisionEvaluator(router)` | A public decision router. |
| `routedExtractionEvaluator(router)` | A public extraction router. |

The Node configuration layer constructs built-in evaluator types `decision`, `extraction`,
and `routing` from their trusted `options`. A `decision` instance constructs a Bridge and
its provider within the CLI/service process. These adapters consume the installed
Decision Bridge 0.1.8 package through its public exports.

The [Core 0.1.8 release](https://github.com/Fayegram-AI/decision-bridge/tree/decision-bridge-v0.1.8)
defines provider request, response, media and model contracts. Existing adapters
accept additive provider responses through those public APIs and retain the
result fields supplied by Core, including native usage metadata. Core's common
decoding determines which native fields enter the common result. Connector's
data limits, saved schema 3 and explicit routing policies remain applicable.
Anthropic output defaults follow the effective request model; omitting
`responseFormat` allows Core to choose JSON schema for models that do not support
forced tools. An explicitly incompatible tool format fails before dispatch.
See [credential configuration](NODE.md#configuration) for environment aliases and
isolated configured runners.

The `live` result validator and [Live controller integrations](INTEGRATIONS.md#live) handle
incoming native notifications separately from request evaluation.

## External evaluators

A registered evaluator declares an immutable nonempty `revision`, optional
`operation: 'decision'|'extraction'|'live'`, and `execute(request, { signal, timeoutMs })`.
An optional `validate(request)` can reject or normalize the request before admission.
It returns a replacement request or `undefined` to preserve the supplied value.

For example, an application can supply an authenticated external client:

```js
import { createRunner, ConnectorError } from '@fayegram-ai/decision-bridge-connectors';

export function createExternalRunner(client) {
    return createRunner({
        evaluators: {
            external: {
                revision: '1', operation: 'decision',
                validate(request) {
                    if (typeof request?.text !== 'string') {
                        throw new ConnectorError('INVALID_RESULT', 'A text request is required.');
                    }
                    return request;
                },
                async execute(request, { signal, timeoutMs }) {
                    const result = await client.decide(request, { signal, timeoutMs });
                    if (typeof result?.approved !== 'boolean') {
                        throw new ConnectorError('INVALID_RESULT', 'External result requires approved.');
                    }
                    return result;
                },
            },
        },
        flows: {
            review: { revision: '1', inputs: ['request'], evaluator: 'external' },
        },
    });
}
```

`client.decide` is application code; adapt the call to that client's API. The client remains
application-owned. This flow records the result and returns an execution summary. Add
connections for delivery, or use [snapshot inspection](DATA.md) to read the result.
The runner supplies cancellation and a remaining time budget. Honor both in the adapter;
an interrupted provider call can leave an unknown outcome requiring reconciliation.

Evaluator output must be bounded plain JSON under the [data limits](DATA.md#limits-and-storage).
The evaluator owns semantic validation of fresh responses; the runner checks JSON capture
and resource bounds. A separate flow `validator` is used for existing-result admission,
imports, and recovered evaluation results. Alternatively, an evaluator can implement
`validateSaved(result, request, context)` for those paths. Enable `inputs: ['result']`
only with one of these saved-result validation contracts.

An external result can retain its own shape when delivered as `raw-result` or interpreted
by a custom policy module. To use the built-in decision policies, normalize it to the
expected answers and provenance contract. See [policy compatibility](POLICIES.md#custom-result-formats).
The `operation` label does not translate a provider's data or register it with a router.

For CLI/service use, an evaluator extension module exports a versioned factory whose
`create()` returns the evaluator instance. Register it through trusted configuration:

```json
{
    "schemaVersion": 3,
    "extensions": [
        { "kind": "evaluator", "name": "external", "module": "./external-evaluator.mjs" }
    ],
    "evaluators": { "assessment": { "type": "external", "options": {} } },
    "flows": {
        "review": { "revision": "1", "inputs": ["request"], "evaluator": "assessment" }
    }
}
```

The extension supplies `apiVersion`, `revision`, an options schema, and `create`; see
[extension registration](INTEGRATIONS.md#extension-registration). The
[offline evaluator](../examples/offline-extension.mjs) demonstrates the factory shape.
Module paths are relative to the configuration file. Provider credentials belong in
trusted client/configuration setup, outside submitted task data.

## Provider and model routing

Use a routed evaluator when a single flow should choose among compatible configured targets.
In the SDK, construct a router from `@fayegram-ai/decision-bridge/routing` and wrap it with
`routedDecisionEvaluator` or `routedExtractionEvaluator`. Their `callOptions` can set a
concrete `route`, narrow `candidates`, supply `labels`, and configure ambiguity callbacks.
The Node `routing` evaluator constructs the corresponding router from `options.routing`;
`options.operation: 'extraction'` selects extraction, otherwise it uses decision routing.

A concrete route selects its target directly. Experimental `auto` mode considers only
declared candidates. It filters for enabled/configured targets, operation/request
capabilities, explicit constraints, and cooldowns. Deterministic policy uses matching
rules and priorities. Decision-assisted policy can invoke a configured selector with
explicit acceptance thresholds. An unresolved choice can request configured clarification
or fail with a routing error. Inspect routing diagnostics and evaluate the policy with
representative requests before relying on automatic selection.

The router consumes its own provider/route contract. Adding an arbitrary external evaluator
to the Connector registry does not add a router candidate. Adapt that service to the router's
contract, select separate flows explicitly, or implement selection within a custom evaluator.
There is no default policy that infers which of all registered evaluators is best for a task.

## Gemini decisions and common media

Native Gemini decisions use the existing `decision` evaluator and
decision routing configuration. Select `provider.type: 'gemini-decisions'`; the default
model is `gemini-decision-1`. The [Gemini configuration](../examples/gemini.config.json)
resolves `DECISION_BRIDGE_GEMINI_API_KEY` through an explicit `$env` reference. After
[copying the examples](../README.md#run-the-offline-example), run the
[sample task](../examples/gemini-task.json) from the application directory:

```text
npx decision-bridge-connectors run --config connector-examples/gemini.config.json --input connector-examples/gemini-task.json
```

Flows use Core's common request/result contracts. Ordered `input` can contain PDF
documents, remote audio/video, and remote images with explicit `mimeType`. Image
`resolution` accepts `low`, `medium`, `high`, or `ultra_high`; video accepts `low`,
`medium`, or `high`. Inline media uses `dataUrl`; remote audio/video uses HTTPS `url`
and `mimeType`, while PDF documents use `dataUrl` or HTTPS `url`. Provider capabilities
and MIME validation still apply. For example:

```js
const request = {
    input: [
        { type: 'document', url: 'https://example.com/report.pdf' },
        { type: 'image', url: 'https://example.com/photo.jpg', mimeType: 'image/jpeg', resolution: 'ultra_high' },
    ],
    questions: { consistent: { type: 'noul', instructions: 'Are the materials consistent?' } },
};
```

CLI JSONL batch submits ordinary Gemini decision calls through the same flow. See
Core's [Gemini guide](https://github.com/Fayegram-AI/decision-bridge/blob/decision-bridge-v0.1.8/docs/guides/GEMINI_DECISIONS.md)
for provider MIME support and transport budgets.

Explicit resolution produces Core result `meta.schemaVersion: 8`. Documents, remote
audio/video or explicit remote-image MIME metadata produce version 7 when resolution
is absent. Existing input forms keep their previous versions. Connector executions,
configuration and transfers continue to use saved schema 3. Saved-result validation,
reprocessing and imports retain the Core result version independently.

Gemini results preserve native probabilities, score legends, confidence metadata,
modality usage and prompt feedback. The configured decision evaluator reuses the
provider's saved-result validation policy. Embedded applications should pass that
policy using the `validationOptions` resolver described below. Provider errors retain
their code and available safety diagnostics under the normal reconciliation rules.

Connector [data limits](DATA.md#limits-and-storage) remain independent: 1 MiB by default,
up to 64 MiB and 500,000 nodes per application value. Gemini's larger SDK node/request
budgets do not raise those limits. Configuration and transport limits also apply;
for example, CLI input remains bounded independently of the store. Direct decision
adapters perform provider preflight before admission; routed evaluators inspect route
eligibility first, with remaining provider-specific validation during evaluation.

Applications own media access, Files API uploads, processing readiness and deletion.
The Connector forwards supplied references through Core and performs no Files lifecycle
management. Gemini decision calls, extraction and Live controller integration retain
their separate contracts.

## Native OpenRouter and extraction contracts

Native OpenRouter decisions work through `decisionEvaluator` and configured
decision routes. Native Extractions work through `extractionEvaluator` and
extraction routes. Node `extraction` evaluator options accept
`provider: { type: "openrouter-extraction", model: "VENDOR/MODEL", ... }`; an
omitted provider type continues to select Cloudflare. Credentials use the
existing environment-reference configuration mechanism.

The Node decision factory reuses the provider's result-validation policy for
saved results, including probability rounding. For an embedded Bridge, pass
`validationOptions: request => provider.getResultValidationOptions?.(request.model) ?? {}`
to `decisionEvaluator` when the provider declares a precision policy.

An extraction provider supplies `validate`, `extract` and synchronous
`validateResult(result, request)`. Saved admission and reprocessing use that
validator without inference. It checks the configured provider and model plus
the field response contract. OpenRouter preserves omitted `truncated` and
nullable `state_text`; Cloudflare continues to require `truncated`.
