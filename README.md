# Decision Bridge Connectors

An embeddable execution and integration SDK for [Decision Bridge](https://github.com/Fayegram-AI/decision-bridge).
Named flows accept requests, events, or existing results; apply independent application policies;
and deliver selected outputs to APIs, scripts, AI tools, queues, files, and application callbacks.
The package also ships a CLI and service host for running configured flows. Applications can
embed the SDK in an existing pipeline or let a host manage incoming work and background workers.
Node features require Node 22.17 or later. Windows spool sources also require libuv 1.51 or
later, included in Node 22.17 and Node 24.2; see [spool runtime requirements](docs/NODE.md).

Built-in evaluators use Decision Bridge for provider calls. Custom evaluators can call
external decision services or rules engines directly. See
[evaluators and provider routing](docs/EVALUATORS.md) for contracts and selection behavior.

## Installation

Install [Decision Bridge Connectors from npm](https://www.npmjs.com/package/@fayegram-ai/decision-bridge-connectors/v/0.1.1)
in the application that will use the SDK or CLI.

```sh
npm install @fayegram-ai/decision-bridge-connectors@0.1.1
npx decision-bridge-connectors --help
```

Installation includes Decision Bridge 0.1.8. Memory storage needs no database setup.
Install optional integration peers only when using their adapters; see
[supported peer versions](docs/INTEGRATIONS.md#optional-dependencies-and-ownership).

## Choose the input and output independently

| Input | Processing |
| --- | --- |
| `request` | Validate and evaluate a request with the flow's configured evaluator. |
| `event` | Map an event through the flow template, then evaluate. |
| `result` | Validate existing evidence and apply policies without inference. |

Each outgoing connection selects a condition and a payload. A raw audit destination can
receive every successful result while an action destination receives only decisions accepted
by its policy profile. Review and abstention remain explicit outcomes; the package does not
invent a winning choice when evidence is insufficient.

Payloads are `raw-result`, `final-decision`, `final-decision-with-result`, `failure`, or `report`.
Conditions are `evaluation.succeeded`, `decision.accepted`, `decision.review`,
`decision.abstained`, `failure`, or `always`. Final decision payloads and decision conditions
require a named policy. Projections, bounded transforms, encoding, and optional output schemas
adapt the selected payload to each recipient. Caller replies have separate selection and state.

## Run the offline example

Copy the bundled examples into an application-owned directory, then run them with the
installed CLI. Run these commands from the application directory where you installed the package:

```sh
node --input-type=module -e "import { cp } from 'node:fs/promises'; await cp('node_modules/@fayegram-ai/decision-bridge-connectors/examples', 'connector-examples', { recursive: true, force: false, errorOnExist: true });"
npx decision-bridge-connectors preview --config connector-examples/offline.config.json --input connector-examples/task.json
npx decision-bridge-connectors run --config connector-examples/offline.config.json --input connector-examples/task.json
npx decision-bridge-connectors run --config connector-examples/offline.config.json --input connector-examples/decision-task.json --reply final --policy action
npx decision-bridge-connectors batch --config connector-examples/offline.config.json --input connector-examples/events.jsonl
```

The example uses a deterministic evaluator, SQLite, two policy profiles, and atomic file outputs.
It writes `connector-examples/state.sqlite` and `connector-examples/out/`. Its permissive structural validator is
for the fixture; real result-admission flows should validate their expected evidence contract.

## Embedded SDK

```js
import { createRunner, createCallbackConnector, ConnectorError } from '@fayegram-ai/decision-bridge-connectors';

const runner = createRunner({
    validators: {
        evidence: {
            revision: '1',
            validate(value) {
                if (!value || typeof value.verified !== 'boolean') throw new ConnectorError('INVALID_RESULT', 'Invalid evidence.', { ambiguous: false, retryable: false });
                return value;
            },
        },
    },
    connectors: {
        audit: createCallbackConnector(async (payload, context) => {
            // Persist to an application-owned destination using context.deliveryId.
            await saveAudit(context.deliveryId, payload);
            return { status: 'acknowledged' };
        }),
    },
    flows: {
        evidence: {
            revision: '1', inputs: ['result'], validator: 'evidence',
            connections: [{ id: 'audit', connector: 'audit', when: 'evaluation.succeeded', payload: 'raw-result' }],
        },
    },
});

try {
    const report = await runner.run({ flow: 'evidence', kind: 'result', result: { verified: true } });
    console.log(report.status, report.deliveryCounts);
} finally {
    await runner.close();
}
```

`saveAudit` is supplied by the application. Register `decisionEvaluator(bridge)`,
`extractionEvaluator(provider)`, or the routed evaluators when the flow performs inference.
Configure `store` for durability; the default memory store is process-local.

## Deployment and extension

| Profile | Store | Use |
| --- | --- | --- |
| Embedded | Memory, SQLite, or PostgreSQL | Application-owned pipeline using the SDK and an appropriate store. |
| Local service | SQLite WAL | Multiple processes on one host and one local database file. |
| Distributed service | PostgreSQL | Shared namespace, leases, quotas, and fenced commits across hosts. |

The SDK root is portable. `/node` supplies configuration, CLI/service hosting, HTTP, files,
and processes. `/sqlite`, `/postgres`, `/mcp`, `/redis`, `/live`, and `/testing` expose focused APIs.
Install optional `pg`, `redis`, and `@modelcontextprotocol/sdk` peers only for integrations you use.
Custom incoming/outgoing connectors, evaluators, validators, policies, transforms, and stores
register through a versioned extension contract.

The default memory store requires no database setup and lasts for one process. SQLite and
PostgreSQL retain execution state across restarts; retention and cleanup still apply. Storage
tracks tasks, evidence, jobs, attempts, and receipts. See [store choice and retention](docs/DATA.md#store-choice-and-retention).

Workers write results and delivery plans to the configured store before dispatch. Retryable
definite failures can retry within their budgets; local validation and encoding rejections
settle without automatic retry.
Interrupted external effects become `unknown`; safe automatic replay requires
an explicitly declared recipient idempotency contract. A receipt records acceptance separately
from downstream completion. This provides durable coordination, not universal exactly-once effects.

The [documentation index](docs/README.md) covers SDK/host boundaries, evaluator selection,
storage, policies, transports, and verification.

## Contributing

Report bugs in the [Connector issue tracker](https://github.com/Fayegram-AI/decision-bridge-connector/issues).
Include the Connector version, runtime version, and a minimal example that reproduces the problem.

To test changes to the [Connector source](https://github.com/Fayegram-AI/decision-bridge-connector),
run these commands from the root of your checkout.

```sh
npm ci --ignore-scripts
npm run verify
```

The [development guide](docs/DEVELOPMENT.md) covers service-enabled tests,
scale checks, and archive verification. `npm run build` creates and tests an npm
`.tgz` containing the SDK, CLI, and documentation.

## License

The SDK, CLI, and bundled documentation are distributed under the [MIT License](LICENSE).
