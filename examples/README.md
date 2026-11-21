# Examples

For a local npm installation, run `npx decision-bridge-connectors` from the application directory.
From the package source root, use `node bin/decision-bridge-connectors.mjs` with the same arguments.
Copy examples into an application-owned directory before running them; the
[offline quickstart](../README.md#run-the-offline-example) shows the installed-package setup.

Run `serve --config FILE` for incoming connections and background workers. Stop with Ctrl+C.
The examples resolve SQLite filenames, file output directories, and extension modules relative
to their configuration file. CLI `--config` and `--input` paths are relative to the directory
where you run the command.
The SDK and CLI default to memory when `store` is omitted. These examples explicitly choose
persistent stores where later commands/workers need saved state; see [storage](../docs/DATA.md#store-choice-and-retention).

| Example | Requirements and use |
| --- | --- |
| `offline.config.json` | No provider credentials; run `task.json`, `decision-task.json`, or `events.jsonl` through the CLI. |
| `jev.config.json` | `JEV_API_KEY`; real provider evaluation. |
| `gemini.config.json` | `DECISION_BRIDGE_GEMINI_API_KEY`; run `gemini-task.json` for native Gemini decisions through the common SDK contract. |
| `http.config.json` | `CONNECTOR_TOKEN`; POST `{"caseId":"example"}` to `http://127.0.0.1:8787/tasks` with Bearer authentication. |
| `mcp.config.json` | Optional MCP SDK peer; configure an MCP client to launch `decision-bridge-connectors serve --config PATH`. Stdio is reserved for protocol traffic. |
| `redis.config.json` | Optional Redis peer and `REDIS_URL`; XADD JSON to field `payload` on `decisions:input`, then read `decisions:output`. |
| `postgres.config.json` | Optional pg peer, `DATABASE_URL`, `WORKFLOW_URL`, `CONNECTOR_TOKEN`; start one ingress role and multiple worker roles against the same configuration. |
| `custom-connector.mjs` | Register as an outgoing extension with `options.url`; wraps evidence in a workflow envelope. |
| `process-receiver.mjs` | Configure a process connector with the Node executable, this script's absolute path in `args`, and a local placement label. |
| `recovery.mjs` | Run `node connector-examples/recovery.mjs PATH` with the configuration for your store. It recovers expired leases and lists up to 25 active tasks. See [state and recovery](../docs/ARCHITECTURE.md#state-and-recovery) for unknown outcomes. |

The offline evaluator is a deterministic fixture for integration learning. Replace it with a
registered real evaluator before interpreting its output as model evidence. Structural schema
validation establishes data shape, not evidence authenticity. Thresholds are illustrative.
Custom evaluators can call an external service directly. Registering several does not select
among them automatically; see [evaluator contracts and routing](../docs/EVALUATORS.md).
