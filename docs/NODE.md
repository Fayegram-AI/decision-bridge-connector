# Node CLI and service

The CLI and persistent hosts drive the same runner exposed by the embedded SDK. A CLI
command loads trusted configuration, constructs its store/evaluators/connectors, invokes
the operation, and closes resources it owns. Built-in evaluator types construct the
corresponding Bridge, provider, or router in the process; custom extensions can call an
external service directly. See [evaluators and routing](EVALUATORS.md).

## Configuration

Configuration uses `schemaVersion: 3`, named `flows`, and optional registries for `evaluators`,
`validators`, `policies`, `transforms`, `connectors`, and `incoming`. Each registry instance has
`type` and `options`. Relative extension paths, SQLite filenames, file and spool locations,
and explicitly configured process working directories resolve from the configuration directory.
Processes receive their argument strings literally. An incoming process defaults to the
configuration directory. An outgoing process with no `cwd` inherits the host's working directory.
Credentials can use `{ "$env": "VARIABLE_NAME" }` references
inside trusted instance options. Offline `config inspect` keeps references unresolved; full
startup validates registrations, flow bindings, policies, and output schemas.
Configuration files must contain valid UTF-8 JSON and fit within 4 MiB; reading stops
when the byte limit is exceeded. Environment references require a nonempty string name.

Built-in decision, extraction and routing evaluators discover default credentials
from the configured runner's `env` option, or `process.env` when it is omitted.
They accept the provider's conventional variable and its `DECISION_BRIDGE_` alias:
`TYPESAFE_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`, `OPENROUTER_API_KEY`,
`CLOUDFLARE_API_TOKEN`, or `ANTHROPIC_API_KEY`. Cloudflare REST also requires
`CLOUDFLARE_ACCOUNT_ID`, with the same alias rule. A defined prefixed value takes
precedence, including an invalid empty value; `GOOGLE_API_KEY` is not a Gemini alias.
Explicit `apiKey`/`accountId` values take precedence. A `$env` reference selects
exactly its named variable and fails if it is missing or empty. Use explicit
credentials or references for third-party endpoints; default discovery is limited
to the provider's official origin. Lazy route configurations capture credentials
at runner construction. Supplying `env: {}` prevents ambient credential discovery.

`loadConfig(path, { signal })`, `createConfiguredRunner`, `createHost` and
`createService` accept startup cancellation signals. Cancellation interrupts pending
configuration reads, extension loading, factory initialization and host preparation.
Resources returned by a cancelled factory are closed when they arrive. Signals detach
after successful startup; call the returned runner, host or service's `close()` method
to shut it down. CLI SIGINT/SIGTERM cancel startup and return exit code 130 after cleanup.
JavaScript already executed by an extension cannot be undone by cancellation.

Built-in source bindings require a valid flow name and input mode. Configured runners and SDK
hosts check direct target availability and input compatibility before consuming sources. Configured
aggregation targets must accept events; member envelopes retain their own input kind. A binding
error fails startup before valid queued evidence can be quarantined as invalid input.
Incoming registry names use the shared identifier contract. Aggregation bindings validate their
rule, member and extraction paths at startup. Hosts require durable storage before opening spool
or Redis sources; worker-only hosts leave those sources unopened.
Incoming factories capture top-level settings, including flow and input mode, at construction.

Built-in stores are `memory`, `sqlite` (`options.path`), and `postgres` (`options.connectionString`).
Omitting `store` selects memory, which lasts only for that invocation/process. Configure a
SQLite file or PostgreSQL for commands that inspect or continue earlier work; use the same
store and namespace across invocations. See [store choice and retention](DATA.md#store-choice-and-retention).
`limits` sets resource bounds. `namespace` isolates execution ownership. `placements` declares
the local resources this worker owns; examples use `local` for file/process destinations.
`limits.pageSize` sets the default number of entries from SDK and CLI list operations (1–100,
default 100). Supply an explicit `limit` through the SDK or CLI `--options` JSON to override it,
up to 100. This setting does not change background scan batch sizes.

## Commands

Run these commands from the application directory where the package is installed:

```sh
npx decision-bridge-connectors config inspect --config config.json
npx decision-bridge-connectors run --config config.json --input submission.json
npx decision-bridge-connectors submit --config config.json --flow review --kind result --input result.json
npx decision-bridge-connectors batch --config config.json --input submissions.jsonl
npx decision-bridge-connectors serve --config config.json --role all
npx decision-bridge-connectors listen webhook --config config.json
npx decision-bridge-connectors status TASK_ID --config config.json
npx decision-bridge-connectors list --config config.json --options page.json
npx decision-bridge-connectors inspect --config config.json
```

Omit `--input` or use `-` for stdin. `batch` processes bounded JSONL records sequentially in
input order. Each line reports success or error, including oversized or invalid UTF-8 records.
Single JSON input and each JSONL record have a 1 MiB wire limit, independent of the store's
`limits.maxBytes`. Raising the store limit does not raise the CLI input limit. Streaming
`import` uses the separate [transfer limits](DATA.md#limits-and-storage).
Stdout is JSON/JSONL except help/version;
diagnostics use stderr. Exit code 0 indicates success, including a policy review or abstention.
The CLI returns 1 for a failed, partial, unknown, or cancelled execution, a runtime or
integration failure, or a batch with a failed record. It returns 2 for invalid arguments,
single-input validation errors, or invalid configuration, and 130 when interrupted by a signal.
Reply modes and final-policy names are checked before execution operations admit or change work.
Cancellation interrupts blocked stdout writes, and diagnostic write failures cannot prevent cleanup.
Interruption also cancels validation for preview, derived admissions and evaluation reconciliation
before their durable changes begin. Already-started database transactions may finish.

`--reply report|raw|final` selects caller output; `final` requires `--policy NAME`. This does not
change configured outgoing connections. `--wait admitted|evaluated|required|settled` and
`--wait-ms` set caller waiting. `submit` admits without executing; use durable storage and a
service/worker sharing that store and namespace for later processing.
A one-shot command drains work it has started when closing; use service hosting for independent
long-lived optional delivery.

Administrative commands are `retry DELIVERY_ID`, `resolve DELIVERY_ID`, `completion DELIVERY_ID`,
`cancel TASK_ID`, `reprocess TASK_ID`, `redeliver TASK_ID --flow NAME`, `reevaluate TASK_ID`,
`export TASK_ID` (NDJSON stdout), `import --flow NAME` (NDJSON input), `recover`, `maintain`, `pause FLOW`, and `resume FLOW`.
`resolve` reads `{ "status": "acknowledged" }` or `{ "status": "failed" }` from input; completion
reads `{ "status": "completed" }` or `{ "status": "failed" }`. `--options FILE` supplies operation
options such as a list cursor/limit or destination pause settings. Keep unknown outcomes held
until receipt evidence supports resolution.

`resolve-evaluation TASK_ID` reads `{ "status": "failed", "reason": "operator evidence" }` or
`{ "status": "succeeded", "reason": "recovered provider response", "result": ... }`.
A recovered result must pass the original flow's result validator; it resumes policy/output
planning without inference. Both resolutions retain an audit reason. Use this operation to
settle ambiguous evaluations after checking provider evidence.

## Persistent hosts

`createHost({ runner, role, sources })` starts incoming sources and scheduling. Roles are `all`,
`ingress`, and `worker`. Ingress-only HTTP endpoints acknowledge durable admission and cannot
execute requests. Worker-only hosts open no incoming listeners. Incoming acknowledgement follows
durable admission; a failed acknowledgement cannot erase an admitted task. Worker loops recover
expired leases, flush aggregation windows, and maintain retention.
Maintenance operations report failures independently: a failed aggregation rule cannot prevent
other rules from flushing or prevent retention maintenance from running.

`host.health()` reports readiness, bounded recent errors, queue counts, and age. Shutdown first
stops ingress, then drains workers within `drainMs`; noncooperative sources are bounded. Ownership
is explicit: a configured runner owns resources it creates; an SDK runner borrows its supplied
store and connectors. Close borrowed resources in the application.
`onError` may return a promise. Synchronous throws and rejected observer promises are contained;
the original worker or source error remains in the host's bounded error history.
Source cleanup failures and timeouts also reach `onError` and the error history, including
during shutdown. Cleanup attempts every started source even when another source fails.
Readiness uses current activity failures separately from that history. Successful worker or
maintenance operations clear their own failure; rejected individual messages do not mark the
host unavailable. A failed iterable source remains unready until the host is restarted.
Messages with an acknowledgement callback require durable storage before admission.
Spool polling advances through sorted pages of queued files and wraps, so retryable files in an
earlier page cannot indefinitely hide later input. Processing-lease recovery has its own cursor.
New spool claims use short directories containing the original file, preserving long and Unicode
filenames through retries and archival. Recovery also accepts earlier flat claim files. Keep
processing, processed and quarantine entries intact when restarting a source.
Claimed entries must be regular files. Intake verifies the file identity before reading from
its opened handle; symbolic links, nonregular entries and changed identities are quarantined.
On Windows, spool sources require libuv 1.51 or later, included in Node 22.17 and Node 24.2.
Older Windows libuv versions can report inconsistent file device IDs. Source creation fails
with `UNSUPPORTED_RUNTIME` before claiming files on those runtimes; upgrade Node to resume intake.
Spool JSON is bounded to 1 MiB on the wire and checked against the portable JSON contract before
fingerprinting. Nonfinite parsed numbers and unsupported nesting or node counts are quarantined
with their original bytes; the source continues to later files. Normalized submission size is
governed separately by the store's byte limit. Transient admission and storage failures retain
their existing recovery behavior.

Runner shutdown waits for in-flight admission, claims, administrative mutations, caller replies
and source acknowledgements. Preview validation/planning and preflight for derived executions
and imports also participate in draining. Already accepted derived/import operations may finish
admission during graceful shutdown; new administrative and preview calls reject with `RUNNER_CLOSED`.
It stops claiming new work and returns an undispatched claim to the queue. Forced shutdown cancels
bounded validation and policy work, including preview and recovered-result validation before
reconciliation changes saved state. Database transactions already started are allowed to finish.
Saved evidence remains available for recovery. Repeated host
shutdown calls wait for the same cleanup operation, including iterable-source closure.
Final host, configured-runner and service shutdown reject when owned resource cleanup fails,
after attempting the remaining cleanup. SDK errors retain aggregate causes; host `onError`
receives sanitized diagnostics. The CLI returns operational failure code 1 if cleanup fails
after an otherwise successful operation, preserving earlier failure or interruption codes.

`createService(config, options)` adds programmatic `reload(newConfig)`. Service hosts use `config.host`
settings, with explicit service options taking precedence, on both startup and reload. Reload
validates the new configuration, host bounds, listener options and source bindings before
stopping current ingress. It resolves HTTP and MCP listener names before cutover and checks the
resolved addresses for conflicts. Replacement listeners return HTTP 503 or a startup error
until all sources have started. Iterable sources and workers begin processing after activation.
Reload pauses old built-in listeners only when their endpoints conflict with the replacement.
If startup fails, the service attempts to reopen any paused listeners and reports a restoration
failure through health. Older workers stay available for pending
jobs and aggregates that need their runtime or placement. This includes failed deliveries
that still permit a retry or can start a chain through reconciliation. A newer compatible
worker can cover equivalent work. Ingress-only generations retire after cutover. Storage and
namespace must remain the same, and the store must be durable. At most
eight generations are retained by default. The SDK option `maxGenerations` must be a positive
safe integer and is validated before resources are created. Health checks reap drained generations. Reaping, reload
and shutdown are serialized, so shutdown also closes a generation whose reload is in progress.
Health includes bounded `cleanupFailures` for generations retired after a cleanup error.
Extension dependency reload and its separate lifetime budget are described in
[extension dependency reload](INTEGRATIONS.md#extension-dependency-reload).
Stopping ingress cancels pending validation. A generation remains available while its started
admissions and programmatic aggregate writes settle and while the resulting work still needs its workers.
Storage identity compares resolved settings, the store factory revision, and the base directory;
JSON property order has no effect. PostgreSQL identity also includes `PG*` driver environment
defaults. Changes to these values, including declared credentials, require a restart. A storage
identity mismatch rejects reload before stopping current ingress. Custom store factories must
declare settings and revisions for external state that can change their storage target. For
custom stores, the service can inspect retained executions through the existing store read
methods to identify recoverable deliveries.
Live reload may keep or raise `limits.maxBytes`; lowering it fails before ingress cutover
because replacement workers must remain able to read retained evidence. Other resource limits
can change independently. Plan a lower-budget deployment separately, accounting for existing
values and pending work before restarting with a smaller value limit.
The CLI supports restart-based configuration changes; programmatic service hosts expose reload
through their application's chosen authenticated control interface.

## HTTP

An incoming `http` connection binds `flow`, optional `inputMode`, and `reply: 'admitted'|'complete'`.
Listeners default to loopback. Non-loopback listeners require authentication. Use `authToken`
or an SDK `authorize` callback and terminate TLS at an application-owned proxy when needed.
For SDK servers returned by `createHttpServer`, an omitted TCP host uses the configured host
or `127.0.0.1`. An explicit non-loopback host, IPC path or pre-bound handle requires authentication.

HTTP and MCP HTTP handlers accept `Host` names `localhost`, `127.0.0.1`, and `[::1]` by
default, with optional ports. Set `allowedHosts: ['bridge.example']` for deployment or
proxy hostnames; entries contain no scheme or port and replace the defaults. Requests
with an `Origin` header are rejected unless it exactly matches a configured
`allowedOrigin`, such as `https://app.example`. CLI and other non-browser clients can
omit `Origin`. Rejected requests return HTTP 403 before body parsing or dispatch.
The same checks apply to exported handlers embedded in an application server.
These checks do not enable CORS or replace authentication. Configure CORS and TLS in
the application proxy when needed; forwarded host headers do not grant access.

| Endpoint | Behavior |
| --- | --- |
| `POST /tasks` | Admit a bound-flow submission; return 202 for durable admission or 200 for a complete-mode wait. |
| `GET /tasks/:id` | Read an execution in the endpoint's flow. |
| `GET /tasks/:id/export` | Stream a consistent NDJSON execution archive. |
| `POST /tasks/import` | Administratively import an NDJSON archive into the endpoint flow. |
| `GET /health` | Read store counts and queue age. |
| `POST /operations/:action` | Invoke the shared service operation with named arguments. |

Snapshot/session inspection and streaming transfer contracts are described in [data access](DATA.md).

Administration is disabled unless `allowAdmin` is explicitly enabled. Flow restrictions apply
to task reads, destination flows, listing, flow pause and delivery administration. Delivery
operations authorize the parent named by `deliveryId`; a separate `taskId` cannot change that scope.
The same resource checks apply to MCP tools. HTTP request bodies default
to 1 MiB. Socket write completion records caller reply as submitted, not proof that the caller
processed the response. Disconnected replies do not retry inference or outgoing delivery.
Bodies must contain valid UTF-8 JSON; malformed encoding is rejected before admission.
HTTP and MCP forward host/request cancellation through admission, preview, derived admissions
and evaluation-recovery validation. Run operations also use it for caller waits.
Cancellation while validation is running prevents admission; cancelling a wait preserves an
already-admitted task for execution or recovery.
`replyTimeoutMs` bounds response writes and defaults to 10000 ms. It applies to task replies,
read responses and error responses. A stalled write closes its connection; task reply uncertainty
is retained independently of successful execution. A response whose headers have been sent is
closed on failure instead of appending a second JSON response.
SDK HTTP hosts can set `allowRun: false` to disable execution requests while serving durable
admission and read operations. Configure `reply: 'admitted'` for this profile.
