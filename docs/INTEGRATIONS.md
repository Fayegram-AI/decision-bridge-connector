# Integrations and extensions

Incoming/outgoing connectors handle transport. Evaluators invoke the configured decision
operation, while policies interpret its result. See [evaluators and provider routing](EVALUATORS.md)
for Bridge adapters, external services, saved-result validators, and route selection.

## Outgoing contracts

An outgoing connector declares `name`, immutable `revision`, `capabilities`, and
`send(payload, context)`. Context includes cancellation, task/evaluation routing context,
stable `deliveryId`/`idempotencyKey`, a fresh `attemptId`, destination settings, and encoding.
The receipt status is `acknowledged`, `submitted`, or `unknown`. Optional `completion` distinguishes
accepted work from `pending`, `completed`, or `failed` downstream execution. A submitted receipt
without confirmed acceptance leaves delivery unknown. Throw `ConnectorError` with explicit
`ambiguous: false` only when the recipient definitely did not accept the action. Without a saved
acceptance checkpoint, errors lacking that explicit disposition leave the outcome unknown,
including coded transport errors and failures to save a receipt. Their error code alone never
authorizes an automatic retry.

Built-in connectors mark local validation and encoding rejections before recipient dispatch
as definite, non-retryable failures (`ambiguous: false`, `retryable: false`). Correct the input
or connector configuration before resubmitting the work.

The runner supplies optional `context.onAcceptance(receipt)`. Call it once per attempt with an
acknowledged receipt and await its result before collecting optional response data. HTTP uses
this hook after a successful status; MCP uses it after receiving a tool result. A saved checkpoint
survives capture timeout, cancellation and worker recovery; an unavailable body is marked
`responseUnavailable: true`. Existing connectors can return their final receipt without using
this hook. Do not report acceptance
until recipient evidence establishes it. The callback rejects late or superseded attempts.

An outgoing `outputSchema` may be a JSON Schema object or boolean. A false schema rejects every
payload. The configured runner uses AJV; SDK runners supply `validateOutput(schema, payload)`.
The callback may return a boolean or complete without a return value, synchronously or as a
promise. Returning false, throwing, rejecting, or exceeding the connection timeout prevents
delivery. Output validation finishes before the payload becomes dispatchable.
Configured AJV schemas with `$async: true` are awaited for results, output payloads and extension
settings. Schema compilation is cached per configured runner; repeated identical schema IDs can
be reused without sharing validation state between runner instances.

| Connector | Semantics |
| --- | --- |
| Callback | Application-owned function; explicitly declare recipient idempotency. |
| HTTP | Bounded request/response; JSON, text, or JSONL; configured method/headers and optional response capture. A successful HTTP response acknowledges acceptance. |
| File | Atomic writes under a configured directory using stable delivery identity; local placement required. `overwrite: true` declares non-idempotent output and cannot enable automatic ambiguous retry. |
| Process | Literal executable and argument array, no shell interpolation; bounded stdin/stdout/stderr and kill grace. Local placement required. |
| Redis Streams | `XADD` receipt identifies the stream entry; it does not establish consumer completion. Redis appends are not declared idempotent. |
| MCP | Official client using stdio or Streamable HTTP; a tool response confirms transport acceptance, while `isError` marks tool completion failed. |

HTTP response capture and process/MCP response receipts can feed an explicitly configured
`nextFlow`. The next flow accepts an event containing the captured response and owns validation.
Configure `chainRetry` separately from recipient `retry`, for example
`{ maxAttempts: 5, timeoutMs: 60000, delayMs: 100, maxDelayMs: 5000 }` for an application that
permits five admission attempts within one minute. Choose the values for your application's
capacity and latency requirements; the package supplies no chain retry defaults.
Use a projection or transform for recipient-specific envelopes. No connector turns response text
into an executable command or policy definition.

## Optional dependencies and ownership

Install the peer for each integration you use, within the ranges declared by the package:

| Integration | Supported peer range | Install |
| --- | --- | --- |
| PostgreSQL | `pg ^8.16.3` | `npm install "pg@^8.16.3"` |
| Redis Streams | `redis ^6.3.0` | `npm install "redis@^6.3.0"` |
| MCP clients and servers | `@modelcontextprotocol/sdk ^1.32.2` | `npm install "@modelcontextprotocol/sdk@^1.32.2"` |

Importing optional entry points does not connect to a service. Install the relevant peer before
constructing its client or pool; MCP connectors also require the SDK when a client is supplied.
Supplied Redis/MCP clients and PostgreSQL pools are borrowed; factories close only resources they own.

### Resource closure and capture

MCP connector construction validates its timeout as a positive 32-bit millisecond integer, with a
30000 ms default. `createMcpConnector(options, { signal })` accepts an optional startup signal.
Cancellation closes the partially initialized owned transport, including its stdio process.
The startup signal detaches after a successful connection and does not close borrowed clients.
Closing a connector rejects new sends and drains admitted calls within their
existing budgets before closing its owned client. Repeated closes share the same result; a borrowed
client remains open. Calls that ignore cancellation can still have uncertain external outcomes.
MCP retains at most 1 MiB of tool response JSON. A response that exceeds this capture budget,
or the store's receipt budget, preserves acknowledged acceptance and tool completion with
`responseUnavailable: true`. Chaining requires an available captured response.
Process connectors checkpoint an acknowledged protocol reply, or exit 0 with a valid JSON
response, before retaining the full reply. A store receipt-budget failure preserves confirmed
acceptance and known completion with `responseUnavailable: true`. Unknown/submitted receipts,
malformed replies and unsuccessful exits do not establish acceptance.
The process connector also preserves a validated acknowledgement if its acceptance callback
exceeds the delivery deadline. A child may close stdin before the full input is written;
acceptance still requires exit 0 and a valid complete reply within the delivery deadline.
HTTP response capture retains the JSON value's own depth allowance within the receipt byte
budget. A syntactically valid JSON body that exceeds the value limit is marked unavailable.

Store `close()` immediately rejects new domain operations with `STORE_CLOSED` and waits for accepted
operations, including staged values, pool acquisition and retries, before releasing owned resources.
Repeated calls share the same shutdown result. A borrowed PostgreSQL pool remains open after the store drains.

## HTTP and MCP hosts

MCP stdio launches an explicit command; external HTTP endpoints require HTTPS outside loopback.
HTTP handlers enforce a captured Host allowlist and reject browser Origin headers unless an exact
`allowedOrigin` is configured. The default Host names are loopback; configure `allowedHosts` for
deployment hostnames. See [HTTP hosting](NODE.md#http) for settings and proxy boundaries.
Listener authentication remains separate, with flow-specific administration disabled by default.
HTTP and MCP permission settings `allowAdmin` and `allowRun` accept booleans; malformed values fail
at startup before operations are exposed.
MCP advertises explicit tool argument schemas for submit, run, preview, reads, and enabled admin operations.
Schemas require exactly one matching input form, accept JSON validation contexts, and expose
options for the specific operation. Redelivery and import require an explicit destination flow.
MCP server factories validate the runner and the permitted flow list before listening or connecting
stdio. Flow entries must be unique, nonempty names available in the runner. HTTP handlers and
direct MCP servers capture their permission settings and flow lists at construction; later
changes to the caller's options do not change the host's authority. Per-request HTTP
initialization failures produce HTTP 500 when a response is possible and close partial resources.
MCP HTTP `maxBytes` defaults to 1 MiB and accepts integers from 1 byte through 64 MiB. Streamed requests are
bounded by their actual bytes; pre-parsed bodies supplied by middleware are validated as JSON
and bounded by their serialized representation. Middleware remains responsible for limiting
the original wire body before parsing. Oversized input returns an empty HTTP 413 response;
malformed input, including numeric overflow, returns empty HTTP 400 when the socket remains writable.
Neither reaches tool dispatch.
The same setting bounds captured tool arguments in HTTP, stdio and direct servers. HTTP also
counts the complete JSON-RPC envelope against its request budget. Raising this budget does not
raise the runner's value limits or the MCP connector's response-retention limit.

HTTP and MCP hosts with an `authorize` callback require an explicit `authorizationTimeoutMs`
between 1 and 2147483647. The callback receives `{ action, args, namespace, signal }`; HTTP
also supplies `flow` and `request`. Return `true` to allow an operation. Timeout fails closed
with `AUTHORIZATION_TIMEOUT` (HTTP 503 or an MCP tool error). Explicit denial remains forbidden.
The signal aborts on deadline, request cancellation, or the optional host `signal`. Observe it
to stop application authorization work. A callback that ignores it cannot dispatch a late
operation; late rejections remain observed. Static bearer authentication needs no callback timeout.
HTTP `drain()` aborts authorization waits before closing sockets. Cancellation of authorization
or a reply does not undo business work already durably admitted. Authorization budgets are
independent of the HTTP reply budget and recipient delivery budgets.
HTTP and MCP combine the host lifetime with request cancellation for admission validation,
preview, derived admissions and evaluation-recovery validation. Run calls also use it for caller
waits. Cancellation during preparation prevents new durable changes; cancelling a wait leaves
already-admitted work available for execution or recovery. Database transactions already started
are allowed to finish.
An MCP connector declaring `idempotent: true` must name an `idempotencyArgument`; the stable
delivery key is inserted into that tool argument. The receiving tool must honor the contract.

## Store extension contracts

Custom execution stores implement schema-v3 metadata/data separation, leased snapshots, paged
inspection, and framed transfer operations declared by `ExecutionStore`. The runner requires
`schemaVersion: 3`, `dataParts: true`, and `snapshots: true` capabilities. Snapshot ownership
must survive live record changes and retirement; expiry/cleanup must preserve other owners.
Hydrated reads must retain the selected metadata version's value ownership until all data
has been read. Concurrent evidence replacement and collection must not invalidate a read.
See [data access](DATA.md) for limits and transfer integrity.

Custom execution stores implement `dueAggregates(namespace, runtimeRevision?)` with ownership
filtering before the page limit. Due scans must progress across calls and wrap, including when
a full page remains un-emitted; retention must likewise progress past pinned groups. Built-in
stores use persisted cursors, with due-scan cursors pruned after ten minutes of inactivity.
Merged candidate pages retain the store's native ordering so cursor comparisons use the same collation.
An `aggregate` update callback is synchronous and pure; an optimistic conflict may
invoke it again. It may return `null` to leave the record unchanged; the operation then returns `null`. PostgreSQL startup skips existing index DDL
and retries transient schema deadlocks so opening another store can coexist with active workers.
Opening an already-current SQLite store checks its metadata through reads, allowing existing
writers in the same process to resume without waiting for an unnecessary schema write lock.
Custom stores must preserve current-attempt acceptance checkpoints during recovery, cancellation
and terminal settlement. Cancellation must serialize with child admission and retain an existing
child link. Flow quotas count active jobs across kinds, and named aggregation quotas count retained
groups across revisions. `/testing` store conformance checks acceptance recovery, concurrent flow
claims, unprepared-retry rejection and rule quota isolation in addition to admission deduplication
and shared visibility.
Custom stores must reject retry without a prepared payload, retain attempt history when clearing
an acknowledged delivery's current error, and serialize aggregation updates with retirement.
Claims must validate placement arrays and match labels exactly. Pause operations reject invalid
targets before writing controls; destination pauses apply only to jobs with a destination.
The store conformance check exercises these placement and pause boundaries.

## Live

`attachLiveController` observes an existing public controller or constructs one from an
application-supplied transport. Native decisions, refusals, stale/matched correlation flags,
and event identity are preserved. Non-decision lifecycle notifications are observable without
new evaluations. Buffers, processing, observers, and shutdown are bounded. Borrowed controllers
and connections remain application-owned unless ownership is explicitly transferred.
`processing` defaults to `run`, which admits and executes each decision. Choose `admit` to
submit without scheduling local evaluation or delivery. `onOutcome` then receives the admission
report. Status counts confirmed admissions in `admitted`; admission-only success does not
increment `completed` or `failed` based on the task's processing state.

`createLiveSource` adds a bounded reconnect loop around an application `connect()` factory.
The target flow must exist and accept results. Hosts validate that binding before starting
workers or sources; standalone startup checks it before connecting. Hosted sources wait for
activation before connecting or subscribing. Closing a staged source cancels that wait.
An ingress-only host selects admission-only processing; an all-role host executes decisions.
Without a host activation promise, standalone startup connects after binding validation.
The factory returns `{ controller, closed: Promise, close() }`; it owns establishing authenticated
transport and signalling disconnection. Use a stable `sessionId` to deduplicate replayed events
across reconnects. Default reconnect budget is ten attempts with exponential backoff starting
at 250 ms and capped at 30 seconds. Exhaustion is exposed through `status()` and `health()`; host readiness becomes false while the source is unavailable. Restart explicitly.
Reconnect timer settings must be positive 32-bit millisecond values. A connection returned after
its attempt times out or is cancelled is closed when it arrives. Repeated source `close()` calls
wait for the same shutdown, and late disconnect rejections remain observed during cleanup.
Attachment `stop()` and source `close()` attempt remaining owned cleanup and reject with an
`AggregateError` when disposal fails. Sources send sanitized cleanup diagnostics to `onError`,
retain their latest 32 cleanup causes for shutdown and remain unready after a cleanup failure.
Cleanup errors from factories returning after shutdown are still reported through `onError`.
This API can be registered as a custom incoming extension.

## Extension registration

```js
export default {
    apiVersion: 1,
    revision: '1',
    schema: {
        type: 'object', additionalProperties: false,
        properties: { prefix: { type: 'string' } },
    },
    create(options) {
        return {
            name: 'application-audit', revision: '1',
            capabilities: { direction: 'outgoing', runtime: 'portable', acknowledgement: 'accepted', idempotent: true },
            async send(payload, context) {
                await applicationSave(context.deliveryId, options.prefix, payload);
                return { status: 'acknowledged' };
            },
        };
    },
};
```

`applicationSave` is application code whose storage must honor the declared idempotency guarantee.
Register the module with `{ kind: 'outgoing', name: 'application-audit', module: './audit.mjs' }`,
then reference `{ type: 'application-audit', options: { prefix: 'decisions' } }` in `connectors`.
Other extension kinds are `incoming`, `evaluator`, `validator`, `policy`, `transform`, and `store`.
Configuration validates each extension's options schema; local dependency-graph content hashes participate in
runtime revision identity. Keep executable modules and configuration under trusted deployment control.

### Extension dependency reload

Configured runners capture local ES modules and imported JSON before invoking factories.
Static imports, re-exports, cycles and literal dynamic imports share captured module instances
within a generation. Changing only a helper or JSON dependency changes the affected extension's
runtime identity. Retained generations load lazy dependencies from their captured bytes.
Declared extension revisions accept printable identifiers through 256 characters; their derived
runtime identities remain bounded.
`import.meta.url`, `filename` and `dirname` retain the original local file location; filesystem
assets read by application code remain application-owned. Installed packages and Connector runtime
modules retain their ordinary package identity; changing installed dependencies requires restart.

Local CommonJS/native modules, computed imports, import phase modifiers and `node:module`
loading APIs are outside the captured graph contract and fail preparation. Use literal ESM
imports for application code. Capturing files that change during preparation fails before
cutover, leaving the current service available.

Graph fingerprints use JavaScript string ordering for logical module paths. Identical captured
graphs have the same fingerprint across host locales. Keep the matching deployment code and
configuration available for pending work across restarts and upgrades. Tasks retain their
captured runtime identity; see [identity and authority](ARCHITECTURE.md#identity-and-authority).

Each graph accepts at most 1024 local modules and 64 MiB of source. Graph capture and module
initialization have a 60-second wait budget. `maxExtensionGraphs`, an SDK option on
`createConfiguredRunner` and `createService`, defaults to 64 distinct graph versions over the
loader owner's lifetime. Unchanged graphs reuse their version; exceeding the limit rejects
preparation. A service shares its loader across reloads. Captured buffers and hook registrations
are released after their last runner closes. Node retains evaluated ESM in its process cache;
restart the process to reclaim that cache. Extension top-level code remains trusted application
code; a wait timeout cannot undo its external effects.

Factories receive `create(options, { signal, baseDir })` with a 60-second startup budget.
Factory definitions may use prototype getters and methods. Registration captures their
declared metadata and calls `create()` with the original factory as its receiver.
Observe the signal during setup and release partially constructed resources before throwing.
The configured runner owns returned resources that provide `close()`. A resource returned after
startup timeout is closed when it arrives. Host ingress shutdown and runner cleanup share one
close promise, so a configured source is released once even when both lifecycle paths run.
Normal shutdown attempts all owned resources and rejects with an `AggregateError` if cleanup
fails. Original SDK causes are retained. Startup failures remain the primary cause when cleanup
also fails; CLI diagnostics expose sanitized errors.
Configured source methods retain their original receiver, including class private fields.
Configured extension instances retain declared metadata from prototype getters, including outgoing
capabilities, evaluator operation and validator provenance. Local placement and operation checks
therefore apply equally to class instances and plain objects.

`/testing` exports `checkConnector` and `checkStore`. Run them against disposable test destinations
and stores: connector checks invoke `send()`, and store checks admit and deduplicate test work.
They supplement integration tests for recipient-specific error, cancellation, and idempotency behavior.
