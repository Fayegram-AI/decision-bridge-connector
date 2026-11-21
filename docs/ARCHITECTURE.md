# Execution model

## Package boundary

Connectors is an embeddable execution runtime with transport adapters and a CLI/service host.
It owns admission, flow configuration, application policies, delivery, scheduling, execution
state, and transport lifecycles. An application can drive the SDK as one pipeline component;
a persistent host can instead own intake and background scheduling.

Built-in evaluators use Decision Bridge for provider requests, validation, inference,
extraction, routing, and native Live interpretation. Custom evaluators can call other
services and own their request/result contracts. The Connector pins the Decision Bridge version in `package.json`.
The built-in adapters use its public exports and construct provider clients and routers
in the application process.

The portable engine depends on asynchronous domain store operations. Node-specific resources
are constructed by `/node` or optional integration entry points. Providers and destinations
run outside database transactions.

## Evaluator and route selection

The caller or incoming binding selects a named flow, and that flow names its evaluator.
Registering an evaluator makes it available for explicit bindings. It does not add automatic
provider selection. `kind: 'result'` uses saved-result validation and skips evaluation.

A routed evaluator delegates provider/model selection to a configured Decision Bridge router.
Concrete routes select one target; experimental auto mode selects among declared candidates
using capability checks, rules/priorities, and an optional configured decision selector.
An arbitrary external evaluator needs explicit selection logic or an adapter that satisfies
the router's provider contract. See [evaluators and routing](EVALUATORS.md).

## Identity and authority

A submission names an authorized flow and exactly one input form. A flow binds its evaluator,
validator, template, policies, and outgoing connections. Callers cannot supply connector URLs,
executables, policy definitions, or extensions through submissions. Optional metadata overrides
are allowlisted; locked values take precedence. Transport endpoints bind permitted flows.

Startup checks extension methods, nonempty string revisions, evaluator/flow operation agreement,
and timer limits before admitting work. Explicit validator names must resolve even when the
evaluator can validate saved results. Default, locked and caller metadata use the same validation.
Registration selectors must be explicit strings. An omitted validator uses the configured
evaluator's saved-result validation; it cannot select another registration by name coercion.
Request/event admission and saved-result validation require their selected evaluator or
validator revision to remain available. Workers dispatch only explicitly registered callable
evaluators and connectors matching their pinned revisions. Missing evaluator contracts fail
before invocation; missing connector contracts hold delivery without a recipient attempt.
Policy and transform module calls also require their pinned revisions; replacement registrations
cannot authorize outputs under an earlier saved revision.
Connections that select a policy must reference a named profile in their flow.
Delivery retry settings reject unknown fields. `dependsOn` must be an array of nonempty connection IDs.
Flow and connection timeouts must fit a positive 32-bit timer (at most 2,147,483,647 ms).
Store leases and host polling, maintenance and drain intervals use the same timer ceiling.
Store limits are checked before database handles or connections are acquired; failed database
setup releases owned resources.

Every execution has a task ID, evaluation ID, immutable plan, root lineage, and flow revision.
Policy decisions have independent IDs and revisions. Delivery IDs remain stable across retries;
each attempt has a fresh ID. Namespace, source, flow, and idempotency key define the admission
deduplication boundary. Reusing a key with different input is a conflict. For derived executions,
the key also binds the original task and whether delivery is enabled: `reprocess` and `redeliver`
cannot reuse the same key for different intentions. Derived keys use the same nonempty identifier
validation as submission keys. Active executions retain
their deduplication protection beyond the configured deduplication period.
Internal child and aggregation admissions also retain their task and admission identity while
the owning chain or un-emitted group can replay admission. Settled owners reject stale attempts
to create a new execution after deduplication expires.

Runtime revisions include evaluator, validator, policy, transform, connector, and flow configuration
identity, including every reachable chained flow. A worker claims jobs whose saved revision
matches its flow fingerprint. Environment-reference names participate in
configuration identity; resolved secret values do not. Change a descriptor revision when rotating
credentials changes recipient identity or behavior. Retain old configuration/modules while their
work is pending, including across restarts.

Deploy one Connector release across the hosts that share a namespace. If flow fingerprints
change, drain or reconcile pending work with the matching runtime. Pending tasks and
aggregation groups retain their saved fingerprints. See [host upgrades](NODE.md#persistent-hosts).

## State and recovery

The SDK and CLI default to process-local memory. File-backed SQLite or PostgreSQL provides
restart persistence for the following state transitions. The store choice is independent of
whether execution is embedded or hosted; see [store choice and retention](DATA.md#store-choice-and-retention).

1. Validate and snapshot input; reserve capacity and atomically persist task, deduplication key,
   evaluation job, and blocked output jobs.
2. Claim evaluation with a lease and fencing token. Persist the invocation marker before calling
   the evaluator and save its result before policy processing.
3. Apply independent policy profiles; persist decisions, selected payloads, and delivery plans.
4. Claim each delivery separately. Persist attempt identity before calling the recipient, then
   record acceptance and downstream completion separately.

A crash after result persistence resumes planning without inference. An expired lease after
invocation leaves an unknown outcome, since the provider or recipient may have acted. Fencing
rejects a stale worker's database writes; it cannot undo external effects. Ambiguous retries are
disabled unless the connector declares idempotency and the connection opts in. Recovery respects
the original attempt limit and activates failure notifications for unresolved outcomes. Required
failure notifications participate in required completion even when evaluation remains unknown.
Failed or expired child admission also activates failure connections. Each originating diagnostic
is retained once, including when different recipients return identical errors; concurrent updates
preserve the combined history. Optional chain diagnostics retain their optional status.
Failure connections use a delivery TTL starting at first eligibility and bypass business-evidence
freshness checks. Their retries and recovery keep that deadline. Each connection retains one
notification lifecycle; later errors remain inspectable without resending a dispatched notification.
Notification failures do not recursively activate the same failure connections.
Thrown callback values are normalized to public errors before classification. Unclassified
failures after provider or recipient invocation remain unknown; explicit definite rejection
metadata retains its failure/retry meaning.
Built-in validation and encoding rejections before dispatch are definite and non-retryable;
see [outgoing contracts](INTEGRATIONS.md#outgoing-contracts).

Invocation-marker and result-write failures stop that worker attempt without planning outputs
from unsaved evidence. Recovery resumes planning when a result was committed, including when its
write response was lost. If no result was saved after invocation, recovery retains an unknown
evaluation for reconciliation. Provider dispatch rechecks the earlier of `deadlineAt` and the
flow timeout after marker persistence and passes only the remaining budget to the evaluator.

Direct Cloudflare extraction can reject queued requests before the worker records
invocation. That rejection settles as `failed` and can trigger failure outputs. See the
[adapter requirements](EVALUATORS.md#cloudflare-extraction).

For an `unknown` evaluation, call `resolveEvaluation(taskId, resolution)` with a nonblank
audit reason. Use `status: 'succeeded'` with the recovered `result` (which may be `null`),
or `status: 'failed'` when the provider confirms failure. The runner checks a recovered
result against the original revision of the flow's validator or its evaluator's `validateSaved`
method. A worker then resumes planning without another provider call. Only one of two competing
resolutions can succeed. If a worker encounters an older
reconciled record with neither a result nor evaluation failure evidence, it returns the task
to `unknown` with an `INVALID_RESOLUTION` diagnostic.

A connector may checkpoint confirmed acceptance through `context.onAcceptance(receipt)` before
collecting optional response data. The runner validates and saves the acknowledged receipt for
the current attempt. Recovery and cancellation preserve that evidence without resending. If
optional HTTP or MCP response capture is interrupted, acceptance remains acknowledged and the
receipt has `responseUnavailable: true`. A checkpoint whose storage writes never succeeded cannot establish
durable acceptance after a crash. Late callbacks from expired or settled attempts are rejected.

`retryDelivery` retries a prepared payload after definite failure within its original attempt/expiry
limits. Output-planning failures have no dispatchable payload and cannot be retried this way.
Use an explicit new execution to validate and plan those outputs again. Workers reject saved
pending deliveries without a prepared payload before invoking the recipient.
Automatic retries wake by the earlier of their retry time, delivery expiry, or evidence freshness
limit. A stopping check settles expired or stale work without sending before the recipient's
requested delay; it preserves uncertainty from earlier attempts.
`resolveDelivery` records evidence that an unknown/held/failed delivery was accepted or failed.
An uncertain attempt remains unresolved after a later retry is rejected, expires, becomes stale,
or is cancelled. A confirmed acceptance or operator resolution settles that uncertainty. A failed
resolution covers attempts made so far; later attempts have their own acceptance outcomes.
Confirming delivery failure records a terminal diagnostic and activates eligible failure connections,
including when the delivery was held before dispatch. Repeating a resolution retains the same
origin and notification lifecycle.
Acknowledged acceptance clears the current delivery error; earlier attempt errors remain in history.
`recordCompletion` accepts a final downstream status of `completed` or `failed` after acceptance.
Connector receipts can also describe pending or untracked completion. `reprocess` creates a new
policy execution from saved evidence without delivery; `redeliver` requires an explicit flow and
creates new delivery identities; `reevaluate` performs new inference. Export/import uses a schema
version and integrity checksum. Import requires an explicit destination flow and revalidates the
saved result; the checksum detects accidental alteration and is not an authenticity signature.
Framed import verifies all metadata, values and the integrity trailer before admission.
Each export captures a consistent task/jobs view and pins immutable values, then streams outside
that capture transaction. Read operations return summaries; use [snapshots and data parts](DATA.md)
for retained evidence. A list page may omit executions retired after selection; follow its
non-null cursor even when the returned page has no items.
For result-only input, the validator receives the supplied JSON validation context, or an
empty object if it was omitted or null. During recovery, reprocessing, redelivery or import
of an evaluated request, the validator receives the normalized request as `validation.request`,
including when normalization produced `null`. If the original context was a JSON object,
its other fields are also available.
Validators can edit their working copy while the execution retains the original evidence.
Reprocessing, redelivery and reevaluation load task evidence without hydrating retained delivery
payloads or receipts. An unavailable job body does not prevent deriving work from intact task evidence.
`preview`, `reprocess`, `redeliver` and `reevaluate` accept an optional `signal` in their options;
`resolveEvaluation` accepts it in an optional third argument. Cancellation interrupts preparation
before admission or reconciliation writes. If preparation fails or is cancelled after preview
capture, its lease is released before the operation rejects. Database transactions already
started are allowed to finish.

## Independence and scheduling

Required and optional outputs have separate claims. Required completion does not wait for
optional outputs. `run` first admits work, then waits using the same options as `execute`.

| `waitFor` | Milestone |
| --- | --- |
| `admitted` | Read the admitted execution without claiming work. |
| `evaluated` | A result is available. A submitted result can satisfy this before policy and output planning. |
| `required` | The report's `requiredSettled` flag is true. This includes required output and chain-admission outcomes. This is the default. |
| `settled` | The report's `settled` flag is true, including optional work. |

Any wait can return when the execution settles or the wait budget expires. On expiry, the
caller receives the most recent observed summary if available. The default `waitMs` is
60000 and includes claim acquisition. An undispatched claim that arrives after expiry or
cancellation returns to the queue.
`unknown` and `held` require intervention and remain retained. A caller wait timeout does not cancel
durably admitted work. Explicit cancellation fences claims; an invoked effect may remain unknown.
PostgreSQL cancellation waits for locked job rows before applying its fencing changes. Internal
admission locks its owner while checking authorization, so owner settlement and new admission
have a defined transaction order.
Cancellation uses that admission lock to preserve the ID of an already-admitted child. It fences
an unadmitted child without cancelling existing children. A parent's acknowledged chain records
admission, independently of the child's eventual processing outcome.

Global evaluation, delivery, optional-delivery, per-flow, and per-destination bounds are coordinated
by the store. A flow or destination can be paused. The flow concurrency bound counts all active,
unexpired evaluation, delivery, chain and failure-planning jobs together, including compatible
workers from other generations.
Flow claim locks use a consistent order across job kinds, before locking selected job rows.
Selection under those locks checks the current job state and due time.
Existing work above a lowered limit drains before more jobs are claimed.
Fixed interval rate controls use persisted next dispatch times. Built-in stores restore the
prior queue state and release the rate reservation when a caller abandons a claim before
dispatch or recovery finds its lease expired before the first execution write. Rates must be
positive finite numbers whose computed next-dispatch timestamp is
a safe integer; these persisted intervals are independent of 32-bit timer limits.
Flow iteration rotates to avoid always preferring the same flow. Explicit output
dependencies and destination ordering gates are checked before dispatch. Ordering can block or
continue after failure; retention preserves a failed predecessor while later ordered work remains.
Local files and processes require placement labels so only workers with those resources claim them.
Namespaces, worker IDs and placement labels use nonempty printable identifiers of at most 256
characters. Worker placements must be a unique array; the runner captures its labels at construction.
Claims use exact label membership. Pause/resume operations require a valid flow or destination
identifier before changing controls.
Destination pauses affect only jobs with that destination; destinationless evaluation, failure
planning and chain jobs continue to use their flow controls.

`maxAgeMs` is checked during output planning and again before dispatch, including retries.
Business outputs whose evidence ages out while queued follow the flow's `stale: 'hold'|'expire'` setting without
invoking the recipient. Delivery expiry is also rechecked after the invocation marker is persisted.

Default bounds: 10,000 active tasks per namespace, 1 MiB per logical application value, four evaluations,
16 deliveries, four optional deliveries, four deliveries per destination, 30-second leases, and
seven-day task and deduplication retention. Flows default to four concurrent jobs, 60-second
evaluation, one-hour delivery expiry, depth eight, and 64 executions per lineage. Unknown work
consumes capacity until reconciled. Maintenance removes bounded pages; hosts run it periodically.
Task and deduplication retirement cursors advance across retained records and wrap after the
last page, so retained work cannot indefinitely hide later eligible records.
`limits.pageSize` sets the default list-page size from 1 through 100, with a default of 100.
An explicit list `limit` overrides it up to 100. Internal claim, maintenance and recovery page
sizes, and snapshot data reads, are independent of this listing default.
The store's `maxBytes` budget applies independently to each admitted value, normalized
request/result, policy decision, outgoing payload, receipt body and operator evidence value.
Metadata and diagnostics have separate bounds; execution size is not a per-value bound.
See [inspection and transfer](DATA.md) for chunking, lease, paging and wire limits. A normalizing validator returns
`undefined` to keep its input; `null` is a valid replacement value and is preserved.
Retention rechecks locked records before deletion and coordinates task and dedup retirement with
admission. Reopening an execution clears its old expiry; its next completion starts a fresh
retention interval. Cancelling a required response chain after delivery produces partial completion.

Caller replies and source acknowledgements have independent task channels. The built-in stores
atomically reserve one live attempt per channel; a concurrent reservation fails with `CONFLICT`
before its callback runs. After expiry, a new attempt may replace the old one. Settlements carry
their `attemptId`, and a superseded attempt fails with `STALE_ATTEMPT` without changing the newer
state or recovery deadline. Reservation writes and report reads consume the callback deadline.
Custom stores must implement these checks in `recordReply` and `recordAcknowledgement` as part of
the same transaction that writes the task. Use a new ID and `expiresAt` for each `in_flight`
reservation, then settle that ID with its outcome. External effects that ignore cancellation
may still finish after expiry; fencing protects stored state, not the external recipient.

Custom stores must also preserve internal `admissionOwner` bindings. While a referenced chain
job remains unresolved or a matching aggregation group remains un-emitted, `admit` must retain
its deduplication identity and `maintain` must retain its admitted task. Import bindings identify
the verified session by `id`; admission requires its live pending owner, matching destination
revision and admission key. Verified pending/completed sessions sharing that key retain the
execution until all close or expire. Owner lookups and admission/retirement checks belong in
the store transaction. `resolvedAttempts` identifies the delivery attempt count covered by
operator reconciliation; earlier attempt history remains intact.
Duplicate admission must compare `policyOnly` and, for source `reprocess`, `lineage.parentId`, as well as the input fingerprint. `get` must return a consistent task/jobs view
while settlement and retirement run concurrently. Hydrated job and aggregation reads must
protect the selected version's value ownership until reading finishes. PostgreSQL holds
the owning row lock during hydration; aggregation callbacks run after that read transaction.

## Aggregation and chaining

Durable aggregation groups named source members under an explicit correlation key and rule revision.
Processing-time windows, duplicate rules, incomplete emission/discard, retention, and group limits
are configured. Each group belongs to its rule configuration and target runtime revision; pending
groups retain their service generation until emission or discard. Late evidence is rejected; use
a new correlation key for a new group. There is no implicit event-time watermark or majority-vote
aggregation. See [incoming connections](INCOMING.md).

Due-group queries select the owning rule generation and exclude already emitted groups. A blocked
rule cannot consume another rule's bounded work page. Ownership indexes are written when
a group is inserted or updated.

A connection's `nextFlow` explicitly maps a captured downstream response into an event submission.
The target must accept events and perform its own mapping/validation. Child admission is durable
and idempotent; child IDs are available through job inspection. Root depth and execution
ceilings persist across chaining, reprocessing, redelivery and reevaluation. A destination can
tighten either ceiling; a more permissive destination cannot reset it. Imports create a newly
authorized root lineage.
A parent records child admission, not the child's eventual business completion. Payloads without
a captured response cannot be chained.
An uncertain child-admission or parent-link write remains recoverable. Recovery reuses the stable
child admission key to attach the existing child instead of creating another execution.

Every connection with `nextFlow` requires an explicit `chainRetry` policy. Set `delayMs` and
`maxDelayMs`, plus `maxAttempts`, `timeoutMs`, or both. Values are positive integers; timer
values must fit a signed 32-bit millisecond timer, and `maxDelayMs` must be at least `delayMs`.
The initial admission counts as an attempt. The elapsed-time budget starts when the parent
delivery is acknowledged, including acknowledgement through operator reconciliation. With both
budgets configured, the first exhausted budget stops admission. Active admission waits use a
monotonic timeout; an early storage timeout retains uncertainty for child-link recovery.
Backoff grows exponentially
with jitter between half and all of the capped delay. These retries never resend the parent.
Queue capacity errors are definite, retryable admission rejections; depth and execution limits
remain permanent failures. Snapshot job pages expose chain `childTaskId`, `admissionAttempts`, `nextAttemptAt`, and
`deadlineAt`; data parts retain diagnostics.
Attempt exhaustion produces `CHAIN_ADMISSION_EXHAUSTED` and a failed chain; elapsed-time
exhaustion produces `CHAIN_ADMISSION_EXPIRED` and an expired chain. Terminal settlement first
links an existing child if an earlier admission succeeded but its response was lost.

Custom stores used with chaining must implement `findChildAdmission(namespace, chainClaim)`.
Use the indexed deduplication key derived from `[namespace, 'chain', nextFlow, chainJobId]`,
validate the active owner lease/token, and return its retained child ID or null. New internal
admissions must validate the owner's token, lease, current attempt budget, and deadline in the
same transaction as insertion. Terminal chain commits must serialize with admission and link
an existing child before recording failure or expiry. Retention must preserve both that child
and its deduplication record while linkage is unsettled. The built-in stores provide these
operations without additional database tables.

Chain retry settings are part of the runtime fingerprint for that flow and any upstream
flow that reaches it. The fingerprint also includes the saved schema version. Retain the
captured response and diagnostics of terminal chains for operator recovery.

Aggregation `maxGroups` applies to the named rule within its namespace, across all retained
revisions. Emitted and discarded groups count until maintenance retires them; updating an
existing group remains possible at capacity. Each producer checks its configured allowance
against that shared retained count. Low-level store records without `ruleId` use a
namespace-wide admission check and do not consume a named rule's quota. Retirement and updates
share a group lock and recheck the retained record before deletion. An updater that finds its
group retired must pass the new-group capacity check before recreating it.

The built-in stores index aggregation `ruleId`. See [schema compatibility](DATA.md#schema-boundary)
when reusing a store or importing an archive.

## Observability and operating limits

Summaries expose state and counts. Immutable data parts preserve original results, policy
reasons, delivery payloads, diagnostics, lineage paths, and receipt evidence. Snapshot job pages
expose attempt counts; framed exports include attempt metadata. `inspect` provides queue counts
and backlog age. The telemetry observer provides bounded aggregate counts/durations and an application export callback; it omits payloads
and credentials. Sensitive evidence still belongs in the execution store, so deploy storage and
transport access controls appropriate to the application.

SQLite is for a local filesystem; network filesystems and separate hosts require PostgreSQL.
PostgreSQL uses indexed queries, short transactions, scope locks for quotas, and row locks for
claims. Both use leases and fencing. External transport implementations that ignore cancellation
can continue side effects after a timeout; their outcome remains unknown. Custom extensions run
as trusted application code and are not a sandbox.
Owned PostgreSQL pools remove broken idle connections without terminating the host; subsequent
operations can reconnect. Applications retain responsibility for errors on borrowed pools.

Background claims scan at most one nonempty page of 128 candidates per call, ordered by priority,
scheduled time, and job ID. A cursor scoped to the worker and claim filters advances through
blocked pages and wraps at the end, so placement, dependency, or ordering restrictions on early
jobs cannot indefinitely hide later runnable work. Priority applies within each scan; a worker
continues its current scan before wrapping to newly added earlier jobs. Explicit task execution
scans that task directly. Maintenance prunes scheduling cursors after ten minutes of inactivity.
