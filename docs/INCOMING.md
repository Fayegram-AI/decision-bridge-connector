# Incoming connections

HTTP, file, stdin, process, spool, and Redis sources each bind a named flow. MCP can expose
several permitted flows. Where supported, `inputMode` selects `submission`, `request`,
`event`, or `result`. The binding controls the flow and the runner applies the same admission
checks as SDK calls. Sources and workers can share one process or run in separate hosts
backed by PostgreSQL.

| Source | Configuration and acknowledgement |
| --- | --- |
| HTTP | `flow`, `port`, `host`, `authToken`, `inputMode`, `reply`; durable admission response or complete-mode wait. An `authorize` callback requires `authorizationTimeoutMs`. |
| File | Set `path` and `flow`. The default `jsonl` format reads one record per line. Set `format` to `json` for a single value. The deduplication key includes the configured path and parsed value. JSONL also includes the record index. |
| Stdin | `flow`, `format`; JSONL by default. |
| Process | `process.command`, literal `process.args`, `flow`, `format`; stdout is input and stderr is bounded diagnostics. |
| Spool | `directory`, `flow`, optional `pollMs`, `leaseMs`; atomically claims `.json` files, then archives after durable admission. |
| Redis | `url`, `stream`, `group`, `flow`, optional `consumer`; consumer-group pending recovery and ACK after durable admission. |
| MCP | Set `transport` to `stdio` or `http` and bind `flows` or `flow`. Uses the official SDK for protocol framing. An `authorize` callback requires `authorizationTimeoutMs`. |
| Live | SDK controller attachment or reconnecting factory; public Decision Bridge notifications retain native correlation. |

Incoming process settings are captured when the source is constructed. `process.cwd` resolves
relative to the configuration directory and defaults to that directory; absolute working
directories are preserved. Executable lookup and argument strings remain literal.

File, stdin, and process inputs accept one JSON value with `format: 'json'` or one
JSON value per line with `format: 'jsonl'`. Their `maxBytes` defaults to 1 MiB per
value or line, respectively. A malformed JSONL record stops that source reader.
Use the [CLI batch command](NODE.md#commands) for per-line parse errors and continued reading.

Spool producers should atomically rename complete files into the directory. Processing files
have heartbeat leases, stale claims are reclaimed, and malformed records move to quarantine.
A definitive input rejection during admission also quarantines the original file. For
example, a PDF exceeding the configured Cloudflare extraction request limit is quarantined
before the provider is called. When the queue is full or storage fails transiently, the input
stays available for retry. If a producer reuses a filename, returning the old claim preserves
the new file and leaves the old claim in the processing directory. Use a local filesystem
with atomic rename and hard-link support.
Processing-directory recovery advances through bounded pages of claims, so live leases in an
earlier page cannot indefinitely hide expired claims later in the directory.

Redis streams use bounded reads, reclaim stale pending entries, and quarantine definitive invalid
input. Transient or uncertain admission failures remain pending; stream/group/entry identity is
retained in admission keys.
`pollMs` defaults to 100 and must be a positive 32-bit millisecond integer. `claimIdleMs`
defaults to 60000 and must be a nonnegative safe integer; zero reclaims pending entries immediately.
Invalid names, timing settings and supplied client methods fail before Redis commands.
Quarantine defaults to `<stream>:quarantine`. The source writes the entry ID and error
there before acknowledging the original entry. Retain the original stream if you need to inspect
the submitted payload by ID. The quarantine destination must differ from the input stream.
Each Redis source instance
supports one consumer. Aborting its open signal or calling `close()` cancels setup, claims,
polling and remaining batch processing; a supplied client stays open. Messages already yielded
retain independent acknowledgement and rejection budgets during ingress drain.
An owned Redis connection stays open while yielded messages settle during `close()`.
Direct close defaults to 10 seconds; `close({ timeoutMs, signal })` supplies a different budget.
Hosts supply `drainMs`, and SDK consumers supply `closeTimeoutMs`. Repeated close calls share
the first cleanup operation and budget. Expiry or cancellation aborts settlement, closes the
owned client and rejects close; later callbacks fail. Cleanup does not acknowledge unsettled
entries. With a supplied client, source closure leaves the connection and yielded callbacks
available to the application.
Construct a new source instance when restarting a stopped Redis consumer; a supplied client
can be reused if it remains connected.

## Custom sources

An iterable source implements `open({ signal })` and yields messages containing `input`, plus
optional `acknowledge`, `reject`, and `reply` callbacks. A hosted source may instead implement
`start({ runner, signal, name, role, whenActive })` and `close({ signal, timeoutMs } = {})`.
Existing zero-argument cleanup methods remain supported. Iterable sources may also implement `close()`;
the host calls it during ingress shutdown to release resources and unblock reads. Acknowledge means source consumption,
reply means communication with the original caller, and an outgoing connection is a separately
configured recipient. They have distinct lifecycles. A source may be consumed through
`consumeSource` or a persistent host; support cancellation and resource ownership explicitly.

Message callbacks run with the original message as their receiver. Reply callbacks
receive the reply value and `{ signal }` cancellation context, including when an
aggregation has not created an execution yet. `consumeSource` reports failed or
unknown replies as reply-stage outcomes and withholds `acknowledge: 'completed'`.
Hosted sources record those failures through `onError` and host error history.
Host diagnostics retain recorded acknowledgement error codes and disposition flags.
Already-recorded admission acknowledgements remain valid.
Failed or unknown acknowledgement receipts without error details produce `ACK_FAILED` or
`ACK_UNKNOWN` diagnostics. A reply or storage failure cannot reject an envelope whose
acknowledgement succeeded or may have succeeded. Individual message failures remain separate
from source readiness.
ACK callbacks may return an object with `status: 'acknowledged'|'submitted'|'unknown'|'failed'`,
optional `receipt` evidence and optional error details. Omitted status means acknowledged;
`submitted` is retained as unknown. SDK consumers preserve external ACK evidence independently
of whether recording that evidence succeeds. Receipt bodies retain their own application-value limits.

Incoming submissions preserve the independently bounded application values described in
[storage limits](DATA.md#limits-and-storage); the envelope does not consume a value's depth allowance.

A hosted source opens its resources in `start` and returns when setup is ready. Schedule
consumption after `whenActive` resolves. Waiting for it inside `start` would block activation.
The host rejects mutations through the supplied runner before activation, and `whenActive`
rejects if startup is abandoned. During reload, custom hosted sources must be able to stage
their resources while the old source remains active. Built-in HTTP and MCP listeners can
pause and reopen an overlapping endpoint.
If startup fails, the host attempts to close each source whose startup was attempted. When cleanup also fails,
the returned `AggregateError` preserves the startup error as its cause and includes cleanup failures.
After an activated source stops, its supplied runner still permits releasing existing snapshot
and import leases through `closeSnapshot` and `closeImport`, including their store equivalents.
New work and new leases remain fenced. Cleanup receives a separate cancellation signal, so
ingress cancellation does not prevent releasing resources within the close budget.

For SDK consumption, `createIterableSource(iterable, { name, revision })` supplies the incoming
connector metadata. Built-in file, stdin, process, spool and Redis sources also support direct
`consumeSource(runner, source)` use. Their `name` defaults to the source type and `revision` to
`'1'`; choose distinct names for separate intake identities. `ConsumableIncomingConnector`
requires metadata and `open()`. HTTP/MCP listeners use the hosted-source contract instead.
Routing compatibility and durability are checked before SDK consumption opens a built-in source.
Spool and Redis require durable storage even when acknowledgement is left to the application.
Redis acknowledgement returns an acknowledged receipt containing stream, group, entry ID and ACK count.
`consumeSource` defaults to four concurrent messages and acknowledgement
after completed processing. `acknowledge: 'admitted'` requires durable storage; `'none'` leaves
acknowledgement to the application. With a durable runner, acknowledgement attempts and optional
receipt evidence are persisted in `sourceAcknowledgement`; summaries expose status and
[snapshot inspection](DATA.md) exposes retained evidence.
An uncertain acknowledgement stays `unknown` for that attempt. A redelivered transport
envelope may trigger another acknowledgement for the same deduplicated task. Callbacks must
tolerate repetition; task deduplication does not establish a transport-wide ACK identity.
Spool claims retain heartbeats while yielded messages await settlement, including concurrent
consumption. Repeating the same settlement shares its result; conflicting settlement is rejected.
Close stops intake and drains unsettled claims within its budget, then releases their heartbeats.
Unsettled input remains recoverable after a drain timeout. Standalone consumers must close the source.
Close also fences claims being read or fingerprinted; unpublished claims stop heartbeating and
remain available for lease recovery.

Consumption deadlines are configurable independently: `processingTimeoutMs` and `sourceTimeoutMs`
default to 60000, `acknowledgementTimeoutMs` and `closeTimeoutMs` to 5000, `replyTimeoutMs` to 10000,
and `observerTimeoutMs` to 1000. Reply callbacks receive a cancellation signal, and backpressure
allows the configured reply budget before pulling another message. `onOutcome(outcome, { signal })`
receives each message's acknowledgement disposition; observer failures cannot change task results.
Cancellation and processing deadlines also reach admission validation. The submission context
in `runner.submit(input, { source, signal })` accepts an optional AbortSignal. Cancellation during
validation prevents admission. A storage operation already in progress may still commit;
cancellation does not withdraw admitted tasks. Known mapping/validation failures are marked
as permanent input errors for spool/Redis quarantine; capacity and uncertain storage failures
remain available for recovery.

## Durable aggregation

```json
{
    "aggregations": {
        "assessment": {
            "revision": "1", "flow": "review", "sources": ["scan", "rules"],
            "windowMs": 30000, "dedupeMs": 86400000, "maxGroups": 1000,
            "duplicate": "reject", "incomplete": "emit", "late": "reject"
        }
    },
    "incoming": {
        "scanner": {
            "type": "spool",
            "options": { "directory": "./scans", "flow": "review", "inputMode": "event" },
            "aggregation": { "rule": "assessment", "member": "scan", "correlationPath": "caseId", "eventIdPath": "id" }
        }
    }
}
```

This is a configuration fragment; provide the target flow and the other member source. The flow
receives `{ aggregateId, status, correlationKey, events, missingSources }`, where each event member
contains its `eventId` and original `event`. Each member keeps its own depth allowance.
All members share the byte and node limits for the generated event. Define the flow's event
template for that shape.
Membership requires an event supplied by every configured source. Source names such as
`constructor` and `toString` are valid identifiers and have the same membership requirements;
an absent source appears in `missingSources` when an incomplete group is emitted.
`member` defaults to the incoming registry name and must belong to the selected rule. An explicit
`eventIdPath` selects the member's semantic event ID, even when the transport supplies an
idempotency key. Without that path, the host uses the input's idempotency key. `correlationPath`
and `eventIdPath` use the shared dotted-path or JSON-pointer syntax; missing or invalid values
in a message are rejected as input errors. Keep semantic IDs stable when the same evidence can
arrive through different files or stream entries.
Retained groups preserve semantic event-ID history, including replaced contributions.

With a durable store, membership and the stable emission ID survive restarts. Emission remains
idempotent even if a worker dies between child admission and marking the group emitted.
The memory store limits these guarantees to the current process; see [store choice](DATA.md#store-choice-and-retention).
Group capacity
includes retained closed windows; maintenance clears expired records. An un-emitted group remains
retained when admission fails, even beyond `dedupeMs`. The deduplication interval starts after
successful emission or discard; concurrent flushes and emission completions cannot alter a
replacement group. Complete or incomplete un-emitted groups are eligible for the next flush,
even before their original collection deadline; pending groups wait for that deadline.
Each flush attempts at most 100 due groups. A failed group remains retained;
the flush attempts the rest of its page before reporting the first failure. Successful emissions
remain committed when another group fails. Store scans advance through due groups and wrap,
so retained failures cannot indefinitely hide later groups. Retirement scans also advance past
pinned groups to remove eligible emitted or discarded windows.
`duplicate` accepts
`reject`, `ignore`, or `replace`; repeated member event IDs are idempotent, including superseded IDs retained by `replace`.
Replaying an older replaced event cannot overwrite the latest evidence. Incomplete
windows default to discard; `emit` submits explicit incomplete evidence. Late evidence requires
a new correlation group. The separate portable `createAggregator` utility is in-memory; use
`createDurableAggregator` for restart persistence.
Rule names accept the shared 256-character identifier limit. Internal source names use a stable
digest when adding the aggregation prefix would exceed that limit; group metadata retains the
full rule name.
