# Execution storage, inspection, and transfer

## Store choice and retention

The SDK and configured CLI default to a memory store. Configure persistence when execution
state must survive a process exit or be shared by workers:

| Store | Lifetime and use |
| --- | --- |
| Memory | Current process only; requires no database setup. |
| SQLite file | Persistent local file shared by processes on one host. `:memory:` remains volatile. |
| PostgreSQL | Persistent shared database for workers on one or several hosts; requires the optional `pg` peer. |

The store holds submitted data, normalized results, policy decisions, prepared deliveries,
attempts, receipts, deduplication identities, leases, aggregation state, and inspection/transfer
sessions. For example, a saved result can resume output planning after a restart without
repeating inference. Recovery of an interrupted external call can instead require explicit
reconciliation; see [state and recovery](ARCHITECTURE.md#state-and-recovery).
Diagnostic indexes merge concurrent planning and terminal job errors atomically. Diagnostic
origin and notification eligibility metadata are additive within saved schema 3. Recovery
initializes a missing notification deadline only for never-dispatched eligible work; attempted
and terminal delivery budgets remain preserved.

File-backed SQLite and PostgreSQL provide durability in either embedded or hosted use.
Admission acknowledgements, spool/Redis source consumption, and service reload require
durable storage. Memory supports embedded and one-process execution without restart recovery.
Separate CLI invocations need the same persistent store and namespace to inspect or continue work.

Persistence follows retention rules. `limits.retentionMs` and `limits.dedupeMs` default to
seven days; task retention starts on completion. Unknown/held work, unsettled producer links,
and active verified import sessions can retain records longer. Maintenance removes eligible
records in bounded batches, and reclaims expired sessions and unreferenced values. This is execution state with an
explicit lifetime; applications own any longer-term audit retention and database backups.

## Summaries and data parts

`submit`, `run`, `execute`, `get`, listing, and administrative operations return
schema-v3 summaries. A summary includes execution/evaluation state, timestamps,
policy/delivery/chain counts, error count, lineage ceilings, and caller/source
channel status. Summary projection reads bounded metadata without loading application
results, output payloads, or receipt bodies. Execution, validation, and administrative work
such as reprocessing can read the evidence needed by that operation.

Use `get(taskId, { includeData: true, maxInlineBytes: 1048576 })` to include whole
data parts that fit the requested budget. `includeData` defaults to false; the inline
budget defaults to 1 MiB and accepts integers through 64 MiB. The runner's `report`
option supplies defaults; submission context and run/execute options can override it.
`data` contains descriptors plus values. `dataComplete`, `omittedCount`, and
`dataOmitted` explain omissions; at most 25 omitted descriptors are included.
The budget includes part descriptors and values, independently of the small summary.
An optional inspection failure returns the summary with `dataError`.

For larger executions, open a consistent leased snapshot:

```js
const lease = await runner.openSnapshot(taskId);
try {
    let after;
    do {
        const page = await runner.listSnapshot(lease.id, { collection: 'data', after });
        for (const part of page.items) {
            // Select by entity, entityId and path before reading sensitive values.
            if (part.entity === 'task' && part.path.join('.') === 'result') {
                const result = await runner.readValue(lease.id, part.id);
                await useResult(result);
            }
        }
        after = page.next;
    } while (after);
} finally {
    await runner.closeSnapshot(lease.id);
}
```

`listSnapshot` defaults to `collection: 'jobs'`, 25 entries per page, with a maximum
of 100. Job entries contain states and counts. Data descriptors identify the owning
task/job, field path, encoded size, SHA-256, and value kind. Cursors and part IDs
belong to that snapshot. `readValue` reconstructs one bounded JSON value. For byte
streaming, call `readSnapshot(snapshotId, partId, { offset, maxBytes })`; it returns
canonical UTF-8 JSON as base64, up to 65,536 decoded bytes, plus `nextOffset`.
Decode UTF-8 across page boundaries because a character may span chunks.

`preview` validates and plans without inference or delivery. It returns a summary
and a `snapshot` lease for inspecting the prepared request, policies, and outputs.
Close preview snapshots when finished.

Snapshot creation atomically captures task/job metadata and pins immutable values.
Later cancellation, settlement, or live-record retirement cannot change that view.
Reads renew its idle lease. Snapshots are namespace scoped; HTTP/MCP also enforce
the captured flow. Closing or expiry makes reads unavailable. Maintenance reclaims
expired sessions and unreferenced values in bounded batches.

## Limits and storage

`limits.maxBytes` defaults to 1 MiB and can increase through 64 MiB per logical
application value: input form, validation/context, normalized request/result, policy
decision, outgoing payload, receipt body, or completion evidence. JSON is plain,
finite, acyclic data with at most 500,000 nodes and depth 100 per value. Composing
an execution from valid values does not apply that value limit to the whole record.
Generated plans are bounded to 64 MiB; metadata and retained diagnostics to 64 KiB.
Diagnostic origin annotations share that budget. History entries whose details exceed it retain
their code and disposition with `detailsUnavailable: true`; the originating job keeps its own
diagnostic within the same limit. Oversized annotations are summarized before terminal settlement.
Configuration files have an independent 4 MiB limit. Transport limits also apply.
Core result metadata versions, including media schemas 7 and 8, are opaque result
data within Connector saved schema 3. A provider's larger request or JSON-node budget
does not enlarge Connector's per-value limits.
Submission, operation and process protocol wrappers are captured separately from application
values. Their full wire messages still obey the transport byte limit. Source and runtime
identity hashing preserve canonical content without charging wrapper depth to captured values.

Built-in stores keep canonical JSON values in immutable 64 KiB binary chunks and
link them to owning records. Identical values can share storage within a namespace.
Once collection starts, a value is excluded from deduplication and new ownership links
before any chunk is deleted. Re-admitting identical content stages an intact replacement
while later maintenance passes finish removing the old chunks.
PostgreSQL reference pins use compatible key-share locks; collection takes an
exclusive lock on one unreferenced blob at a time. See the PostgreSQL
[row-lock contract](https://www.postgresql.org/docs/18/explicit-locking.html).
PostgreSQL hydrated job and aggregation reads hold their owning metadata row lock until reading
finishes, so concurrent replacement and collection cannot remove their selected evidence.
Aggregation callbacks and subsequent staged writes run after the read transaction ends.
Transactions publish metadata and ownership together; network I/O occurs after
snapshot capture, without holding task/job database locks for a client connection.

Transfer defaults are `maxTransfers: 16` active snapshot/import sessions per
namespace, `transferIdleMs: 300000`, and `maxTransferBytes: 1073741824` wire bytes
per transfer. Completed import sessions retain their slot until closed or expired.
Raise these store limits explicitly for an application's workload. The idle lease
is renewable; it is separate from HTTP response and source-processing budgets.

## Framed export and import

`exportTask(taskId, { signal })` returns an async iterable of byte chunks. It opens
and closes its own snapshot. `importTask(byteIterable, { flow, signal })` verifies
the stream and admits a saved result into an explicitly selected destination flow.
Consume/write chunks as they arrive; collecting them defeats the streaming memory
boundary. The importer also accepts a synchronous iterable of byte chunks.

The wire format is newline-terminated canonical JSON frames with increasing `seq`:
one schema-v3 `header`, fragmented task/job `record` frames, unique value `chunk`
frames sorted by SHA-256 in code point order, and one `end` trailer. Value order is independent
of host locale and database collation. Each frame is at most 256 KiB; record/value
fragments carry at most 64 KiB decoded base64 data. Metadata uses value hashes
instead of physical storage IDs. A SHA-256 chain covers the ordered canonical
frames before the trailer; every value is separately size/hash checked. Missing,
reordered, conflicting, corrupt, oversized, or truncated input fails before admission.
Checksums detect alteration; they do not authenticate an archive's author.

If import reports an invalid value order, create a new archive with `exportTask` or a fresh
snapshot. Reordering existing frames invalidates the integrity chain.

Imports validate the saved result with the destination flow, retaining the original
validation context and normalized evaluated request when present. They create a new
root lineage and do not restore archived jobs, attempts, acknowledgements, or external
effects. The archive checksum supplies a stable destination admission key, allowing
completion retries to find the same imported execution.

MCP and JSON operation clients use explicit sessions:

1. `openSnapshot(taskId)`, then `readExportFrames(snapshotId, { after, limit })`.
   Export pages default to one frame and allow at most eight. Repeating a cursor
   returns the same frames. Close with `closeSnapshot`.
2. `openImport({ flow })`, `appendImport(transferId, frame)` for each frame, then
   `finishImport(transferId)`. Repeating an identical sequence is idempotent;
   conflicting reuse fails. Finish requires the verified trailer and the pinned
   destination revision. Close with `closeImport`, including on failure.

Verified sessions retain their admitted execution and deduplication identity through finish
retries, including a failed completion write. Matching sessions share that protection until all
are closed or expire. Completed sessions also retain their execution while active, so a repeated
finish can return the same task. Closing or expiry releases this retention and prevents a stale
validation from admitting new work for that session.

HTTP exposes `GET /tasks/:id/export` and administrative `POST /tasks/import` as
NDJSON streams. The endpoint binds the destination flow. JSON inspection/session
operations use `POST /operations/:action`. Authentication, Host/Origin policy,
authorization callbacks, namespace/flow restrictions, cancellation, and response
backpressure apply to these endpoints. HTTP `replyTimeoutMs` bounds each complete
transfer and defaults to 10 seconds; configure a suitable budget for large archives.

The CLI writes `export ID` directly to stdout and reads `import --flow NAME` from
`--input FILE` or stdin. CLI `--reply raw` and `--reply final --policy NAME` read the
selected value through a snapshot. Ordinary report output remains a summary.

## Schema boundary

Configuration, reports, persisted stores, process envelopes, and transfer headers
use schema version 3. Use a fresh database/schema for this contract. Opening a
database containing schema-1 or schema-2 Connector tables fails with
`UNSUPPORTED_SCHEMA` before modifying those tables. No automatic migration, reset,
or older archive import is provided. Preserve existing databases and archives
separately when switching contracts.
