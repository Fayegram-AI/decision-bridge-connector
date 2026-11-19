# Changelog

## 0.1.1

- Require Node 22.17 or later. Windows spool sources require libuv 1.51 or later and
  reject incompatible runtimes before claiming input. Compare file identities without
  numeric rounding, preserving exact device and inode checks when rejecting replacements.
- Order exported value hashes independently of host and database collation so archives
  remain importable with numeric database ordering. Keep valid existing export sessions resumable.
- Preserve extension factory prototype metadata and the original `create()` receiver for
  programmatic registration and configured modules, including class private state.
- Verify claimed spool entries as regular files before reading through their opened handles.
  Quarantine symbolic links, nonregular entries and changed file identities while continuing intake.
- Preserve successful process acknowledgements when the child closes stdin before a large
  input finishes writing; failed exits and malformed replies still require recovery.
- Make extension graph fingerprints independent of host locale so compatible workers
  can process the same queued work across deployments with different locale settings.
- Point npm package metadata to the Connector source repository and issue tracker.
- Preserve database collation when paging aggregation retries so retained groups remain
  reachable when string ordering differs from JavaScript.
- Retry retained complete and incomplete aggregation groups on the next flush without waiting
  for the original collection deadline, preserving bounded scans and pending-window deadlines.
- Cancel partially initialized owned MCP transports during startup, including stdio processes.
  Preserve source cleanup failures alongside the primary host startup failure.
- Preserve SDK source ACK evidence through settlement-storage failures; accept failed/submitted
  receipts and retain returned acknowledgement diagnostics. Fence spool claims still being prepared
  when intake closes so their heartbeats cannot survive an unpublished message.
- Apply terminal diagnostic and notification rules to operator-confirmed delivery failures.
  Release extension source captures after preparation ownership ends and accept valid long declared
  revisions without changing established shorter identities. Report Live owned cleanup failures,
  attempt remaining disposal and preserve aggregate SDK causes with sanitized diagnostics.
- Keep source acknowledgement and rejection mutually consistent after reply or settlement-storage failures;
  report returned failed/unknown acknowledgements. Expose aggregate resource cleanup failures through SDK
  shutdown and sanitized CLI diagnostics while attempting all remaining cleanup.
- Retain complete failure diagnostics once, merge concurrent history atomically, and notify on failed or
  expired child admission. Failure connections receive their delivery TTL at first eligibility and remain
  available after business evidence expires; retries preserve that deadline.
- Honor configured MCP input budgets during tool dispatch. Validate Redis source bounds before commands.
  Support direct SDK consumption of built-in iterable sources and retain spool heartbeats until settlement.
- Capture local ESM/JSON extension dependency graphs for automatic reload, including re-exports, cycles
  and literal lazy imports. Retain earlier code generations while their work drains and bound graph loading.
- Preserve hosted reply receivers, validate reply callbacks before admission, and
  pass cancellation context to aggregation replies. Surface unsuccessful replies
  in host diagnostics and SDK source outcomes; SDK consumption withholds completed
  acknowledgement after an unsuccessful reply.
- Retain each policy error once when planning failure notifications.
- Preserve acknowledgement error codes and disposition flags in host diagnostics.
- Preserve offline replay and import of decisions at the JSON-node limit when
  selecting a provider-specific saved-result validation policy.
- Consume Decision Bridge 0.1.8 through public APIs. Support native Gemini selection
  and common schema-7/8 media through existing evaluators, retaining Connector schema 3 and resource limits.
- Resolve built-in provider credential aliases from the configured runner's environment,
  including lazy routes. Preserve explicit credentials and exact environment references;
  prevent isolated runners from borrowing ambient provider credentials.
- Require nonempty string environment selectors and reject malformed UTF-8 configuration.
  Enforce the 4 MiB configuration limit while reading.
- Propagate startup cancellation through configuration, extensions, resource factories and
  host preparation. Close late factory resources and cancel CLI startup on SIGINT/SIGTERM.
- Accept Core 0.1.8 additive responses, usage counters and model-dependent Anthropic
  output defaults through the existing evaluator and Live adapters.
- Resolve incoming process working directories from the configuration directory and capture process settings before consumption.
- Permit source cleanup to release existing leases after ingress stops, forward bounded
  close contexts, and report cleanup failures. Drain yielded Redis settlements before closing owned clients.
- Validate Live flow bindings before startup, defer connections until host activation, and
  admit without executing on ingress-only hosts. Direct attachments support admission-only processing.
- Gate replacement sources until activation and restore overlapping built-in listeners after failed reloads.
- Retain worker generations while programmatic aggregate writes finish, and preserve member value depth
  through aggregation and saved requests.
- Recover expired undispatched claims without consuming their rate reservations.
- Detect conflicts between listener names and their resolved addresses before reload cutover.
- Preserve recovery across reload with custom stores that use the original pending-job query.
- Restore retry state and release rate reservations for abandoned claims.
- Retain large scalar receipt fields alongside boundary-depth response data.
- Enforce listener authentication on SDK HTTP binds and reject conflicting configured
  HTTP and MCP addresses before reload cutover.
- Retain compatible workers for failed deliveries that still allow retry or chained
  admission through reconciliation.
- Preserve the full JSON depth allowance for operator-supplied delivery responses
  and downstream completion evidence.
- Reject malformed bearer credentials before hosting HTTP or MCP. Validate listener settings
  before reload cutover and retain compatible workers while admissions finish.
- Reject messages requiring durable acknowledgement before admitting them on a volatile host.
- Preserve boundary-depth HTTP responses through storage and chaining. Keep confirmed process
  acceptance when an acknowledgement callback stalls.
- Bound execution waits across claim acquisition and return late claims to the queue.
- Report oversized and invalid UTF-8 batch records individually and continue with later records.
- Preserve independent JSON value limits across HTTP/MCP operations, process envelopes,
  source identity hashing and captured flow definitions. Keep existing hashes for normal inputs.
- Checkpoint confirmed process acceptance before optional reply retention, preserving
  completion when the stored response cannot fit its budget.
- Return CLI exit code 1 for runtime/storage/integration failures and 2 for local validation.
- Retain imported executions and admission identities while matching verified sessions remain
  active, and fence stale finish attempts after their session closes or expires.
- Preserve independent application-value limits when consuming iterable source submissions.
- Propagate per-call cancellation through preview, reprocessing, redelivery, reevaluation,
  and evaluation reconciliation, including HTTP, MCP, and CLI preparation paths.
- Release preview snapshot leases when projection fails or cancellation prevents returning
  the preview. Derived executions read task evidence without loading unrelated job bodies.
- Support native OpenRouter decisions, extraction and routing through public Core exports.
- Select extraction providers through the public factory; preserve Cloudflare as
  the default for existing Node configurations. Delegate saved extraction validation
  to the configured provider so optional OpenRouter fields and Cloudflare requirements
  retain their protocol semantics.
- Reuse configured native decision precision policies for saved-result admission.
- Verify explicit paired core/Connector archives in installed consumers.
- Allow package verification from a clean npm cache, with registry fallback for missing
  metadata and dependencies. Explicit `--offline` verification uses a prepared cache.
- Require pinned callable evaluators for request and event admission. Worker dispatch
  requires explicitly registered callable evaluators and connectors. Missing evaluator
  contracts fail before invocation; missing connector contracts hold delivery without
  a recipient attempt.

## 0.1.0

- Named flows with request, event, and existing-result admission, independent policy profiles,
  recipient conditions, projections, bounded transforms, and JSON/text/JSONL output.
- Asynchronous memory, SQLite, and PostgreSQL domain stores with atomic admission/deduplication,
  independent jobs, leases, fencing, revision ownership, capacity limits, and bounded retention.
- Immutable chunked data, summary reports, leased snapshots, and integrity-checked streaming transfers.
- Values undergoing bounded garbage collection are excluded from reuse, preserving complete
  evidence when identical large values are admitted again.
- PostgreSQL evidence ownership remains protected during reads across concurrent aggregation,
  completion updates, and garbage collection.
- Saved-result recovery, independent delivery attempts and receipts, required/optional completion,
  explicit unknown outcomes, reconciliation, reprocessing, redelivery, and reevaluation.
- Embedded SDK, CLI, HTTP, official MCP stdio/Streamable HTTP, Redis Streams, file/spool/process
  sources, and public Live controller integration with optional reconnect lifecycle.
- Persistent host roles, programmatic configuration reload, health and telemetry interfaces,
  durable source aggregation, response chaining, placement, rate, ordering, and dependency controls.
- Live reload rejects a lower stored-value budget before ingress cutover, preserving access
  to evidence admitted by earlier generations; increases remain supported.
- Versioned extension registries, conformance helpers, TypeScript declarations, version 3 schemas,
  installed-package checks, service integration coverage, and independent-process qualification.

Uses Decision Bridge 0.1.5 through its published public APIs.
