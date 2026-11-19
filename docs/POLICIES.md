# Mapping, application policy, and explicit aggregation

These portable APIs accept JSON configuration and run without Node-specific imports.
Built-in projections and conditions do not execute expressions as JavaScript. Trusted
module extensions are separately registered. `runner.ready` validates flow templates
and policies before admission, then each execution snapshots its configuration.

## Request templates and projections

`mapRequest(event, template)` maps an external event into a request. A template has
an optional `id`, a required printable `revision`, and a `request` projection:

```js
const template = {
  id: 'failure-triage',
  revision: '1',
  request: {
    state: { log: { $path: 'detail.log' } },
    questions: {
      $literal: {
        route: {
          type: 'choice',
          instructions: 'Classify the failure using only the evidence.',
          criteria: {
            dependency: 'A required dependency is missing.',
            assertion: 'An assertion failed.',
            unknown: 'The evidence does not establish a category.',
          },
        },
      },
    },
  },
};
```

The mapper requires the projected request to be an object. The selected evaluator
then performs operation-specific request and capability validation. Mapping itself
does not select a model, invent missing questions, or contact a provider. Keep task
IDs, correlation, source identities, and template revisions outside the strict
Decision Bridge request.

For a flow, `template` is the request projection itself: use the `request` member of
the standalone template above. The flow passes that projected value to its evaluator;
an external evaluator can define a different request contract. See [evaluators](EVALUATORS.md).

`project(value, specification)` uses the same language for outgoing payloads:

| Specification | Behavior |
| --- | --- |
| `{ "$path": "answers.route.choice" }` | Select a field; missing fields fail. |
| `{ "$path": "/answers/route/choice" }` | Use JSON Pointer; `~0` and `~1` escape `~` and `/`. |
| `{ "$path": "$" }` or `{ "$path": "" }` | Select the entire input. |
| `{ "$path": "optional", "required": false }` | Use JSON `null` if the field is absent. |
| `{ "$path": "optional", "default": { "value": 0 } }` | Use the literal default only when absent; existing `null` is preserved. |
| `{ "$literal": { "$path": "ordinary data" } }` | Preserve an entire literal value without interpreting its members. |
| Other objects and arrays | Recursively construct an output object or array. |
| String, number, boolean, or null | Copy the literal value. |

Dollar-prefixed object keys require `$literal` unless they are documented
projection directives. Property traversal rejects prototype-related segments.
Functions, accessors, sparse arrays, cycles, and lossy JSON values are rejected.
Outputs are copied and deeply frozen. Projection paths operate on the selected
payload, so a `raw-result` connection uses `answers.route.choice`, while a
`final-decision-with-result` connection uses `result.answers.route.choice`.

Outgoing payload selection is independent of SDK report formatting. A connection
with `payload: 'report'` receives `{ taskId, evaluationId, policies, errors, result }`;
it is a planning payload that can contain application data. It is subject to the
outgoing value limit and any configured projection/transforms. SDK summaries and
`includeData` options are described under [inspection](DATA.md#summaries-and-data-parts).

## Application decisions

`applyPolicy(result, policy, { operation = 'decision', correlation, questionRevision })`
consumes a result already validated by the operation's evaluator and returns an
immutable policy record:

```js
{
  status: 'accepted', // alternatively 'review' or 'abstained'
  policyId: 'failure-triage',
  policyRevision: '1',
  outcome: { route: 'dependency' },
  reasons: [/* evaluated predicates and the selected rule */],
}
```

A policy has a required `revision`, optional `id`, optional `abstain`, `review`,
and `accept` conditions, optional `otherwise`, and optional `outcome` projection.
Matching rules have explicit precedence: **abstain, then review, then accept**.
If no rule matches, the default is `abstained`; `otherwise: 'review'` is also
supported. An outcome projection is evaluated only for an accepted decision.
There is no default acceptance and no `otherwise: 'accepted'` setting.

Optional `correlation` JSON and a printable `questionRevision` are copied into
every decision disposition. The runner supplies the admitted `correlationId` as
`correlation` and the explicit `questionRevision`. Flow defaults, locks, and override
permissions govern these fields. They remain outside the bridge request.

```js
const policy = {
  id: 'failure-triage',
  revision: '1',
  abstain: { question: 'route', type: 'choice', value: 'unknown' },
  accept: {
    question: 'route', type: 'choice',
    minProbability: 0.90, minMargin: 0.20,
    abstainOptions: ['unknown'],
  },
  otherwise: 'review',
  outcome: { route: { $path: 'answers.route.choice' } },
};
```

The thresholds in these examples are illustrative configuration. Choose thresholds
using evaluation data and the application’s requirements.

The raw result is preserved. A valid but uncertain answer produces review or
abstention according to policy; malformed policy, missing questions, incompatible
types, or failed outcome projections produce a policy-stage operational error.
For an eligible probability source, every referenced condition is checked even
when another branch matches, so an unknown question cannot hide behind
short-circuit boolean evaluation.

### Conditions

Conditions combine using `{ all: [...] }`, `{ any: [...] }`, or `{ not: condition }`.
`all` and `any` require nonempty arrays. Each leaf requires an actual comparison;
an empty question predicate cannot accidentally authorize an action.

| Question type | Available comparisons |
| --- | --- |
| `choice` | `value` equals the selected typed value; `minProbability` bounds its probability; `minMargin` bounds its margin over the runner-up; `abstainOptions` excludes listed selections from that predicate. |
| `noul` | Inclusive `min` and/or `max` bounds on the probability. |
| `score` | Inclusive `min` and/or `max` bounds on the rubric score; paired `atLeastLevel` and `minMass` bound the probability mass at or above a rubric level. |
| `box` | `minConfidence`, if supplied by the provider; a normalized `region` with `relation: 'inside'` or `'intersects'`. `inside` is the default. Intersections must have positive area. |

Choice values retain their primitive type: boolean `true` and string `"true"` are
different values. Unknown `value` or `abstainOptions` entries are configuration
mismatches at evaluation. `abstainOptions` is an exclusion on a choice predicate;
use a top-level `abstain` condition to produce the distinct abstained status.

Noul acceptance can establish either answer. For example, accepting `max: 0.10`
or `min: 0.90` with a review default creates a review band between the thresholds.
An accepted result below the lower threshold establishes the false answer; the
status `accepted` itself never means true.

Scores use rubric indices `0` through `N - 1`, not an implicit unit scale.
`atLeastLevel` accepts a nonnegative numeric index or an exact, unique label from
a labelled `scoreLevels` legend. Probability keys remain numeric index strings,
including for labelled rubrics. Legacy text-only rubric descriptions do not become
label identifiers. A score policy checks the legend/probability index structure and
score consistency; the originating evaluator owns validation against the request's
exact rubric and revision.

Box coordinates are normalized fractions. A returned box does not establish that
an object is present. Combine the geometry predicate with a separate presence
`noul` condition when presence matters:

```js
const accept = { all: [
  { question: 'present', type: 'noul', min: 0.95 },
  { question: 'target', type: 'box', minConfidence: 0.90,
    region: { top: 0, bottom: 1, left: 0, right: 0.5 }, relation: 'inside' },
] };
```

### Operation-specific and provenance rules

Extraction and Live retain their own payloads. Apply explicit path predicates,
such as `{ path: 'extraction.answers.amount.value', min: 10 }`, with the matching
`operation`. A path predicate supports `equals`, `min`, and `max`. Missing paths
are errors by default. For a field that is optional by its native contract,
explicitly set `missing: 'unmatched'`: an absent path then produces a false
predicate with a `missing: true` trace. It does not invent a numeric value or
suppress validation of other predicates. Existing numeric fields must still be
finite numbers; an existing `null` or string is a type mismatch for `min`/`max`.
Equality compares JSON content. Question predicates are restricted to the
`decision` operation.

For example, a Live refusal can match an explicit refusal `abstain` rule while an
acceptance condition on optional native `probability` uses `missing: 'unmatched'`.
Every configured rule is still evaluated and checked. `missing` is available only
on path predicates, and only the literal value `'unmatched'` is supported.

Decision policies require `meta.providerKind` and `meta.probabilitySource`.
Generative results must retain `elicited` probabilities and cannot contain native
answer confidence. Optional `allowedProbabilitySources`, for example `['native']`,
produces an explicit abstained eligibility result when the source is not allowed.
Eligibility is checked after policy and provenance validation, before evaluating
result predicates. This lets a policy requiring native box confidence abstain on
an excluded generative result, which has no native confidence. An allowed source
must still provide every field its predicates require. This setting is available
only for decision operations.

Policy evaluation does not normalize probability distributions or reinterpret
their source. It deliberately does not revalidate an already validated result using
a guessed probability-sum tolerance. Loading untrusted saved records requires the
evaluator's import validation and trusted validation settings, including the
provider's declared probability precision. A serialized result must not select its
own validation tolerance. The policy API is not a substitute for result validation.
The adapter can select trusted import validation options from the resolved saved
request; see [saved-result recovery](./ARCHITECTURE.md#state-and-recovery).

### Custom result formats

Built-in decision policies require the answers and provenance described above,
including when their predicates use JSON paths. An external provider's arbitrary
JSON can instead be interpreted by a registered policy module. Configure the flow
profile as `{ type: 'module', name: 'external-policy', options: {} }`; register
`external-policy` in the SDK `policies` registry, or as a configured policy extension.
Its instance declares `revision` and
`execute(result, options, { signal })`, returning a policy decision with `status`
`accepted`, `review`, or `abstained`, `reasons`, and `policyRevision`. The runner adds
the evaluation/policy identities. Saved-result validation remains the evaluator or
validator's responsibility; see [external evaluators](EVALUATORS.md#external-evaluators).

## Explicit fan-in

Incoming tasks remain independent unless the application explicitly uses
`createAggregator(rule)`. The aggregator is an in-memory, bounded fan-in utility;
it does not silently turn every connector into a shared evidence source.

```js
const aggregator = createAggregator({
  id: 'test-and-log-evidence', revision: '1',
  sources: ['tests', 'logs'],
  key: 'correlationId',
  windowMs: 30_000,
  dedupeMs: 60_000,
  maxGroups: 1000, maxEvents: 10_000, maxBytes: 8 * 1024 * 1024,
  duplicate: 'ignore', late: 'reject', incomplete: 'emit',
});

const outcome = aggregator.add({
  source: 'tests', eventId: 'tests-123', correlationId: 'deployment-456',
  event: { failures: 2 },
});
```

Every configured source is required. Each group retains one current event per
source, ordered by the `sources` list when emitted. Envelopes accept `source` as
a string or `{ connector, eventId? }`, a required event ID, the `event`, and
optional `correlationId`, `causationId`, and `occurredAt` fields. A correlation path
must resolve to a nonempty string or finite number; numeric `1` and string `"1"`
are separate keys.

The window measures processing time from the first admitted event. `occurredAt`
is carried as source data and does not change window timing. The aggregator owns
no timer: call `flush()` regularly to expire idle groups. Tests and controlled
hosts can pass `{ now: epochMilliseconds }` to `add`, `flush`, and `close`.
Backward wall-clock changes are clamped to the last observed time.

| API | Result |
| --- | --- |
| `add(envelope, { now? })` | `{ status, aggregate?, aggregateId?, reason?, expired }`; status is `pending`, `complete`, `duplicate`, or `late`. `expired` contains window outcomes discovered during this call. |
| `flush({ now? })` | Array of newly expired `incomplete` or `discarded` aggregates. |
| `inspect()` | Pending/tombstone counts, retained event IDs, serialized byte count, and window summaries. |
| `close({ now? })` | Emits unfinished outcomes once, releases state, and rejects future additions. |

Complete and expired aggregates carry the aggregation ID, rule revision,
correlation key, source/event identities, ordered evidence, missing source names,
and timing. Feed a `complete` aggregate into an explicitly configured task/template.
Choose deliberately whether an `incomplete` outcome is usable evidence. A
`discarded` outcome is a processing report, not an instruction to evaluate.

Duplicate behavior is explicit:

- `ignore` retains the first contribution from each source.
- `reject` throws for a repeated event ID or a second contribution from that source.
- `replace` replaces a source's contribution with a new event ID. A repeated event
  ID remains a duplicate and never reopens or updates a completed aggregate.

Reusing a retained source event ID under a different correlation key is an error.
Source event IDs must identify immutable source events; the aggregator recognizes
IDs and does not authenticate their content. The runner's task idempotency check remains
separate.

With `late: 'reject'`, a new event for a closed group is reported as `late` while
the tombstone exists. With `late: 'new-window'`, it starts a new generation; old
event IDs remain recognized during their retention period. `incomplete: 'emit'`
reports expired partial evidence as `incomplete`; the default `discard` reports it
as `discarded`.

`dedupeMs` defaults to `windowMs`. Completed groups release evidence and retain
source event ID tombstones for `dedupeMs` from completion; expired groups retain
them for `dedupeMs` from the deadline. Duplicate and late-event guarantees end when
tombstones expire, after `close()`, or when the process restarts. This utility does
not claim durable aggregation or exactly-once execution.

`maxGroups` includes tombstones. `maxEvents` includes retained IDs from replacements
and completed groups. Events rejected, ignored as a second source contribution,
or reported as late are not admitted or retained. `maxBytes` bounds retained
serialized JSON, not JavaScript engine allocation overhead. Capacity exhaustion
rejects admission atomically and requires backpressure or expiry; it never
silently evicts a live deduplication record. A rejected admission also cannot
consume an expired aggregate's emission.
