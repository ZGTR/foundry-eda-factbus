# factbus design

Short version: a producer writes each fact to an outbox table in the same transaction as
its own data. factbus publishes the fact to an EventBridge bus after the commit and sweeps
up anything that missed. Each consumer has its own SQS queue and a DynamoDB (or SQL)
inbox, so it applies each event once. Side effects that must not repeat go through
`once`.

The code cites this document as "design §N" and cites the decision IDs in the last
table. Each ID names a rule plus the test or measurement behind it, so a comment like
`(K3)` points at the reason, not at a private document.

## §1 Publishing

**Answer: commit first, publish right after, sweep for anything missed.**

1. The service inserts its domain row and the outbox row in one transaction
   (`SqlProducer.stage`). A fact exists if and only if the write it describes committed.
2. After `COMMIT`, and before the response returns, the producer reads the staged rows
   back and calls `PutEvents`: one call per 10 entries, each under 1 MB. Accepted
   entries are marked sent. This step never raises into the caller (R6-4).
3. A scheduled sweeper runs every minute. It locks unsent rows older than 30 s with
   `FOR UPDATE SKIP LOCKED`, one chunk of at most 10 at a time, up to 500 rows per run.
   Overlapping sweeps skip each other's rows (K5). Sent rows are purged after 7 days.

Publishing is **at least once**. The sweep and the commit path do not lock against each
other. A transaction that stays open past 30 s and then commits can be published by
both, and a crash between `PutEvents` and the mark republishes on the next sweep.
Consumers absorb the copies on the event `id` (K4). That is why the inbox exists.

"Sent" means **accepted by the bus**. Delivery to targets is each consumer's job, not
the outbox's.

| Outcome | What the row does | Why |
| --- | --- | --- |
| Accepted | `sent` | normal path |
| Content reject (`MalformedDetail`, `InvalidArgument`, over 256 KB) | `failed`, never retried, metric `OutboxRowsFailed` | R7-26 |
| IAM or account reject | stays `unsent`, metric `OutboxDeploymentRejected` | config problem, so failing the rows would strand the backlog |
| Throttle or network error | stays `unsent`; the next sweep retries | the sweeper is the retry layer; the SDK is built with `total_max_attempts=1` (P10) |

`PutEvents` to a missing bus answers HTTP 200 and drops the event (P11). The publisher
checks the bus with `DescribeEventBus` before every sweep and at least every 60 s on the
commit path, and publishes nothing to a bus it cannot confirm (C0b-18). A service that
wants a startup check calls `producer.readiness.confirm_now()` itself. If the bus is deleted while the service runs, at
most 60 s of rows are marked sent and lost. `republish_since` recovers them, and
consumers absorb any copies that did arrive (K4).

## §2 Consuming

**Answer: one queue per consumer, dedupe on the event id, report failures item by item,
and back off by receive count.**

Each consumer subscribes with one EventBridge rule on the bus, matched on `source` and
`detail-type`. The rule feeds the consumer's own queue and DLQ. For each record in a
batch:

1. **Dedupe** on the envelope's `id` (legacy: `event_id`), never on EventBridge's own
   `id`, which changes on every replay (K1). A copy is acknowledged without running the
   effect.
2. **Version guard** (K3). If the service supplies `version_of`, an event at or below
   the subject's applied version is dropped as a success. A status only moves forward
   by rank.
3. **One transaction**: the inbox record, the version update and the service's own
   writes commit together or not at all. If two workers race on one event, one effect
   happens and the other reads DUPLICATE (C0b-17).
4. **Failure**: the handler sets the message's visibility to the backoff wait for its
   receive count and reports only that item in `batchItemFailures`. If
   `ChangeMessageVisibility` fails too, the item is still reported (C0b-19).

Backoff after receives 1 to 9: 30 s, 2 m, 10 m, 30 m, 1 h, 2 h, 2 h, 2 h, 2 h. Receive 10
dead-letters, so a poison message reaches the DLQ after about 9.7 h (F9, F13). Test
profiles may scale the schedule, and the scaled schedule reports itself as not real
(X3).

For effects that call a rate-limited provider there are two more parts, available when
you build `factbus.application.Consumer` yourself (the `@consumer` decorator does not wire
them). A breaker opens
after 5 provider errors in a minute and then sends one probe per 30 s. A per-instance
token bucket blocks rather than defers, because a deferral spends a receive. Neither
disables the event source mapping: a handler that switches off its own trigger needs a
second function to switch it back on.

## §3 Side effects that must not repeat: `once`

**Answer: claim before the call, treat an unknowable outcome as final.**

An idempotency cache that expires an "in progress" record runs the function again after
a crash, and that repeats the side effect. `once` instead:

1. claims the key with a conditional write (`pending`, owner, deadline);
2. calls the function once. `CertainlyNotSent` releases the claim. Any other exception
   marks the key `unknown`, and nothing calls it again;
3. records `done` with the JSON result, which every later call returns.

A crash between the claim and the record leaves `pending`. A later call past the
deadline turns it into `unknown` without calling. The guarantee: at most one attempt
that might have taken effect, per key, while the row is retained. Retention must
outlive every replay window (queue plus DLQ retention plus any archive replay).

With `DynamoClaimStore`, claims live in the consumer's inbox table under `claim#<key>`,
so one table per consumer holds inbox, version and claim items. `SqlClaimStore` keeps
them in one SQL table (`upgrade_claims_0001`) with the same contract: the claim is an
insert that does nothing on an existing key (`ON CONFLICT DO NOTHING` on PostgreSQL,
`ON DUPLICATE KEY UPDATE claim_key = claim_key` with an owner read-back on MySQL), then a
takeover only of a row past its `expires_at`; `settle` and `release` are updates and
deletes conditional on owner and `pending`. Both engines are proved in
`tests/integration/test_sql_claims.py`, including the claim race and a crash after the
claim.

Inside a consumer, an `Unknown` raised by `once` fails the message like any other error,
so it retries with backoff (each retry raises `Unknown` again without calling) and
reaches the DLQ after about 9.7 h. The DLQ alarm is how an operator learns of it.

## Retention

One rule: **inbox and claim retention must exceed every way an old event can come
back.** Defaults: inbox and `once` keep 35 days. Queue and DLQ keep 14 days each (28 in
total). The bus archive keeps 30 days and refuses 35 or more.

The consumer enforces the rule itself rather than trusting every replay path: it refuses
an event older than `max_event_age` (30 days) before it reads the inbox, and reports it
as a batch item failure, so it backs off and reaches the DLQ like any other failure. The
age counts from the earliest timestamp the event carries (EventBridge `time`, envelope
`time` or `occurred_at`); an event with none is malformed. SQL tables are not expired by
TTL, so `SqlInbox.purge` and `SqlClaimStore.purge` delete rows older than 35 days in
bounded batches; the five days between the two limits are the margin. A claim row whose
own `once` retention is longer than the purge window is kept.

## Decision index

| ID | Rule | Proof |
| --- | --- | --- |
| A7 | The architecture gate lives in this repo (`scripts/check_eventing_boundaries.py`, 9 clauses), because generic service gates refuse a library this small | the gate's own blinded fixtures |
| A14 | One SQL implementation for PostgreSQL 16 and MySQL 8.0 | `tests/integration` on both engines; failing, never skipping, when an engine is absent |
| C0b-5, F11 | The bus archive replays history, and the inbox absorbs the replay | archive kept 90 days |
| C0b-15 | The kill switch lives in IaC, so the next apply does not quietly turn a thrown switch back on | `enabled` on the `consumer` module; `docs/kill-switch-runbook.md` |
| C0b-17 | A unique-key or conditional-check conflict on the inbox record is DUPLICATE, not an error | `test_sql_inbox`, `test_dynamodb_inbox` |
| C0b-18 | Nothing is published to a bus that cannot be confirmed | `test_readiness` |
| C0b-19 | A failed visibility change still reports the item | `test_consume` |
| E-3 | The kit never knows which field is the version; the service injects `version_of` | gate clause 6 |
| F7, K1 | Dedupe on the envelope id, which survives an EventBridge replay | `test_consume` |
| F8, K3 | Older versions are dropped; status moves forward only | `test_consume`, inbox suites |
| F9, F13 | Real backoff schedule, DLQ after receive 10 | `test_consumer_domain` |
| K-1 | The kit imports no contract or product package | gate clause 6 |
| K4 | Publishing is at least once; the consumer inbox absorbs a double publish | inbox suites, `make e2e` replay arm |
| K5 | Overlapping sweeps never publish a row twice | `test_sql_outbox` (integration) |
| O3 | LocalStack's Lambda runtime is not the proof of a real event source mapping; a real account is | `examples/hello` reports which path ran |
| P2, P3 | LocalStack 3.8's v2 EventBridge provider supports archive and replay | measured locally |
| P10 | `PutEvents` responses can carry fewer entries than sent; the kit does not assume one per entry. The SDK's own retries are off | `test_outbox_domain` |
| P11 | `PutEvents` to a missing bus answers 200 | `test_eventbridge_localstack` |
| R1-AGE | An event older than `max_event_age` (30 days) is refused before the inbox; inbox and claim rows are purged only after 35 days | `test_consume` (age arms), `test_sql_claims` (purge during consumption) |
| R6-4 | Publish after commit and before the response: Lambda freezes the environment once the response returns | `test_publish_and_sweep` |
| R7-26 | A permanent content reject is `failed` and never retried | `test_publish_and_sweep` |
| X3 | Scaled backoff exists for tests only and says so | `BackoffSchedule.is_real` |
