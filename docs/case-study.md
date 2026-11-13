# Case study: StreamFlow

*Processing a Kafka stream into PostgreSQL without losing or doubling events, and proving it.*

StreamFlow is a portfolio project I built in seven phases: design, streaming core, reliability, dashboard,
performance, hardening and final verification. This case study explains the problem, the design and what I learned.
Every number below comes from committed evidence, and each section links to it.

## 1. Problem and motivation

"Consume from Kafka, write to a database" sounds simple until something fails:
- A worker dies after writing to the database but before committing its Kafka offset, so the event is delivered
  again.
- The database is down for 20 seconds. A naive retry policy dead-letters thousands of healthy events.
- A malformed record can block its partition forever, or be skipped silently.
- Kafka retention deletes events nobody has read yet.
- At the end of all that, nobody can say whether every event actually arrived.

I wanted a project where these failure modes are the subject: deliberately triggered, automatically tested and
honestly documented. It also had to show Java, Spring Boot and Kafka, which my earlier projects (Python and FastAPI)
do not.

**Scope, by design:** a local, single-user system on one laptop. That means no cloud, no paid services, a single
broker and a single database. Production limitations are documented rather than hidden.

## 2. Architecture and technology choices

The [architecture diagram is in the README](../README.md#architecture).

One Spring Boot 4 application is built into one image and run in separate **runtime roles**:
- `migrate`: schema, topics and initial consumer offsets;
- `api`: REST, WebSocket, simulations, replay and retention;
- `worker`: processing and dead-letter ingestion;
- `generate`: one seeded run;
- `loadgen`: benchmarks.

Workers are separate processes so they can be killed and scaled ([ADR-0001](adr/0001-single-codebase-runtime-roles.md)).
ArchUnit tests forbid dependencies between role packages.

Choices that shaped everything else:

| Choice | Why | Rejected alternative |
|---|---|---|
| Spring JDBC (`JdbcClient`), explicit SQL | The guarantee rests on what happens inside one transaction; it should be visible in the code ([ADR-0002](adr/0002-spring-jdbc-over-jpa.md)) | JPA (hidden flushes and caching) |
| Deduplication ledger in PostgreSQL | The database enforces "applied once" with a primary key | Kafka transactions: they cannot include PostgreSQL, so they cannot give end-to-end exactly-once |
| Versioned JSON envelope, strict decoding | Simple, inspectable, and validated | Schema Registry and Avro (an extra service) |
| Testcontainers with real Kafka and PostgreSQL | Spring Kafka's behaviour had to be verified, not assumed | Embedded or mocked brokers |
| Single-node KRaft, Docker Compose, memory limits per container | Runs on a laptop with 11.7 GB of usable memory ([ADR-0012](adr/0012-local-first-resource-budget.md)) | Kubernetes |

## 3. The event-processing pipeline

1. **Publish.** The simulator builds events with a deterministic generator: the same seed gives the same content,
   verified by a SHA-256 hash over 100,000 events. Each event gets a time-ordered UUIDv7 id. Events are keyed by
   user id and published with `acks=all` and an idempotent producer. Every publication outcome is recorded:
   acknowledged, definitely failed or ambiguous.
2. **Consume.** Workers poll the six-partition topic. Auto-commit is off and `auto.offset.reset=none`, so a
   consumer never silently jumps to a new position.
3. **Apply.** In one PostgreSQL transaction, the event id is inserted into the ledger `processed_events` with
   `ON CONFLICT DO NOTHING`. Only if it is new are the event row, the minute aggregates, the latency histogram and
   the run statistics written.
4. **Commit offsets.** This happens only after the transaction has committed (`AckMode.BATCH`).
5. **Observe.** The api reads aggregates and indexed rows, and Kafka lag through AdminClient. It pushes one
   snapshot per second over WebSocket; slow browsers are dropped rather than waited for.

## 4. At-least-once delivery and database idempotency

A crash can always happen between "effects committed" and "offset committed". So delivery is at least once, and
idempotency has to come from the database ([ADR-0004](adr/0004-delivery-semantics.md),
[ADR-0005](adr/0005-offset-commit-after-db-commit.md)). The ledger row and the effects commit together or not at
all. A redelivered event conflicts on the ledger, changes nothing and only increments a duplicate counter.

**Reconciliation** closes the loop ([ADR-0009](adr/0009-run-reconciliation.md)). After a run drains, the producer's
acknowledged events are compared with the ledger. The result is PASS, FAIL (missing or unexpected events) or
INCOMPLETE. Duplicates, dead letters and replays are reported separately, never folded into one number.

I deliberately avoid the phrase "exactly-once delivery". The claim is at-least-once delivery with duplicate-free
effects, and [`reliability.md`](reliability.md) states its boundaries: retry counts are per assignment, per-key order
is lost after dead-lettering, and acknowledged data needs the single broker's disk.

## 5. Retry, dead-letter and replay design

Failures are classified, because each kind needs a different reaction ([ADR-0006](adr/0006-failure-classification.md)):

| Category | Example | Reaction |
|---|---|---|
| Bad record | Undecodable or invalid event | Dead-lettered on the first attempt; the partition moves on |
| Transient | Deadlock, serialization conflict, lock timeout | 3 attempts in place, then dead-lettered |
| Infrastructure | PostgreSQL or Kafka unreachable, failed offset commit | Retried without limit (back-off capped at 10 s, below the poll interval); never dead-lettered |

**Dead letters** ([ADR-0007](adr/0007-dead-letters-and-replay.md)) keep their original topic, partition, offset, key,
value bytes, identity headers, exception and category. The source offset is committed only after the dead-letter
record is acknowledged. If dead-letter publication fails, the partition waits rather than skipping.

**Replay** republishes the original event with its original id, so the ledger makes it idempotent. Dead letters
are claimed with `SELECT … FOR UPDATE` in id order, so concurrent replay requests replay each one once. A replay
interrupted by an api crash is recovered by a sweeper. Replay and discard require an operator key and are
rate-limited.

## 6. Crash-recovery testing

The claims are proven with **real JVM crashes**: a child JVM calls `Runtime.halt()` at an exact point. These are
the cases, with their evidence in [`reliability.md`](reliability.md), claims 5–6, 15, 18 and 34:
- before the database commit, which must lose nothing;
- after the commit but before the offset commit, which must double nothing (about 1,000 redeliveries suppressed in
  one test);
- after a dead-letter publication, where the dead letter is republished but stored once;
- in the middle of a replay;
- after a partial batch fallback.

Outage tests pause the real containers under load. A PostgreSQL pause produces no dead letters and no rebalance. A
Kafka broker pause makes workers reconnect by themselves. The whole reliability suite runs **twice in CI, once per
processing mode**. The benchmark harness adds SIGKILL tests under load: one of two workers was killed mid-drain and
partitions stalled for 18.5 s (median). Between 573 and 1,359 redeliveries were suppressed per run, and every run
reconciled PASS ([benchmarks §6](benchmarks/README.md#6-worker-failure-and-recovery)).

I checked every Spring Kafka behaviour the design depends on with a pinned test. There are ten questions, from "are
offsets before a failed record committed?" to "what happens when retention deletes my position?"
([`verification/spring-kafka-behavior.md`](verification/spring-kafka-behavior.md)).

## 7. The batch-processing optimisation

**Measuring first.** Phase 4 suggested one worker managed about 700–800 events/s. The benchmark reproduced it:
725 events/s (654–869, n = 5).
- **CPU:** neither PostgreSQL (1.1 of 12 CPUs) nor the worker (0.8) was CPU-bound.
- **Profiling:** Java Flight Recorder showed the consumer threads almost always blocked in socket reads. Attributing
  those samples, 49.8 % were waiting on `COMMIT` and most of the rest on five sequential statement round trips per
  record.
- **WAL flushes:** `pg_stat_io` showed one flush per transaction at about 1 ms on the Docker VM's virtual disk.

So the bottleneck was per-record synchronous latency, not CPU and not a missing index
([benchmarks §1](benchmarks/README.md#1-the-phase-4-throughput-ceiling-investigated)).

**Optimisation 1.** Combining the four effect writes into one statement with data-modifying CTEs gave **725 →
941 events/s**, about +30 % at the median. After that, the commit was clearly the dominant cost.

**Optimisation 2: batch mode** ([ADR-0013](adr/0013-batch-processing-mode.md)). One transaction covers a whole poll
batch:
- set-based `unnest` inserts, with rows sorted by event id so concurrent batches lock in the same order;
- only events newly inserted into the ledger get effects.

If anything fails, the batch rolls back and is processed record by record. The first failing record is reported
to Spring Kafka as `BatchListenerFailedException(index)`, so offsets are committed only for records whose effects
are durable. Retry and dead-letter semantics are unchanged.

## 8. Before-and-after results

Measured with one worker × 3 threads and 6 partitions, as medians with ranges; every run reconciled exactly. Source:
[`benchmarks/README.md`](benchmarks/README.md); all 97 matrix runs are in [`results.md`](benchmarks/results.md);
raw JSON is in [`benchmarks/results/`](../benchmarks/results/).

| | Record, before | Record, after opt. 1 | Batch |
|---|---|---|---|
| Drain throughput | 725 /s (654–869) | 941 /s (783–983) | **15,548 /s (14,276–16,598)** |
| Events per database transaction | 1 | 1 | 410 |
| WAL fsyncs per 1,000 events | — | 817 | 1.8 |
| Latency p50 / p95 / p99 at 500 /s offered | — | 7.1 / 548 / 1,463 ms | 5.8 / 27.7 / 361 ms |

Batch mode is **16.5×** faster at the default worker shape, and its latency was equal or better at 250, 500 and
750 events/s offered. With that evidence, and the reliability suite passing in both modes, batch became the default.
Record mode remains available and tested as the reference.

**Scaling** on one host was sub-linear in both modes. Record mode is limited by durable commits (1 → 6 consumers:
2.7–2.8×). Batch mode is limited by the laptop's CPU, levelling off at 12–15k events/s. One worker with six threads
matched six one-thread workers while using far less CPU.

**What I did not explain:** one of three batch runs at 750 events/s had a p95 of 7.7 s that the other two did not
show. I kept it in the ranges and documented it as an uncertainty instead of dropping it.

## 9. Challenges, trade-offs and lessons learned

**Tests found real bugs that reasoning had missed.**
- **Batch mode, Phase 5:** running the reliability suite in batch mode first failed three tests, each a real gap.
  Dead letters recorded the exception wrapper instead of the cause. The record-by-record fallback doubled the
  reaction time to a database outage. And Spring Kafka clears the error handler's state after a successful batch,
  which reset a retrying record's attempt count.
- **Broker outage, Phase 6:** a new Kafka outage test found that a failed offset commit after the outage made Spring
  Kafka pause the consumer for an in-memory batch retry. The reassigned partitions then never resumed. Failed
  commits now cause a redelivery that the ledger absorbs.
- **Retention, Phase 6:** a deleted Kafka position produced about 75 identical errors per minute and no record of
  what was lost. Now the worker pauses only that partition, records the gap with the number of events lost, and
  leaves recovery to a deliberate operator step. Reconciliation still reports the lost events as missing.

**Trade-offs I accepted:**
- Infrastructure failures block their partition rather than dead-lettering. Kafka holds the backlog, and the
  backlog window is measured (14 h at 1,000 events/s with 1 GiB per partition).
- Per-key order is not preserved for dead-lettered and replayed events; the analytics are order-independent.
- Batch mode adds a little latency at very low traffic in exchange for 16× throughput.
- One broker and one database keep the project runnable on a laptop, at the cost of production-grade durability.

**Lessons:**
- **Make the guarantee a database constraint, not a code path.** The ledger's primary key absorbs every
  redelivery, whichever bug or crash caused it.
- **Verify the framework.** Three Spring Kafka behaviours differed from what I would have assumed. Pinned tests found
  them before they could corrupt data.
- **Profile before optimising.** The plausible explanation ("PostgreSQL is busy") was wrong. Native-method samples
  pointed straight at commit latency.
- **Separate quantities.** Offered traffic, acknowledged publications, unique effects and duplicate deliveries are
  four different numbers. Keeping them apart is what makes reconciliation trustworthy.
- **Write down what is not known.** An unexplained outlier, untested partition counts and single-host limits are
  stated plainly.

## Evidence index

| Topic | Where |
|---|---|
| Claims and their tests | [`reliability.md`](reliability.md) |
| Performance, profiling, scaling | [`benchmarks/README.md`](benchmarks/README.md), [`methodology.md`](benchmarks/methodology.md) |
| Framework behaviour | [`verification/spring-kafka-behavior.md`](verification/spring-kafka-behavior.md) |
| Decisions | [`adr/`](adr/) |
| Fresh-clone walkthrough with screenshots | [`walkthrough.md`](walkthrough.md) |
| Hardening audit | [`planning/phase-6-hardening.md`](planning/phase-6-hardening.md) |
| CI | [GitHub Actions](https://github.com/gladwynL/StreamFlow/actions/workflows/ci.yml) |
