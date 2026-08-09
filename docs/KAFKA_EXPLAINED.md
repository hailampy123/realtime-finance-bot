# Kafka, Explained Through This Project

Written for someone new to Kafka. Every setting shown here is one this repo
actually uses — file and line cited — so you're learning your own system rather
than a tutorial's.

Companion: [`MAKE_UP_EXPLAINED.md`](MAKE_UP_EXPLAINED.md) covers the AWS side.

---

## 1. The one idea that makes everything else click

**Kafka is an append-only log, not a queue.**

A queue is destructive: you pop a message, it's gone. Kafka isn't. A message is
appended to a file, sits there for a fixed **retention period**, and *any number
of independent readers* can read it — each tracking its own position. Reading
doesn't consume.

```text
md.trades.v1  ──►  [msg][msg][msg][msg][msg][msg]  ──► (new writes append here)
                     ▲              ▲         ▲
                     │              │         │
              Databricks      your notebook  smoke test
              (position 2)    (position 4)   (position 6)
```

Three consequences that shape this whole project:

- Databricks reading trades doesn't stop your notebook reading the same trades.
- Nobody has to be listening when a message is written. The producer doesn't care.
- **Messages expire on a timer, not when read.** Kafka here is a *buffer*, not
  storage. Unity Catalog is the system of record; Kafka holds the last 24 hours.

That last point is why `make down` losing in-flight data is acceptable by
design, and why the backfill layer exists at all.

---

## 2. Topics, partitions, offsets

A **topic** is a named log. Yours, from
[`create_topics.py`](../scripts/create_topics.py):

| Topic | Partitions | Retention | Actually receives data? |
|---|---|---|---|
| `md.trades.v1` | 6 | 24 h | **Yes** — the only one |
| `md.book.top.v1` | 6 | 6 h | no producer yet |
| `md.book.depth.v1` | 6 | 2 h | no producer yet |
| `md.bars.v1` | 3 | 48 h | no producer yet |
| `news.articles.v1` | 3 | 7 d | no producer yet |
| `ops.metrics.v1` | 1 | 7 d | no producer yet |
| `_dlq.*` (5 topics) | 1 each | 7 d | nothing writes them yet |

A **partition** is a shard of a topic — an independent log file. `md.trades.v1`
has 6, so it's really six logs under one name. Partitions are how Kafka scales:
six partitions can be written and read in parallel.

An **offset** is a message's position within one partition. Numbering starts at 0
and only increases.

```text
md.trades.v1
├── partition 0   offset: 0 1 2 3 4 5 ...
├── partition 1   offset: 0 1 2 3 ...
├── partition 2   offset: 0 1 ...
├── partition 3   offset: 0 1 2 3 4 5 6 7 8 9 ...   ← e.g. BTCUSDT (illustrative)
├── partition 4   offset: 0 1 2 ...
└── partition 5   offset: 0 ...
```

**Ordering is guaranteed within a partition, and nowhere else.** Partition 3's
offset 5 came after its offset 4. But partition 3 offset 5 versus partition 1
offset 5 — no guarantee whatsoever. This matters enormously for market data, and
§3 is how the project handles it.

---

## 3. Keys — how a message picks its partition

Every Kafka message has an optional **key**. Yours, from
[`models.py:37-39`](../ingest/core/models.py#L37-L39):

```python
def kafka_key(self) -> bytes:
    """Key on venue|symbol so one instrument on one venue stays ordered."""
    return f"{self.venue}|{self.venue_symbol}".encode()
```

Kafka hashes the key and takes it modulo the partition count. So:

```text
key "binance|BTCUSDT"  --hash--> always partition 3
key "binance|ETHUSDT"  --hash--> always partition 1
key "coinbase|BTC-USD" --hash--> always partition 5   (different key! different partition)
```

The partition numbers above are invented for illustration — the hash decides
which one, and you'd have to look to find out. What's guaranteed is the *mapping*:
a given key always resolves to the same partition, as long as the partition count
doesn't change.

**Same key always lands on the same partition.** Since ordering holds within a
partition, every `binance|BTCUSDT` trade stays in exchange order relative to
every other `binance|BTCUSDT` trade. That's the guarantee the design needs: you
can't have trade #101 appear before trade #100 for a given instrument on a given
venue.

Two things follow that are worth internalising:

**Cross-venue ordering does not exist.** A Binance BTC trade and a Coinbase BTC
trade are different keys, so they may sit in different partitions with no
relative ordering. Any code comparing them must use the `event_ts_us` field, not
arrival order.

**One key = one partition = one broker doing the work.** BTCUSDT has far more
volume than DOGE-USD, so whichever partition it hashes to gets hammered while
others idle. This is
*partition skew*, and the design [documents it rather than pre-optimising](superpowers/specs/2026-08-07-finance-data-ai-platform-design.md#L161-L162).
The fix would be a composite key, at the cost of losing per-instrument ordering —
a bad trade at this scale.

If you send a message with **no** key, Kafka round-robins it across partitions and
you get no ordering guarantee at all.

---

## 4. Retention — your data has an expiry date

From the table in §2: trades live **24 hours**, then Kafka deletes them. Not when
read — on a timer, unconditionally.

```text
retention.ms = 86400000    (24h, set in create_topics.py)
```

So if nothing consumes `md.trades.v1` within 24 hours, that data is gone forever.
This is the single most surprising Kafka behaviour for newcomers, and it's load-
bearing here: it's exactly why the [data-layer spec](superpowers/specs/2026-08-08-data-layer-batch-history-and-serving-design.md)
treats Kafka as a transient buffer and re-derives history from public archives.

`compression.type=zstd` is also set per topic — messages are stored compressed,
which for repetitive JSON-ish records is a large win.

---

## 5. Producers — writing, and your actual settings

From [`producer.py:19-29`](../ingest/core/producer.py#L19-L29):

```python
"acks": "all",
"enable.idempotence": True,
"compression.type": "zstd",
"linger.ms": 20,
"batch.size": 262_144,
"max.in.flight.requests.per.connection": 5,
"retries": 10_000_000,
"delivery.timeout.ms": 300_000,
```

**`acks="all"`** — how many replicas must confirm a write before it's considered
successful. `0` = fire and forget, `1` = leader only, `all` = every in-sync
replica. `all` is slowest and safest.

**`enable.idempotence=True`** — the important one. Without it, a network hiccup
causes a retry, and if the *first* attempt actually succeeded you now have a
duplicate. Idempotence gives each message a sequence number so the broker
recognises and discards the retry.

> **The limit worth knowing:** idempotence only dedupes retries *within one
> producer session*. Restart the producer and the sequence resets, so a
> restart-time duplicate is still possible. That's why the design *also* dedupes
> on a natural key (the trade id) downstream — belt and braces, deliberately.

**`linger.ms=20` + `batch.size=262144`** — batching. Rather than one network call
per trade, wait up to 20 ms or until 256 KB accumulates, then send as a batch.
Trades throughput at ~200-400 msg/s, so this is a big efficiency win for 20 ms of
latency that doesn't matter in a minutes-to-seconds system.

**`retries=10_000_000` + `delivery.timeout.ms=300_000`** — looks like "retry
forever," but the timeout is the real bound: retry as much as you can for 5
minutes, then fail. The huge retry count just means "the timeout decides, not the
counter."

**`max.in.flight.requests.per.connection=5`** — how many unacknowledged batches
can be in flight. Above 1 this would normally risk reordering on retry; with
idempotence enabled, librdkafka preserves order anyway, so 5 is safe. (Idempotence
actually *requires* `acks=all` and in-flight ≤ 5, so these settings are coupled,
not independently chosen.)

---

## 6. Consumers, groups, and offsets

A **consumer group** is a set of consumers sharing a topic's partitions. Kafka
assigns each partition to exactly one consumer in the group, so work splits
without duplication. Add a consumer, partitions redistribute. With 6 partitions,
a 7th consumer in the same group sits idle.

Kafka stores each group's position — its **committed offset** — so a restarting
consumer resumes rather than restarting from zero. Different groups are fully
independent: two groups read the same messages without interfering.

Your smoke test is a neat illustration
([`smoke_test.py:26-27`](../scripts/smoke_test.py#L26-L27)):

```python
"group.id": f"smoke-{int(time.time())}",
"auto.offset.reset": "latest",
```

A **brand-new group id every run** (timestamped), plus `latest`. A new group has
no committed offset, and `auto.offset.reset` decides what happens then —
`earliest` would replay everything retained, `latest` starts from now. So the
smoke test asserts *"trades are flowing right now,"* not *"trades exist
somewhere in the last 24h."* Exactly the right question for a smoke test, and a
different group id each run means it never inherits a stale position.

---

## 7. Replication — surviving a broker failure

`replication_factor=2` (default in `create_topics.py`), so each partition exists
on 2 of your brokers: one **leader** handling all reads and writes, one
**follower** copying it. Followers that are caught up form the **ISR** (in-sync
replicas). Lose the leader, a follower is promoted.

`min.insync.replicas = max(1, rf - 1)` = **1** here.

That's the floor for accepting writes. And it interacts with `acks=all` in a way
worth understanding: `acks=all` means *"all replicas currently in the ISR,"* not
*"all replicas that exist."* If a broker dies, the ISR shrinks to 1, and
`acks=all` is then satisfied by a **single** replica — writes keep flowing, with
weaker durability than the name suggests. That's the intended trade here
(availability over strictness on a disposable sandbox), but it's not what
`acks=all` sounds like it promises.

Local dev overrides this to `--replication-factor 1` in `make stream-local`,
since a single-broker Docker Kafka can't replicate anything.

---

## 8. Bootstrap servers, and why the DNS keeps changing

```text
b-1.fdaikafka...:9196,b-2.fdaikafka...:9196
```

`bootstrap.servers` is **not** "the server." It's a *seed list*. The client
connects to any one of them, asks for cluster metadata (which brokers exist,
which leads which partition), then connects **directly to the leader** for each
partition it needs. You list two so that one being down doesn't stop the initial
handshake.

MSK generates fresh broker DNS names every time the cluster is created — and this
account is wiped weekly. That's precisely why
[`bootstrap.sh`](../scripts/bootstrap.sh) writes the endpoint into a Databricks
secret scope on every run instead of anyone hardcoding it in a notebook.

### Three ports, three different endpoints

| Port | Where | Auth | Used by |
|---|---|---|---|
| 9092 | local Docker only | none, plaintext | `make compose-up` / `make stream-local` |
| 9096 | inside the VPC | SASL/SCRAM + TLS | the EC2 producer host, `create_acls.py` |
| 9196 | public internet | SASL/SCRAM + TLS | Databricks, your laptop |

These are **different endpoints with different DNS names**, not the same broker on
different ports. Mixing them up is a common early mistake, and 9096 vs 9196 is
why `bootstrap.sh` tracks `bootstrap_brokers_private` and
`bootstrap_brokers_public` separately.

---

## 9. Security — three separate things that get conflated

| Layer | Question | This project |
|---|---|---|
| Encryption | can someone read it in transit? | **TLS** |
| Authentication | who are you? | **SASL/SCRAM-SHA-512** |
| Authorization | what may you do? | **ACLs** |

`security.protocol=SASL_SSL` means both SASL authentication *and* TLS encryption.
(`SASL_PLAINTEXT` would authenticate but send unencrypted — never use it over the
internet.)

**SCRAM** is a challenge-response password scheme: the password isn't sent over
the wire, even inside TLS. Your username/password live in AWS Secrets Manager and
are handed to the cluster by `aws_msk_scram_secret_association`.

**ACLs** are per-resource permission rules. Yours, from
[`create_acls.py`](../scripts/create_acls.py), grant `User:*` — *any
authenticated principal* — `ALL` operations on topics, groups, the cluster, and
transactional ids. That's deliberately wide: **authentication plus the
security-group IP allowlist are the access control here**, not per-topic
authorization. Narrowing it would mean an ACL per new topic and consumer group,
where a missing one shows up as a confusing runtime auth error.

### The one Kafka setting that shaped the entire deployment

```text
allow.everyone.if.no.acl.found
```

`true` → a resource with no ACL is open to everyone. `false` → **deny by
default**: no ACL means no access.

MSK refuses to enable public access unless this is `false`. But flipping it to
`false` before ACLs exist locks out *every* client — including the one that would
create the ACLs, since `CreateAcls` itself needs `Alter` on the cluster. A
cluster tightened too early can't be repaired from any Kafka client; only by
loosening the broker config again (`make unlock`).

That single setting is why `make up` applies the cluster three times and SSHes
into an EC2 box. Full reasoning: [`SETUP.md`](SETUP.md) §5f.

---

## 10. Serialization — why `kafka-console-consumer` shows you garbage

**Kafka does not care what your messages contain.** Keys and values are opaque
byte arrays. Structure is entirely your problem.

Most projects solve it with a *schema registry* — a server that hands out schema
ids, with a magic byte prefixed to each message. This project deliberately has
none ([`codec.py:1-7`](../ingest/core/codec.py#L1-L7)): AWS Glue Schema Registry
needs IAM that can't be created here, and a self-hosted registry would die in the
weekly wipe.

Instead: **bare Avro datums.** `fastavro.schemaless_writer` produces the encoded
record with no prefix, no envelope, no id. Producer and Spark reader load the
*same* `ingest/schemas/trade.v1.avsc` from git, so schema drift is impossible by
construction.

The version travels in a **Kafka header** instead
([`producer.py:68-73`](../ingest/core/producer.py#L68-L73)). Headers are
key/value metadata alongside the message body:

```python
headers=[
    ("schema_version", b"1"),
    ("venue",          b"binance"),
    ("is_backfill",    b"false"),
    ("source",         b"STREAM"),
]
```

Useful because a consumer can route or filter on a header **without decoding the
payload** — which is how a Spark reader picks the right schema before parsing.

> **Practical consequence:** you cannot read these messages with generic tooling.
> `kafka-console-consumer` will print binary noise, because it has no idea it's
> Avro and no schema to apply. To inspect data you need something that loads the
> `.avsc` — [`scripts/consume_example.py`](../scripts/consume_example.py) or
> `scripts/smoke_test.py`. When your messages look like garbage, this is why, and
> it isn't a bug.

---

## 11. Backpressure — what happens when Kafka can't keep up

Not Kafka itself, but the adjacent decision every producer has to make: exchange
WebSockets push data at their pace, regardless of whether you can publish it. So
there's a bounded in-memory queue between parsing and publishing, and a policy
for when it fills ([`queue.py:24-31`](../ingest/core/queue.py#L24-L31)):

| Topic | Policy | Reasoning |
|---|---|---|
| `md.trades.v1` | **BLOCK** | Never drop a trade. Block, take the gap, repair it over REST |
| `md.bars.v1` | **BLOCK** | Same |
| `news.articles.v1` | **BLOCK** | Same |
| `md.book.top.v1` | DROP_OLDEST | A newer quote supersedes an older one |
| `md.book.depth.v1` | DROP_OLDEST | Recoverable from the next snapshot |
| `ops.metrics.v1` | DROP_OLDEST | Telemetry isn't worth stalling ingestion for |

The asymmetry is the point. Dropping a trade is unrecoverable and silent —
the one failure this system must not have. Dropping a depth update costs nothing,
because the next snapshot rebuilds the book.

A **DLQ** (dead-letter queue) is the related idea: a topic for messages that
couldn't be processed, so a bad message doesn't halt the pipeline or vanish. The
`_dlq.*` topics exist but nothing writes to them yet — an unparseable frame
currently causes a WebSocket reconnect instead.

---

## 12. Two behaviours that will confuse you at some point

**A producer against a topic that doesn't exist yet fails for minutes.** Auto-
creation is off in both environments — MSK defaults to off, and the local Docker
broker sets `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` explicitly — so a topic
exists only once `create_topics.py` has made it. Meanwhile librdkafka caches topic
metadata, refreshing for an *unknown* topic only every few minutes.
So a producer started before `create_topics.py` keeps failing well after the
topics appear. That's why `bootstrap.sh` step 8 explicitly restarts the container
rather than waiting it out — restarting forces a fresh metadata fetch.

**A consumer can be perfectly healthy and see nothing.** With
`auto.offset.reset=latest` and a group that has already committed offsets, you
only get messages produced *after* you connect. Silence usually means "nothing
new right now," not "broken." Use a fresh `group.id` and `earliest` if you want
to read what's already there.

---

## 13. Glossary

| Term | Meaning |
|---|---|
| **Topic** | A named, append-only log. `md.trades.v1` |
| **Partition** | An independently-ordered shard of a topic. Ordering exists only here |
| **Offset** | A message's position within one partition |
| **Key** | Optional bytes that determine the partition. Same key → same partition → ordered |
| **Header** | Key/value metadata beside the payload, readable without decoding it |
| **Producer** | Writes messages |
| **Consumer** | Reads messages. Reading is non-destructive |
| **Consumer group** | Consumers sharing partitions; offsets are committed per group |
| **Broker** | One Kafka server. You run 2 |
| **Leader / follower** | Per partition: one broker serves it, others replicate it |
| **ISR** | In-sync replicas — followers currently caught up |
| **Replication factor** | Copies of each partition. Yours: 2 |
| **`min.insync.replicas`** | Minimum ISR size to accept writes. Yours: 1 |
| **Retention** | How long messages live before unconditional deletion |
| **Bootstrap servers** | Seed list for discovering the cluster, not the cluster itself |
| **SASL/SCRAM** | Password authentication where the password never crosses the wire |
| **ACL** | Per-resource authorization rule |
| **DLQ** | Dead-letter topic for unprocessable messages |
| **Idempotent producer** | Sequence numbers let the broker discard retry duplicates |
| **MSK** | AWS's managed Kafka. Same Kafka, AWS operates the brokers |
