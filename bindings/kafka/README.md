# Kafka Protocol Binding for OpenCDC - Version 0.3.0-wip

**Binding:** `kafka`
**Version:** 0.3.0-wip (wire protocol 0.3, document revision 0; core specification v0.7.0)
**Profile in this revision:** Durable Mode, partitioned delivery with a control topic and a transaction topic, JSON event data (structured or binary content mode)
**Requirement identifiers:** `B-KFK-*`, tracked in [requirements.yaml](requirements.yaml)
**Status:** Working draft, first pass for working-group review

## Abstract

The Kafka Protocol Binding for OpenCDC defines how an OpenCDC stream is written to, and read from, Apache Kafka topics.
It is an application of the CloudEvents Kafka protocol binding and reuses the CloudEvents JSON event format.
It adds the stream-level semantics OpenCDC requires and CloudEvents does not define: a topic layout with a compacted control topic and an ordered transaction topic, record keying, retention rules that keep schemas and transaction markers available across the replay window, and how a client obtains stream metadata without a session.

## Table of Contents

1. [Introduction](#1-introduction)

- 1.1. [Conformance](#11-conformance)
- 1.2. [Relation to Kafka and CloudEvents](#12-relation-to-kafka-and-cloudevents)
- 1.3. [Content Modes](#13-content-modes)
- 1.4. [Channel Layout](#14-channel-layout)
- 1.5. [Security](#15-security)
- 1.6. [Transport Capabilities](#16-transport-capabilities)
- 1.7. [Versioning and Compatibility](#17-versioning-and-compatibility)

2. [Use of OpenCDC Attributes](#2-use-of-opencdc-attributes)

3. [Kafka Message Mapping](#3-kafka-message-mapping)

- 3.1. [Record Unit](#31-record-unit)
- 3.2. [Structured Content Mode](#32-structured-content-mode)
- 3.3. [Binary Content Mode](#33-binary-content-mode)
- 3.4. [Record Keys and Partitioning](#34-record-keys-and-partitioning)
- 3.5. [Non-Event Records](#35-non-event-records)
- 3.6. [Size Limits](#36-size-limits)

4. [Stream Profile and Delivery Properties](#4-stream-profile-and-delivery-properties)

- 4.1. [Capability Axes](#41-capability-axes)
- 4.2. [Delivery Properties](#42-delivery-properties)

5. [Delivery Protocol](#5-delivery-protocol)

- 5.1. [Startup](#51-startup)
- 5.2. [Start Position](#52-start-position)
- 5.3. [Schema Resolution](#53-schema-resolution)
- 5.4. [Transaction Completion and the Transaction Topic](#54-transaction-completion-and-the-transaction-topic)
- 5.5. [Replay Window and Retention](#55-replay-window-and-retention)
- 5.6. [Publishing Durability](#56-publishing-durability)
- 5.7. [Heartbeat](#57-heartbeat)
- 5.8. [Stream Reconfiguration](#58-stream-reconfiguration)
- 5.9. [Failure Behavior](#59-failure-behavior)

6. [Stream Address and Client Configuration](#6-stream-address-and-client-configuration)

- 6.1. [Client Configuration](#61-client-configuration)
- 6.2. [AsyncAPI Description](#62-asyncapi-description)

7. [Mapping from Existing Implementations](#7-mapping-from-existing-implementations)

- 7.1. [Kafka Connect](#71-kafka-connect)
- 7.2. [Debezium](#72-debezium)

8. [Decisions, Deferrals, and Open Items](#8-decisions-deferrals-and-open-items)

- 8.1. [Decisions Taken](#81-decisions-taken)
- 8.2. [Deferred Features](#82-deferred-features)
- 8.3. [Deferred Core Items](#83-deferred-core-items)
- 8.4. [Open Items](#84-open-items)

9. [References](#9-references)

## 1. Introduction

[OpenCDC][core] is a vendor-neutral specification for the structure and semantics of database change events, built on [CloudEvents][ce].
The core specification is transport-neutral.
This document is an OpenCDC *protocol binding*: it defines how an OpenCDC stream is carried over [Apache Kafka][kafka] topics, or over any service that implements the Kafka protocol and provides the capabilities listed in 1.6.

The binding is optional to implement and normative when claimed.
No binding is required for core conformance.
The conformance claim string is:

> OpenCDC 0.3 / Kafka Binding 0.3

Kafka separates the component that writes a stream from the component that stores and delivers it.
A claim under this binding is therefore made for a **stream deployment**: a publisher together with the Kafka topics it writes, as configured.
The claim holds only when the publisher and the deployment both satisfy their requirements.
A publisher implementation MAY state that it *implements the publisher requirements* of this binding; that statement is not a claim for any particular deployment.
A client reading a claimed stream deployment is in the "claimed transport binding profile" case of [C-COMP-1][core-a1] for the delivered guarantees stated in 4.2, and in no other case.

This revision defines one profile: Durable Mode, partitioned delivery with a control topic and a transaction topic, and JSON event data in either structured or binary content mode.
A single-partition profile that preserves a total order, Ephemeral Mode, and other data encodings are deferred (Section 8).

References of the form "core Section N" or "core Appendix A" are to the [OpenCDC specification][core].
Bare section numbers refer to this document.
Kafka configuration properties are named as in the [Kafka documentation][kafka-docs] (for example `cleanup.policy`); a deployment on a Kafka-compatible service satisfies a rule by providing the equivalent behavior.

### 1.1. Conformance

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC2119][rfc2119].

Four parties appear in requirements:

- **Publisher.** The component that writes OpenCDC events to Kafka: the OpenCDC producer itself, or a component acting for it in the same process (for example, a Kafka Connect worker running a source connector and its converter).
  Every requirement applies to the publisher unless it names another party.
- **Deployment.** The Kafka cluster and the configuration of the stream's topics (partition counts, retention, compaction, replication, access control).
  The deployment is the delivery layer of core Terms and Definitions.
- **Client.** A process reading the stream.
  Client requirements are protocol mechanics: what a client must do to read the stream correctly and what it must be prepared to receive.
  Guidance on what a consumer does with the events it receives remains in the core, [Appendix A][core-appendix-a], and is not restated here.
- **Relay.** A component that copies the stream's records from one set of topics to another, within or across clusters (B-KFK-36).

Requirement identifiers are stable once allocated.
A withdrawn requirement keeps its number and is marked withdrawn in the register; numbers are never reused.

### 1.2. Relation to Kafka and CloudEvents

This specification does not prescribe how topics are provisioned, how a publisher is deployed, or how consumer groups are managed.
It begins at the records a publisher writes and the topic configuration those records rely on.

This binding is an **application of** the CloudEvents [Kafka protocol binding][ce-kafka].
Every record it defines is a CloudEvents Kafka message in the structured or binary content mode of that binding, and the [JSON event format][json-format] is reused unchanged.
Unlike the WSS + AsyncAPI binding, nothing here requires departing from the CloudEvents binding: Kafka already batches records at the protocol level, so one event per record carries no framing cost worth avoiding, and the CloudEvents Kafka binding's exclusion of the batch content mode costs nothing.

Where the CloudEvents Kafka binding leaves a choice open, this binding makes it: the record key (the CloudEvents binding makes key mapping implementation-specific, [section 3.1][ce-kafka-key]), which topics a stream uses, how they are configured, and which records may appear on each.
What it adds on top is the stream-level contract CloudEvents does not define: schema availability without a session, transaction completion across partitions, retention across the replay window, and the topics a client must read.

The envelope values of every event, including `specversion` and `cdcspecversion`, are those the core defines.
This binding inherits them verbatim and does not restate or qualify them (see Section 8, deferred core item 1).

This binding does not use Kafka transactions ([KIP-98][kip-98]) to express OpenCDC transactions.
A publisher MAY use them for its own delivery guarantees (5.6), but the OpenCDC transaction boundary is the TRX_COMMIT marker, as core [Section 10.5.4][core-10-5-4] requires.

**Format composition is reserved.** This revision defines JSON event data completely and defines no other encoding.
A future OpenCDC format binding that wishes to compose with this transport binding will do so by a revision of this document that names the combination and states which clauses of Section 3 the format replaces.
Until then, a stream deployment claiming this binding carries JSON only.

### 1.3. Content Modes

CloudEvents defines three content modes: *structured*, *binary*, and *batch*.
The CloudEvents Kafka binding supports the first two, and so does this binding.
Binary mode is what core [Section 12][core-12] identifies as suited to Kafka: CloudEvents attributes travel as record headers, so a stream processor can filter on `ce_type` or group on `ce_cdcxid` without parsing the record value.
Structured mode keeps an event self-contained in one value, which survives tools that drop or rewrite headers.

- **B-KFK-1.** Each event MUST be carried in one Kafka record in either structured mode (3.2) or binary mode (3.3).
  The content mode is the publisher's choice, and at any time every record it writes to a stream's data topics and transaction topic MUST use the same mode.
- **B-KFK-2.** Records on the control topic MUST use structured mode, whatever mode the data topics use.
  The control topic is small, is read once at startup, and must remain decodable after passing through any tooling.
- **B-KFK-3 (Client).** A client MUST support both content modes and MUST determine the mode of each event record from its `content-type` header, as the CloudEvents Kafka binding specifies ([section 3][ce-kafka-mapping]).

### 1.4. Channel Layout

A stream on Kafka is a set of topics with fixed roles.
The *channel* of core Terms and Definitions corresponds to a partition, not a topic ([core Appendix B.4][core-b4]).

- **B-KFK-4.** A stream MUST consist of exactly one control topic, exactly one transaction topic, and one or more data topics, all in one Kafka cluster.
  A topic MUST NOT carry records of more than one stream.
- **B-KFK-5 (Deployment).** The control topic MUST have exactly one partition and a `cleanup.policy` of exactly `compact`.
  The value `compact,delete` does not satisfy this rule, because time-based deletion would remove schema versions (4.2).
- **B-KFK-6 (Deployment).** The transaction topic MUST have exactly one partition and a `cleanup.policy` of `delete`.
- **B-KFK-7 (Deployment).** Every data topic MUST have a `cleanup.policy` of `delete`, `compact`, or `compact,delete`.
  On a data topic with compaction enabled, the deployment MUST set `min.compaction.lag.ms` explicitly; its default of 0 would give the stream a replay window of zero (B-KFK-45).
  Records older than a compacted topic's lag are table state, at least the latest record per key and possibly no other, rather than an OpenCDC changelog (B-KFK-63).
  A deployment whose clients need complete transactions and their TRX_COMMIT markers beyond the replay window MUST NOT enable compaction on its data topics (B-KFK-63).
  State taken from the compacted region of a multi-partition data topic is not valid across a TRUNCATE (3.5); a deployment whose clients need it to be gives that topic a single partition, does not enable compaction on it, or does not capture TRUNCATE for its tables (3.5), none of which the broker enforces for it.
- **B-KFK-67 (Deployment).** A data topic whose `cleanup.policy` includes `compact` MUST NOT receive a record with a null key.
  The deployment MUST NOT give a data topic such a `cleanup.policy`, at creation or later, while the publisher is configured to write null keys to it (B-KFK-21); where it needs compaction, it first configures the publisher to key those records by `subject`.

*Why compaction is admitted.* Compaction on a data topic is a broker storage optimization: once a later record for a key supersedes an earlier one, the broker need not keep the earlier one.
Kafka never compacts a record newer than the topic's `min.compaction.lag.ms`, and that lag bounds the replay window (B-KFK-45), so every event a client reads within the window is intact and in order, exactly as on a `delete` topic.
Beyond the window, each partition keeps at least the latest record per key, and possibly no other, and B-KFK-63 applies.

That retained state is not a substitute for a snapshot, and a reader that takes it as current table state is right only in part:

- A DELETE remains the latest record for its key, so a reader that inspects the event sees the row as removed.
  A reader that treats any non-null value as a present row sees the removal only if the publisher wrote a tombstone (B-KFK-62), and only if its scan reaches the tombstone within `delete.retention.ms` of the log cleaner's first pass over it ([KIP-534][kip-534]), which comes no earlier than `min.compaction.lag.ms` after the tombstone is written.
- After a primary-key change, the old key keeps its last record unless the publisher wrote a tombstone for it (B-KFK-62).
- On a compacted topic a TRUNCATE is keyed by its subject (B-KFK-21, B-KFK-67) and removes no row key.
  Replayed in offset order on a single-partition topic it still takes effect correctly; on a multi-partition topic, beyond the window, nothing orders it against the table's rows in other partitions.
- For a keyless table keyed by its `subject` (B-KFK-21), every row shares one key, so beyond the window compaction can reduce the whole table to a single record rather than one per row, and every record of the table lands on one partition whatever the topic's partition count.
  A data topic carrying such a table SHOULD NOT have compaction enabled unless the deployment accepts that outcome, for example because the table holds at most one row; a deployment that must prevent it can reject compaction for the topic at the broker with a `create.topic.policy.class.name` or `alter.config.policy.class.name` policy, which applies whatever the publisher, though no stock policy performs this check and managed Kafka services may not accept one.
  A keyless table with substantial change volume can use null keys instead (B-KFK-21), and a record with a null key cannot be written to a compacted topic at all (B-KFK-67).

Many CDC deployments write delete tombstones so that a compacted topic can seed a new sink.
Whether that is adequate for a given sink is a deployment decision; a sink that needs accurate table state takes a fresh snapshot.

- **B-KFK-8.** The control and transaction topics SHOULD be named `<stream>.opencdc.control` and `<stream>.opencdc.transactions`, where `<stream>` is a name the deployment chooses for the stream.
  Data topics MAY have any legal Kafka topic name.
  Client configuration names the topics (6.1); the convention lets an operator derive them from the stream name.
- **B-KFK-9.** Each topic MUST carry only the records listed for its role below.

| Topic role  | Partitions | `cleanup.policy` | Records                                                                  | Record key (3.4)                          |
| ----------- | ---------- | ---------------- | ------------------------------------------------------------------------ | ----------------------------------------- |
| Control     | 1          | `compact`        | STREAM_METADATA; durable OBJECT_METADATA; `ddl.*`                         | Fixed per record kind (B-KFK-23)          |
| Transaction | 1          | `delete`         | TRX_COMMIT; HEARTBEAT                                                    | `cdcxid` for TRX_COMMIT; none for HEARTBEAT |
| Data        | Fixed, 1 or more | `delete`, or compaction with a lag (B-KFK-7) | `dml.*` (including TRUNCATE where captured, 3.5), `snapshot.READ`; tombstones on compacted topics (B-KFK-62) | Row identity, or the subject (B-KFK-21)   |

*Why a transaction topic with one partition.* Data events are spread over many partitions and lose their cross-partition order.
The transaction topic restores it: because it has one partition, one writer (B-KFK-60), and the publisher writes markers in commit order (B-KFK-43), the sequence of TRX_COMMIT records on it, after deduplication by `(source, id)`, is the source commit order of the stream.
Duplicates appear only after a publisher restart (B-KFK-47).
It is also where liveness is signalled, since HEARTBEAT on a single partition says something about the whole stream.

#### 1.4.1. Example

A stream named `finance-orders` capturing two tables, abridged and reformatted from `kafka-topics.sh --describe`:

```text
Topic: finance-orders.opencdc.control  PartitionCount: 1  RF: 3
  cleanup.policy=compact  max.message.bytes=8650752
  min.insync.replicas=2  unclean.leader.election.enable=false
Topic: finance-orders.opencdc.transactions  PartitionCount: 1  RF: 3
  cleanup.policy=delete  retention.ms=777600000  retention.bytes=-1
  min.insync.replicas=2  unclean.leader.election.enable=false
Topic: finance-orders.FINANCE.ORDERS  PartitionCount: 6  RF: 3
  cleanup.policy=delete  retention.ms=604800000  retention.bytes=-1
  segment.ms=86400000  max.message.bytes=8650752
  min.insync.replicas=2  unclean.leader.election.enable=false
Topic: finance-orders.FINANCE.ORDER_LINES  PartitionCount: 6  RF: 3
  cleanup.policy=delete  retention.ms=604800000  retention.bytes=-1
  segment.ms=86400000  max.message.bytes=8650752
  min.insync.replicas=2  unclean.leader.election.enable=false
```

The transaction topic's `retention.ms` (9 days) exceeds the data topics' `retention.ms` plus `segment.ms` plus the broker's default `log.retention.check.interval.ms` (7 days, 1 day, and 5 minutes), as B-KFK-35 requires.
`max.message.bytes` (8.25 MiB) is set on every stream topic, the control topic included, so an event up to about 8 MiB fits with room for its key, headers, and batch overhead (B-KFK-27).

### 1.5. Security

The core's transport-security guidance ([core Section 14.2][core-14-2]) is SHOULD-level and defers the mechanism to the binding.
This binding fixes the mechanism.

- **B-KFK-10 (Deployment).** Every listener used by the publisher, by clients, or by relays for a claimed stream MUST use TLS 1.2 or later (security protocol `SSL` or `SASL_SSL`).
  `PLAINTEXT` and `SASL_PLAINTEXT` listeners do not satisfy this binding.
- **B-KFK-11 (Publisher and Client).** The publisher and every client MUST validate the broker's certificate chain and host identity and MUST NOT proceed past a failed validation.
  Setting `ssl.endpoint.identification.algorithm` to an empty value disables host identity validation; a component so configured is not operating under this binding.
- **B-KFK-12.** Principals MUST authenticate with mutual TLS or a SASL mechanism (`SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, or `GSSAPI`).
  SASL `PLAIN` is permitted only over TLS.
  Credentials MUST NOT appear in record headers, record keys, or topic names; core S-AUTH-2 already forbids them in events.
- **B-KFK-13 (Deployment).** The deployment MUST NOT grant `Write` on a stream's topics, or `Write` on the publisher's `TransactionalId`, to any principal other than the publisher, or a relay writing to its own target topics.
  It MUST restrict `Alter` (which also authorizes `CreatePartitions`), `AlterConfigs`, and `Delete` (which permits `DeleteRecords`) on stream topics to administrators.
  A principal that can write to a data topic can inject events that clients cannot distinguish from the producer's; one that can alter a topic can undo the configuration this binding relies on (4.2, 5.5, 5.8).
- **B-KFK-14 (Deployment).** The stream is the unit of authorization.
  Kafka ACLs are topic-grained, and every principal that can read the control topic can read every captured table's schema and the stream's `tables` declaration.
  A deployment that must keep a table, or its schema, from some readers MUST place that table in a separate stream.
  Core producer rules S-AUTH-2, S-AUTHZ-1, and S-AUTHZ-2 ([core Section 14.1][core-14-1]) apply unchanged; masking configured per consumer identity (S-AUTHZ-2) is realized by a separate stream per identity.

*Note.* S-AUTHZ-1 is directed at session-aware producers.
A Kafka publisher is not session aware (4.1) and cannot tailor what one reader receives; that is why B-KFK-14 moves per-table authorization to the choice of stream boundaries.

### 1.6. Transport Capabilities

Every OpenCDC binding declares the transport capabilities it relies on, so that a near-miss transport, or a Kafka-compatible service missing a feature, can identify the clause it fails.

| Capability                          | Kafka                                                  | Consequence                                                                     |
| ----------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------- |
| Per-client session                  | No; the publisher never observes readers               | `session_aware: false` (4.1); STREAM_METADATA and schema via control topic (5.1) |
| Ordered delivery                    | Per partition only                                     | Channel = partition; cross-partition order restored from the transaction topic (5.4) |
| Multiple ordered channels           | Topics times partitions                                | Multi-channel delivery; TRX_COMMIT required for every transaction (4.1)        |
| Per-record metadata headers         | Yes (record format v2, Kafka 0.11 and later)           | Binary content mode available (3.3)                                            |
| Key-based partition assignment      | Yes; the partitioner is a client-side function         | Row identity fixes the partition (3.4); relays preserve partition numbers (B-KFK-36) |
| Broker-side retention               | Yes, per topic, by time or size                        | Replay window is the data topics' time retention (5.5)                          |
| Log compaction                      | Yes, per key, never within `min.compaction.lag.ms`     | Control topic keeps every schema version (4.2); data topics MAY compact beyond the replay window (B-KFK-7) |
| Consumer-driven replay              | Seek by offset or by record timestamp                  | No producer involvement in replay (5.2)                                         |
| Idempotent and transactional producer | Yes                                                  | Per-partition order survives retries (4.2); transactions optional (5.6)         |
| Record size limit                   | `max.message.bytes`, `message.max.bytes`, `max.request.size` | Fail closed on oversized events; no chunking (3.6)                        |
| In-band liveness                    | None                                                   | HEARTBEAT events on the transaction topic (5.7)                                 |
| Acknowledgement of consumption      | Consumer group offsets, invisible to the publisher     | No acknowledgement semantics; retention is independent of consumption (5.9)     |
| Partition count mutability          | Partitions can be added at runtime                     | Forbidden while the stream exists (5.8)                                         |

### 1.7. Versioning and Compatibility

- The binding version is `<wire>.<revision>`: the first two components are the OpenCDC wire protocol this document maps (`0.3`); the third is this document's revision.
  `-wip` marks a draft.
  Revisions correct or extend the binding without a wire change; a wire change produces a new `<wire>`.
- **B-KFK-15.** A publisher MUST emit only event types and attributes defined by the wire protocol version it declares in STREAM_METADATA and carries in `cdcspecversion`.
  A client that reads an event with a `cdcspecversion` it does not support MUST stop consuming the stream at that record and MUST NOT skip it.
- The binding version is not carried on the wire.
  A stream deployment states its claim, binding version included, out of band, for example with the client configuration it publishes (6.1).
- **B-KFK-16.** *Withdrawn.* It governed the stream descriptor, which was removed (Section 8, decision 15).
- Unknown members of an event are governed by the core's closed-world schema rules ([core Section 2.4][core-2-4]).

## 2. Use of OpenCDC Attributes

This specification does not redefine any OpenCDC attribute.
It fixes how these are used on this transport ([core Section 3.3][core-3-3]):

- `cdcpos` is the logical resume position.
  It is not a Kafka offset and is never passed back to the publisher; clients position themselves with Kafka offsets (5.2).
  A client persists `cdcpos` as the position of record and treats offsets as its transport-specific realization ([core Appendix B.4][core-b4]).
- **B-KFK-17.** `cdcpos` values, and every other event field, MUST NOT encode Kafka coordinates (cluster id, topic, partition, or offset).
  Coordinates change when a relay copies the stream (B-KFK-36); an event that embeds them would differ between copies.
- `sequence` values, when emitted, are comparable only among records of one partition.
  They establish no order between partitions (R-POS-0).
- `cdcxid` identifies the transaction a data event belongs to, and is the key of that transaction's TRX_COMMIT record (3.4).
  In binary mode it is also the `ce_cdcxid` header, so a stream processor can group by transaction without parsing the value.
- `partitionkey` is advisory.
  It does not determine the record key unless it satisfies the record-key rule (B-KFK-24).
- `dataschema` is resolved against the OBJECT_METADATA records of the control topic (5.3).
- `cdcspecversion` is the wire protocol version and is the client's compatibility check (1.7).

## 3. Kafka Message Mapping

### 3.1. Record Unit

- **B-KFK-18.** Each Kafka record MUST carry exactly one OpenCDC event.
  The CloudEvents batch format MUST NOT be used.
  Kafka record batches, producer batching, and fetch boundaries are transport mechanics and carry no OpenCDC meaning; in particular a client MUST NOT infer transaction completion from them.

### 3.2. Structured Content Mode

- **B-KFK-19.** In structured mode the record value MUST be the event in the CloudEvents JSON event format, UTF-8 encoded, and the record MUST carry a `content-type` header with the value `application/cloudevents+json; charset=UTF-8`.
  The header is optional in the CloudEvents Kafka binding; it is required here so that a client can always distinguish the two modes.
  A publisher MAY also add `ce_` headers ([section 3.3.3][ce-kafka-structured]); if it does, each MUST equal the corresponding attribute in the value, and the value is authoritative.

Structured mode, an INSERT on `FINANCE.ORDERS` (value abridged):

```text
------------------ Message -------------------
Topic Name: finance-orders.FINANCE.ORDERS
Partition:  4
------------------- key ----------------------
{"ORDER_ID":1001}
------------------ headers -------------------
content-type: application/cloudevents+json; charset=UTF-8
------------------- value --------------------
{
  "specversion":     "1.1",
  "id":              "7f3a2b10-e14c-4d8a-9f62-3c1d8e4b5a09",
  "source":          "//oracle-prod.acme.com/ORCL/FINANCE",
  "subject":         "FINANCE.ORDERS",
  "type":            "com.acme.cdc.dml.INSERT",
  "time":            "2026-03-22T14:23:01.000Z",
  "datacontenttype": "application/json",
  "dataschema":      "schema-ORDERS-v2",
  "cdcspecversion":  "0.3",
  "cdcxid":          "1510528009.5.13.7625",
  "cdctxorder":      0,
  "cdcpos":          "0000012C000004D2:14",
  "data": {
    "table":       { "schema": "FINANCE", "name": "ORDERS" },
    "primary_key": ["ORDER_ID"],
    "before":      null,
    "after":       { "ORDER_ID": 1001, "STATUS": "PENDING", "AMOUNT": "199.99" },
    "_null_columns": [], "_lob_overflow": [],
    "pos": { "lsn": "0000012C000004D2", "lsn_offset": 14,
             "source_timestamp": "2026-03-22T14:23:01.000Z" }
  }
}
-----------------------------------------------
```

### 3.3. Binary Content Mode

- **B-KFK-20.** In binary mode the record value MUST be the event's `data` serialized as JSON, UTF-8 encoded, and the `content-type` header MUST carry the event's `datacontenttype` (`application/json`).
  Every other attribute present in the event MUST appear as exactly one header named `ce_` followed by the attribute name, with a UTF-8 string value in the attribute's CloudEvents canonical string form (an Integer such as `cdctxorder` is a decimal string).
  A publisher MUST NOT write a header for an absent attribute and MUST NOT repeat a header key.

The same INSERT in binary mode:

```text
------------------ Message -------------------
Topic Name: finance-orders.FINANCE.ORDERS
Partition:  4
------------------- key ----------------------
{"ORDER_ID":1001}
------------------ headers -------------------
ce_specversion: 1.1
ce_id: 7f3a2b10-e14c-4d8a-9f62-3c1d8e4b5a09
ce_source: //oracle-prod.acme.com/ORCL/FINANCE
ce_subject: FINANCE.ORDERS
ce_type: com.acme.cdc.dml.INSERT
ce_time: 2026-03-22T14:23:01.000Z
ce_dataschema: schema-ORDERS-v2
ce_cdcspecversion: 0.3
ce_cdcxid: 1510528009.5.13.7625
ce_cdctxorder: 0
ce_cdcpos: 0000012C000004D2:14
content-type: application/json
------------------- value --------------------
{ "table": { "schema": "FINANCE", "name": "ORDERS" },
  "primary_key": ["ORDER_ID"], "before": null,
  "after": { "ORDER_ID": 1001, "STATUS": "PENDING", "AMOUNT": "199.99" },
  "_null_columns": [], "_lob_overflow": [],
  "pos": { "lsn": "0000012C000004D2", "lsn_offset": 14,
           "source_timestamp": "2026-03-22T14:23:01.000Z" } }
-----------------------------------------------
```

*Note.* Every OpenCDC event has `data`, so a binary-mode record never has a null value and is never a compaction tombstone ([CloudEvents Kafka binding section 3.2.2][ce-kafka-binary]).

### 3.4. Record Keys and Partitioning

The record key decides the partition, and the partition is the only unit in which Kafka preserves order.
The rules below keep every change to one row in one partition, which is what per-row order depends on.

- **B-KFK-21.** The key of a data-topic record MUST be a deterministic function of the row identity of the event it carries, and MUST NOT be null except as permitted below.
  Row identity is:

| Event                                                          | Row identity                                        |
| -------------------------------------------------------------- | --------------------------------------------------- |
| `dml.INSERT`, `dml.UPDATE`, `dml.UPSERT`, `snapshot.READ`      | The key values of `after`                           |
| `dml.DELETE`                                                   | The key values of `before`                          |
| `dml.TRUNCATE`                                                 | The `subject`, or no key (below)                    |

  The key values are those of the `primary_key` columns declared in OBJECT_METADATA, whether the source table's primary key or a configured surrogate key; a table whose `primary_key` is empty is keyless and has no per-row identity.
  For a keyless table the key is the `subject` alone, so every row of the table shares one key; alternatively, the publisher MAY write a null key for the table's `dml.INSERT`, `dml.UPDATE`, `dml.UPSERT`, `snapshot.READ`, and `dml.DELETE` records.
  A `dml.TRUNCATE` MAY likewise carry a null key and no key schema, which a schema-registry key serializer writes without registering anything.
  Whether a publisher writes null keys for a subject, and the key's encoding otherwise, are the publisher's choice and MUST NOT change for the life of the stream.
  On a data topic that has compaction enabled and carries more than one subject, the key MUST also include the `subject`, so that compaction does not treat rows of different tables with equal key values as one row.

*Note.* Kafka assigns partitions by hashing the key's bytes, so the encoding decides each row's partition, and changing it would move rows between partitions, which B-KFK-22 forbids.
No client reads the key: row identity for applying changes comes from `primary_key` in OBJECT_METADATA ([core Appendix A.4][core-a4], C-KEY-1), and a consumer's row identity, including C-KEY-1's full-row fallback for a keyless table, is independent of the record key.
A key built from the key columns alone, as existing CDC connectors commonly write it, satisfies this rule.

*Note.* A record with a null key is not a tombstone: a tombstone has a key and a null value (B-KFK-62), while a null-keyed record has an event as its value.

*Note.* Kafka rejects a record without a key on a topic whose `cleanup.policy` includes `compact` (`INVALID_RECORD`).
A publisher cannot reliably learn a topic's `cleanup.policy` when it writes, so this binding leaves prevention to the deployment (B-KFK-67).
A rejected record is a non-retriable error that stops the whole stream (B-KFK-53); because the publisher advances its checkpoint only after acknowledgement (B-KFK-47), it sends the record again once the deployment corrects the configuration.

*Note.* A `subject` key has a different shape from a keyed table's key-column struct, so the record keys on one data topic need not share a schema: a TRUNCATE keyed by `subject` beside keyed rows, or a keyless table beside keyed tables.
This binding carries JSON without a schema registry, where that is harmless; an encoding that validates keys against a registry (8.2 item 3) cannot use a one-schema-per-topic key subject strategy for such a topic.
A null key registers nothing.

- **B-KFK-22.** The partition of a data-topic record MUST be a deterministic function of its key and the topic's partition count, and the function MUST NOT change for the life of the stream.
  Records with equal keys on one topic MUST be written to the same partition.
  A record with a null key (B-KFK-21) is exempt: the producer's partitioner chooses its partition, which can differ between sends of the same event, including a resend after a publisher restart (B-KFK-47).

*Note.* A keyless table written with null keys to a topic with more than one partition has its rows spread across partitions, and a null-keyed TRUNCATE can land in any partition.
As on any multi-partition topic, a client that needs events in order uses `cdcpos`, `cdcxid`, and the transaction topic (5.4), not partition arrival; a deployment that wants such a table's events in partition order gives it a single-partition topic.
- **B-KFK-23.** On the control topic the key MUST be `opencdc:stream-metadata` for STREAM_METADATA, `opencdc:object-metadata:` followed by the event `id` for OBJECT_METADATA, and `opencdc:ddl:` followed by the event `id` for a `ddl.*` event.
  On the transaction topic the key of a TRX_COMMIT record MUST be its `cdcxid`, and a HEARTBEAT record MUST have no key.
- **B-KFK-24.** The event's `partitionkey` attribute, when present, is carried unchanged ([CloudEvents Kafka binding section 3.1][ce-kafka-key]).
  A publisher MUST NOT use it as the record key unless doing so satisfies B-KFK-21.

*Why OBJECT_METADATA and DDL events are keyed by `id`.* Each schema version has its own CloudEvents `id`, and a re-emission of an unchanged version repeats it.
Keying by `id` means compaction never removes a version or a DDL event (4.2), while repeated emissions of the same event collapse to one record.

*Why the key is not the transaction.* Core P-ORD-6 suggests giving all events of a transaction one `partitionkey`.
On Kafka that places a row's changes in different partitions whenever they fall in different transactions, which loses per-row order.
This binding keys by row instead and restores transaction structure from `cdcxid` and the transaction topic (Section 8, deferred core item 4).

*Note.* An UPDATE that changes a primary key is keyed by its new key, so it can land in a different partition from earlier changes to the same row.
Likewise a TRUNCATE is keyed by subject alone and lands in one partition, apart from the table's row events, and a null-keyed TRUNCATE lands in whichever partition the producer chooses, possibly a different one if it is sent again; DDL events are on the control topic (B-KFK-9).
A client applying partitions independently can then apply such an event before earlier changes it should follow.
A client that orders transactions by the transaction topic (5.4) is unaffected.
The hazard for key changes is avoided by emitting a DELETE under the old key and an INSERT under the new key, which keeps the DELETE ordered with the row's earlier changes; whether an OpenCDC producer may represent a key change that way is Section 8, deferred core item 8.

### 3.5. Non-Event Records

- **B-KFK-25.** A publisher MUST NOT write any record that is not an OpenCDC event to a data topic or the transaction topic, including records with a null value (tombstones), except as B-KFK-62 permits on compacted data topics.
  The control topic carries only events.
  A publisher MUST NOT write tombstones to the control topic.
- **B-KFK-62.** On a data topic with compaction enabled, a publisher MAY write a tombstone after a `dml.DELETE`, and SHOULD write one for the old key after a `dml.UPDATE` that changes the primary key.
  A tombstone is a record with a null value and the key, and therefore the partition, of the row it removes (B-KFK-21, B-KFK-22).
  It MUST NOT carry a `content-type` or `ce_` header, it is not an event, and it is never counted in `event_count`.
  A client MUST NOT treat a tombstone as an OpenCDC event: it has no `id`, `cdcxid`, or position, takes no part in deduplication by `(source, id)`, and does not count toward a transaction's completeness.
  Whether a client otherwise ignores tombstones or uses one as a signal that its key was removed is consumer processing and outside this binding.

*Note.* Every tombstone on a claimed stream follows the event that removed its key, so a client that treats tombstones as deletes removes the row once from the event and once from the tombstone, with the same result.

*Note.* An OpenCDC DELETE carries its before image, so it is never itself a tombstone and does not remove its key under compaction; the tombstone is what lets compaction eventually drop a deleted row.
Without the tombstone for the old key, a primary-key change leaves the row's previous record under that key, and a sink that bootstraps from the compacted region restores a row that no longer exists.
A TRUNCATE is keyed by its subject (B-KFK-21), so compaction keeps it alongside every earlier row of the table under their own keys; on a multi-partition topic, beyond the window, nothing orders it against those rows, and a bootstrapping sink can restore rows the TRUNCATE removed.
A deployment that needs correct state across a TRUNCATE avoids this configuration (B-KFK-7).

*Note.* Whether a TRUNCATE at the source reaches the stream as a `dml.TRUNCATE` depends on the source engine and the producer, not on this binding: some engines do not record a TRUNCATE where a capture layer can read it, and some producers capture it only when configured to, independently of `ddl_capture`, which governs only `ddl.*` events (core [Section 9.2][core-9-2]).
A stream can therefore carry no `dml.TRUNCATE` for a table that was truncated at the source, and a client cannot take the absence of one as evidence that none occurred.

### 3.6. Size Limits

- **B-KFK-26.** This binding defines no application-level chunking or truncation of events.
  Truncating LOB values, DDL text, or records to a size ceiling is a payload change governed by the core ([Section 6.2][core-6-2], [Section 6.3][core-6-3], [Section 9][core-9]); a publisher that truncates is not emitting the core's events and MUST NOT claim this binding for that stream.
- **B-KFK-27 (Publisher and Deployment).** The largest event a stream admits is set by the deployment's broker `message.max.bytes` and topic `max.message.bytes`, which the deployment MUST set on every stream topic, the control topic included, and by the publisher's `max.request.size`, which MUST be at least the topic limit.
  These limits apply to the whole record batch, including key, headers (every attribute, in binary mode), and batch overhead, not to the event alone.
  A record the producer or broker rejects as too large is a configuration error: the publisher MUST stop publishing the stream and MUST NOT truncate, drop, skip, or divert the event (for example to a dead-letter topic).

*Note.* Kafka clients return a record batch larger than `fetch.max.bytes` or `max.partition.fetch.bytes` rather than stalling, so a client needs no fetch configuration to make progress past a large event.
It does need memory for the largest record the topics accept, which it can read from their `max.message.bytes` (B-KFK-65).

## 4. Stream Profile and Delivery Properties

Kafka is sessionless and partitioned.
That fixes some capability axes, and it means that whatever the producer emitted, a client receives several ordered channels rather than one.
This section states what the deployment guarantees about the delivered stream, which is the part of C-COMP-1 a binding exists to settle.

### 4.1. Capability Axes

The publisher emits STREAM_METADATA as usual ([core Section 10.4][core-10-4]); the binding constrains which values a claiming publisher may declare ([core Section 2.2a][core-2-2a]).

| STREAM_METADATA field                  | Value under this binding                        | Why                                                                                                   |
| -------------------------------------- | ----------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `ordering_scope`                       | `"channel"`                                     | A Kafka publisher always chooses partitions in-process, so its emission channels are the partitions (B-KFK-31) |
| `transaction_interleaving`             | Producer-declared                               | Per-partition contiguity is preserved as emitted (B-KFK-32)                                           |
| `session_aware`                        | `false` or absent                               | The publisher cannot observe readers (B-KFK-28)                                                       |
| `transaction_boundaries`               | `"commit_all"`                                  | Markers are the only completion mechanism that survives partitioning (B-KFK-29)                       |
| `transaction_marker_delivery`          | `"transaction_metadata_channel"`                | The transaction topic (B-KFK-29)                                                                      |
| `schema_delivery.schema_on_change`     | `true`                                          | Core requirement, unchanged                                                                           |
| `schema_delivery.schema_on_reconnect`  | Producer-declared                               | Realized by the control topic when declared ([core Section 4.5.2][core-4-5-2]); see B-KFK-30         |
| `schema_delivery.schema_on_each_event` | Producer-declared                               | Required by the core only when `schema_on_reconnect` is not declared (P-SCHEMA-4)                     |
| `schema_delivery.schema_by_reference`  | Producer-declared                               | A schema registry is a supplement; `cdcschemauri` never replaces the control topic ([core Section 4.4][core-4-4]) |
| `sequence_continuity`                  | Producer-declared                               | Describes `pos.lsn`, not Kafka offsets                                                                |
| `transaction_visibility`               | `"committed_only"`                              | Core default                                                                                          |
| `ddl_capture`                          | Producer-declared                               | DDL events are control-topic records (B-KFK-9)                                                        |
| `tables`                               | Every captured table of *this stream*           | Each table's row events are on one data topic (B-KFK-30)                                              |

- **B-KFK-28.** STREAM_METADATA MUST declare `session_aware: false` or omit it.
- **B-KFK-29.** STREAM_METADATA MUST declare `transaction_boundaries: "commit_all"` and `transaction_marker_delivery: "transaction_metadata_channel"`, and the publisher MUST emit a TRX_COMMIT for every transaction, whatever `transaction_interleaving` it declares.
  This realizes P-TRX-7 as a requirement: a client of a partitioned stream cannot prove completion of any transaction, single-event or not, without a marker.
- **B-KFK-30.** The DML, TRUNCATE, and `snapshot.READ` events of a subject MUST all be written to one data topic, which MUST NOT change for the life of the stream, so that a client configured with a subject's data topic receives every row event of that subject.
  Reconnect coverage MUST be declared as the core requires (P-SCHEMA-2, P-SCHEMA-4).
  Under `ordering_scope: "channel"` the core permits `schema_on_reconnect: true` with `session_aware: false` ([core Section 4.5.2][core-4-5-2]), and the control topic is the durable control channel that realizes it; `schema_on_each_event: true` is then optional.
- **B-KFK-31.** STREAM_METADATA MUST declare `ordering_scope: "channel"`.
  A Kafka publisher always chooses each record's partition in its own process, whether by the default partitioner, a partition-routing transform, or a custom partitioner, so its emission channels are the partitions ([core Section 2.2a][core-2-2a]).
  The value is fixed by claiming this binding, not configured per deployment; how the publisher partitions is governed separately, by B-KFK-21 and B-KFK-22.

*Note.* Declaring `"channel"` gives up nothing a client of this binding could use: the delivered stream is ordered per partition only (4.2), whatever the emission declaration says.
A stream on a single partition that preserves one total order would declare `"stream"`; that profile is deferred (Section 8).

### 4.2. Delivery Properties

The core charters four delivery properties for bindings to govern ([core Section 12][core-12]).
Under this binding a client may rely on the following about the delivered stream, and on nothing stronger: order and the declared `transaction_interleaving` hold within each partition; across partitions there is no order, so a transaction's events arrive in `cdctxorder` sequence within each partition but not across partitions, and a client reassembles them by `cdcxid` and `cdctxorder` ([core Section 8.3][core-8-3], [core Appendix A.4][core-a4]); the source commit order of transactions is the order of the transaction topic.
A client that needs a total order across tables assembles it from the transaction topic (5.4).
A single-partition profile that would preserve `ordering_scope: "stream"` end to end is deferred (Section 8).

- **B-KFK-32 (Ordering preservation).** Within each partition of each topic, the publisher MUST write records in the producer's emitted order.
  The publisher MUST configure its Kafka producer so that retries cannot reorder records within a partition: `enable.idempotence=true`, or `max.in.flight.requests.per.connection=1`.
  Events written to the transaction topic are therefore in emitted order, which for TRX_COMMIT is source commit order (P-ORD-1).
- **B-KFK-33 (Control-channel retention).** The deployment MUST keep, on the control topic, the latest STREAM_METADATA, every OBJECT_METADATA version, and every DDL event the publisher has written, for the life of the stream.
  With `cleanup.policy=compact`, the keys of B-KFK-23, and no tombstones (B-KFK-25), compaction removes only superseded STREAM_METADATA.
- **B-KFK-34 (Schema retention across the replay window).** Because the control topic never removes a schema version, every OBJECT_METADATA version that governs a retained data event remains retrievable, which realizes R-POS-7 for the whole replay window.
  In addition, the publisher MUST NOT write a record to a data topic whose `dataschema` names an OBJECT_METADATA version until that version's control-topic record has been acknowledged by the cluster.
  A client that sees a data-topic event can therefore always find its schema on the control topic (5.3).
- **B-KFK-35 (Marker retention parity).** The deployment MUST set `retention.bytes=-1` on the transaction topic and on every data topic whose `cleanup.policy` includes `delete`; on a `compact`-only topic the setting has no effect.
  It MUST set the transaction topic's `retention.ms` greater than the maximum, over the data topics, of the topic's replay horizon plus `segment.ms` plus the broker's `log.retention.check.interval.ms`, where a data topic's replay horizon is its `min.compaction.lag.ms` if compaction is enabled and its `retention.ms` otherwise.
  If any data topic with `cleanup.policy=delete` has `retention.ms=-1`, the transaction topic MUST as well.
  Kafka deletes whole segments, so a data record can outlive its topic's `retention.ms` by up to one segment roll; the margin keeps the marker for every retained data event (P-RET-1, R-POS-6).
- **B-KFK-36 (Intermediary integrity).** A relay that copies a stream MUST copy all of its topics, control and transaction topics included, and MUST deliver them under a configuration that satisfies 1.4 and 4.2.
  It MUST preserve, for every record: the partition number, the order within the partition, the key, the value, and the `content-type` and `ce_` headers.
  It MUST NOT drop, filter, merge, split, or re-key events, and MUST NOT recompute partitions from keys (partitioners differ between Kafka client libraries).
  As the one exception to preserving values and headers, a relay MAY convert the whole stream between structured and binary mode; if it does, it MUST restore each attribute to its core type (for example, `cdctxorder` from the header string `"0"` to the JSON integer `0`), which a generic CloudEvents converter does not do.
  A relay that renames topics changes the stream's address; clients of the copy are configured with the new names (6.1).
  A relay that violates this is not covered by the stream deployment's claim.

*Note.* The producer settings in B-KFK-32, B-KFK-46, B-KFK-48, and B-KFK-60 belong to whatever creates the Kafka producer, which is not always the component that produces the events.
In Kafka Connect the worker creates it, so a connector cannot establish these settings itself (7.1).

*Note.* A relay does not preserve offsets.
Committed consumer-group offsets from the source cluster are not valid on the target; a client that moves to a copy repositions by `cdcpos` and `(source, id)`, or by an offset translation the relay provides as a convenience (for example MirrorMaker 2 checkpoints).

## 5. Delivery Protocol

Kafka has no session, so this section describes how a client finds a stream, where it starts, how it resolves schemas and completes transactions, and what the publisher and deployment guarantee over time.

### 5.1. Startup

- **B-KFK-37.** Before writing the first record to any data topic or the transaction topic, the publisher MUST write STREAM_METADATA to the control topic and MUST have it acknowledged by the cluster.
  On every later start, before writing any other record, the publisher MUST ensure that the latest STREAM_METADATA on the control topic is the one in effect for this start.
  How it does so is the publisher's choice: it MAY write it unconditionally on every start, or it MAY read the control topic to its end and write only what differs, provided the comparison guarantees that the control topic then holds exactly what it would hold after an unconditional write.

*Note.* An unconditional write on every start is always compliant and needs no memory of earlier writes: STREAM_METADATA has a fixed key on a compacted topic (B-KFK-23), so an unchanged rewrite collapses under compaction and clients treat it as no change (B-KFK-52).
A comparison must cover the full content of the record, including `sequence_continuity`, which a restart after a failover or source change can alter ([core Section 8.4.3][core-8-4-3]), and must be made after the publisher has fenced any earlier instance (B-KFK-60), so that no other writer can change the control topic between the read and the write.

- **B-KFK-38 (Client).** Before processing any record from a data topic or the transaction topic, a client MUST hold the control topic's state as of its end offset at startup: the last record for each key.
  It gets there either by reading the control topic from its beginning, or by restoring a cache of that state it persisted earlier, together with the control-topic offset the cache reflects, and reading onward from that offset to the end.
  It MUST locate the stream's topics from its configuration (6.1), and MUST process STREAM_METADATA before any data event ([core Appendix A.1][core-a1]).

*Note.* Keeping the last record per key, in offset order on one partition, is how this binding realizes the core's advice to keep the latest metadata revision per key ([core Appendix B.4][core-b4]).
No revision field is needed (Section 8, deferred core item 5).

*Note.* The control topic is read outside any consumer group: a client assigns its one partition directly rather than subscribing, commits no offsets for it, and positions itself at the beginning or at its cached offset.
Every client instance reads it in full, because a consumer group would give the partition to one member and leave the others without STREAM_METADATA and schemas.

### 5.2. Start Position

A client chooses its own start position; the publisher is not involved (R-POS-2-MC).
The positions a client can start from are:

| Start                   | Meaning                                                            | Mechanism                                                     |
| ----------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------- |
| Resume                  | Continue after the records this client has processed               | Offsets the client committed, per partition, for every stream topic |
| Earliest                | Oldest retained records                                            | Beginning offset of each partition                            |
| Latest                  | Records written after the client starts                            | End offset of each partition                                  |
| Timestamp               | Records written at or after a point in time                        | `offsetsForTimes` on record timestamps (B-KFK-41)             |

- **B-KFK-39 (Client).** A resuming client MUST resume every partition of every stream topic it reads, the transaction topic included, from offsets it committed only after processing the corresponding records.
  A client MUST treat offsets as the mechanical checkpoint and `cdcpos` as the logical position of record ([core Appendix B.4][core-b4]).
- **B-KFK-40 (Client).** A client MUST NOT let `auto.offset.reset` reposition it silently.
  It MUST configure `auto.offset.reset=none` and set its initial positions explicitly.
  A committed offset that is no longer valid, whether because retention removed the records or because the group's committed offsets expired (`offsets.retention.minutes`), is a gap that MUST be surfaced (5.9), never converted into a jump to earliest or latest.
- **B-KFK-41.** Record timestamps are publish times.
  A publisher SHOULD leave the record timestamp to the Kafka producer or configure `message.timestamp.type=LogAppendTime` on the stream's topics, and MUST NOT set it to the event `time`, `pos.source_timestamp`, or any other source time.
  Time-based retention is evaluated against the record timestamp; source times would shorten the replay window whenever the publisher is catching up.

*Note.* Earliest and timestamp starts land mid-transaction in general: the first records of a partition may belong to transactions whose earlier events are no longer retained, or were written before the chosen time.
Such a transaction never completes (0..`event_count`-1 is unreachable), and its marker is on the transaction topic at or after the start.
How a consumer treats that boundary is consumer guidance ([core Appendix A.7][core-a7], [core Appendix A.10][core-a10]).

### 5.3. Schema Resolution

- **B-KFK-42 (Client).** When a client reads a data event whose `dataschema` is not in its schema cache, it MUST read the control topic to its current end before treating the event as undecodable.
  Under B-KFK-34 the schema is on the control topic by the time the data record exists; a miss is a race between independently progressing partitions, not corruption ([core Appendix A.2][core-a2]).

- **B-KFK-59.** When the publisher writes a `ddl.CREATE` whose `dataschema` is a forward reference ([core Section 9.1][core-9-1]), it MUST write the named OBJECT_METADATA to the control topic after the CREATE, and both MUST be acknowledged before the subject's first DML or `snapshot.READ` record is written.
  Because DDL events and OBJECT_METADATA share the control topic's single partition, the CREATE and its governing OBJECT_METADATA are on one channel with the OBJECT_METADATA following, as the core requires ([core Section 4.1][core-4-1]).

A client that meets a forward-referencing `ddl.CREATE` buffers it until the named OBJECT_METADATA arrives, which it does later on the control topic ([core Appendix A.2][core-a2]); that case is not a miss under B-KFK-42.

A client that keeps consuming the control topic for the life of its session (B-KFK-52) rarely meets this case.
Schema versions are cached by `id`, never overwritten by table, so that a replayed position resolves its governing version.

### 5.4. Transaction Completion and the Transaction Topic

- **B-KFK-43.** The publisher MUST write a transaction's TRX_COMMIT to the transaction topic after all of that transaction's events have been written to data topics and, for DDL events, the control topic, and in source commit order relative to every other TRX_COMMIT.
  It MUST NOT let a TRX_COMMIT become visible to clients before all of the transaction's event records are acknowledged, either by waiting for their acknowledgements before sending the marker or by writing the transaction's records and its marker in one Kafka transaction (visible to `read_committed` clients only on commit).
  Asynchronous sends to different partitions complete in any order, so writing the marker last is not by itself enough.
- **B-KFK-61 (Client).** A client MUST NOT commit a transaction-topic offset past a TRX_COMMIT until it has finished processing that transaction.
  Offsets are committed per partition; committing the marker's offset while the transaction's data offsets are behind would lose the marker on resume, against R-POS-6 ([core Section 10.5.4][core-10-5-4]).
- **B-KFK-64 (Client).** A client assembling transactions MUST include the DDL events on the control topic, matched by `cdcxid` and counted by `cdctxorder` toward `event_count` like any other event.
  A DDL event whose transaction's TRX_COMMIT precedes the client's start position on the transaction topic is outside the range the client is consuming, and the client MUST NOT process it as a new change.

*Note.* A client holds the whole control topic's state at startup (B-KFK-38), so it holds DDL events from before its start position; the transaction topic tells it which of them are in range.
The control topic grows with every DDL event, including the statement text under `ddl_capture: "verbatim"`, and startup reads all of it; pruning is deferred (Section 8).
- **B-KFK-44.** Any filtering the publisher applies (by table, operation, column value, or any other criterion) MUST take effect before `cdctxorder` values are assigned and before TRX_COMMIT `event_count` and `distribution` are computed.
  `cdctxorder` MUST be dense over the events published, and `event_count` and `distribution` MUST describe exactly those events.
  A transaction none of whose events is published produces no TRX_COMMIT.

With B-KFK-43 satisfied, a TRX_COMMIT tells a client that every event of the transaction is durable on its data topic; if the client does not yet hold all ordinals `0..event_count-1`, it is behind on some partition, not missing data.
Because the transaction topic is a single partition written in commit order, it is also the stream's commit-order spine: a client that applies transactions in the order of their first-seen markers, discarding duplicate markers by `(source, id)`, applies them in source commit order across all tables.
Assembly strategies, bounded waits, and subset consumption by `distribution` are consumer guidance ([core Appendix A.10][core-a10], [core Appendix B.4][core-b4]).

### 5.5. Replay Window and Retention

- **B-KFK-45 (Deployment).** A stream's replay window is set by its data topics' configuration: it is the smallest, over the data topics, of `retention.ms` for a topic whose `cleanup.policy` is `delete`, and of `min.compaction.lag.ms` (or `retention.ms`, if smaller and the policy also includes `delete`) for a topic with compaction enabled.
  It is unbounded when every data topic has `cleanup.policy=delete` and `retention.ms=-1`.
  The window belongs to the deployment, not the publisher, and a client reads it from the topic configuration (B-KFK-65).
  Records older than the window may still be present; they are outside the claim.

*Note.* The window is not declared by the publisher, because the publisher does not own it and a declared copy would drift whenever an operator changes a topic.
An operator that reduces retention or a compaction lag removes replayable positions; a client resuming from one meets the gap handling of B-KFK-40.
Operators are expected to publish the window they intend to keep alongside the stream address (6.1).
- **B-KFK-63 (Client).** On a data topic whose `cleanup.policy` includes `compact`, a client MUST treat records whose record timestamp is older than the topic's `min.compaction.lag.ms` as table state, not as a changelog, whether or not the publisher writes tombstones (B-KFK-62): compaction removes superseded records for a key regardless of tombstones, and may not yet have removed all of them.
  It MUST NOT expect transactions among those records to be complete or their TRX_COMMIT markers to be available, MUST NOT take a marker's presence on the transaction topic as evidence that the transaction's records survive on this topic (B-KFK-35 requires the transaction topic's `retention.ms` to exceed, for every data topic, that topic's replay horizon plus `segment.ms` plus the broker's `log.retention.check.interval.ms`), and MUST expect `cdctxorder` gaps among them.
  Records inside the lag are an unaltered changelog, as on any other data topic.

*Note.* B-KFK-63 relies on record timestamps being publish times (B-KFK-41).
A client that bootstraps from compacted state and then continues into the changelog crosses from state to events at the lag boundary; how it reconciles the two is consumer guidance, comparable to the snapshot-to-steady-state transition ([core Appendix A.8][core-a8]).

*Note.* Size-based retention (`retention.bytes`) is excluded by B-KFK-35, because a window measured in bytes cannot be compared across topics of different volume.
A size limit also deletes a partition's oldest segments whenever it grows past the limit, so a burst of changes can remove records younger than the replay window.
Tiered storage ([KIP-405][kip-405]) changes where segments live, not when they are deleted, and is compatible with this binding on topics whose `cleanup.policy` is `delete`.
Apache Kafka (as of 4.3) rejects tiered storage on a topic whose `cleanup.policy` includes `compact`, so it does not apply to the control topic or to a compacted data topic; a Kafka-compatible service may differ.

### 5.6. Publishing Durability

This revision defines Durable Mode only ([core Section 15.1][core-15-1]).

- **B-KFK-46 (Publisher and Deployment).** The publisher MUST write with `acks=all`.
  On every stream topic the deployment MUST set `unclean.leader.election.enable=false`, MUST set `min.insync.replicas` to at least 2, and MUST use a replication factor of at least `min.insync.replicas` plus 1 (3 or more is typical).
  With one in-sync replica, `acks=all` acknowledges a single broker's write, which does not meet core Section 15.1's durability obligation; unclean leader election discards acknowledged records, which is a gap no client can detect.
- **B-KFK-47.** The publisher MUST NOT advance its source checkpoint (for example, a Kafka Connect source offset) past an event until that event's record has been acknowledged by the cluster.
  After a restart it resumes from its checkpoint and re-emits events with their original `id` values; the resulting duplicates are permitted (R-POS-5) and are resolved by `(source, id)`.
- **B-KFK-60.** Exactly one producer instance at a time MUST write a stream's control topic and transaction topic, and each data-topic partition MUST be written by at most one producer instance at a time.
  A publisher that can run more than one instance (for example, after a failover) MUST fence the previous instance before writing, using a Kafka `transactional.id` or an equivalent mechanism.
  Idempotence orders records from one producer session only; two writers on the transaction topic would interleave markers out of commit order, and a stale writer on the control topic could overwrite a newer STREAM_METADATA.
- **B-KFK-48.** The publisher MAY use Kafka transactions (for example, Kafka Connect exactly-once source support, [KIP-618][kip-618]).
  A Kafka transaction is not an OpenCDC transaction, and a client MUST NOT use Kafka transaction boundaries in place of TRX_COMMIT.
  Clients SHOULD read with `isolation.level=read_committed`.

*Note.* A client reading `read_uncommitted` from a transactional publisher can see records of aborted Kafka transactions.
Those records carry the original event `id` values of events the publisher will write again, so deduplication on `(source, id)` absorbs them, but they arrive before their schema and marker guarantees hold.

### 5.7. Heartbeat

- **B-KFK-49.** HEARTBEAT events MUST be written to the transaction topic.
  This revision places no HEARTBEAT on data topics.
  The HEARTBEAT obligation ([core Section 10.1][core-10-1]; `heartbeat_interval_seconds`) is met only by HEARTBEAT events; Kafka broker or consumer-group liveness does not count.

HEARTBEAT on this binding is a liveness signal for the whole stream.
It is not a transaction-completion signal: across channels, completion comes only from TRX_COMMIT ([core Section 10.1][core-10-1]).
A data partition that receives nothing for a long time is normal; a transaction topic that receives nothing for longer than `heartbeat_interval_seconds` is not.

### 5.8. Stream Reconfiguration

- **B-KFK-50.** Before writing the first event for a subject added to the stream, the publisher MUST write STREAM_METADATA updated as the core requires when `tables` changes, and MUST have it acknowledged.
  A new subject's data topic reaches clients through their configuration, not through the stream (B-KFK-52).
- **B-KFK-51 (Deployment).** The partition count of a stream topic MUST NOT change while the stream exists.
  Adding partitions changes the partition of existing keys, which breaks per-row order across the change.
  Repartitioning is done by creating a new stream (new stream name, new topics) and migrating clients to it.
- **B-KFK-52 (Client).** A client MUST keep reading the control topic while it consumes the stream, and MUST stop consuming the stream if a stream topic's partition count, as Kafka metadata reports it, changes while the client is consuming (B-KFK-51).
  When STREAM_METADATA adds a subject whose events the client processes, the client MUST read that subject's data topic from its beginning offset once it is configured with it.
  A client MUST treat STREAM_METADATA whose content is identical to the one it holds as no change.

A client that reads only some tables is configured with those tables' data topics and reads the transaction topic in full.
When a TRX_COMMIT's `distribution` names a subject the client processes but has no topic for, its configuration is incomplete.
A client configured with a topic pattern (for example Kafka Connect `topics.regex`) picks up a new subject's topic without reconfiguration.

### 5.9. Failure Behavior

Failures fall in three places: establishment (a client or publisher cannot authenticate, is not authorized, or cannot find the stream), steady state (errors raised while reading or writing), and abnormal loss (a component stops without reporting).
Kafka reports failures as client exceptions and broker error codes; the table names both where they differ.

| Condition                                                              | Kafka signal                                                                   | Party     | Disposition                                                                 |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------ | --------- | --------------------------------------------------------------------------- |
| Authentication failed                                                  | `SaslAuthenticationException`, `SslAuthenticationException`                    | Both      | Retry only with new credentials                                             |
| Not authorized for a stream topic or consumer group                    | `TopicAuthorizationException`, `GroupAuthorizationException`                   | Both      | No                                                                          |
| A configured stream topic does not exist                               | `UNKNOWN_TOPIC_OR_PARTITION` persisting past metadata refresh                  | Client    | No; the client configuration is wrong                                       |
| Leader change, broker restart, network interruption                    | Retriable errors (`NOT_LEADER_OR_FOLLOWER`, `NETWORK_EXCEPTION`, timeouts)     | Both      | Yes; handled by the Kafka client                                            |
| Consumer-group rebalance                                               | Partitions revoked and assigned                                                | Client    | Yes; resume assigned partitions from committed offsets (B-KFK-39)           |
| Committed offset outside retained data, or expired                     | `OffsetOutOfRangeException`, `NoOffsetForPartitionException`                   | Client    | No automatic retry; surface the gap (B-KFK-40)                              |
| Record not decodable, or of an unsupported `cdcspecversion`            | `RecordDeserializationException`, or binding-level                             | Client    | No; stop at the record (B-KFK-15, B-KFK-53)                                 |
| Partition count of a stream topic changes                              | Metadata                                                                       | Client    | No; stop (B-KFK-52)                                                         |
| Marker missing past the bounded wait                                   | (binding-level)                                                                | Client    | Alert; consumer guidance ([core Appendix A.10][core-a10])                   |
| Event larger than the stream admits                                    | `RecordTooLargeException`, `MESSAGE_TOO_LARGE`                                 | Publisher | No; stop publishing until the operator acts (B-KFK-27)                      |
| Another publisher instance took over                                   | `ProducerFencedException`, `InvalidProducerEpochException`                     | Publisher | No; this instance stops                                                     |
| Not enough in-sync replicas                                            | `NOT_ENOUGH_REPLICAS`, `NOT_ENOUGH_REPLICAS_AFTER_APPEND`                      | Publisher | Yes, with backoff; the checkpoint does not advance (B-KFK-47)               |

- **B-KFK-53.** On any non-retriable error, the publisher MUST stop publishing the stream, and a client MUST stop consuming it at the failing record.
  Neither MAY skip the record, divert it to a dead-letter topic, or continue past it under an error-tolerance setting (for example Kafka Connect `errors.tolerance=all`).
  Skipping converts a recoverable failure into an undetected gap ([core Section 15.1][core-15-1]).
- **B-KFK-54.** Nothing a client does is an acknowledgement to the publisher.
  Committed offsets are the client's own checkpoint, the publisher never observes them, and retention removes records whether or not any client has read them.
  A client MUST NOT treat a successful offset commit, a clean shutdown, or a rebalance as confirmation that the stream observed its progress.

## 6. Stream Address and Client Configuration

This binding defines no discovery record.
A client reads a stream from topics it is configured with, as any Kafka consumer does, and learns everything else from the stream and from Kafka.

Three sources are involved.
**Stream declarations** (capability axes, tables, heartbeat interval, schema delivery) belong to STREAM_METADATA on the control topic ([core Section 2.2a][core-2-2a], producer-sole-emitter invariant).
**Client configuration** names the stream's topics: its control topic, its transaction topic, and the data topics the client reads.
**Topic configuration** (partition counts, retention, compaction, size limits) belongs to the deployment, and a client reads it from Kafka (B-KFK-65).
The content mode is carried on every record (B-KFK-3), and every event carries its `subject`.

### 6.1. Client Configuration

- **B-KFK-55.** The address of a stream is the bootstrap servers of its Kafka cluster and the names of its control topic, its transaction topic, and its data topics, or a pattern that matches them.
  Whoever operates a claimed stream deployment MUST make the address available to authorized clients; how it does so (configuration, a catalog, an AsyncAPI document) is not specified.
- **B-KFK-66 (Client).** A client MUST read the stream's control topic and transaction topic, and the data topic of every subject whose events it processes.
  A client that processes only some subjects checks transaction completeness for those subjects with `distribution` ([core Appendix A.10][core-a10]).
- **B-KFK-65 (Deployment).** The deployment MUST grant `Describe` and `DescribeConfigs` on every stream topic to each principal authorized to read the stream, so that a client can read partition counts, `cleanup.policy`, `retention.ms`, `min.compaction.lag.ms`, and `max.message.bytes` from Kafka.
- **B-KFK-56.** *Withdrawn.* It governed the stream descriptor, which was removed (Section 8, decision 15).
- **B-KFK-57.** *Withdrawn.* It governed the stream descriptor, which was removed (Section 8, decision 15).

### 6.2. AsyncAPI Description

- **B-KFK-58.** A stream deployment MAY additionally publish an [AsyncAPI][asyncapi] 3.x document describing its topics, using the [AsyncAPI Kafka bindings][asyncapi-kafka].
  If it does, every `topicConfiguration` and `partitions` value in that document MUST match the deployment, and the document is informative.
  It is one way to distribute the client configuration of B-KFK-55.

An AsyncAPI template for this binding, analogous to the WSS + AsyncAPI binding's, is deferred (Section 8).

## 7. Mapping from Existing Implementations

This section is informative.
It records how existing implementations relate to this binding, from their published documentation: 7.1 covers the Kafka Connect runtime, which many CDC connectors share, and 7.2 covers Debezium, a set of source connectors that runs on it.
It is not an implementation plan and does not assert that any option, as documented, produces OpenCDC-conformant output.
Rows rest on the public documentation only; a *verified* column will be added once captured records are analysed (Section 8, open item 5).

"Fixed" means the binding requires a specific value; "excluded" means the option produces a stream that is not OpenCDC-conformant, or a deployment that does not satisfy this binding, and so cannot be claimed; "out of scope" means a provisioning or producer-internal concern the binding does not see; "converted" means a semantic mapping the publisher must implement, not a rename; "add" means a capability the implementation does not document.

### 7.1. Kafka Connect

A source connector running on [Kafka Connect][kafka-connect] does not write to Kafka itself.
The worker creates the Kafka producer, runs any transforms, applies the key, value, and header converters, and commits the connector's source offsets.
Several publisher requirements are therefore met, or broken, by the worker's configuration rather than by the connector, and a Connect-based stream deployment's claim depends on both.

| Kafka Connect setting                                             | Disposition                     | Note                                                                                                                           |
| ----------------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Worker `producer.acks`, `producer.enable.idempotence`, `producer.max.in.flight.requests.per.connection` | Fixed | `acks=all`, with idempotence on or one request in flight (B-KFK-32, B-KFK-46) (1)                                   |
| Connector `producer.override.*`, worker `connector.client.config.override.policy` | Mapped with constraint | Per-connector producer settings must also satisfy B-KFK-32 and B-KFK-46; the worker's policy decides whether they apply (1) |
| Worker `exactly.once.source.support`, connector `exactly.once.support` ([KIP-618][kip-618]) | Optional | Fences a previous task generation (B-KFK-60) and writes records and markers in one Kafka transaction, which satisfies the visibility part of B-KFK-43 (2) |
| Connector `tasks.max`                                             | Mapped with constraint          | One writer for the transaction topic, and at most one per data partition (B-KFK-60) (3)                                        |
| `errors.tolerance=all`, `errors.deadletterqueue.topic.name`       | Excluded                        | Skips or diverts records (B-KFK-53)                                                                                            |
| `key.converter`                                                   | Mapped with constraint          | Its output is the record key, whose encoding must not change for the life of the stream (B-KFK-21)                             |
| `value.converter`                                                 | Mapped                          | Writes the event in the declared content mode (B-KFK-1, B-KFK-19, B-KFK-20)                                                    |
| `header.converter`                                                | Mapped with constraint          | In binary mode, must write `ce_` header values as UTF-8 strings (B-KFK-20)                                                     |
| `transforms` (single message transforms)                          | Mapped with constraint          | Filtering or rewriting after ordinals and markers are assigned breaks B-KFK-44 (4)                                             |
| `topic.creation.enable`, `topic.creation.*` (KIP-158)             | Mapped                          | Topics must be created with the configuration of 1.4, 4.2, and 5.6; broker auto-creation with defaults does not satisfy it     |
| Source offset commit                                              | Carried                         | Connect commits a source offset only once the producer has acknowledged the record (B-KFK-47)                                  |

Notes:

1. A connector cannot set producer properties itself.
   They come from the worker configuration, with the `producer.` prefix, or from the connector configuration, with the `producer.override.` prefix, where the worker's override policy permits it; the effective values are what the claim rests on.
2. Exactly-once source support must be enabled on every worker in the cluster and supported by the connector.
   It is optional under this binding: B-KFK-43 can also be met by waiting for acknowledgements before writing a marker, and B-KFK-60 by any other fencing mechanism.
3. A connector that runs one task per source satisfies B-KFK-60 only if a task from an earlier generation cannot still be writing after a rebalance.
   Connect fences such tasks when exactly-once source support is enabled; without it, the deployment needs another way to exclude a second writer.
   A connector that runs several tasks for one stream has several producers, and would need all markers written by one of them, or a separate stream per task.
4. Transforms run before the converters.
   Where the OpenCDC ordinals and markers are assigned (by the connector, a transform, or the converter) decides which transforms may still filter records.

A sink connector reads the stream through the framework's consumer-group subscription, which delivers records from every subscribed topic interleaved, commits offsets for each, and spreads partitions across tasks.
That subscription suits the data topics, but not the control topic.

| Kafka Connect sink setting                                        | Disposition                     | Note                                                                                                                           |
| ----------------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `topics`, `topics.regex`                                          | Mapped with constraint          | Data topics, and the transaction topic where one task assembles transactions; never the control topic (5)                      |
| Control topic                                                     | Add                             | Each task reads it with its own consumer, assigned rather than subscribed, before `put()` processes data (B-KFK-38, B-KFK-52) (5) |
| `tasks.max` greater than 1                                        | Mapped with constraint          | Splits a table's partitions across tasks, so transaction assembly needs one task or a repartitioning stage (6)                 |

5. Through the framework subscription, the control topic's offset is committed like any other, so after a restart it resumes where it left off rather than at the beginning, and with more than one task only one task receives its single partition.
   A task-owned consumer avoids both; `SinkTaskContext.pause()` and `offset()` can approximate it within the subscription, but less simply.
6. A task that holds only some partitions of a table cannot verify a transaction's completeness, because `distribution` counts events per subject, not per partition.
   The choices are those of core Appendix B.4: a single reader, a stage that regroups events by `cdcxid`, or per-partition apply as a weaker service level.

### 7.2. Debezium

This subsection records how the configuration surface of [Debezium][debezium] source connectors, with the [Debezium CloudEvents converter][debezium-ce], relates to this binding.
Debezium's change event envelope and its CloudEvents mapping still require the OpenCDC payload and metadata transformation the core defines.
Runtime settings are in 7.1.

| Debezium option                                                    | Disposition                     | Note                                                                                                                           |
| ----------------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `value.converter=io.debezium.converters.CloudEventsConverter`     | Fixed                           | Structured mode only today; binary mode is documented as future work (1)                                                       |
| CloudEvents `serializer.type`, `data.serializer.type`             | Fixed `json`                    | Avro is reserved for a format binding (1.2)                                                                                    |
| CloudEvents `extension.attributes.enable`                         | Converted                       | The `iodebezium*` extensions are not OpenCDC attributes; the `cdc*` extensions are required (2)                                |
| CloudEvents `metadata.source` (`id:generate`)                     | Converted                       | Debezium derives `id` from content; the core requires UUID v4 for DML and DDL (3)                                              |
| `topic.prefix`                                                    | Mapped                          | Natural `<stream>` name (B-KFK-8)                                                                                              |
| Default topic per table (`<prefix>.<schema>.<table>` on PostgreSQL) | Carried                        | Data topics; one subject per topic (B-KFK-30)                                                                                  |
| Default record key (key columns as a struct, via `key.converter`) | Carried                         | A deterministic function of row identity (B-KFK-21); the encoding must not change for the life of the stream                   |
| Null key for a table with an empty `primary_key`                  | Carried, with constraint        | Permitted by B-KFK-21; a data topic with compaction must not receive it (B-KFK-67), so a deployment that compacts the topic first keys such tables by table name, or configures a surrogate key (`message.key.columns`) declared as `primary_key` |
| Truncate event (`op: t`), null key                                | Carried, with constraint        | Valid as written (B-KFK-21); a data topic with compaction must not receive it (B-KFK-67), so a deployment that compacts the topic first adds a transform that keys the event by its `subject` and leaves the event value unchanged. Debezium writes truncate events only when `skipped.operations` does not list `t`, and its default lists `t`, so they are suppressed unless the deployment changes it; even then, some source databases do not record a TRUNCATE where a connector can capture it (3.5). A consumer watching the stream alone cannot tell either case from a table that was never truncated. |
| Primary key change as DELETE plus CREATE (`__debezium.newkey`, `__debezium.oldkey` headers) | Open          | Whether OpenCDC admits this representation is a core question (Section 8, deferred core item 8)                                |
| Topic routing SMT (`ByLogicalTableRouter`)                        | Mapped with constraint          | Several subjects per data topic are allowed, each on one topic only (B-KFK-30); on a compacted topic `key.enforce.uniqueness=true` is required (B-KFK-21) (4) |
| Partition routing SMT (`PartitionRouting`)                        | Mapped with constraint          | Must be a deterministic function of row identity (B-KFK-21, B-KFK-22)                                                          |
| `message.key.columns`                                             | Mapped with constraint          | Only immutable columns preserve per-row partitioning; mutable key columns move a row between partitions (B-KFK-21)             |
| `tombstones.on.delete`                                            | Mapped with constraint          | `true` only on compacted data topics (B-KFK-62); `false` otherwise, since tombstones are not events (B-KFK-25)                 |
| `provide.transaction.metadata`                                    | Converted                       | Debezium `END` records map to TRX_COMMIT; `BEGIN` records have no OpenCDC counterpart (5)                                      |
| `topic.transaction` (`<prefix>.transaction`)                      | Mapped                          | Transaction topic; must have one partition (B-KFK-6) and hold HEARTBEAT (B-KFK-49)                                             |
| `heartbeat.interval.ms`, `topic.heartbeat.prefix`                 | Converted                       | Debezium heartbeats are source-offset keep-alives on their own topic; OpenCDC HEARTBEAT goes to the transaction topic (B-KFK-49) |
| Schema history topic (log-based connectors that keep one)        | Out of scope, and add           | Internal DDL history is not OBJECT_METADATA; the control topic must be added (B-KFK-33) (6)                                    |
| `include.schema.changes` (schema change topic)                    | Mapped                          | Like Debezium's schema change topic, DDL events stay off data topics; they go to the control topic (B-KFK-9)                   |
| `decimal.handling.mode=double`                                    | Excluded                        | Loses precision (P-TYPE-4)                                                                                                     |
| `time.precision.mode`, `binary.handling.mode`                     | Converted                       | Wire encoding follows the type system's `logical_type`, whatever the Debezium representation                                   |
| `column.exclude.list`, `table.include.list`                       | Mapped                          | Define the emission schema and captured tables; OBJECT_METADATA follows changes (core Section 4.1)                              |
| `skipped.operations`                                              | Mapped with constraint          | Ordinals and markers over the published set (B-KFK-44) (7)                                                                    |
| `snapshot.mode`                                                   | Out of scope                    | Snapshot events are `snapshot.READ` on data topics                                                                             |
| `event.processing.failure.handling.mode=warn` or `skip`           | Excluded                        | Skips events (B-KFK-53)                                                                                                        |

Notes:

1. The Debezium documentation states that only structured mapping mode is available and that binary mode is expected in a future release.
   A Debezium-based publisher can claim this binding with `contentMode: structured` today.
2. The Debezium CloudEvents example carries `iodebeziumop`, `iodebeziumlsn`, `iodebeziumtxid`, `iodebeziumtxtotalorder`, and `iodebeziumtxdatacollectionorder`.
   The OpenCDC counterparts are `type`, `pos`, `cdcxid`, and `cdctxorder`; `iodebeziumtxtotalorder` is 1-based in the published example while `cdctxorder` is 0-based (core Section 3.3).
   Whether extra extension attributes may accompany the `cdc*` attributes is Section 8, deferred core item 3.
3. The published example shows `id` values such as `name:test_server;lsn:29274832;txId:565`: content-derived and replay-stable, but not UUID v4.
   The core requires both UUID v4 and replay stability for DML and DDL `id` values, which a random UUID meets only if it is persisted before emission (core Section 11.2).
4. Routing several tables to one topic keeps them in one set of partitions, but a client can no longer choose tables by topic.
   Clients then select tables by `subject` rather than by topic.
5. Debezium's `END` record carries `event_count` and `data_collections`, which the core notes is semantically equivalent to `distribution` (core Section 10.5.3).
   The `END` record is written to the transaction topic in the Connect pipeline; whether it is written in commit order relative to other `END` records, and after its data records are acknowledged (B-KFK-43), must be verified.
6. Schema history topics store DDL for the connector's own recovery and are not intended for clients.
   The binding's control topic is a different artifact: it carries OBJECT_METADATA for clients, retains every version, and is required.
7. Skipping `u` (updates) while keeping inserts and deletes yields a stream that is conformant over what it emits but is not a faithful changelog; see the WSS + AsyncAPI binding's Section 5.7 and its deferred core item 4.

## 8. Decisions, Deferrals, and Open Items

### 8.1. Decisions Taken

Proposed by the drafter, 23 September 2026, for working-group review.
None is ratified; each is a place where this draft made a call so that review has something concrete to accept or reject.

1. The binding is an application of the CloudEvents Kafka binding, supporting both of its content modes (1.2, 1.3).
   The control topic is fixed to structured mode (B-KFK-2).
2. One profile: partitioned delivery.
   The delivered guarantee is order per partition, with transaction commit order recovered from the transaction topic (4.2).
   A single-partition profile that preserves a total order is deferred rather than half-specified.
3. Topic layout of one control topic, one transaction topic, and data topics (1.4); OBJECT_METADATA and DDL events only on the control topic; HEARTBEAT only on the transaction topic.
4. Records are keyed by row identity, not by transaction, departing from the P-ORD-6 suggestion for this transport (3.4).
5. *Superseded by decision 15.* Deployment declarations were carried in a stream descriptor record on the control topic rather than in headers on STREAM_METADATA.
6. Conformance parties are Publisher, Deployment, Client, and Relay.
   The register `who` vocabulary in [binding-formatting-decisions.md][formatting] lists `Endpoint`, which fits a binding whose endpoint both emits and delivers; Kafka separates the two, so this binding uses `Publisher` and `Deployment` instead (1.1).
7. Strict retention: `retention.bytes=-1` on data and transaction topics (B-KFK-35).
   Recorded as open item 3 in case the working group prefers a declared byte budget.
8. An AsyncAPI document is optional (6.2).
   Unlike the WSS + AsyncAPI binding, the description format is not part of the binding's name or purpose.
9. Revised before first circulation after a drafting review, which found and fixed: descriptor misread as a binary-mode CloudEvent (B-KFK-2, B-KFK-3); `ddl.CREATE` forward references against core Section 9.1 (B-KFK-34, B-KFK-59); durability weaker than core Section 15.1 (B-KFK-46); no single writer for the transaction topic (B-KFK-60); marker visibility only SHOULD (B-KFK-43); markers lost by early offset commits (B-KFK-61); relay mode conversion, ACL coverage, size headroom, and HEARTBEAT wording (B-KFK-36, B-KFK-13, B-KFK-27, 5.7).
10. Data-topic compaction is admitted, with `min.compaction.lag.ms` bounding the replay window (B-KFK-7, B-KFK-62, B-KFK-63).
    The first draft forbade it outright; that was revised on 23 September 2026 after review, because compacted change topics are common practice as table-state sources and compaction beyond the window does not affect any replayable event.
11. DDL events are carried on the control topic, not on data topics (B-KFK-9, B-KFK-59, B-KFK-64), revised on 23 September 2026 after review.
    Core Section 2.5 permits DDL on a channel separate from DML, subject-keyed DDL had no order against a table's other partitions anyway, and the single-partition control topic keeps each `ddl.CREATE` on one channel with its OBJECT_METADATA without copying schema onto data topics.
12. Record keys follow row identity from `primary_key`, in an encoding the publisher chooses and keeps (B-KFK-21); a null key is admitted only for a keyless table's rows and for TRUNCATE, and never reaches a compacted topic (B-KFK-67), revised on 23 September 2026 after review.
    The first draft recommended a JSON-array encoding that included the table name; that would have moved every row of an existing Debezium deployment to a different partition, while no client reads the key's contents.
13. `ordering_scope` is fixed at `"channel"` (B-KFK-31), revised on 23 September 2026 after review.
    The first draft recommended it only for publishers that partition in-process, which on Kafka is every publisher; a conditional value would have tied the declaration to partitioner configuration.
14. Partition counts, retention, compaction, and size limits are read from Kafka rather than declared by the publisher (B-KFK-45, B-KFK-65), revised on 23 September 2026 after review.
    The first draft declared `replayWindow`, `maxEventBytes`, partition counts, and a compaction flag in the descriptor, but those are set by the broker operator, which a connector usually neither controls nor can see, and a copy would drift when a topic is reconfigured.
15. The stream descriptor is removed, and clients are configured with the stream's topics as Kafka consumers are (6.1, B-KFK-55, B-KFK-66), revised on 23 September 2026 after review.
    Every member the descriptor carried was client configuration, carried on each record (content mode, `subject`), or topic configuration read from Kafka, and keeping a copy on the control topic was the source of repeated drift and ownership problems.
    B-KFK-16, B-KFK-56, and B-KFK-57 are withdrawn.
16. Ordering a TRUNCATE in the compacted region is a documented limitation rather than a client or topic mechanism: a deployment that needs transaction fidelity or correct state across a TRUNCATE beyond the replay window avoids multi-partition compaction for those tables (B-KFK-7), adopted on 23 September 2026 after review.

### 8.2. Deferred Features

Not in this revision; each would be a document revision, not a wire change:

1. **Single-partition ordered profile.** A stream on one data topic with one partition preserves `ordering_scope: "stream"` end to end.
   Claiming it needs a structural guarantee that the partition count cannot change (for example, an `Alter` and `AlterConfigs` ACL policy), which this revision does not define ([core Appendix B.4][core-b4]).
2. **Ephemeral Mode.** Requires a core-approved loss signal.
3. **Format composition** (Avro, Protocol Buffers), including schema-registry serializers and the key subject strategy that a data topic mixing key shapes requires (B-KFK-21); uniformity is not to be achieved by making key columns nullable.
4. **AsyncAPI template** with Kafka channel bindings and an `x-opencdc` extension naming the stream's topic roles.
5. **HEARTBEAT on data topics**, for per-partition liveness.
6. **Schema and DDL pruning.** Removing OBJECT_METADATA versions and DDL events that no retained data event needs, with the tombstone rules that would allow it (R-POS-7).

### 8.3. Deferred Core Items

Surfaced by this binding, deliberately not raised as core changes at this stage.
Recorded so they are not lost:

1. **CloudEvents `specversion`.** The core and envelope schema require `"1.1"`; the published CloudEvents specification is `1.0.2`.
   On Kafka this is not academic: CloudEvents SDK readers generally reject a `specversion` they do not recognise, and in binary mode the value is the `ce_specversion` header a CloudEvents-aware tool reads first.
   This binding inherits the core's value (the same item as the WSS + AsyncAPI binding's deferred core item 3).
2. **Reconnect coverage for sessionless single-sequence producers.** Core Section 6.4 lets a sessionless producer ensure schema availability "by having emitted a durable schema record in the stream" surfaced on a control channel, but Section 4.5.2 makes `schema_on_reconnect: true` with `session_aware: false` non-conformant in a single-channel stream, and P-SCHEMA-4 then forces Schema on Each Event.
   A producer that emits one sequence and publishes it to Kafka is therefore forced to embed schema in every event even though this binding supplies a retained control channel.
   This binding is not affected, because it fixes `ordering_scope: "channel"` (B-KFK-31), under which the core permits the control-channel path.
   The inconsistency remains for sessionless single-sequence producers outside a binding, and would return for the deferred single-partition profile; the core could permit the control-channel path whenever a claimed binding realizes it.
3. **Open or closed envelope.** Core Section 2.4 says closed-world validation never applies to the CloudEvents envelope, but the envelope schema sets `additionalProperties: false`.
   On Kafka, binary mode makes every extension attribute a `ce_` header, and existing publishers add their own.
   The core should say which rule holds.
4. **P-ORD-6 on partitioned transports.** Transaction-aligned `partitionkey` values break per-row order on Kafka (3.4).
   P-ORD-6 is scoped to a "partitioned single-channel transport", a combination the unbundled axes no longer describe.
5. **`metadata_revision`.** Core Appendix B.4 tells consumers to keep "the highest `metadata_revision` per key", but no such field is defined, and the STREAM_METADATA schema is closed.
   This binding uses offset order on a single-partition compacted topic instead (5.1).
6. **Registry drift.** R-POS-7, P-RET-1, and P-INT-1 appear in the core body but not in `registry/requirements.yaml`, and P-TRX-7 is registered as MUST while its text is SHOULD.
   This binding cites R-POS-7 and P-RET-1 by their body definitions.
7. **Trace attribute names.** Core Section 13.1 names `trace_id` and `correlation_id` as extension attributes.
   CloudEvents attribute names are lower-case letters and digits only, so neither maps to a `ce_` header as named; the CloudEvents distributed tracing extension uses `traceparent`.
8. **Primary key changes.** The core does not say how an UPDATE that changes a row's primary key is represented.
   Debezium emits a DELETE for the old key and a CREATE for the new one, which on Kafka keeps each key's history in one partition and lets compaction clear the old key; this binding assumes a single UPDATE keyed by the new key (3.4 note, B-KFK-62).
   The core should say whether the DELETE and INSERT pair is conformant and, if so, how the two events are correlated.
9. **Replay window.** The core uses "replay window" normatively (R-POS-6, R-POS-7, P-RET-1, Sections 12 and 15.1, Appendix B.3) but does not define it, and attributes it two ways: R-POS-7 speaks of "the replay window the producer supports", while Section 15.1 and Appendix B.3 speak of "the deployment's supported replay window".
   On Kafka the window is set by topic retention, which the broker operator owns, so this binding treats it as a deployment property (B-KFK-45).
   The core should define the term and assign it to one party.
10. **Operation capture declaration.** The core declares DDL capture (`ddl_capture`) but has no field saying whether a producer emits `dml.TRUNCATE`, so a client cannot tell a stream that captures TRUNCATE from one that does not (3.5).

### 8.4. Open Items

1. **Conventions update.** Add `Publisher` and `Deployment` to the register `who` vocabulary in [binding-formatting-decisions.md][formatting] (8.1, decision 6), and generalize `bindings/tools/check_binding.py`, which is currently specific to the WSS + AsyncAPI binding (identifier prefix, AsyncAPI template, close-code table).
2. **Key encoding.** Resolved in favor of the publisher's choice, fixed for the life of the stream (B-KFK-21, 8.1 decision 12), so that existing deployments keep their partition assignment.
   What remains open is whether to recommend an encoding for new publishers, for tooling.
3. **Size-based retention.** Whether to admit `retention.bytes` with a declared byte budget instead of excluding it (B-KFK-35).
4. **Conformance fixtures.** Positive and negative record examples and topic configurations linked to `B-KFK-*` identifiers, in a `conformance/` directory.
5. **Validation against captured Debezium output** with the CloudEvents converter, to add the *verified* column to Section 7.
6. **Kafka-compatible services.** A short note, per service, of which 1.6 capabilities it lacks (for example log compaction or record headers on some tiers).
7. **Descriptor media type.** *Closed:* the descriptor was removed (8.1, decision 15).
8. **TRUNCATE on compacted data topics.** *Closed as a documented limitation:* beyond the replay window of a multi-partition compacted topic nothing orders a TRUNCATE against the table's rows in other partitions (1.4, 3.5 note), just as nothing guarantees any transaction there (B-KFK-63); a deployment that needs correct state across a TRUNCATE avoids that configuration (B-KFK-7, 8.1 decision 16).
   A client rule comparing positions, and TRUNCATE on the transaction topic with retention for the life of the stream, were considered and not adopted; tombstoning every key of the table was rejected, because log-based capture records no row keys for a TRUNCATE.

## 9. References

- [OpenCDC][core] Open Change Data Capture Specification
- [CloudEvents][ce] CloudEvents Specification
- [Kafka Protocol Binding for CloudEvents][ce-kafka]
- [JSON Event Format][json-format] JSON Event Format for CloudEvents
- [Apache Kafka][kafka] and its [documentation][kafka-docs]
- [Kafka Connect][kafka-connect]
- [KIP-98][kip-98] Exactly Once Delivery and Transactional Messaging
- [KIP-405][kip-405] Kafka Tiered Storage
- [KIP-534][kip-534] Retain tombstones and transaction markers for approximately delete.retention.ms milliseconds
- [KIP-618][kip-618] Exactly-Once Support for Source Connectors
- [AsyncAPI][asyncapi] AsyncAPI Specification 3.1.0
- [AsyncAPI Kafka Bindings][asyncapi-kafka]
- [RFC2119][rfc2119] Key words for use in RFCs to Indicate Requirement Levels
- [Debezium][debezium] and its [CloudEvents converter][debezium-ce]
- [OpenCDC Binding Conventions][formatting]

[core]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md
[core-2-2a]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#22a-capability-axes-authority-and-legal-combinations
[core-2-4]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#24-closed-world-schemas
[core-3-3]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#33-cloudevents-extension-attributes-for-opencdc
[core-4-4]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#44-schema-delivery-modes
[core-4-5-2]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#452-schema-on-reconnect-optional-default-off
[core-6-2]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#62-type-fidelity-obligations
[core-6-3]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#63-lob-handling-obligations
[core-4-1]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#41-schema-availability-guarantee
[core-8-3]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#83-transaction-boundaries
[core-8-4-3]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#843-canonical-discontinuity-scenarios
[core-9]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#9-ddl-events
[core-9-1]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#91-ddl-payload-structure
[core-9-2]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#92-truncate
[core-10-1]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#101-heartbeat
[core-10-4]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#104-stream_metadata
[core-10-5-4]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#1054-layering-and-replay
[core-12]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#12-transport-bindings
[core-14-1]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#141-producer-security-rules-normative
[core-14-2]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#142-security--privacy-considerations-informative
[core-15-1]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#151-durable-mode
[core-appendix-a]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#appendix-a-consumer-reference----reading-and-parsing-opencdc-streams-informative
[core-a1]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#a1-step-1----connect-and-read-stream_metadata-first
[core-a2]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#a2-step-2----acquire-and-cache-the-schema-object_metadata
[core-a4]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#a4-step-4----order-and-assemble
[core-a8]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#a8-special-events
[core-a7]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#a7-step-7----replay-resume-and-sequence-continuity
[core-a10]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#a10-multi-channel-transaction-completeness-trx_commit
[core-b4]: https://github.com/open-cdc-hq/open-cdc-spec/blob/main/spec/OpenCDC-Specification.md#b4-kafka-consumption-guidance-informative
[formatting]: ../binding-formatting-decisions.md
[ce]: https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md
[ce-kafka]: https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md
[ce-kafka-mapping]: https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md#3-kafka-message-mapping
[ce-kafka-key]: https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md#31-key-mapping
[ce-kafka-binary]: https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md#32-binary-content-mode
[ce-kafka-structured]: https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/kafka-protocol-binding.md#33-structured-content-mode
[json-format]: https://github.com/cloudevents/spec/blob/main/cloudevents/formats/json-format.md
[kafka]: https://kafka.apache.org
[kafka-docs]: https://kafka.apache.org/documentation/
[kafka-connect]: https://kafka.apache.org/documentation/#connect
[kip-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
[kip-405]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage
[kip-534]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-534%3A+Retain+tombstones+and+transaction+markers+for+approximately+delete.retention.ms+milliseconds
[kip-618]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Exactly-Once+Support+for+Source+Connectors
[asyncapi]: https://www.asyncapi.com/docs/reference/specification/v3.1.0
[asyncapi-kafka]: https://github.com/asyncapi/bindings/blob/master/kafka/README.md
[rfc2119]: https://tools.ietf.org/html/rfc2119
[debezium]: https://debezium.io/documentation/reference/stable/
[debezium-ce]: https://debezium.io/documentation/reference/stable/integrations/cloudevents.html
