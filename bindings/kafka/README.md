# Kafka Protocol Binding for OpenCDC - Version 0.3.0-wip

**Binding:** `kafka`
**Version:** 0.3.0-wip (wire protocol 0.3, document revision 0; core specification v0.7.0)
**Profile in this revision:** Durable Mode, partitioned delivery with a control topic and a transaction topic, JSON event data (structured or binary content mode)
**Requirement identifiers:** `B-KFK-*`, tracked in [requirements.yaml](requirements.yaml)
**Status:** Working draft, first pass for working-group review

## Abstract

The Kafka Protocol Binding for OpenCDC defines how an OpenCDC stream is written to, and read from, Apache Kafka topics.
It is an application of the CloudEvents Kafka protocol binding and reuses the CloudEvents JSON event format.
It adds the stream-level semantics OpenCDC requires and CloudEvents does not define: a topic layout with a compacted control topic and an ordered transaction topic, record keying, retention rules that keep schemas and transaction markers available across the replay window, and discovery of the stream without a session.

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

- 5.1. [Discovery and Startup](#51-discovery-and-startup)
- 5.2. [Start Position](#52-start-position)
- 5.3. [Schema Resolution](#53-schema-resolution)
- 5.4. [Transaction Completion and the Transaction Topic](#54-transaction-completion-and-the-transaction-topic)
- 5.5. [Replay Window and Retention](#55-replay-window-and-retention)
- 5.6. [Publishing Durability](#56-publishing-durability)
- 5.7. [Heartbeat](#57-heartbeat)
- 5.8. [Stream Reconfiguration](#58-stream-reconfiguration)
- 5.9. [Failure Behavior](#59-failure-behavior)

6. [Stream Discovery](#6-stream-discovery)

- 6.1. [Stream Address](#61-stream-address)
- 6.2. [Stream Descriptor](#62-stream-descriptor)
- 6.3. [AsyncAPI Description](#63-asyncapi-description)

7. [Mapping from Existing Implementations](#7-mapping-from-existing-implementations)

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
What it adds on top is the stream-level contract CloudEvents does not define: schema availability without a session, transaction completion across partitions, retention across the replay window, and discovery.

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
  Every record on a stream's data topics and transaction topic MUST use the content mode declared in the stream descriptor (6.2).
- **B-KFK-2.** Event records on the control topic MUST use structured mode, whatever mode the descriptor declares.
  The control topic is small, is read once at startup, and must remain decodable after passing through any tooling.
- **B-KFK-3 (Client).** A client MUST support both content modes and MUST determine the mode of each event record from its `content-type` header, as the CloudEvents Kafka binding specifies ([section 3][ce-kafka-mapping]).
  On the control topic a client MUST classify each record by its key (B-KFK-23) before applying that rule: the record keyed `opencdc:descriptor` is not a CloudEvent and is never parsed as one.

### 1.4. Channel Layout

A stream on Kafka is a set of topics with fixed roles.
The *channel* of core Terms and Definitions corresponds to a partition, not a topic ([core Appendix B.4][core-b4]).

- **B-KFK-4.** A stream MUST consist of exactly one control topic, exactly one transaction topic, and one or more data topics, all in one Kafka cluster.
  A topic MUST NOT carry records of more than one stream.
- **B-KFK-5 (Deployment).** The control topic MUST have exactly one partition and a `cleanup.policy` of exactly `compact`.
  The value `compact,delete` does not satisfy this rule, because time-based deletion would remove schema versions (4.2).
- **B-KFK-6 (Deployment).** The transaction topic MUST have exactly one partition and a `cleanup.policy` of `delete`.
- **B-KFK-7 (Deployment).** Every data topic MUST have a `cleanup.policy` of `delete`, `compact`, or `compact,delete`.
  On a data topic with compaction enabled, `min.compaction.lag.ms` MUST be at least the declared `replayWindow` (B-KFK-45), so that no event inside the replay window is ever compacted, and the topic's descriptor entry MUST declare `"compacted": true` (6.2).
  A stream with a compacted data topic MUST NOT declare an `unbounded` replay window.
  Beyond the replay window a compacted data topic holds table state, the latest record per key, rather than an OpenCDC changelog (B-KFK-63).

*Why compaction is admitted.* A compacted change topic doubles as a table-state source: a new sink can bootstrap from the latest record per key without a fresh snapshot, and existing CDC deployments commonly use it this way, writing delete tombstones for the purpose.
Kafka never compacts records newer than `min.compaction.lag.ms`, so with the lag at least the replay window, every event a client may replay is intact, exactly as on a `delete` topic.
Only the region beyond the window changes, from nothing to table state.
- **B-KFK-8.** The control and transaction topics SHOULD be named `<stream>.opencdc.control` and `<stream>.opencdc.transactions`, where `<stream>` is the stream name (6.2).
  Data topics MAY have any legal Kafka topic name.
  In every case the stream descriptor, not the naming convention, is authoritative.
- **B-KFK-9.** Each topic MUST carry only the records listed for its role below.

| Topic role  | Partitions | `cleanup.policy` | Records                                                                  | Record key (3.4)                          |
| ----------- | ---------- | ---------------- | ------------------------------------------------------------------------ | ----------------------------------------- |
| Control     | 1          | `compact`        | Stream descriptor; STREAM_METADATA; durable OBJECT_METADATA; `ddl.*`     | Fixed per record kind (B-KFK-23)          |
| Transaction | 1          | `delete`         | TRX_COMMIT; HEARTBEAT                                                    | `cdcxid` for TRX_COMMIT; none for HEARTBEAT |
| Data        | Fixed, 1 or more | `delete`, or compaction with a lag (B-KFK-7) | `dml.*` (including TRUNCATE), `snapshot.READ`; tombstones on compacted topics (B-KFK-62) | Row identity, or the subject (B-KFK-21)   |

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
  cleanup.policy=delete  retention.ms=691200000  retention.bytes=-1
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

The transaction topic's `retention.ms` (8 days) exceeds the data topics' `retention.ms` plus `segment.ms` (7 days plus 1 day) by more than the retention check interval, as B-KFK-35 requires.
`max.message.bytes` (8.25 MiB) leaves 256 KiB above the descriptor's `maxEventBytes` (8 MiB) for keys, headers, and batch overhead (B-KFK-27).

### 1.5. Security

The core's transport-security guidance ([core Section 14.2][core-14-2]) is SHOULD-level and defers the mechanism to the binding.
This binding fixes the mechanism.

- **B-KFK-10 (Deployment).** Every listener used by the publisher, by clients, or by relays for a claimed stream MUST use TLS 1.2 or later (security protocol `SSL` or `SASL_SSL`).
  `PLAINTEXT` and `SASL_PLAINTEXT` listeners do not satisfy this binding.
- **B-KFK-11 (Publisher and Client).** The publisher and every client MUST validate the broker's certificate chain and host identity and MUST NOT proceed past a failed validation.
  Setting `ssl.endpoint.identification.algorithm` to an empty value disables host identity validation; a component so configured is not operating under this binding.
- **B-KFK-12.** Principals MUST authenticate with mutual TLS or a SASL mechanism (`SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, or `GSSAPI`).
  SASL `PLAIN` is permitted only over TLS.
  Credentials MUST NOT appear in record headers, record keys, topic names, or the stream descriptor; core S-AUTH-2 already forbids them in events.
- **B-KFK-13 (Deployment).** The deployment MUST NOT grant `Write` on a stream's topics, or `Write` on the publisher's `TransactionalId`, to any principal other than the publisher, or a relay writing to its own target topics.
  It MUST restrict `Alter`, `AlterConfigs`, `CreatePartitions`, and `Delete` (which permits `DeleteRecords`) on stream topics to administrators.
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
- **B-KFK-16 (Client).** A client MUST ignore descriptor members it does not recognise.
  A client MUST refuse a stream whose descriptor `bindingVersion` has a `<wire>` component it does not support.
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

- **B-KFK-21.** The key of a data-topic record MUST be a deterministic function of the row identity of the event it carries, and MUST NOT be null.
  Row identity is:

| Event                                                          | Row identity                                        |
| -------------------------------------------------------------- | --------------------------------------------------- |
| `dml.INSERT`, `dml.UPDATE`, `dml.UPSERT`, `snapshot.READ`      | The key values of `after`                           |
| `dml.DELETE`                                                   | The key values of `before`                          |
| `dml.TRUNCATE`                                                 | The `subject`                                       |

  The key values are those of the table's `primary_key` columns.
  For a table with an empty `primary_key`, they are the values of a unique key the publisher selects and keeps for the life of the table, and for a table with neither, the row identity is the `subject` alone.
  The key's encoding is the publisher's choice and MUST NOT change for the life of the stream.
  On a data topic that has compaction enabled and carries more than one subject, the key MUST also include the `subject`, so that compaction does not treat rows of different tables with equal key values as one row.

*Note.* Kafka assigns partitions by hashing the key's bytes, so the encoding decides each row's partition, and changing it would move rows between partitions, which B-KFK-22 forbids.
No client reads the key: row identity for applying changes comes from `primary_key` in OBJECT_METADATA ([core Appendix A.4][core-a4], C-KEY-1).
A key built from the key columns alone, as existing CDC connectors commonly write it, satisfies this rule, provided the publisher does not write a null key for a table with neither a primary nor a unique key (Section 7).

- **B-KFK-22.** The partition of a data-topic record MUST be a deterministic function of its key and the topic's partition count, and the function MUST NOT change for the life of the stream.
  Records with equal keys on one topic MUST be written to the same partition.
- **B-KFK-23.** On the control topic the key MUST be `opencdc:descriptor` for the stream descriptor, `opencdc:stream-metadata` for STREAM_METADATA, `opencdc:object-metadata:` followed by the event `id` for OBJECT_METADATA, and `opencdc:ddl:` followed by the event `id` for a `ddl.*` event.
  On the transaction topic the key of a TRX_COMMIT record MUST be its `cdcxid`, and a HEARTBEAT record MUST have no key.
- **B-KFK-24.** The event's `partitionkey` attribute, when present, is carried unchanged ([CloudEvents Kafka binding section 3.1][ce-kafka-key]).
  A publisher MUST NOT use it as the record key unless doing so satisfies B-KFK-21.

*Why OBJECT_METADATA and DDL events are keyed by `id`.* Each schema version has its own CloudEvents `id`, and a re-emission of an unchanged version repeats it.
Keying by `id` means compaction never removes a version or a DDL event (4.2), while repeated emissions of the same event collapse to one record.

*Why the key is not the transaction.* Core P-ORD-6 suggests giving all events of a transaction one `partitionkey`.
On Kafka that places a row's changes in different partitions whenever they fall in different transactions, which loses per-row order.
This binding keys by row instead and restores transaction structure from `cdcxid` and the transaction topic (Section 8, deferred core item 4).

*Note.* An UPDATE that changes a primary key is keyed by its new key, so it can land in a different partition from earlier changes to the same row.
Likewise a TRUNCATE is keyed by subject alone and lands in one partition, apart from the table's row events, and DDL events are on the control topic (B-KFK-9).
A client applying partitions independently can then apply such an event before earlier changes it should follow.
A client that orders transactions by the transaction topic (5.4) is unaffected.
The hazard for key changes is avoided by emitting a DELETE under the old key and an INSERT under the new key, which keeps the DELETE ordered with the row's earlier changes; whether an OpenCDC producer may represent a key change that way is Section 8, deferred core item 8.

### 3.5. Non-Event Records

- **B-KFK-25.** A publisher MUST NOT write any record that is not an OpenCDC event to a data topic or the transaction topic, including records with a null value (tombstones), except as B-KFK-62 permits on compacted data topics.
  The only non-event record on the control topic is the stream descriptor.
  A publisher MUST NOT write tombstones to the control topic.
- **B-KFK-62.** On a data topic with compaction enabled, a publisher MAY write a tombstone after a `dml.DELETE`, and SHOULD write one for the old key after a `dml.UPDATE` that changes the primary key.
  A tombstone is a record with a null value and the key, and therefore the partition, of the row it removes (B-KFK-21, B-KFK-22).
  It MUST NOT carry a `content-type` or `ce_` header, it is not an event, and it is never counted in `event_count`.
  A client MUST NOT treat a tombstone as an OpenCDC event: it has no `id`, `cdcxid`, or position, takes no part in deduplication by `(source, id)`, and does not count toward a transaction's completeness.
  Whether a client otherwise ignores tombstones or uses one as a signal that its key was removed is consumer processing and outside this binding.

*Note.* Every tombstone on a claimed stream follows the event that removed its key, so a client that treats tombstones as deletes removes the row once from the event and once from the tombstone, with the same result.

*Note.* An OpenCDC DELETE carries its before image, so it is never itself a tombstone and does not remove its key under compaction; the tombstone is what lets compaction eventually drop a deleted row.
Without the tombstone for the old key, a primary-key change leaves the row's previous record under that key, and a sink that bootstraps from the compacted region restores a row that no longer exists.
A TRUNCATE is keyed by its subject (B-KFK-21), so compaction keeps it alongside every earlier row of the table under their own keys, and a bootstrapping sink restores rows the TRUNCATE removed.
This revision does not fix the TRUNCATE case (Section 8, open item 8).

### 3.6. Size Limits

- **B-KFK-26.** This binding defines no application-level chunking or truncation of events.
  Truncating LOB values, DDL text, or records to a size ceiling is a payload change governed by the core ([Section 6.2][core-6-2], [Section 6.3][core-6-3], [Section 9][core-9]); a publisher that truncates is not emitting the core's events and MUST NOT claim this binding for that stream.
- **B-KFK-27 (Publisher and Deployment).** The descriptor MUST declare `maxEventBytes`, the largest serialized record value the stream admits in UTF-8 bytes before compression.
  The deployment MUST configure broker `message.max.bytes`, topic `max.message.bytes` on every stream topic, the control topic included, and the publisher MUST configure `max.request.size`, so that a record whose value is at most `maxEventBytes` is accepted.
  These limits apply to the whole record batch, including key, headers (every attribute, in binary mode), and batch overhead, so they MUST exceed `maxEventBytes` by enough to cover them.
  An event larger than `maxEventBytes`, or a record the broker rejects as too large, is a configuration error: the publisher MUST stop publishing the stream and MUST NOT truncate, drop, skip, or divert the event (for example to a dead-letter topic).

*Note.* Kafka clients return a record batch larger than `fetch.max.bytes` or `max.partition.fetch.bytes` rather than stalling, so a client needs no fetch configuration to make progress past a large event.
It does need memory for `maxEventBytes`.

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
| `tables`                               | Every captured table of *this stream*           | Equals the union of the descriptor's data-topic `subjects` (B-KFK-30)                                 |

- **B-KFK-28.** STREAM_METADATA MUST declare `session_aware: false` or omit it.
- **B-KFK-29.** STREAM_METADATA MUST declare `transaction_boundaries: "commit_all"` and `transaction_marker_delivery: "transaction_metadata_channel"`, and the publisher MUST emit a TRX_COMMIT for every transaction, whatever `transaction_interleaving` it declares.
  This realizes P-TRX-7 as a requirement: a client of a partitioned stream cannot prove completion of any transaction, single-event or not, without a marker.
- **B-KFK-30.** The `tables` declaration MUST equal the union of the `subjects` of the descriptor's data topics, and DML, TRUNCATE, and `snapshot.READ` events for a subject MUST appear only on data topics that list it.
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
- **B-KFK-33 (Control-channel retention).** The deployment MUST keep, on the control topic, the latest stream descriptor, the latest STREAM_METADATA, every OBJECT_METADATA version, and every DDL event the publisher has written, for the life of the stream.
  With `cleanup.policy=compact`, the keys of B-KFK-23, and no tombstones (B-KFK-25), compaction removes only superseded descriptors and superseded STREAM_METADATA.
- **B-KFK-34 (Schema retention across the replay window).** Because the control topic never removes a schema version, every OBJECT_METADATA version that governs a retained data event remains retrievable, which realizes R-POS-7 for the whole replay window.
  In addition, the publisher MUST NOT write a record to a data topic whose `dataschema` names an OBJECT_METADATA version until that version's control-topic record has been acknowledged by the cluster.
  A client that sees a data-topic event can therefore always find its schema on the control topic (5.3).
- **B-KFK-35 (Marker retention parity).** The deployment MUST set `retention.bytes=-1` on the transaction topic and on every data topic.
  It MUST set the transaction topic's `retention.ms` greater than the maximum, over the data topics, of the topic's replay horizon plus `segment.ms` plus the broker's `log.retention.check.interval.ms`, where a data topic's replay horizon is its `min.compaction.lag.ms` if compaction is enabled and its `retention.ms` otherwise.
  If any data topic with `cleanup.policy=delete` has `retention.ms=-1`, the transaction topic MUST as well.
  Kafka deletes whole segments, so a data record can outlive its topic's `retention.ms` by up to one segment roll; the margin keeps the marker for every retained data event (P-RET-1, R-POS-6).
- **B-KFK-36 (Intermediary integrity).** A relay that copies a stream MUST copy all of its topics, control and transaction topics included, and MUST deliver them under a configuration that satisfies 1.4 and 4.2.
  It MUST preserve, for every record: the partition number, the order within the partition, the key, the value, and the `content-type` and `ce_` headers.
  It MUST NOT drop, filter, merge, split, or re-key events, and MUST NOT recompute partitions from keys (partitioners differ between Kafka client libraries).
  As the one exception to preserving values and headers, a relay MAY convert the whole stream between structured and binary mode; if it does, it MUST restore each attribute to its core type (for example, `cdctxorder` from the header string `"0"` to the JSON integer `0`), which a generic CloudEvents converter does not do.
  A relay that renames topics or converts the content mode MUST rewrite the stream descriptor accordingly; the descriptor is the only record a relay may rewrite.
  A relay that violates this is not covered by the stream deployment's claim.

*Note.* A relay does not preserve offsets.
Committed consumer-group offsets from the source cluster are not valid on the target; a client that moves to a copy repositions by `cdcpos` and `(source, id)`, or by an offset translation the relay provides as a convenience (for example MirrorMaker 2 checkpoints).

## 5. Delivery Protocol

Kafka has no session, so this section describes how a client finds a stream, where it starts, how it resolves schemas and completes transactions, and what the publisher and deployment guarantee over time.

### 5.1. Discovery and Startup

- **B-KFK-37.** Before writing the first record to any data topic or the transaction topic, the publisher MUST write the stream descriptor and STREAM_METADATA to the control topic and MUST have both acknowledged by the cluster.
- **B-KFK-38 (Client).** Before processing any record from a data topic or the transaction topic, a client MUST read the control topic from its beginning to its end offset as of startup, keeping the last record for each key.
  It MUST locate the stream's topics from the descriptor it read, and MUST process STREAM_METADATA before any data event ([core Appendix A.1][core-a1]).

*Note.* Keeping the last record per key, in offset order on one partition, is how this binding realizes the core's advice to keep the latest metadata revision per key ([core Appendix B.4][core-b4]).
No revision field is needed (Section 8, deferred core item 5).

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

*Note.* A client reads the whole control topic at startup (B-KFK-38), so it sees DDL events from before its start position; the transaction topic tells it which of them are in range.
The control topic grows with every DDL event, including the statement text under `ddl_capture: "verbatim"`, and startup reads all of it; pruning is deferred (Section 8).
- **B-KFK-44.** Any filtering the publisher applies (by table, operation, column value, or any other criterion) MUST take effect before `cdctxorder` values are assigned and before TRX_COMMIT `event_count` and `distribution` are computed.
  `cdctxorder` MUST be dense over the events published, and `event_count` and `distribution` MUST describe exactly those events.
  A transaction none of whose events is published produces no TRX_COMMIT.

With B-KFK-43 satisfied, a TRX_COMMIT tells a client that every event of the transaction is durable on its data topic; if the client does not yet hold all ordinals `0..event_count-1`, it is behind on some partition, not missing data.
Because the transaction topic is a single partition written in commit order, it is also the stream's commit-order spine: a client that applies transactions in the order of their first-seen markers, discarding duplicate markers by `(source, id)`, applies them in source commit order across all tables.
Assembly strategies, bounded waits, and subset consumption by `distribution` are consumer guidance ([core Appendix A.10][core-a10], [core Appendix B.4][core-b4]).

### 5.5. Replay Window and Retention

- **B-KFK-45 (Publisher and Deployment).** The descriptor MUST declare `replayWindow`: an ISO 8601 duration no longer than any data topic's `retention.ms` (where it deletes by time) or `min.compaction.lag.ms` (where it compacts), or `unbounded` when every data topic has `cleanup.policy=delete` and `retention.ms=-1`.
  The deployment MUST NOT reduce retention or the compaction lag below the declared window.
  Records older than the declared window may still be present; they are outside the claim.
- **B-KFK-63 (Client).** On a data topic whose descriptor entry declares `"compacted": true`, a client MUST treat records whose record timestamp is older than the declared `replayWindow` as table state, not as a changelog.
  It MUST NOT expect transactions among those records to be complete or their TRX_COMMIT markers to be available, and MUST expect `cdctxorder` gaps among them.
  Records inside the window are an unaltered changelog, as on any other data topic.

*Note.* B-KFK-63 relies on record timestamps being publish times (B-KFK-41).
A client that bootstraps from compacted state and then continues into the changelog crosses from state to events at the window boundary; how it reconciles the two is consumer guidance, comparable to the snapshot-to-steady-state transition ([core Appendix A.8][core-a8]).

*Note.* Size-based retention (`retention.bytes`) is excluded by B-KFK-35, because a window measured in bytes cannot be compared across topics of different volume.
Tiered storage ([KIP-405][kip-405]) changes where segments live, not when they are deleted, and is compatible with this binding.

### 5.6. Publishing Durability

This revision defines Durable Mode only ([core Section 15.1][core-15-1]).

- **B-KFK-46 (Publisher and Deployment).** The publisher MUST write with `acks=all`.
  On every stream topic the deployment MUST set `unclean.leader.election.enable=false`, MUST set `min.insync.replicas` to at least 2, and MUST use a replication factor of at least `min.insync.replicas` plus 1 (3 or more is typical).
  With one in-sync replica, `acks=all` acknowledges a single broker's write, which does not meet core Section 15.1's durability obligation; unclean leader election discards acknowledged records, which is a gap no client can detect.
- **B-KFK-47.** The publisher MUST NOT advance its source checkpoint (for example, a Kafka Connect source offset) past an event until that event's record has been acknowledged by the cluster.
  After a restart it resumes from its checkpoint and re-emits events with their original `id` values; the resulting duplicates are permitted (R-POS-5) and are resolved by `(source, id)`.
- **B-KFK-60.** Exactly one producer instance at a time MUST write a stream's transaction topic, and each data-topic partition MUST be written by at most one producer instance at a time.
  A publisher that can run more than one instance (for example, after a failover) MUST fence the previous instance before writing, using a Kafka `transactional.id` or an equivalent mechanism.
  Idempotence orders records from one producer session only; two writers on the transaction topic would interleave markers out of commit order.
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

- **B-KFK-50.** Before writing the first record to a data topic added to the stream, or the first event for a subject added to the stream, the publisher MUST write an updated descriptor to the control topic, and STREAM_METADATA updated as the core requires when `tables` changes, and MUST have both acknowledged.
- **B-KFK-51 (Deployment).** The partition count of a stream topic MUST NOT change while the stream exists.
  Adding partitions changes the partition of existing keys, which breaks per-row order across the change.
  Repartitioning is done by creating a new stream (new stream name, new topics) and migrating clients to it.
- **B-KFK-52 (Client).** A client MUST keep reading the control topic while it consumes the stream, MUST subscribe to data topics added by a later descriptor starting at their beginning offsets, and MUST stop consuming the stream if the partition count a topic reports differs from the count in the descriptor.

A client that reads only some tables uses the descriptor's `subjects` to choose data topics, and the transaction topic in full.
When a TRX_COMMIT's `distribution` names a subject the client has no topic for, the descriptor it holds is stale.

### 5.9. Failure Behavior

Failures fall in three places: establishment (a client or publisher cannot authenticate, is not authorized, or cannot find the stream), steady state (errors raised while reading or writing), and abnormal loss (a component stops without reporting).
Kafka reports failures as client exceptions and broker error codes; the table names both where they differ.

| Condition                                                              | Kafka signal                                                                   | Party     | Disposition                                                                 |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------ | --------- | --------------------------------------------------------------------------- |
| Authentication failed                                                  | `SaslAuthenticationException`, `SslAuthenticationException`                    | Both      | Retry only with new credentials                                             |
| Not authorized for a stream topic or consumer group                    | `TopicAuthorizationException`, `GroupAuthorizationException`                   | Both      | No                                                                          |
| Control topic, or a descriptor-listed topic, does not exist            | `UNKNOWN_TOPIC_OR_PARTITION` persisting past metadata refresh                  | Client    | No; the stream address or descriptor is wrong                               |
| Descriptor missing, invalid, or of an unsupported wire version         | (binding-level)                                                                | Client    | No (B-KFK-16)                                                               |
| Leader change, broker restart, network interruption                    | Retriable errors (`NOT_LEADER_OR_FOLLOWER`, `NETWORK_EXCEPTION`, timeouts)     | Both      | Yes; handled by the Kafka client                                            |
| Consumer-group rebalance                                               | Partitions revoked and assigned                                                | Client    | Yes; resume assigned partitions from committed offsets (B-KFK-39)           |
| Committed offset outside retained data, or expired                     | `OffsetOutOfRangeException`, `NoOffsetForPartitionException`                   | Client    | No automatic retry; surface the gap (B-KFK-40)                              |
| Record not decodable, or of an unsupported `cdcspecversion`            | `RecordDeserializationException`, or binding-level                             | Client    | No; stop at the record (B-KFK-15, B-KFK-53)                                 |
| Partition count differs from the descriptor                            | Metadata                                                                       | Client    | No; stop (B-KFK-52)                                                         |
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

## 6. Stream Discovery

A client needs two things to read a stream: where it is, and what it consists of.
The stream address answers the first; the stream descriptor, kept on the control topic, answers the second.

Two kinds of statement are involved.
**Stream declarations** (capability axes, tables, heartbeat interval, schema delivery) belong to STREAM_METADATA ([core Section 2.2a][core-2-2a], producer-sole-emitter invariant).
**Deployment declarations** (topic names, partition counts, content mode, replay window, size limit) belong to this binding and live in the descriptor.
The descriptor is normative for the stream deployment (B-KFK-56).

### 6.1. Stream Address

- **B-KFK-55.** The address of a stream is the bootstrap servers of its Kafka cluster and the name of its control topic.
  Whoever operates a claimed stream deployment MUST make the address available to authorized clients; how it does so (configuration, a catalog, an AsyncAPI document) is not specified.

### 6.2. Stream Descriptor

The descriptor is a JSON object, the value of the control-topic record with key `opencdc:descriptor`, carried with the `content-type` `application/vnd.opencdc.kafka-descriptor+json` (a proposed, unregistered media type; Section 8, open item 7).
Its schema is published as [stream-descriptor.schema.json](stream-descriptor.schema.json), and the example below as [descriptor-example.json](descriptor-example.json).

- **B-KFK-56.** The descriptor MUST validate against `stream-descriptor.schema.json` and MUST describe the deployment as it is: every data topic of the stream is listed, with its current partition count and the subjects that may appear on it.
- **B-KFK-57.** The descriptor MUST NOT restate or contradict stream declarations.
  STREAM_METADATA remains the authority for them; the descriptor carries only what a client needs to find and read the topics.
  The data-topic `subjects` are a permitted derived restatement of `tables` and MUST agree with it (B-KFK-30).

```json
{
  "binding": "kafka",
  "bindingVersion": "0.3.0",
  "stream": "finance-orders",
  "contentMode": "binary",
  "controlTopic": "finance-orders.opencdc.control",
  "transactionTopic": "finance-orders.opencdc.transactions",
  "dataTopics": [
    {
      "topic": "finance-orders.FINANCE.ORDERS",
      "partitions": 6,
      "subjects": ["FINANCE.ORDERS"]
    },
    {
      "topic": "finance-orders.FINANCE.ORDER_LINES",
      "partitions": 6,
      "subjects": ["FINANCE.ORDER_LINES"]
    }
  ],
  "replayWindow": "P7D",
  "maxEventBytes": 8388608
}
```

| Member             | Required | Meaning                                                                                              |
| ------------------ | -------- | ---------------------------------------------------------------------------------------------------- |
| `binding`          | Yes      | `kafka`                                                                                              |
| `bindingVersion`   | Yes      | This document's version without `-wip`; clients check the `<wire>` component (B-KFK-16)              |
| `stream`           | Yes      | Stream name; the `<stream>` of the naming convention (B-KFK-8)                                       |
| `contentMode`      | Yes      | `structured` or `binary`, for data and transaction topics (B-KFK-1)                                  |
| `controlTopic`     | Yes      | Name of the topic holding this descriptor; a copy whose value differs was not rewritten by its relay (B-KFK-36) |
| `transactionTopic` | Yes      | Name of the transaction topic                                                                        |
| `dataTopics`       | Yes      | One entry per data topic: `topic`, `partitions`, `subjects` (B-KFK-30, B-KFK-52), and `compacted` (optional, default `false`; B-KFK-7) |
| `replayWindow`     | Yes      | ISO 8601 duration or `unbounded` (B-KFK-45)                                                          |
| `maxEventBytes`    | Yes      | Largest record value admitted, UTF-8 bytes before compression (B-KFK-27)                             |

*Why the descriptor is its own record.* It could have been carried as headers on the STREAM_METADATA record ([core Appendix B.3][core-b3] allows either).
A separate record keeps the producer's event untouched: a relay that renames topics rewrites the descriptor, never an OpenCDC event, which preserves the core's producer-sole-emitter invariant (Section 8, decision 5).

### 6.3. AsyncAPI Description

- **B-KFK-58.** A stream deployment MAY additionally publish an [AsyncAPI][asyncapi] 3.x document describing its topics, using the [AsyncAPI Kafka bindings][asyncapi-kafka].
  If it does, every `topicConfiguration` and `partitions` value in that document MUST match the deployment, and the document is informative: where it and the descriptor differ, the descriptor governs.

An AsyncAPI template for this binding, analogous to the WSS + AsyncAPI binding's, is deferred (Section 8).

## 7. Mapping from Existing Implementations

This section is informative.
It records how the configuration surface of [Debezium][debezium] source connectors running on Kafka Connect, with the [Debezium CloudEvents converter][debezium-ce], relates to this binding, from the published documentation.
It is not an implementation plan and does not assert that any option, as documented, produces OpenCDC-conformant output: Debezium's change event envelope and its CloudEvents mapping still require the OpenCDC payload and metadata transformation the core defines.
Rows rest on the public documentation only; a *verified* column will be added once captured records are analysed (Section 8, open item 5).

"Fixed" means the binding requires a specific value; "excluded" means the option produces a stream that is not OpenCDC-conformant, or a deployment that does not satisfy this binding, and so cannot be claimed; "out of scope" means a provisioning or producer-internal concern the binding does not see; "converted" means a semantic mapping the publisher must implement, not a rename; "add" means a capability the implementation does not document.

| Debezium or Kafka Connect option                                  | Disposition                     | Note                                                                                                                           |
| ----------------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `value.converter=io.debezium.converters.CloudEventsConverter`     | Fixed                           | Structured mode only today; binary mode is documented as future work (1)                                                       |
| CloudEvents `serializer.type`, `data.serializer.type`             | Fixed `json`                    | Avro is reserved for a format binding (1.2)                                                                                    |
| CloudEvents `extension.attributes.enable`                         | Converted                       | The `iodebezium*` extensions are not OpenCDC attributes; the `cdc*` extensions are required (2)                                |
| CloudEvents `metadata.source` (`id:generate`)                     | Converted                       | Debezium derives `id` from content; the core requires UUID v4 for DML and DDL (3)                                              |
| `topic.prefix`                                                    | Mapped                          | Natural `<stream>` name (B-KFK-8)                                                                                              |
| Default topic per table (`<prefix>.<schema>.<table>` on PostgreSQL) | Carried                        | Data topics; one subject per topic (B-KFK-30)                                                                                  |
| Default record key (key columns as a struct, via `key.converter`) | Carried                         | A deterministic function of row identity (B-KFK-21); the encoding must not change for the life of the stream                   |
| Null key for a table with neither a primary nor a unique key      | Add                             | B-KFK-21 forbids a null key; key such tables by table name, or give each its own single-partition topic                        |
| Primary key change as DELETE plus CREATE (`__debezium.newkey`, `__debezium.oldkey` headers) | Open          | Whether OpenCDC admits this representation is a core question (Section 8, deferred core item 8)                                |
| Topic routing SMT (`ByLogicalTableRouter`)                        | Mapped with constraint          | Several subjects per data topic are allowed if listed in the descriptor; on a compacted topic `key.enforce.uniqueness=true` is required (B-KFK-21) (4) |
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
| Kafka Connect `errors.tolerance=all`, dead-letter queue           | Excluded                        | B-KFK-53                                                                                                                       |
| Kafka Connect `exactly.once.support` (KIP-618)                    | Optional                        | Satisfies the visibility half of B-KFK-43 when the marker shares a Kafka transaction with its data                             |
| Producer `acks`, `enable.idempotence`, `max.in.flight.requests.per.connection` | Fixed               | `acks=all`, idempotence on (B-KFK-32, B-KFK-46); verify the effective worker and override configuration                        |
| `topic.creation.*` (KIP-158)                                      | Mapped                          | Must create topics with the configuration of 1.4 and 4.2; broker auto-creation with defaults does not                         |
| Source offsets committed after producer acknowledgement           | Carried                         | Kafka Connect's offset commit waits for outstanding acknowledgements (B-KFK-47)                                                |

Notes:

1. The Debezium documentation states that only structured mapping mode is available and that binary mode is expected in a future release.
   A Debezium-based publisher can claim this binding with `contentMode: structured` today.
2. The Debezium CloudEvents example carries `iodebeziumop`, `iodebeziumlsn`, `iodebeziumtxid`, `iodebeziumtxtotalorder`, and `iodebeziumtxdatacollectionorder`.
   The OpenCDC counterparts are `type`, `pos`, `cdcxid`, and `cdctxorder`; `iodebeziumtxtotalorder` is 1-based in the published example while `cdctxorder` is 0-based (core Section 3.3).
   Whether extra extension attributes may accompany the `cdc*` attributes is Section 8, deferred core item 3.
3. The published example shows `id` values such as `name:test_server;lsn:29274832;txId:565`: content-derived and replay-stable, but not UUID v4.
   The core requires both UUID v4 and replay stability for DML and DDL `id` values, which a random UUID meets only if it is persisted before emission (core Section 11.2).
4. Routing several tables to one topic keeps them in one set of partitions, but a client can no longer choose tables by topic.
   The descriptor must list every subject a topic may carry.
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
5. Deployment declarations live in a stream descriptor record on the control topic rather than in headers on STREAM_METADATA (6.2).
   The alternative is the one core Appendix B.3 mentions first; the working group should choose.
6. Conformance parties are Publisher, Deployment, Client, and Relay.
   The register `who` vocabulary in [binding-formatting-decisions.md][formatting] lists `Endpoint`, which fits a binding whose endpoint both emits and delivers; Kafka separates the two, so this binding uses `Publisher` and `Deployment` instead (1.1).
7. Strict retention: `retention.bytes=-1` on data and transaction topics (B-KFK-35).
   Recorded as open item 3 in case the working group prefers a declared byte budget.
8. An AsyncAPI document is optional (6.3).
   Unlike the WSS + AsyncAPI binding, the description format is not part of the binding's name or purpose.
9. Revised before first circulation after a drafting review, which found and fixed: descriptor misread as a binary-mode CloudEvent (B-KFK-2, B-KFK-3); `ddl.CREATE` forward references against core Section 9.1 (B-KFK-34, B-KFK-59); durability weaker than core Section 15.1 (B-KFK-46); no single writer for the transaction topic (B-KFK-60); marker visibility only SHOULD (B-KFK-43); markers lost by early offset commits (B-KFK-61); relay mode conversion, ACL coverage, size headroom, and HEARTBEAT wording (B-KFK-36, B-KFK-13, B-KFK-27, 5.7).
10. Data-topic compaction is admitted, with `min.compaction.lag.ms` at least the replay window (B-KFK-7, B-KFK-62, B-KFK-63).
    The first draft forbade it outright; that was revised on 23 September 2026 after review, because compacted change topics are common practice as table-state sources and compaction beyond the window does not affect any replayable event.
11. DDL events are carried on the control topic, not on data topics (B-KFK-9, B-KFK-59, B-KFK-64), revised on 23 September 2026 after review.
    Core Section 2.5 permits DDL on a channel separate from DML, subject-keyed DDL had no order against a table's other partitions anyway, and the single-partition control topic keeps each `ddl.CREATE` on one channel with its OBJECT_METADATA without copying schema onto data topics.
12. Record keys follow row identity with a unique-key fallback, never null, in an encoding the publisher chooses and keeps (B-KFK-21), revised on 23 September 2026 after review.
    The first draft recommended a JSON-array encoding that included the table name; that would have moved every row of an existing Debezium deployment to a different partition, while no client reads the key's contents.
13. `ordering_scope` is fixed at `"channel"` (B-KFK-31), revised on 23 September 2026 after review.
    The first draft recommended it only for publishers that partition in-process, which on Kafka is every publisher; a conditional value would have tied the declaration to partitioner configuration.

### 8.2. Deferred Features

Not in this revision; each would be a document revision, not a wire change:

1. **Single-partition ordered profile.** A stream on one data topic with one partition preserves `ordering_scope: "stream"` end to end.
   Claiming it needs a structural guarantee that the partition count cannot change (for example, an `AlterConfigs` and `CreatePartitions` ACL policy), which this revision does not define ([core Appendix B.4][core-b4]).
2. **Ephemeral Mode.** Requires a core-approved loss signal.
3. **Format composition** (Avro, Protocol Buffers), including schema-registry serializers.
4. **AsyncAPI template** with Kafka channel bindings and an `x-opencdc` extension equivalent to the descriptor.
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

### 8.4. Open Items

1. **Conventions update.** Add `Publisher` and `Deployment` to the register `who` vocabulary in [binding-formatting-decisions.md][formatting] (8.1, decision 6), and generalize `bindings/tools/check_binding.py`, which is currently specific to the WSS + AsyncAPI binding (identifier prefix, AsyncAPI template, close-code table).
2. **Key encoding.** Resolved in favor of the publisher's choice, fixed for the life of the stream (B-KFK-21, 8.1 decision 12), so that existing deployments keep their partition assignment.
   What remains open is whether to recommend an encoding for new publishers, for tooling.
3. **Size-based retention.** Whether to admit `retention.bytes` with a declared byte budget instead of excluding it (B-KFK-35).
4. **Conformance fixtures.** Positive and negative record examples and topic configurations linked to `B-KFK-*` identifiers, in a `conformance/` directory.
5. **Validation against captured Debezium output** with the CloudEvents converter, to add the *verified* column to Section 7.
6. **Kafka-compatible services.** A short note, per service, of which 1.6 capabilities it lacks (for example log compaction or record headers on some tiers).
7. **Descriptor media type.** Whether to register `application/vnd.opencdc.kafka-descriptor+json` or choose another name.
8. **TRUNCATE on compacted data topics.** Compacted state keeps rows a TRUNCATE removed (3.5 note).
   Options include tombstoning every key of the table (which requires the publisher to know them), excluding TRUNCATE from compacted streams, or declaring that compacted state is not valid across a TRUNCATE.

## 9. References

- [OpenCDC][core] Open Change Data Capture Specification
- [CloudEvents][ce] CloudEvents Specification
- [Kafka Protocol Binding for CloudEvents][ce-kafka]
- [JSON Event Format][json-format] JSON Event Format for CloudEvents
- [Apache Kafka][kafka] and its [documentation][kafka-docs]
- [KIP-98][kip-98] Exactly Once Delivery and Transactional Messaging
- [KIP-405][kip-405] Kafka Tiered Storage
- [KIP-618][kip-618] Exactly-Once Support for Source Connectors
- [AsyncAPI][asyncapi] AsyncAPI Specification 3.1.0
- [AsyncAPI Kafka Bindings][asyncapi-kafka]
- [RFC2119][rfc2119] Key words for use in RFCs to Indicate Requirement Levels
- [Debezium][debezium] and its [CloudEvents converter][debezium-ce]
- [OpenCDC Binding Conventions][formatting]

[core]: ../../spec/OpenCDC-Specification.md
[core-2-2a]: ../../spec/OpenCDC-Specification.md#22a-capability-axes-authority-and-legal-combinations
[core-2-4]: ../../spec/OpenCDC-Specification.md#24-closed-world-schemas
[core-3-3]: ../../spec/OpenCDC-Specification.md#33-cloudevents-extension-attributes-for-opencdc
[core-4-4]: ../../spec/OpenCDC-Specification.md#44-schema-delivery-modes
[core-4-5-2]: ../../spec/OpenCDC-Specification.md#452-schema-on-reconnect-optional-default-off
[core-6-2]: ../../spec/OpenCDC-Specification.md#62-type-fidelity-obligations
[core-6-3]: ../../spec/OpenCDC-Specification.md#63-lob-handling-obligations
[core-4-1]: ../../spec/OpenCDC-Specification.md#41-schema-availability-guarantee
[core-8-3]: ../../spec/OpenCDC-Specification.md#83-transaction-boundaries
[core-9]: ../../spec/OpenCDC-Specification.md#9-ddl-events
[core-9-1]: ../../spec/OpenCDC-Specification.md#91-ddl-payload-structure
[core-10-1]: ../../spec/OpenCDC-Specification.md#101-heartbeat
[core-10-4]: ../../spec/OpenCDC-Specification.md#104-stream_metadata
[core-10-5-4]: ../../spec/OpenCDC-Specification.md#1054-layering-and-replay
[core-12]: ../../spec/OpenCDC-Specification.md#12-transport-bindings
[core-14-1]: ../../spec/OpenCDC-Specification.md#141-producer-security-rules-normative
[core-14-2]: ../../spec/OpenCDC-Specification.md#142-security--privacy-considerations-informative
[core-15-1]: ../../spec/OpenCDC-Specification.md#151-durable-mode
[core-appendix-a]: ../../spec/OpenCDC-Specification.md#appendix-a-consumer-reference----reading-and-parsing-opencdc-streams-informative
[core-a1]: ../../spec/OpenCDC-Specification.md#a1-step-1----connect-and-read-stream_metadata-first
[core-a2]: ../../spec/OpenCDC-Specification.md#a2-step-2----acquire-and-cache-the-schema-object_metadata
[core-a4]: ../../spec/OpenCDC-Specification.md#a4-step-4----order-and-assemble
[core-a8]: ../../spec/OpenCDC-Specification.md#a8-special-events
[core-a7]: ../../spec/OpenCDC-Specification.md#a7-step-7----replay-resume-and-sequence-continuity
[core-a10]: ../../spec/OpenCDC-Specification.md#a10-multi-channel-transaction-completeness-trx_commit
[core-b3]: ../../spec/OpenCDC-Specification.md#b3-transport-specific-implementation-notes
[core-b4]: ../../spec/OpenCDC-Specification.md#b4-kafka-consumption-guidance-informative
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
[kip-98]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
[kip-405]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage
[kip-618]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Exactly-Once+Support+for+Source+Connectors
[asyncapi]: https://www.asyncapi.com/docs/reference/specification/v3.1.0
[asyncapi-kafka]: https://github.com/asyncapi/bindings/blob/master/kafka/README.md
[rfc2119]: https://tools.ietf.org/html/rfc2119
[debezium]: https://debezium.io/documentation/reference/stable/
[debezium-ce]: https://debezium.io/documentation/reference/stable/integrations/cloudevents.html
