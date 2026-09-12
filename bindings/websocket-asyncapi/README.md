# WSS + AsyncAPI Protocol Binding for OpenCDC - Version 0.3.0-wip

**Binding:** `wss-asyncapi`
**Version:** 0.3.0-wip (wire protocol 0.3, document revision 0; core specification v0.7.0)
**Profile in this revision:** Durable Mode, JSON batch, `opencdc.json` subprotocol
**Requirement identifiers:** `B-WSA-*`, tracked in [requirements.yaml](requirements.yaml)
**Status:** Working draft, revised after the 12 September 2026 review

## Abstract

The WSS + AsyncAPI Protocol Binding for OpenCDC defines how an OpenCDC stream is
delivered over a secure WebSocket connection and how the endpoint is described
in an AsyncAPI document so that clients can be generated from the description.
It reuses the CloudEvents JSON event format and JSON batch format, and adds the
stream-level semantics that OpenCDC requires and CloudEvents does not define.

## Table of Contents

1. [Introduction](#1-introduction)

- 1.1. [Conformance](#11-conformance)
- 1.2. [Relation to WebSockets, CloudEvents, and AsyncAPI](#12-relation-to-websockets-cloudevents-and-asyncapi)
- 1.3. [Content Modes](#13-content-modes)
- 1.4. [Handshake](#14-handshake)
- 1.5. [Security](#15-security)
- 1.6. [Transport Capabilities](#16-transport-capabilities)
- 1.7. [Versioning and Compatibility](#17-versioning-and-compatibility)

2. [Use of OpenCDC Attributes](#2-use-of-opencdc-attributes)

3. [WebSocket Message Mapping](#3-websocket-message-mapping)

- 3.1. [Message Unit](#31-message-unit)
- 3.2. [Frame Type](#32-frame-type)
- 3.3. [Event Data Encoding](#33-event-data-encoding)
- 3.4. [Size Limits](#34-size-limits)

4. [Stream Profile and Delivery Properties](#4-stream-profile-and-delivery-properties)

- 4.1. [Capability Axes](#41-capability-axes)
- 4.2. [Delivery Properties](#42-delivery-properties)

5. [Session Protocol](#5-session-protocol)

- 5.1. [Start Position](#51-start-position)
- 5.2. [First-Event Sequence and the Replay Boundary](#52-first-event-sequence-and-the-replay-boundary)
- 5.3. [Steady State](#53-steady-state)
- 5.4. [Cursor Validity and the Replay Window](#54-cursor-validity-and-the-replay-window)
- 5.5. [Heartbeat and Keep-Alive](#55-heartbeat-and-keep-alive)
- 5.6. [Backpressure](#56-backpressure)
- 5.7. [Filtering and Stream Identity](#57-filtering-and-stream-identity)
- 5.8. [Failure Behavior and Close Codes](#58-failure-behavior-and-close-codes)

6. [AsyncAPI Description](#6-asyncapi-description)

- 6.1. [Required Document Elements](#61-required-document-elements)
- 6.2. [`ws` Channel Binding](#62-ws-channel-binding)
- 6.3. [`x-opencdc` Extension](#63-x-opencdc-extension)
- 6.4. [Messages and Schemas](#64-messages-and-schemas)
- 6.5. [Template](#65-template)

7. [Mapping from Existing Implementations](#7-mapping-from-existing-implementations)

8. [Decisions, Deferrals, and Open Items](#8-decisions-deferrals-and-open-items)

- 8.1. [Decisions Taken](#81-decisions-taken)
- 8.2. [Deferred Features](#82-deferred-features)
- 8.3. [Deferred Core Items](#83-deferred-core-items)
- 8.4. [Open Items](#84-open-items)

9. [References](#9-references)

## 1. Introduction

[OpenCDC][core] is a vendor-neutral specification for the structure and
semantics of database change events, built on [CloudEvents][ce]. The core
specification is transport-neutral. This document is an OpenCDC *protocol
binding*: it defines how an OpenCDC stream is carried over a secure
[WebSocket][rfc6455] connection and described by an [AsyncAPI][asyncapi]
document.

The binding is optional to implement and normative when claimed. An endpoint
that claims it is bound by every MUST in this document. No binding is required
for core conformance. The conformance claim string is:

> OpenCDC 0.3 / WSS+AsyncAPI Binding 0.3

Because the endpoint that claims this binding both emits and delivers the
stream, the claim covers emission conformance (core) and delivery conformance
(this binding). A client receiving directly from a claimed endpoint is in the
"receive events directly from the producer" case of [C-COMP-1][core-a1].

This revision defines one profile: Durable Mode, JSON batch messages, the
`opencdc.json` subprotocol. Ephemeral Mode, other encodings, browser clients,
and client-to-server control messages are deferred (Section 8).

The name is deliberate. This binding is *WebSocket Secure described by
AsyncAPI*. Other WebSocket-based bindings would be separate documents with
their own names and identifier prefixes.

References of the form "core Section N" or "core Appendix A" are to the
[OpenCDC specification][core]. Bare section numbers refer to this document.

### 1.1. Conformance

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
interpreted as described in [RFC2119][rfc2119].

Three parties appear in requirements:

- **Endpoint.** The producer serving the WebSocket endpoint. Every requirement
  applies to the endpoint unless it names another party.
- **Client.** The connecting process. Client requirements are protocol
  mechanics: what a valid handshake contains and what a client must be
  prepared to receive. Guidance on what a consumer does with the events it
  receives remains in the core, [Appendix A][core-appendix-a], and is not
  restated here.
- **Relay.** A delivery-layer component that terminates a client connection
  and re-emits the stream on another (B-WSA-20).

Requirement identifiers are stable once allocated. A withdrawn requirement
keeps its number and is marked withdrawn in the register; numbers are never
reused.

### 1.2. Relation to WebSockets, CloudEvents, and AsyncAPI

This specification does not prescribe how a stream endpoint is provisioned
(for example, a REST call that creates a named stream resource). Provisioning
is a control-plane concern of the endpoint. The binding begins at the
WebSocket opening handshake.

This specification reuses the CloudEvents [JSON event format][json-format] and
[JSON batch format][json-batch-format] unchanged. It is **not** an application
of the CloudEvents [WebSockets protocol binding][ce-ws]: that binding
negotiates one event per WebSocket message under the `cloudevents.*`
subprotocols and excludes the batch format. Change data capture streams reach
hundreds of thousands of events per second, and per-message framing at that
rate is a measurable cost; this binding therefore uses the batch format as its
message unit and registers its own subprotocol (1.4). What it adds on top of
the formats is the stream-level contract CloudEvents does not define: start
position, schema coverage on resume, replay window, heartbeat, backpressure,
and close semantics.

The envelope values of every event, including `specversion` and
`cdcspecversion`, are those the core defines. This binding inherits them
verbatim and does not restate or qualify them (see Section 8, deferred core
item 3).

[AsyncAPI][asyncapi] is a description format, not a transport. Its
[WebSockets binding object][asyncapi-ws] describes only the HTTP handshake
(method, query parameters, headers). Everything else in this document is
OpenCDC-defined and is carried in the AsyncAPI document through ordinary
AsyncAPI objects plus one specification extension (6.3).

**Format composition is reserved.** This revision defines the JSON mapping
completely and defines no other. A future OpenCDC format binding that wishes
to compose with this transport binding will do so by a revision of this
document that names the combination, registers its subprotocol, and states
which clauses of Section 3 the format replaces. Until then, an endpoint
claiming this binding emits JSON only.

### 1.3. Content Modes

CloudEvents defines three content modes: *structured*, *binary*, and *batch*.
WebSocket messages carry no per-message metadata headers, so *binary* mode is
not available.

- **B-WSA-1.** Events MUST be carried in *structured* mode, grouped into
  *batch* messages (3.1). All CloudEvents attributes and all `cdc*` extension
  attributes appear as top-level keys of each event object alongside `data`
  ([core Section 12][core-12]).

### 1.4. Handshake

The [opening handshake][rfc6455-section-4] MUST follow [RFC6455][rfc6455].

- **B-WSA-2.** The client MUST include the `Sec-WebSocket-Protocol` header in
  the opening handshake, listing one or more OpenCDC subprotocols it supports
  in order of preference ([RFC6455 section 1.9][rfc6455-section-1-9]). The
  server MUST select exactly one it supports and return it in
  `Sec-WebSocket-Protocol`. If the client offers none the server supports, the
  server MUST reject the handshake before upgrade with HTTP 400 (1.4.2) and
  MUST NOT send a `Sec-WebSocket-Protocol` header.
- **B-WSA-3.** The subprotocols defined by this revision are:

| Subprotocol    | Message format                            | Frame type |
| -------------- | ----------------------------------------- | ---------- |
| `opencdc.json` | [JSON batch format][json-batch-format]    | Text       |

  All endpoints MUST support `opencdc.json`. The token is a proposed OpenCDC
  identifier and is not registered with IANA; it identifies the message format
  only. The wire protocol version is carried in every event as
  `cdcspecversion` and declared in the first event of every session (5.2), not
  in the token (1.7).

- **B-WSA-4.** The channel address (path) carries no session parameters. The
  start position (5.1) is a query parameter; credentials are HTTP headers or a
  declared security scheme (1.5). Neither MAY be encoded in the path or in the
  URL userinfo component.

#### 1.4.1. Example

Client request:

```text
GET /streams/finance-orders?position=now HTTP/1.1
Host: cdc.example.com:9102
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==
Sec-WebSocket-Protocol: opencdc.json
Sec-WebSocket-Version: 13
Authorization: Bearer eyJhbGciOi...
```

Server response:

```text
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: HSmrc0sMlYUkAGmm5OPpG2HaGWk=
Sec-WebSocket-Protocol: opencdc.json
```

#### 1.4.2. Handshake Rejection

Conditions that can be evaluated before the upgrade MUST be rejected before
the upgrade, with an HTTP status, so that a client never pays the cost of a
101 followed by an immediate close.

- **B-WSA-45.** The server MUST reject the handshake with the HTTP status
  below when the condition is known at handshake time. The response body
  SHOULD carry a short plain-text reason. Conditions that arise only after
  the upgrade use the close codes of 5.8.

| Condition                                                  | HTTP status |
| ---------------------------------------------------------- | ----------- |
| No supported subprotocol offered; `position` missing, malformed, or holding more than one selector | 400 |
| Authentication failed                                      | 401         |
| Authenticated but not authorized for this stream           | 403         |
| Stream not found                                           | 404         |
| `position` is a `cdcpos` this endpoint did not issue, or is outside the replay window | 409 |
| Endpoint or retained change log unavailable; retry later   | 503         |

### 1.5. Security

The core's transport-security guidance ([core Section 14.2][core-14-2]) is
SHOULD-level and defers the mechanism to the binding. This binding fixes the
mechanism.

- **B-WSA-5.** The endpoint MUST be served over TLS (`wss`). Plaintext `ws`
  endpoints do not satisfy this binding. TLS 1.2 or later. The server MUST
  present a certificate chain that validates under the client's configured
  trust store and MAY require client certificates.
- **B-WSA-46 (Client).** The client MUST validate the server's certificate
  chain and host identity and MUST NOT proceed past a failed validation. A
  client that disables validation is not operating under this binding.
- **B-WSA-6.** Credentials MUST NOT appear in the connection URL. The AsyncAPI
  document declares the accepted security schemes (`httpApiKey` in a header,
  `http` bearer, `oauth2`, or `X509`). HTTP Basic is permitted only over TLS
  and only via the `Authorization` header.
- **B-WSA-47.** If the credential presented at the handshake expires or is
  revoked while the connection is open, the server MAY close with `4000`
  (5.8). It MUST NOT continue to deliver events past the point at which it
  determines the principal is no longer authenticated.
- **B-WSA-7.** The core's producer security rules
  ([core Section 14.1][core-14-1]: S-AUTH-2, S-AUTHZ-1, S-AUTHZ-2) apply
  unchanged. The endpoint's `tables` declaration reflects only what the
  connecting principal is authorized to receive.
- **B-WSA-48.** Authorization is evaluated as of the session, not as of the
  event. Replay MUST NOT deliver events for tables or rows the connecting
  principal is not currently authorized to receive, even if the principal was
  authorized when those events were first emitted. If a change in
  authorization alters the endpoint's captured-table set, Section 5.7 applies.

**Browser clients are out of scope for this revision.** The browser WebSocket
API cannot set an `Authorization` header, so header-authenticated clients as
specified here are native clients. A browser profile (for example, a
short-lived single-use ticket) is deferred (Section 8).

### 1.6. Transport Capabilities

Every OpenCDC binding declares the transport capabilities it relies on, so
that a near-miss transport can identify the clause it fails.

| Capability                        | WebSocket over TLS                       | Consequence                                                     |
| --------------------------------- | ---------------------------------------- | --------------------------------------------------------------- |
| Per-client session                | Yes                                      | `session_aware: true` required (4.1)                            |
| Ordered, reliable byte stream     | Yes (TCP)                                | Order as sent is preserved to consumption; source commit order and transaction contiguity are the endpoint's obligation, not TCP's (4.1, 4.2) |
| Virtual channels per connection   | No                                       | One connection is one channel; multi-channel out of scope       |
| Per-message metadata headers      | No                                       | Structured mode only (1.3)                                      |
| Message framing                   | Text/binary frames with fragmentation    | Large events by fragmentation; no application chunking (3.4)    |
| Subprotocol negotiation           | `Sec-WebSocket-Protocol`                 | Format agreed at handshake (1.4)                                |
| Broker-side retention             | No                                       | Replay served from the endpoint's retained change log (5.4)     |
| Flow control                      | TCP backpressure; no acknowledgement     | Push mode; no application acknowledgement (5.3, 5.6)            |
| Keep-alive                        | Ping/Pong control frames                 | Not a substitute for HEARTBEAT (5.5)                            |
| Handshake parameters              | HTTP GET query string and headers        | Start position and credentials at handshake (1.4, 5.1)          |
| Close signalling                  | Close frame, not guaranteed to arrive    | Clients MUST handle abnormal closure (5.8)                      |

### 1.7. Versioning and Compatibility

- The binding version is `<wire>.<revision>`: the first two components are
  the OpenCDC wire protocol this document maps (`0.3`); the third is this
  document's revision. `-wip` marks a draft. Revisions correct or extend the
  binding without a wire change; a wire change produces a new `<wire>`.
- **B-WSA-55.** An endpoint MUST emit only event types and attributes defined
  by the wire protocol version it declares in STREAM_METADATA and carries in
  `cdcspecversion`. A client generated from a document for wire `0.3` that
  receives an event with a `cdcspecversion` it does not support MUST treat it
  as a protocol error and SHOULD close with `4002`.
- Unknown members of the `x-opencdc` extension (6.3) MUST be ignored by
  clients. Unknown members of an event are governed by the core's closed-world
  schema rules ([core Section 2.4][core-2-4]).

## 2. Use of OpenCDC Attributes

This specification does not redefine any OpenCDC attribute. It fixes how
these are used on this transport ([core Section 3.3][core-3-3]):

- `cdcpos` is the resume handle. Its value is passed back verbatim as the
  `position` query parameter on reconnect (5.1). The endpoint MUST accept only
  `cdcpos` values it issued for this stream (5.4); the client MUST NOT
  construct or modify them.
- `sequence` is comparable within one connection. Because the endpoint
  delivers one ordered channel, the session-scoped comparability the core
  grants is the comparability a client actually gets.
- `cdcspecversion` is the wire protocol version and is the client's
  compatibility check (1.7).
- `partitionkey` is advisory and has no effect on this transport. Endpoints
  MAY emit it; clients have no use for it here.

## 3. WebSocket Message Mapping

### 3.1. Message Unit

The WebSocket message is a batch of events. This follows the existing
GoldenGate Data Streams convention (a JSON array of records per message, sized
by a server-side buffer) and is the only message unit this binding defines.

- **B-WSA-8.** Each WebSocket message MUST be one CloudEvents
  [JSON batch][json-batch-format]: a JSON array of structured-mode event
  objects. A batch containing exactly one event is still a one-element array,
  so a client parses every message the same way.
- **B-WSA-9.** The server MUST NOT send an empty batch. Liveness is signalled
  by HEARTBEAT events (5.5) and by Ping frames, never by empty messages.
- **B-WSA-10.** Batch boundaries carry no semantics. Events within a batch are
  in emitted order, and the concatenation of batches on a connection is the
  emitted sequence. A transaction MAY span batches. A client MUST NOT infer
  transaction completion from a batch boundary; completion follows
  [core Section 8.3][core-8-3] (next `cdcxid`, HEARTBEAT, or TRX_COMMIT where
  declared).
- **B-WSA-11.** An endpoint SHOULD prefer batch boundaries that coincide with
  transaction boundaries when its buffering permits. This is a latency
  convenience, not a correctness requirement, and a client MUST NOT rely on it.
- **B-WSA-49.** The server MUST bound the time an event waits in an unfilled
  batch. It MUST flush any non-empty batch within the linger bound it
  advertises as `x-opencdc.maxBatchLingerMs` (6.3), and a HEARTBEAT event MUST
  NOT be held past that bound. The linger bound MUST be shorter than the
  declared `heartbeat_interval_seconds` so that the batch mechanism cannot
  cause a missed heartbeat.

### 3.2. Frame Type

- **B-WSA-12.** Under `opencdc.json`, messages MUST be sent as WebSocket
  **text** frames (UTF-8). An endpoint MUST NOT send binary data frames on a
  connection negotiated as `opencdc.json`.

### 3.3. Event Data Encoding

- **B-WSA-13.** Each event's `datacontenttype` MUST be `application/json`
  (the core JSON encoding). The AsyncAPI document's `defaultContentType` is
  `application/cloudevents-batch+json` and describes the WebSocket message,
  not the event `data`; the two describe different layers.

### 3.4. Size Limits

Three sizes are distinct: the size of one serialized event, the size of one
batch message after reassembly, and the size of one WebSocket frame. Frame
size is the transport's concern and is not limited by this binding.

- **B-WSA-14.** The binding defines no application-level chunking or
  truncation of events. An event larger than one frame is carried by
  [RFC6455 fragmentation][rfc6455-section-5-4], which the WebSocket layer
  reassembles transparently. Truncating LOB values, DDL text, or records to a
  size ceiling is a payload change governed by the core
  ([Section 6.2][core-6-2], [Section 6.3][core-6-3], [Section 9][core-9]); an
  endpoint that truncates is not emitting the core's events and MUST NOT claim
  this binding for that stream.
- **B-WSA-15.** An endpoint MUST advertise `x-opencdc.maxEventBytes` and
  `x-opencdc.maxMessageBytes` (6.3), both as UTF-8 byte counts of the
  serialized JSON before any transport compression. `maxMessageBytes` MUST be
  at least `maxEventBytes`. A client SHOULD size its receive buffer to
  `maxMessageBytes`.
- **B-WSA-50.** The server MUST split batches so that no message exceeds
  `maxMessageBytes`; splitting changes nothing but batch boundaries
  (B-WSA-10). An event that exceeds `maxEventBytes` is a server-side
  configuration error: the server MUST close with `4013` and MUST NOT
  truncate, drop, or skip the event. After a `4013` close the endpoint's
  operator must raise the limit or correct the source; the client cannot make
  progress past that position until then.

## 4. Stream Profile and Delivery Properties

A WebSocket endpoint is one ordered channel with a session per client. That
fixes several capability axes and several delivery properties. TCP preserves
the order the endpoint sends; it does not establish source commit order or
transaction contiguity. Those are obligations the endpoint takes on by
claiming this profile.

### 4.1. Capability Axes

The endpoint emits these values in STREAM_METADATA as usual
([core Section 10.4][core-10-4]); the binding constrains which values a
claiming endpoint may declare ([core Section 2.2a][core-2-2a]).

| STREAM_METADATA field                  | Value under this binding                          | Why                                                                                   |
| -------------------------------------- | ------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `ordering_scope`                       | `"stream"`                                        | One totally ordered sequence                                                          |
| `transaction_interleaving`             | `"none"`                                          | Endpoint MUST emit transactions contiguously in source commit order (P-ORD-1); an endpoint that cannot MUST NOT claim the binding |
| `session_aware`                        | `true`                                            | The server observes connect and disconnect                                            |
| `schema_delivery.schema_on_change`     | `true`                                            | Core requirement, unchanged                                                           |
| `schema_delivery.schema_on_reconnect`  | `true`                                            | Reconnect coverage via in-stream re-emission ([core Section 4.5.2][core-4-5-2], [core Section 6.4][core-6-4], P-CONN-1) |
| `sequence_continuity`                  | Producer-declared                                 | An endpoint whose retained positions are stable across sessions and replicas (B-WSA-52) MAY declare `"guaranteed"` |
| `transaction_boundaries`               | Producer-declared, any legal value                | With `"none"` interleaving TRX_COMMIT is optional; P-TRX-7 does not apply since the endpoint delivers directly |
| `transaction_marker_delivery`          | Either value if markers are emitted               | One channel; the values are equivalent here                                           |
| `ddl_capture`                          | Producer-declared, consistent with any filtering  | See B-WSA-36                                                                          |
| `tables`                               | Captured tables of *this endpoint* after filtering | Each endpoint is its own stream (5.7)                                                 |

### 4.2. Delivery Properties

The core charters four delivery properties for bindings to govern
([core Section 12][core-12]).

- **B-WSA-16 (Ordering preservation).** The server MUST deliver events on a
  connection in the endpoint's emitted order. `sequence`, `cdctxorder`, and
  `(pos.lsn, pos.lsn_offset)` are preserved as emitted.
- **B-WSA-17 (Control-channel retention).** Not applicable. There is no control
  channel; STREAM_METADATA and OBJECT_METADATA coverage are established per
  session (5.2).
- **B-WSA-18 (Schema coverage).** The server MUST NOT deliver a data event
  (DML, snapshot, TRUNCATE, DDL) on a connection unless the OBJECT_METADATA
  version governing that event has already been delivered on the same
  connection, either as a session-scoped re-emission (B-WSA-24) or as a
  durable event earlier in the session. To honour this for every position it
  accepts as a start position, the server MUST retain every OBJECT_METADATA
  version that governs any retained data event. This is the binding's
  realization of R-POS-2 and R-POS-7 and is made precise in 5.2.
- **B-WSA-19 (Marker retention parity).** If the endpoint emits TRX_COMMIT
  markers, they are in-band on the same channel and retained with the data
  events they complete. P-RET-1 is satisfied structurally.
- **B-WSA-20 (Relay integrity).** A relay MUST preserve event order, event
  identity (`source`, `id`), event content, and STREAM_METADATA declarations.
  A relay MAY re-batch (split or merge batches) and MAY re-fragment frames,
  since batch and frame boundaries carry no semantics (B-WSA-10). A relay MUST
  NOT drop, reorder, or rewrite events, and MUST NOT merge or split events. A
  relay that violates this is not covered by the endpoint's binding claim.

## 5. Session Protocol

### 5.1. Start Position

- **B-WSA-21.** The handshake MUST carry exactly one start selector in the
  query parameter `position`. A missing parameter, a repeated parameter, or a
  value matching more than one form below MUST be rejected with HTTP 400
  (1.4.2). The forms are:

| Value                              | Meaning                                                      | Server behavior                                                         |
| ---------------------------------- | ------------------------------------------------------------ | ----------------------------------------------------------------------- |
| `now`                              | Changes committed after the connection is accepted           | Timestamp lookup at accept time                                         |
| `earliest`                         | Oldest position the server retains                           | Oldest retained position; schema coverage per 5.2                       |
| [RFC3339][rfc3339] timestamp       | First event whose `pos.source_timestamp` is at or after it   | Lookup on source commit time; see below                                 |
| a `cdcpos` value                   | Resume after the event with this position                    | Cursor validation (5.4), then replay boundary (5.2)                     |

  Timestamp selectors MUST use the `Z` (UTC) designator; a timestamp with an
  offset or without a designator is malformed. Precision is that of
  `pos.source_timestamp` as emitted by the endpoint; a selector with finer
  precision is truncated. The comparison is against the source commit time,
  not the endpoint's receive time. A timestamp earlier than the oldest
  retained event MUST be rejected with HTTP 409, not silently promoted to
  `earliest`; a timestamp later than the newest retained event begins at
  `now`.

  `cdcpos` values MUST be percent-encoded per [RFC3986][rfc3986] when placed
  in the query string and decoded before comparison. The endpoint MUST NOT
  issue `cdcpos` values that equal `now` or `earliest` or parse as RFC3339.

- **B-WSA-22.** A resume request is valid when its `position` is a `cdcpos`
  this endpoint issued for this stream and its configuration (5.7), within
  the replay window (5.4). Timestamps and keywords are start selectors, not
  resume handles: the endpoint guarantees no relationship between a timestamp
  start and a prior session's delivery.

*Note.* A client that restarts from a timestamp rather than a saved `cdcpos`
accepts either duplication or a gap at the boundary; which one, and how the
client checkpoints, is consumer guidance ([core Appendix A.7][core-a7]) and
outside this binding.

*Note.* The parameter is named `position` so that no implementation's spelling
is privileged. The four forms are the GoldenGate Data Streams `begin`
vocabulary. An implementation MAY accept a historical parameter name as an
alias; if both the alias and `position` are present the request is malformed
(HTTP 400).

### 5.2. First-Event Sequence and the Replay Boundary

Two boundaries matter on a resumed connection: the **cursor** (the `cdcpos`
the client supplied) and the **replay start** (the position from which the
server actually begins delivering durable events). They are not the same,
and the difference is where schema coverage is established.

On every accepted connection, whether initial or resumed:

- **B-WSA-23.** The server MUST emit STREAM_METADATA as the first event of the
  session, before any other event ([core Section 10.4][core-10-4], Single
  Channel Stream). This event is session-scoped and carries no `cdcpos`.
- **B-WSA-24.** The server MUST then emit the current OBJECT_METADATA for every
  active table before the first data event of the session
  ([core Section 4.5.2][core-4-5-2], P-CONN-1). These re-emissions are
  session-scoped, carry no `cdcpos`, and are not part of the durable stream.
  Their `dataschema` ids and `schema_version` values are those of the durable
  versions they repeat.
- **B-WSA-25 (Replay start).** The server MUST then deliver durable events in
  order from the replay start `P`, defined as follows for a cursor `C`. Let
  `T` be the set of tables active on the endpoint. Let `P0` be the minimum,
  over `t` in `T`, of the position of the OBJECT_METADATA version of `t`
  governing at `C`. Then repeat: if any data event in `[P, C]` for some table
  is governed by an OBJECT_METADATA version positioned before `P`, set `P` to
  that version's position. `P` is the fixpoint. For a timestamp or `earliest`
  selector, `C` is the first event position selected and the same closure
  applies. For `now`, `P = C` and the session-scoped re-emissions of B-WSA-24
  are the complete coverage.

  Delivery from `P` includes every durable event in `[P, C]` (data and schema)
  and continues past `C`. Events in `[P, C]` are duplicates of events the
  client has already seen; they are permitted under at-least-once (B-WSA-31)
  and are resolved by `(source, id)`.

*Why the closure.* Rewinding only to the earliest *currently governing*
schema is not enough. With table B schema v1 at position 5, table A schema v1
at 10, B data at 20 governed by B v1, B schema v2 at 100, and a cursor at 150,
the currently governing versions are A v1 (10) and B v2 (100), so `P0 = 10`;
but B's data at 20 needs B v1 at 5, which is before 10. The closure sets
`P = 5`. Without it the client receives undecodable data at 20.

*Note for the working group.* The closure is correct and core-compliant but
can rewind a long way after schema changes on a busy stream. A session-scoped
re-emission of the versions governing *at the cursor* (rather than the
current versions) would make `P = C` always and eliminate the rewind. That
requires amending core Section 4.5.2 and R-POS-2, and is deferred (Section 8,
deferred core item 2). Until then this binding relies on the closure.

- **B-WSA-56 (Cursor inside a transaction).** A cursor may fall between two
  events of one transaction. Replay from `P` re-delivers that transaction from
  its first event, so the client sees the transaction whole. The client MUST
  NOT assume that the first data event after the cursor begins a new
  transaction.

Shape of a resumed session with the example above (events shown
individually; on the wire they arrive grouped into batches):

```text
[STREAM_METADATA]              session-scoped
[OBJECT_METADATA A v1]         session-scoped re-emission (current)
[OBJECT_METADATA B v2]         session-scoped re-emission (current)
[OBJECT_METADATA B v1]  pos 5  durable, replay start P
[OBJECT_METADATA A v1]  pos 10 durable
[dml.INSERT B ...]      pos 20 durable, governed by B v1
...
[OBJECT_METADATA B v2]  pos 100 durable, the schema-change event
...
[dml.UPDATE A ...]      pos 150 durable, the cursor (duplicate)
[dml.INSERT B ...]      pos 151 durable, first new event
[HEARTBEAT]
```

### 5.3. Steady State

- **B-WSA-26.** The server pushes events continuously without waiting for
  client acknowledgement. No acknowledgement message exists in this binding,
  and no server behavior depends on client progress. A normal closure by the
  client (`1000`) is not an acknowledgement of any event.
- **B-WSA-27.** The client MUST NOT send data frames on the connection. A
  client data frame is a protocol error; the server MAY close with `4002`.
  (Reserved: a future revision may define a client-to-server control message;
  none is defined here.)

*Note.* Because the server never learns what the client has applied, the
client alone decides when to persist a `cdcpos` checkpoint. That decision is
consumer guidance ([core Appendix A.7][core-a7]).

### 5.4. Cursor Validity and the Replay Window

- **B-WSA-28.** The set of `cdcpos` values a server will accept as a start
  position is its **replay window**. The server MUST document the window's
  lower bound as a duration or a retention-policy reference in
  `x-opencdc.replayWindow` (6.3), and SHOULD keep the window at least as long
  as the longest client outage it intends to tolerate.
- **B-WSA-29.** A start position outside the replay window MUST be refused
  before upgrade with HTTP 409 (1.4.2). The server MUST NOT silently begin
  elsewhere or deliver from `earliest`. Silently substituting a position
  converts a resumable gap into an undetected one
  ([core Section 15.1][core-15-1]).
- **B-WSA-51 (Cursor scope).** A `cdcpos` is scoped to one stream and one
  stream configuration (5.7). A cursor presented to a different stream, or
  after a configuration change that the endpoint cannot prove
  replay-compatible, MUST be refused with HTTP 409. The endpoint SHOULD make
  `cdcpos` values self-identifying (for example, by embedding a stream and
  configuration identifier) so that this check is cheap.
- **B-WSA-52 (Cursor stability).** A `cdcpos` MUST remain valid, with the
  same meaning, across restarts of the endpoint and across replicas of the
  endpoint that serve the same stream, for as long as it is inside the replay
  window. Event `id`, `cdcpos`, `schema_version`, and TRX_COMMIT contents
  MUST be identical on replay to their original emission, on every replica.
  An endpoint that cannot guarantee this MUST declare
  `sequence_continuity: "reset"` and MUST refuse cursors it did not itself
  issue.
- **B-WSA-30.** Replay MUST satisfy the Replay Guarantee Summary of
  [core Section 8.2][core-8-2]: schema before data, order preserved on
  `(pos.lsn, pos.lsn_offset)`, original `id` values preserved. `sequence` MAY
  differ from the original delivery.
- **B-WSA-31.** Delivery is at-least-once (R-POS-5) for every event with
  position after `C`. Duplicates, including the rewound segment `[P, C]`, are
  resolved by `(source, id)`. This binding defines no exactly-once wire mode:
  exactly-once effects require the client to coordinate deduplication,
  application state, and checkpoints; the transport supplies none of that.

### 5.5. Heartbeat and Keep-Alive

- **B-WSA-32.** The HEARTBEAT obligation ([core Section 10.1][core-10-1];
  `heartbeat_interval_seconds` in STREAM_METADATA) is met only by HEARTBEAT
  events on the data stream. Ping/Pong control frames MAY be used for
  transport liveness but carry no `cdcpos` and no `source_lag_ms`, and MUST
  NOT be counted as HEARTBEAT.
- **B-WSA-33.** A server SHOULD send Ping frames at an interval shorter than
  any intermediary idle timeout it is deployed behind, and MAY close a
  connection that fails to answer within a documented grace period with `1001`
  or `4004`.

### 5.6. Backpressure

The transport offers no acknowledgement; the only flow control is TCP. This
revision defines Durable Mode only ([core Section 15.1][core-15-1]).

- **B-WSA-34.** A server that cannot write because the client is slow MUST
  block or buffer and MUST NOT drop, skip, or coalesce events. If its
  buffering limit is reached it MUST close with `4011`; the client resumes
  from its saved `cdcpos`, and the events it did not receive are inside the
  replay window by construction. The endpoint SHOULD document its buffering
  limit alongside `replayWindow`.
- **B-WSA-35.** *Withdrawn.* Ephemeral Mode delivery was defined here in the
  previous revision and is deferred (Section 8). `x-opencdc.operationalMode`
  is fixed at `durable` in this revision.

### 5.7. Filtering and Stream Identity

An endpoint MAY apply server-side filtering: by object name, operation type,
column value, partition, tag, or token. Filtering is applied **before** the
endpoint's OpenCDC stream is formed. The filtered result is the stream; it
has its own STREAM_METADATA, its own `tables`, and its own contiguous
ordinals. Nothing downstream can tell that a filter exists, except that the
declarations describe the filtered set.

- **B-WSA-36.** Filtering that removes DDL events MUST be reflected in
  `ddl_capture: "none"` in STREAM_METADATA. An endpoint MUST NOT declare
  `"verbatim"` or `"structural"` and then withhold emission-schema-affecting
  DDL from the stream (P-DDL-1 to P-DDL-3).
- **B-WSA-38.** Filtering MUST NOT remove individual columns from an image;
  that is a schema change governed by [core Section 4][core-4]. Filtering
  operates on whole events.
- **B-WSA-53 (Dense ordinals).** Within a transaction, `cdctxorder` MUST be
  assigned over the events the endpoint emits, 0-based and contiguous, after
  filtering. Native sequence numbers of the source (for example a per-record
  operation sequence) are inputs to this assignment, not the assignment
  itself. Gaps are a conformance error (T-ORDER).
- **B-WSA-39 (Markers over the emitted set).** If the endpoint emits
  TRX_COMMIT, `event_count` and `distribution`
  ([core Section 10.5.3][core-10-5-3]) MUST be computed over the emitted
  events of the transaction, so that the core's completeness check
  (`0..event_count-1`) holds exactly. Markers computed over the unfiltered
  source transaction are a defect a client cannot distinguish from data loss.
- **B-WSA-54 (Wholly filtered transactions).** A transaction none of whose
  events pass the filter produces no events and no TRX_COMMIT. Its `cdcxid`
  never appears on the stream.
- **B-WSA-37 (Configuration change).** The filter configuration, the
  captured-table set, and the schema-delivery declarations together form the
  stream's **configuration identity**. If any of them changes while a
  connection is open, the server MUST either emit a new STREAM_METADATA
  in-band at the point of change (when the change is replay-compatible and
  the core permits in-band re-emission, [core Section 10.4][core-10-4]) or
  close with `4012` so the client reconnects. A change that is not
  replay-compatible (for example, a narrowed row filter, which would make
  previously issued cursors point into a differently numbered stream) MUST
  close with `4012`, and cursors issued before the change MUST be refused
  thereafter (B-WSA-51).

*Filtered changes are not a filtered view.* A row-value filter selects change
events whose image matches a predicate. It does not maintain the set of rows
matching the predicate: a row that stops matching is not signalled as a
delete, and a row that starts matching is not signalled as an insert. An
endpoint that offers row-value filtering MUST document, in `info.description`
of its AsyncAPI document, which image (before, after, or either) the predicate
is evaluated against. Consumers that need a filtered *view* must derive it
from the unfiltered stream.

*Note.* Filtering out `UPDATE` while keeping `INSERT` and `DELETE` yields a
stream that is conformant over what it emits but is not a faithful changelog
of the captured tables. STREAM_METADATA has no advisory field for this in
v0.7.0 (Section 8, deferred core item 4).

### 5.8. Failure Behavior and Close Codes

Failures fall in three places: before the upgrade (HTTP status, 1.4.2), after
the upgrade (WebSocket close frame with a code from the table below), and
abnormal loss (no close frame at all).

Application close codes use the [RFC6455 private range][rfc6455-section-7-4-2]
4000-4999. A reason string, when present, MUST be UTF-8 and MUST NOT exceed
123 bytes ([RFC6455 section 5.5.1][rfc6455-section-5-5-1]).

| Code   | Name                          | Sent when                                                                                   | Client retry |
| ------ | ----------------------------- | ------------------------------------------------------------------------------------------- | ------------ |
| `1000` | Normal closure                | Client-initiated orderly close, or server shutdown after flushing                           | As needed    |
| `1001` | Going away                    | Server shutting down; client resumes from saved `cdcpos`                                    | Yes, backoff |
| `1006` | Abnormal closure (local only) | Connection lost without a close frame; never sent, observed by the client                   | Yes, backoff |
| `4000` | Unauthorized                  | Credential expired or revoked after upgrade (B-WSA-47)                                      | With new credential |
| `4001` | Forbidden                     | Authorization withdrawn after upgrade                                                       | No           |
| `4002` | Protocol error                | Client sent a data frame; incompatible `cdcspecversion` (B-WSA-55); other violation of 5.3  | No           |
| `4004` | Liveness timeout              | Client failed to answer Pings within the grace period                                       | Yes          |
| `4011` | Backpressure limit            | B-WSA-34                                                                                    | Yes, backoff |
| `4012` | Stream reconfigured           | B-WSA-37                                                                                    | Yes, then expect HTTP 409 for stale cursors |
| `4013` | Event size exceeded           | B-WSA-50                                                                                    | No, until the operator acts |
| `4020` | Stream unavailable            | The producer stopped or the retained change log became unreachable after upgrade            | Yes, backoff |

- **B-WSA-40.** A server MUST use the code above whose condition applies, and
  MUST NOT use `1000` for any error condition. A client MUST treat `1006`
  (no close frame received) as a resumable interruption equivalent to `1001`
  and MUST NOT interpret any close, including `1000`, as confirmation that
  the server observed its progress.
- **B-WSA-57.** Codes `4003` and `4010` from the previous revision are
  withdrawn: their conditions are evaluated before upgrade and are reported as
  HTTP 400 and 409 respectively (1.4.2). A server MUST NOT emit them.

## 6. AsyncAPI Description

A claiming endpoint MUST publish an [AsyncAPI 3.x][asyncapi] document that
clients can use directly with `@asyncapi/generator` and other AsyncAPI
tooling. The document is part of the binding's conformance surface: it is what
makes the binding *WSS + AsyncAPI* rather than a bare WebSocket binding.

Two kinds of statement appear in the document. **Stream declarations**
(capability axes, tables, heartbeat interval) belong to STREAM_METADATA
([core Section 2.2a][core-2-2a], producer-sole-emitter invariant); the
document MAY echo them and the echo is informative. **Endpoint declarations**
(subprotocols, size limits, linger, replay window, operational mode) belong to
this binding, live in the `x-opencdc` extension, and are normative for the
endpoint: an endpoint MUST behave as its extension declares.

Description conformance (the document is valid and complete under this
section) is distinct from generation success (a particular generator produces
a working client). The binding requires the former and specifies the latter
only to the extent of B-WSA-58.

### 6.1. Required Document Elements

- **B-WSA-41.** The document MUST be retrievable by an authorized client from a
  URL the endpoint documents (for example, returned when the stream resource
  is created, or served at a well-known path adjacent to the channel address).
  It MUST declare `asyncapi: 3.x.y` (3.1.0 or later 3.x), `info`, at least one
  server with `protocol: wss`, exactly one channel per stream endpoint,
  exactly one `receive` operation per channel, and the message of 6.4.
- **B-WSA-42 (Perspective).** The published document is written from the
  **client's** perspective: it describes the application a client generator
  will produce, so its operation is `receive`, its `info.description` MUST
  say that the document is written for client generation and that the server
  pushes events, and `x-opencdc.perspective` MUST be `client`.
- **B-WSA-43.** The channel MUST carry a `ws` channel binding declaring the
  `position` query parameter as required and the `Sec-WebSocket-Protocol`
  header as required (6.2). It MUST NOT carry a `ws` operation or message
  binding (those objects are reserved and empty in
  [bindingVersion 0.1.0][asyncapi-ws]).
- **B-WSA-44.** `defaultContentType` MUST be
  `application/cloudevents-batch+json`. The server object's `security` MUST
  list the schemes of 1.5.
- **B-WSA-58.** The endpoint MUST validate its published document with the
  AsyncAPI parser and MUST record, in `info.description` or an external
  reference from it, the parser version and at least one generator and
  template combination with which a client has been produced and exercised
  against the endpoint. Generic generator compatibility is not assumed.

*Why the perspective is fixed.* AsyncAPI 3.x documents describe the operations
of the application they are about, and AsyncAPI advises against deriving a
receiver document from a sender one. A producer documenting *itself* would
write `send`. This binding fixes the client perspective because the document's
purpose here is client generation, following the GoldenGate Data Streams
pattern. A producer MAY additionally publish a server-perspective document;
that is not the document the binding requires.

*What generation covers.* A generated client provides transport and model
code: handshake, subprotocol offer, batch parsing, event typing. It does not
provide CDC runtime behavior: checkpointing, deduplication on `(source, id)`,
transaction assembly, schema-version tracking. Those remain the consumer's
work ([core Appendix A][core-appendix-a]).

### 6.2. `ws` Channel Binding

```yaml
bindings:
  ws:
    bindingVersion: 0.1.0
    method: GET
    query:
      type: object
      required: [position]
      properties:
        position:
          description: >
            Start selector, exactly one of: "now", "earliest", an RFC 3339
            UTC timestamp ending in Z, or a percent-encoded cdcpos value
            previously issued by this endpoint for this stream.
          type: string
      additionalProperties: false
    headers:
      type: object
      required: [Sec-WebSocket-Protocol]
      properties:
        Sec-WebSocket-Protocol:
          description: >
            Comma-separated list of offered subprotocols in preference
            order. Must include opencdc.json.
          type: string
          pattern: '(^|,\s*)opencdc\.json(\s*,|$)'
        Authorization:
          type: string
```

### 6.3. `x-opencdc` Extension

One extension object at the channel level carries the endpoint declarations.
Its schema is published as [x-opencdc.schema.json](x-opencdc.schema.json).
Required members are marked; clients MUST ignore members they do not
recognise (1.7).

```yaml
x-opencdc:
  binding: wss-asyncapi          # required; const
  bindingVersion: "0.3.0"        # required; this document's version without -wip
  perspective: client            # required; const (B-WSA-42)
  subprotocols: [opencdc.json]   # required; offered by this endpoint (1.4)
  operationalMode: durable       # required; const in this revision (5.6)
  replayWindow: P7D              # required; ISO 8601 duration or a URI (B-WSA-28)
  maxEventBytes: 16777216        # required; B-WSA-15
  maxMessageBytes: 67108864      # required; B-WSA-15
  maxBatchLingerMs: 250          # required; B-WSA-49
  closeCodes:                    # optional; only if the endpoint adds codes beyond 5.8
    - code: 4030
      name: tenantSuspended
```

### 6.4. Messages and Schemas

The channel carries exactly one message object, `opencdcBatch`, whose payload
is a non-empty array (`minItems: 1`, B-WSA-9) whose items are `oneOf` the
OpenCDC event schemas for wire 0.3. Per-event-family message objects MAY be
placed under `components/messages` for tooling; they are not channel
messages.

- **B-WSA-59.** The `oneOf` MUST cover every event type the endpoint may
  emit, which under wire 0.3 is: STREAM_METADATA, OBJECT_METADATA, HEARTBEAT,
  `dml.INSERT`, `dml.UPDATE`, `dml.DELETE`, `dml.UPSERT`, `dml.TRUNCATE`,
  DDL, `snapshot.READ`, and TRX_COMMIT. An endpoint that does not emit a
  stretch type (for example snapshots) MAY omit its branch and MUST then
  never emit that type on the channel. The branches MUST be mutually
  exclusive on the CloudEvents `type` attribute, which the closed operation
  vocabulary of [core Section 3.2][core-3-2] provides.
- **B-WSA-60 (Schema dialect).** The OpenCDC schemas are JSON Schema
  2020-12 ([core envelope schema][core-envelope]); AsyncAPI's default Schema
  Object is Draft 07. Every schema reference MUST therefore be a
  Multi-Format Schema Object with
  `schemaFormat: application/schema+json;version=draft-2020-12`
  ([AsyncAPI multi-format schema][asyncapi-schema]), and the endpoint MUST
  verify that its chosen parser and generator support that dialect
  (B-WSA-58).
- **B-WSA-61 (Resolvable references).** Every `$ref` in the published
  document MUST resolve to an immutable resource: a pinned release URL, a
  content-addressed URL, or a copy bundled under `components/schemas`.
  Moving-branch URLs are not permitted in a published document. Each event
  branch MUST validate a complete structured-mode event, that is, the
  envelope schema composed (`allOf`) with the per-type `data` schema.

### 6.5. Template

The template is also published as [asyncapi-template.yaml](asyncapi-template.yaml).
It bundles the envelope and per-type schemas by reference to the placeholder
`SCHEMA_BASE`; a publisher replaces `SCHEMA_BASE` with a pinned URL or inlines
the schemas (B-WSA-61).

```yaml
asyncapi: 3.1.0
id: urn:example:opencdc:finance-orders
info:
  title: FINANCE.ORDERS change stream
  version: "1.0.0"
  description: >
    OpenCDC 0.3 / WSS+AsyncAPI Binding 0.3. This document is written from the
    client's perspective for code generation; the server pushes events.
    Row-value filters, if any, are evaluated against the after image.
    Validated with @asyncapi/parser 3.x; client generated and exercised with
    @asyncapi/generator and the websocket-client-template (see release notes).
defaultContentType: application/cloudevents-batch+json

servers:
  prod:
    host: cdc.example.com:9102
    protocol: wss
    protocolVersion: "13"
    security:
      - $ref: '#/components/securitySchemes/bearer'

channels:
  ordersStream:
    address: /streams/finance-orders
    title: FINANCE.ORDERS OpenCDC stream
    bindings:
      ws:
        bindingVersion: 0.1.0
        method: GET
        query:
          type: object
          required: [position]
          properties:
            position:
              type: string
          additionalProperties: false
        headers:
          type: object
          required: [Sec-WebSocket-Protocol]
          properties:
            Sec-WebSocket-Protocol:
              type: string
              pattern: '(^|,\s*)opencdc\.json(\s*,|$)'
            Authorization:
              type: string
    x-opencdc:
      binding: wss-asyncapi
      bindingVersion: "0.3.0"
      perspective: client
      subprotocols: [opencdc.json]
      operationalMode: durable
      replayWindow: P7D
      maxEventBytes: 16777216
      maxMessageBytes: 67108864
      maxBatchLingerMs: 250
    messages:
      opencdcBatch:
        $ref: '#/components/messages/opencdcBatch'

operations:
  onEvents:
    action: receive
    channel:
      $ref: '#/channels/ordersStream'
    summary: Receive OpenCDC event batches in emitted order.

components:
  securitySchemes:
    bearer:
      type: http
      scheme: bearer
  messages:
    opencdcBatch:
      name: opencdcBatch
      contentType: application/cloudevents-batch+json
      payload:
        schemaFormat: application/schema+json;version=draft-2020-12
        schema:
          type: array
          minItems: 1
          items:
            oneOf:
              - $ref: '#/components/schemas/streamMetadata'
              - $ref: '#/components/schemas/objectMetadata'
              - $ref: '#/components/schemas/heartbeat'
              - $ref: '#/components/schemas/dml'
              - $ref: '#/components/schemas/ddl'
              - $ref: '#/components/schemas/trxCommit'
  schemas:
    envelope:
      schemaFormat: application/schema+json;version=draft-2020-12
      schema:
        $ref: 'SCHEMA_BASE/opencdc-envelope.schema.json'
    streamMetadata:
      schemaFormat: application/schema+json;version=draft-2020-12
      schema:
        allOf:
          - $ref: 'SCHEMA_BASE/opencdc-envelope.schema.json'
          - properties:
              type:
                pattern: '\\.cdc\\.meta\\.STREAM_METADATA$'
              data:
                $ref: 'SCHEMA_BASE/opencdc-stream-metadata.schema.json'
    objectMetadata:
      schemaFormat: application/schema+json;version=draft-2020-12
      schema:
        allOf:
          - $ref: 'SCHEMA_BASE/opencdc-envelope.schema.json'
          - properties:
              type:
                pattern: '\\.cdc\\.meta\\.OBJECT_METADATA$'
              data:
                $ref: 'SCHEMA_BASE/opencdc-object-metadata.schema.json'
    heartbeat:
      schemaFormat: application/schema+json;version=draft-2020-12
      schema:
        allOf:
          - $ref: 'SCHEMA_BASE/opencdc-envelope.schema.json'
          - properties:
              type:
                pattern: '\\.cdc\\.meta\\.HEARTBEAT$'
              data:
                $ref: 'SCHEMA_BASE/opencdc-heartbeat.schema.json'
    dml:
      schemaFormat: application/schema+json;version=draft-2020-12
      schema:
        allOf:
          - $ref: 'SCHEMA_BASE/opencdc-envelope.schema.json'
          - properties:
              type:
                pattern: '\\.cdc\\.(dml\\.(INSERT|UPDATE|DELETE|UPSERT|TRUNCATE)|snapshot\\.READ)$'
              data:
                $ref: 'SCHEMA_BASE/opencdc-dml.schema.json'
    ddl:
      schemaFormat: application/schema+json;version=draft-2020-12
      schema:
        allOf:
          - $ref: 'SCHEMA_BASE/opencdc-envelope.schema.json'
          - properties:
              type:
                pattern: '\\.cdc\\.ddl\\.(CREATE|ALTER|DROP)$'
              data:
                $ref: 'SCHEMA_BASE/opencdc-ddl.schema.json'
    trxCommit:
      schemaFormat: application/schema+json;version=draft-2020-12
      schema:
        allOf:
          - $ref: 'SCHEMA_BASE/opencdc-envelope.schema.json'
          - properties:
              type:
                pattern: '\\.cdc\\.meta\\.TRX_COMMIT$'
              data:
                $ref: 'SCHEMA_BASE/opencdc-trx-commit.schema.json'
```

*Note.* The envelope schema constrains `type` only to the family
(`dml|ddl|meta|snapshot`), so the envelope alone does not make branches
mutually exclusive. Each composed branch in the template therefore adds its
own `type` pattern; that is what satisfies B-WSA-59. The repository's
`opencdc-dml.schema.json` covers the DML family including TRUNCATE, and
`snapshot.READ` shares the DML data shape, so the `dml` branch is one branch
for that whole set. Publishers should confirm both points against the pinned
schema release they bundle (B-WSA-61).

## 7. Mapping from Existing Implementations

This section is informative. It records how the configuration surface of the
[Oracle GoldenGate Data Streams][ogg-ds] service (26ai, `ogg:dataStream`)
relates to this binding, from the published documentation. It is not an
implementation plan and does not assert that any option, as documented,
produces OpenCDC-conformant output: a native CloudEvents envelope around
GoldenGate records still requires the OpenCDC payload and metadata
transformation the core defines. Rows marked *documented* rest on the public
documentation only; a *verified* column will be added once captured sessions
are analysed (Section 8, open item 6).

"Fixed" means the binding requires a specific value; "excluded" means the
option produces a stream that is not OpenCDC-conformant and so cannot be
claimed; "out of scope" means a provisioning or producer-internal concern the
binding does not see; "converted" means a semantic mapping the producer must
implement, not a rename.

| Data Streams option                                          | Disposition                       | Note                                                                                                                                 |
| ------------------------------------------------------------ | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `cloudEventsFormat`                                          | Fixed `true`                      | `type`, `source`, `specversion`, and `cdc*` attributes must be the OpenCDC values, not placeholders (1)                              |
| `encoding: json`                                             | Carried                           | Core JSON encoding                                                                                                                   |
| `encoding.json.model: op`                                    | Fixed `op`                        | `row` model has no before-image and no operation type per row                                                                        |
| `beforeImageName` / `afterImageName`                         | Fixed to core names               | [core Section 5.2][core-5-2]                                                                                                         |
| `operationStrings`                                           | Excluded                          | Operation vocabulary is closed and carried in `type` ([core Section 3.2][core-3-2])                                                  |
| `flatten`, `flattenDelimiter`                                | Excluded                          | Changes image structure                                                                                                              |
| `omitNullColumnValues`                                       | Excluded                          | Conflates absent and null ([core Section 5.3][core-5-3])                                                                             |
| `treatAllColumnsAsStrings`                                   | Excluded                          | Type fidelity ([core Section 6.2][core-6-2])                                                                                         |
| `maxLobSize`, `maxDDLTextSize`, `maxRecordSize`              | Excluded only if they truncate or chunk | A size option is not itself nonconformant; truncation or chunking is (B-WSA-14). A limit that causes the endpoint to fail closed (`4013`) is conformant (2) |
| `truncateOrChunk`                                            | Excluded                          | B-WSA-14; rely on frame fragmentation                                                                                                |
| `includeStreamMetadataRecord`                                | Fixed `true`                      | B-WSA-23                                                                                                                             |
| `includeObjectMetadataRecords`                               | Fixed `true`                      | B-WSA-24; OBJECT_METADATA must carry the core's column descriptors and `json_schema` block ([core Section 4.2][core-4-2])            |
| `includeJSONSchemaRecord`                                    | Superseded                        | In-band JSON Schema records describing record shapes are not an OpenCDC event type. Static message schemas belong in the AsyncAPI document; runtime table schemas are OBJECT_METADATA (3) |
| `metaFields.*`                                               | Converted                         | `pos` to `cdcpos`/`pos{}`; `xid` to `cdcxid`; `opType` to `type`; `opTs` to `time`; `csn` to `pos.lsn`; `opSeqno` to `cdctxorder` after dense renumbering (B-WSA-53); `primaryKeys` to OBJECT_METADATA; table fields to [core Section 5.1][core-5-1]. Each is a semantic conversion whose scope, base, and replay stability must be verified (4) |
| `staticFields`, `tokens`, `env`, `sys`                       | Out of scope                      | Producer enrichment; must not collide with core field names ([core Section 2.4][core-2-4]) (5)                                       |
| `qualityOfService: atLeastOnce` / `exactlyOnce`              | Documented                        | Neither name establishes Durable Mode conformance; B-WSA-34 and B-WSA-52 are what must hold. `exactlyOnce` is not a wire claim (B-WSA-31) |
| `qualityOfService: atMostOnce`                               | Excluded in this revision         | Ephemeral Mode deferred (5.6)                                                                                                        |
| `rules[].filter.objectNames`                                 | Mapped                            | Defines the endpoint's captured tables (5.7)                                                                                         |
| `rules[].filter.operationTypes`                              | Mapped with constraint            | Excluding `DDL` requires `ddl_capture: "none"` (B-WSA-36) (6)                                                                        |
| `rules[].filter.columnValues`, `partitionNames`, `tags`, `userTokens` | Mapped with constraint   | Row-level filtering; dense ordinals and markers over the emitted set (B-WSA-53, B-WSA-39); predicate image documented (5.7)          |
| `bufferSize`                                                 | Mapped                            | Drives batch size; no semantics (B-WSA-10); a linger bound must be added (B-WSA-49)                                                  |
| `tcpKeepAliveTimeout`                                        | Mapped                            | Transport keep-alive, not HEARTBEAT (B-WSA-32)                                                                                       |
| `source.trail`, `source.path`                                | Out of scope                      | Provisioning                                                                                                                         |
| `begin=` query parameter                                     | Mapped to `position`              | Same four value forms (5.1); alias permitted; both present is HTTP 400                                                               |
| Hang on purged position                                      | Excluded                          | B-WSA-29 requires HTTP 409 before upgrade                                                                                            |
| `ws://user:pass@host` examples                               | Excluded                          | 1.5                                                                                                                                  |
| `asyncapi: '3.0.0'`, `protocol: ws`                          | Upgrade                           | 3.1.0 or later 3.x; `wss`                                                                                                            |
| No subprotocol negotiation                                   | Add                               | B-WSA-2, `opencdc.json`                                                                                                              |

Notes:

1. The published Data Streams CloudEvents example shows `specversion: "1.0"`,
   `type: com.example.someevent`, `source: /mycontext`. Under OpenCDC, `type`
   is the operation vocabulary value, `source` identifies the producer/stream,
   the `cdc*` extensions are mandatory, and `specversion` is whatever the
   core requires (Section 8, deferred core item 3). The `"1.0"` value is not
   in itself a defect.
2. A verbatim DDL statement the producer cannot carry in full is the open
   working-group item on truncated verbatim DDL (core change log item 23).
   Until resolved, an endpoint that truncates DDL text should declare
   `ddl_capture: "structural"` and carry the structural form.
3. Data Streams sends its schema records in-band, before data and metadata
   records. The difference from the core is one of kind, not placement: those
   records describe the *message* shape (a static fact, suited to the
   AsyncAPI `components/schemas`), whereas OBJECT_METADATA describes the
   *table* schema in effect (a runtime fact that changes with DDL).
4. In particular, whether `opSeqno` is 0- or 1-based, whether it is assigned
   before or after filtering, whether `opTs` is source commit time, and
   whether `pos` is stable across trail regeneration and replica failover
   (B-WSA-52) can only be settled against captured records.
5. GoldenGate user tokens commonly carry an application correlation id. The
   core's `correlation_id` field ([core Section 13.1][core-13-1]) is the
   conformant place for that case.
6. Filtering out `UPDATE` while keeping `INSERT` and `DELETE` is discussed
   under 5.7.

## 8. Decisions, Deferrals, and Open Items

### 8.1. Decisions Taken

By the project lead, September 2026:

- Bindings are separate documents that may contain MUSTs; each has its own
  register; the core is not changed for bindings at this stage.
- The AsyncAPI document is a MUST (Section 6); it takes the client
  perspective (B-WSA-42); the message unit is the batch (3.1).
- This binding reuses the CloudEvents JSON and batch formats but is not an
  application of the CloudEvents WebSockets binding (1.2, 1.4).
- Binding versions are `<wire>.<revision>` (1.7), adopted from the
  12 September review's F-01 as a refinement of lockstep versioning.
- From the 12 September review, accepted: W-01 (dense ordinals, B-WSA-53,
  B-WSA-39, B-WSA-54, B-WSA-51), W-02 in part (Durable-only profile,
  B-WSA-35 withdrawn, endpoint declarations normative), W-03 (replay boundary
  closure, B-WSA-25, B-WSA-56), W-04 (Section 6 contract, B-WSA-58 to
  B-WSA-61), W-06 (pre-upgrade rejection, B-WSA-45, B-WSA-57), W-07 in part
  (size and linger, B-WSA-15, B-WSA-49, B-WSA-50), W-08 in part (B-WSA-46 to
  B-WSA-48; browser clients out of scope), W-09 (relay re-batching,
  B-WSA-20; B-WSA-55), F-02 (composition reserved), F-04 (B-WSA-22, B-WSA-26
  reworded), F-05 (register rewritten by hand).
- Discarded from the review as out of place in a public binding: an
  implementation plan for GoldenGate in Section 7, phased work packages and
  owner roles, and generated-TOC tooling. The review's conformance scenario
  matrix is retained as the seed for a separate `conformance/` fixture list
  once the requirement text is stable.

### 8.2. Deferred Features

Not in this revision; each would be a document revision, not a wire change:

1. **Ephemeral Mode** (former B-WSA-35). Requires a core-approved loss signal;
   the 8.4.2 reset flags signal comparability, not loss.
2. **Browser clients.** Requires an authentication path that does not depend
   on request headers.
3. **Format composition** (Avro, Protocol Buffers over `opencdc.*` binary
   subprotocols).
4. **Client-to-server control messages** (B-WSA-27 reservation).

### 8.3. Deferred Core Items

Surfaced by this binding, deliberately not raised as core changes at this
stage. Recorded so they are not lost:

1. **T-ORDER and filtered streams.** No core change needed: dense renumbering
   (B-WSA-53) complies. Recorded for the avoidance of doubt.
2. **At-cursor schema re-emission on resume.** Amending core Section 4.5.2
   and R-POS-2 to permit session-scoped re-emission of the versions governing
   at the cursor would let `P = C` always (5.2 note) and remove the rewind
   cost. Until then this binding uses the closure.
3. **CloudEvents `specversion`.** The core and envelope schema require
   `"1.1"`; the referenced CloudEvents specification defines `"1.0"`. This
   binding inherits the core's value and makes no independent claim of
   CloudEvents-version compatibility until the core reconciles it.
4. **Operation-filtered streams.** Whether STREAM_METADATA should gain an
   advisory field (5.7 note).

### 8.4. Open Items

1. **Schema publication.** A pinned, immutable URL for the wire 0.3 schemas,
   so that `SCHEMA_BASE` in 6.5 can be replaced (B-WSA-61).
2. **Toolchain pin.** The parser, generator, and template versions recorded
   under B-WSA-58, once an endpoint has exercised them.
3. **Binding Framework section in the core**, which also resolves the core
   Section 12 title collision. Deferred with all core changes; this document
   already carries the framework's mandatory elements.
4. **Conformance fixtures.** Positive and negative wire examples linked to
   `B-WSA-*` identifiers, in a `conformance/` directory, seeded from the
   review's scenario matrix.
5. **Snapshot branch composition** in the template (6.5 note), to confirm
   against the pinned schema release.
6. **Validation against captured Data Streams sessions** with
   `cloudEventsFormat: true`, to add the *verified* column to Section 7.

## 9. References

- [OpenCDC][core] Open Change Data Capture Specification
- [OpenCDC envelope schema][core-envelope]
- [CloudEvents][ce] CloudEvents Specification
- [JSON Event Format][json-format] JSON Event Format for CloudEvents
- [WebSockets Protocol Binding for CloudEvents][ce-ws]
- [AsyncAPI][asyncapi] AsyncAPI Specification 3.1.0
- [AsyncAPI Multi-Format Schema Object][asyncapi-schema]
- [AsyncAPI WebSockets Bindings][asyncapi-ws]
- [RFC2119][rfc2119] Key words for use in RFCs to Indicate Requirement Levels
- [RFC3339][rfc3339] Date and Time on the Internet: Timestamps
- [RFC3986][rfc3986] Uniform Resource Identifier (URI): Generic Syntax
- [RFC6455][rfc6455] The WebSocket Protocol
- [Oracle GoldenGate Data Streams][ogg-ds] Components of Oracle GoldenGate Data Streams

[core]: ../../spec/OpenCDC-Specification.md
[core-envelope]: ../../schemas/opencdc-envelope.schema.json
[core-2-2a]: ../../spec/OpenCDC-Specification.md#22a-capability-axes-authority-and-legal-combinations
[core-2-4]: ../../spec/OpenCDC-Specification.md#24-closed-world-schemas
[core-3-2]: ../../spec/OpenCDC-Specification.md#32-operation-type-vocabulary
[core-3-3]: ../../spec/OpenCDC-Specification.md#33-cloudevents-extension-attributes-for-opencdc
[core-4]: ../../spec/OpenCDC-Specification.md#4-schema-delivery----object_metadata-and-producer-schema-modes
[core-4-2]: ../../spec/OpenCDC-Specification.md#42-object_metadata-event-structure
[core-4-5-2]: ../../spec/OpenCDC-Specification.md#452-schema-on-reconnect-optional-default-off
[core-5-1]: ../../spec/OpenCDC-Specification.md#51-table-identity
[core-5-2]: ../../spec/OpenCDC-Specification.md#52-common-dml-payload-fields
[core-5-3]: ../../spec/OpenCDC-Specification.md#53-value-encoding
[core-6-2]: ../../spec/OpenCDC-Specification.md#62-type-fidelity-obligations
[core-6-3]: ../../spec/OpenCDC-Specification.md#63-lob-handling-obligations
[core-6-4]: ../../spec/OpenCDC-Specification.md#64-consumer-reconnection-and-schema-availability
[core-8-2]: ../../spec/OpenCDC-Specification.md#82-replay-rules
[core-8-3]: ../../spec/OpenCDC-Specification.md#83-transaction-boundaries
[core-9]: ../../spec/OpenCDC-Specification.md#9-ddl-events
[core-10-1]: ../../spec/OpenCDC-Specification.md#101-heartbeat
[core-10-4]: ../../spec/OpenCDC-Specification.md#104-stream_metadata
[core-10-5-3]: ../../spec/OpenCDC-Specification.md#1053-payload
[core-12]: ../../spec/OpenCDC-Specification.md#12-transport-bindings
[core-13-1]: ../../spec/OpenCDC-Specification.md#131-trace-context
[core-14-1]: ../../spec/OpenCDC-Specification.md#141-producer-security-rules-normative
[core-14-2]: ../../spec/OpenCDC-Specification.md#142-security--privacy-considerations-informative
[core-15-1]: ../../spec/OpenCDC-Specification.md#151-durable-mode
[core-appendix-a]: ../../spec/OpenCDC-Specification.md#appendix-a-consumer-reference----reading-and-parsing-opencdc-streams-informative
[core-a1]: ../../spec/OpenCDC-Specification.md#a1-step-1----connect-and-read-stream_metadata-first
[core-a7]: ../../spec/OpenCDC-Specification.md#a7-step-7----replay-resume-and-sequence-continuity
[ce]: https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md
[ce-ws]: https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/websockets-protocol-binding.md
[json-format]: https://github.com/cloudevents/spec/blob/main/cloudevents/formats/json-format.md
[json-batch-format]: https://github.com/cloudevents/spec/blob/main/cloudevents/formats/json-format.md#4-json-batch-format
[asyncapi]: https://www.asyncapi.com/docs/reference/specification/v3.1.0
[asyncapi-schema]: https://www.asyncapi.com/docs/reference/specification/v3.1.0#multiFormatSchemaObject
[asyncapi-ws]: https://github.com/asyncapi/bindings/blob/master/websockets/README.md
[rfc2119]: https://tools.ietf.org/html/rfc2119
[rfc3339]: https://www.rfc-editor.org/rfc/rfc3339
[rfc3986]: https://www.rfc-editor.org/rfc/rfc3986
[rfc6455]: https://tools.ietf.org/html/rfc6455
[rfc6455-section-1-9]: https://tools.ietf.org/html/rfc6455#section-1.9
[rfc6455-section-4]: https://tools.ietf.org/html/rfc6455#section-4
[rfc6455-section-5-4]: https://tools.ietf.org/html/rfc6455#section-5.4
[rfc6455-section-5-5-1]: https://tools.ietf.org/html/rfc6455#section-5.5.1
[rfc6455-section-7-4-2]: https://tools.ietf.org/html/rfc6455#section-7.4.2
[ogg-ds]: https://docs.oracle.com/en/database/goldengate/core/26/coredoc/distribute-datastream-componentsofoggds.html
