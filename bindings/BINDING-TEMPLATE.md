# <Transport> Protocol Binding for OpenCDC - Version <wire>.<rev>-wip

**Binding:** `<short-name>`
**Version:** <wire>.<rev>-wip (wire protocol <wire>, document revision <rev>; core specification v<x.y.z>)
**Profile in this revision:** <the one profile this revision defines>
**Requirement identifiers:** `B-<TAG>-*`, tracked in [requirements.yaml](requirements.yaml)
**Status:** Working draft

## Abstract

Two or three sentences: what the binding maps OpenCDC onto, and what it
reuses from CloudEvents versus adds.

## Table of Contents

Hand-written, with anchor links, listing every numbered heading.

## 1. Introduction

One paragraph naming the transport and the external specifications relied on,
with versions. The conformance claim string. Who the claiming party is
(endpoint, delivery layer, or both) and what that implies for C-COMP-1.
The sentence defining "core Section N" references.

### 1.1. Conformance

RFC 2119 keywords. Name the conformance parties (endpoint, client, relay or
delivery layer) and state that every requirement applies to the endpoint
unless it names another party. State that client rules are protocol
mechanics, not consumer processing guidance (core Appendix A). State that
requirement identifiers are stable once allocated and never reused.

### 1.2. Relation to <Transport> and CloudEvents

What this binding does not prescribe (provisioning, transfer settlement, and
so on). Which CloudEvents formats or bindings it reuses unchanged, and whether
it is or is not an application of the corresponding CloudEvents protocol
binding, with the reason. What OpenCDC adds on top.

### 1.3. Content Modes

Structured, binary, batch: which are supported and why.

### 1.4. <Handshake | Event Formats | Channel Layout>

The transport's connection or channel establishment rules. Subprotocol,
topic naming, header conventions as applicable. Include a raw example.

### 1.5. Security

The mechanism fixed for the core Section 14.2 floor.

### 1.6. Transport Capabilities

Table: capability, what the transport provides, consequence for the binding.
Mandatory; lets a near-miss transport identify the clause it fails.

### 1.7. Versioning and Compatibility

How the binding version relates to the wire version; how a client detects an
incompatible wire version; how unknown extension members and unknown event
members are treated.

## 2. Use of OpenCDC Attributes

Which OpenCDC attributes the binding fixes the use of on this transport
(`cdcpos`, `sequence`, `partitionkey`, `cdcxid`), and which it leaves alone.

## 3. <Transport> Message Mapping

Message unit, frame or record type, headers if any, event data encoding,
large events. Examples of a message in each supported content mode.

## 4. Stream Profile and Delivery Properties

### 4.1. Capability Axes

Table: each STREAM_METADATA axis, the value the binding fixes or the
constraint it applies, and why.

### 4.2. Delivery Properties

One requirement per chartered property: ordering preservation,
control-channel retention, schema retention across the replay window, marker
retention parity, intermediary integrity. Where not applicable, say so.

## 5. <Session | Delivery> Protocol

Session-aware transports: start position, first-event sequence and the
replay boundary (cursor versus actual replay start, with a multi-table
schema example), steady state, cursor validity and replay window, heartbeat,
backpressure, filtering and stream identity, failure behavior. Sessionless
transports: discovery of STREAM_METADATA and schema, retention, replay by
transport offset, marker delivery, failure behavior.

Failure behavior is mandatory in every binding: separate pre-establishment
rejection from post-establishment closure from abnormal loss; give a table
of codes or statuses with the retry disposition for each; state that no
close or disconnect is an acknowledgement of consumer progress.

## 6. <Description | Discovery>

What a claiming endpoint publishes so clients can find and use the stream
(AsyncAPI document, topic-naming convention plus control topic, manifest).
Any specification extension, and the rule that STREAM_METADATA remains the
authority for stream declarations.

## 7. Mapping from Existing Implementations

Informative. Per implementation used as a starting point: table of its
configuration surface with each option's disposition (fixed, mapped,
excluded, out of scope), with numbered notes.

## 8. Decisions, Deferrals, and Open Items

8.1 decisions taken (by whom, when, including review findings accepted and
discarded); 8.2 deferred features (each a document revision, not a wire
change); 8.3 deferred core items the binding surfaced but does not raise;
8.4 open items with what closes each.

## 9. References

Bulleted list of references, then all link definitions.

[core]: ../../spec/OpenCDC-Specification.md
