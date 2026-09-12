# OpenCDC Binding Conventions

Decisions taken for the first two OpenCDC bindings (Kafka; WSS + AsyncAPI),
September 2026, so that every binding reads the same way. The model is the
CloudEvents `bindings/` directory; where OpenCDC departs from it, the reason
is given. The WSS + AsyncAPI binding is the worked example of all of this.

## 1. Where bindings live and how they relate to the core

- The core specification is transport-neutral and contains **no binding
  rules**. It gets one short Binding Framework section (the extension slot:
  what a binding is, the claim grammar, the non-weakening rule) and nothing
  else. Kafka and WebSocket appear in the core only as illustrative mentions
  in informative appendices.
- Each protocol binding is a **separate document** in its own directory:
  `bindings/<short-name>/README.md`. Format bindings (Avro, Protobuf) go in a
  sibling `formats/` directory, as CloudEvents does. Transport and format
  bindings are orthogonal; a combined "Avro on Kafka" document is not
  admitted.
- A binding is **optional to implement, normative when claimed**. It may and
  does contain MUSTs. Core conformance requires no binding.
- The binding references the core so an implementer can find the rule being
  realized. Write these as "core Section 8.2" or "core Appendix A.7", linked
  (see 5). Bare numbers ("5.2") refer to the binding document itself. State
  this convention once in the Introduction.

## 2. Naming and versioning

- **Title:** `<Transport> Protocol Binding for OpenCDC - Version <wire>-wip`,
  for example `Kafka Protocol Binding for OpenCDC - Version 0.3-wip`. This is
  CloudEvents' title form exactly.
- **Short name** (directory name, `binding` field): lower-case, hyphenated,
  carrying whatever distinguishes it from sibling bindings on the same
  transport: `kafka`, `websocket-asyncapi`.
- **Version is `<wire>.<revision>`.** The first two components are the wire
  protocol version the binding maps (currently `0.3`); the third is the
  binding document's own revision, starting at 0. `-wip` marks a draft and is
  dropped at release. A correction to one binding bumps its revision and
  never forces a wire change; a wire change produces a new `<wire>` for every
  binding. This is CloudEvents' pattern (bindings at `1.0.3`, envelope at
  `1.0`). The front matter also records the core document version tracked
  (`v0.7.0`), since document and wire versions differ.
- **Claim string** uses the wire component only:
  `OpenCDC 0.3 / Kafka Binding 0.3`. Composable with a format binding:
  `OpenCDC 0.3 / Kafka Binding 0.3 / Avro Format 0.3`. The `x-opencdc` (or
  equivalent) declaration carries the full `<wire>.<revision>`.
- **Profile line.** The front matter names the single profile the revision
  defines (for WSS: Durable, JSON batch, `opencdc.json`). Optional modes that
  are not fully specified are deferred, not half-specified.
- Versions are recorded in `registry/versions.yaml` under a `bindings:` key
  (see `versions-fragment.yaml`).

## 3. Document skeleton

Follow `BINDING-TEMPLATE.md`. The first three top-level sections and the last
are CloudEvents' skeleton verbatim; sections 4 to 8 are OpenCDC additions,
because OpenCDC bindings carry stream semantics that CloudEvents bindings do
not.

```text
Abstract
Table of Contents            hand-written, anchor links
1. Introduction
   1.1. Conformance           RFC 2119 keywords; client-mechanics vs consumer-guidance sentence
   1.2. Relation to <Transport> and CloudEvents
   1.3. Content Modes
   1.4. Handshake | Event Formats | Channel Layout   (with a raw example)
   1.5. Security
   1.6. Transport Capabilities                        (table; mandatory)
   1.7. Versioning and Compatibility
2. Use of OpenCDC Attributes
3. <Transport> Message Mapping
4. Stream Profile and Delivery Properties
   4.1. Capability Axes       table of STREAM_METADATA axis -> fixed value / constraint / why
   4.2. Delivery Properties   one requirement per chartered property
5. Session | Delivery Protocol             (must include a failure-behavior section)
6. Description | Discovery
7. Mapping from Existing Implementations   (informative)
8. Decisions, Deferrals, and Open Items
9. References
```

Rules within the skeleton:

- Section numbers carry a trailing dot in headings (`## 1. Introduction`,
  `### 1.1. Conformance`), as CloudEvents does. Start at 1, not 0.
- Section 1.2 must say explicitly which CloudEvents formats or bindings the
  document reuses unchanged, and whether the binding **is or is not an
  application of** the corresponding CloudEvents protocol binding, with the
  reason. (WSS + AsyncAPI is *not* an application of the CloudEvents
  WebSockets binding, because it uses the batch format for throughput; Kafka
  should state its own position on the CloudEvents Kafka binding, including
  binary versus structured mode.)
- Section 1.6 (transport capabilities table) and Section 4 (profile and the
  four chartered delivery properties) are mandatory in every binding. They
  are what let a near-miss transport find the clause it fails, and what
  connect the binding to core Section 12.
- Section 4.2 addresses all four chartered properties by name even when one
  is not applicable; say "not applicable" and why.
- Section 8 has four parts: decisions taken (who, when, and which review
  findings were accepted or discarded), deferred features, deferred core
  items (surfaced but not raised, since the core is frozen for bindings at
  this stage), and open items. Nothing in Section 8 changes the normative
  meaning of Sections 1 to 6.
- Failure behavior is mandatory (Section 5): separate pre-establishment
  rejection (HTTP status, broker error) from post-establishment closure from
  abnormal loss; tabulate codes with a retry disposition; state that no close
  is an acknowledgement of consumer progress.
- Format composition is **reserved** in a first revision: specify the JSON
  mapping completely and say that any other encoding will arrive as a
  revision naming the combination. Do not write forward-looking exceptions
  for formats that do not yet exist.

## 4. Requirements and the register

- Every normative rule is a bullet of the form
  `- **B-<TAG>-<n>.** <text>` or, for the chartered delivery properties,
  `- **B-<TAG>-<n> (<name>).** <text>`. `<TAG>` is a short upper-case tag
  unique across bindings, recorded in `bindings/README.md`: `KFK`, `WSA`.
  Number sequentially in allocation order; do not restart per section.
  **Identifiers are stable once allocated.** Never renumber to restore
  reading order; a later-added requirement takes the next free number
  wherever it sits in the document. A withdrawn requirement keeps its number,
  carries level `WITHDRAWN` in the register, and its number is never reused.
- CloudEvents does not number requirements. OpenCDC does, because the core
  register tooling depends on it. Keep the convention.
- Each binding has its own `requirements.yaml`, same shape as the core
  register (`meta`, then `requirements[]` of `id`, `level`, `requirement`,
  `who`, `section`). `section` is the binding-document section. `who` is
  `Endpoint`, `Client`, `Endpoint and Client`, or `Relay`. `level` is `MUST`,
  `SHOULD`, `MAY`, `N/A`, or `WITHDRAWN`.
- **Write register rows by hand.** Do not extract them from the document
  with a sentence splitter: colons and semicolons inside requirements
  truncate the summary and drop the normative verb. Each row is a one-line
  summary that carries the MUST/SHOULD/MAY of its source clause. The quality
  gate checks that every row's level matches a verb present in the summary.
  Binding registers are never merged into `registry/requirements.yaml`, and
  core identifiers (`P-*`, `R-*`, `S-*`, `C-*`) are never duplicated; cite
  them by ID in prose instead.
- Client-directed rules are protocol mechanics (what a valid request
  contains, what a client must be prepared to receive). Consumer *processing*
  guidance stays in core Appendix A; do not restate it.

## 5. Links and references

- Use **reference-style** Markdown links (`[text][ref]`) everywhere, with all
  link definitions collected at the end of the document after the References
  section. This is the CloudEvents house style and keeps the prose readable.
- Links into the core are **relative** and anchored:
  `[core-8-2]: ../../spec/OpenCDC-Specification.md#82-replay-rules`. GitHub
  anchors are the heading lower-cased, punctuation removed except hyphens,
  spaces to hyphens; a heading containing ` -- ` yields four hyphens. Verify
  anchors mechanically against the live spec headings before committing.
- External specifications are cited at section granularity where a specific
  clause is relied on (`[rfc6455-section-5-4]`), as CloudEvents does.
- Section 9 References is a bulleted list of the specifications cited, one
  line each, followed by the link definitions.

## 6. Prose and formatting

- Hard-wrap prose at roughly 80 columns. Tables, code blocks, link
  definitions, and TOC lines may run longer.
- Raw wire examples (handshake, record layout) go in `text` fenced blocks;
  YAML and JSON in their own fences.
- Informative notes are italic-led paragraphs (`*Note.* ...`) placed after
  the requirement they qualify. Notes addressed to the working group start
  `*Note for the working group.*`.
- Plain English. No marketing language, no transition filler, few em-dashes.

## 7. Things the Kafka binding should decide early

These are the places where the WSS + AsyncAPI binding had to make a call
that has no obvious Kafka analogue, listed so the two documents land with the
same shape:

1. Position of the binding relative to the CloudEvents Kafka binding
   (application of it, or reuse of formats only), and which content modes are
   supported.
2. How the four delivery properties are realized on Kafka: partition ordering,
   compacted control topic for STREAM_METADATA and schema history
   (R-POS-7), marker-topic retention versus data-topic retention (P-RET-1),
   and what counts as an intermediary.
3. Discovery: how a consumer finds STREAM_METADATA without a session (the
   binding's Section 6), and whether an AsyncAPI document is required, since
   AsyncAPI has a Kafka binding object too.
4. Which STREAM_METADATA axes the binding fixes (`session_aware: false` at
   minimum) and what a claiming producer may declare for `ordering_scope`
   given partition count.
5. The starting-point implementation for Section 7, if any.

Core Appendix B.4 (Kafka Consumption Guidance) already prefigures much of
this and was written to be lifted into the binding.
