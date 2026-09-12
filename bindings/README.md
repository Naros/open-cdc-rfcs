# OpenCDC Protocol Bindings

A **protocol binding** defines how an OpenCDC stream is carried over a specific
transport. The core specification is transport-neutral and contains no
binding rules; it defines the event model and the producer contract and
charters four delivery properties for bindings to govern
([core Section 12][core-12]): ordering preservation, control-channel
retention, schema and marker retention across the replay window, and
intermediary integrity.

Payload encodings are the province of **format bindings**, which live in the
sibling [`formats/`](../formats/) directory (mirroring CloudEvents'
`bindings/` and `formats/` split). A transport binding and a format binding
are orthogonal and compose; a single document combining both is not admitted.

Every protocol binding is:

- **Separate.** Its own document, in its own directory here, with its own
  requirements register.
- **Optional to implement, normative when claimed.** No binding is required
  for core conformance. An endpoint that claims a binding is bound by its
  MUSTs.
- **Versioned as `<wire>.<revision>`.** The first two components are the
  wire protocol version the binding maps (`0.3`); the third is the binding
  document's own revision, so a correction to one binding never forces a wire
  change. `-wip` marks a draft. This is CloudEvents' pattern: bindings are
  `1.0.3` while the envelope stays `1.0`.
- **Composable in the claim.** `OpenCDC 0.3 / Kafka Binding 0.3`,
  `OpenCDC 0.3 / WSS+AsyncAPI Binding 0.3`, or with a format binding,
  `OpenCDC 0.3 / Kafka Binding 0.3 / Avro Format 0.3`.
- **Non-weakening.** A binding never relaxes a core requirement and never
  redefines type semantics.

## Bindings

| Directory                                      | Binding                          | Identifier prefix | Status  |
| ---------------------------------------------- | -------------------------------- | ----------------- | ------- |
| [kafka/](kafka/)                               | Kafka Protocol Binding           | `B-KFK-*`         | Drafting  |
| [websocket-asyncapi/](websocket-asyncapi/)     | WSS + AsyncAPI Protocol Binding  | `B-WSA-*`         | 0.3.0-wip |

## Layout

```text
bindings/
  README.md                       this index
  BINDING-TEMPLATE.md             document skeleton every binding follows
  binding-formatting-decisions.md conventions, with the reasoning
  <name>/
    README.md                     the binding document (authoritative)
    requirements.yaml             binding-scoped requirements register
    <artifacts>                   optional: description templates, extension schemas
  conformance/                    (planned) positive and negative fixtures linked to requirement IDs
```

CloudEvents keeps bindings as flat files. OpenCDC uses one directory per
binding because each carries a register and, where relevant, description
templates; the document itself is always `README.md` so the directory renders
it on GitHub.

See [binding-formatting-decisions.md](binding-formatting-decisions.md) for the
conventions (naming, numbering, identifiers, links, versioning) and
[BINDING-TEMPLATE.md](BINDING-TEMPLATE.md) for the skeleton.

## Governance

Bindings are admitted per [GOVERNANCE.md](../GOVERNANCE.md). The acceptance
gate lives there, not here, so it can evolve without a binding revision.

[core-12]: ../spec/OpenCDC-Specification.md#12-transport-bindings
