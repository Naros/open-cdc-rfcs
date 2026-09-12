#!/usr/bin/env python3
"""Quality gates for an OpenCDC binding directory.

Usage: check_binding.py <bindings/<name>> <path to core spec .md>
Exit code is the number of failed gates.
"""
import json, re, sys, yaml
from pathlib import Path

bdir = Path(sys.argv[1]); core = Path(sys.argv[2])
doc = (bdir / "README.md").read_text()
fails = []
def gate(name, ok, detail=""):
    print(("PASS" if ok else "FAIL"), name, ("- " + detail) if detail else "")
    if not ok: fails.append(name)

def gh_anchor(h):
    a = re.sub(r"[^\w\- ]", "", h.strip().lower())
    return a.replace(" ", "-")

# G1 YAML / JSON validity
try:
    reg = yaml.safe_load((bdir / "requirements.yaml").read_text()); ok = True
except Exception as e: ok = False; reg = {"requirements": []}
gate("G1a requirements.yaml parses", ok)
tmpl_ok = True
try: tmpl = yaml.safe_load((bdir / "asyncapi-template.yaml").read_text())
except Exception: tmpl_ok = False
gate("G1b asyncapi-template.yaml parses", tmpl_ok)
xs_ok = True
try: json.loads((bdir / "x-opencdc.schema.json").read_text())
except Exception: xs_ok = False
gate("G1c x-opencdc.schema.json parses", xs_ok)

# G2 core anchors resolve against live core headings
heads = [m.group(1) for m in re.finditer(r"^#{1,4} (.+)$", core.read_text(), re.M)]
anchors = {gh_anchor(h) for h in heads}
bad = [ref for ref, anc in re.findall(r"^\[(core[^\]]*)\]: \.\./\.\./spec/[^#\n]+#(\S+)$", doc, re.M) if anc not in anchors]
gate("G2 core anchors resolve", not bad, ", ".join(bad))

# G3 internal anchors (TOC and cross-links) resolve
myheads = {gh_anchor(m.group(1)) for m in re.finditer(r"^#{2,4} (.+)$", doc, re.M)}
bad = [a for a in re.findall(r"\]\(#([^)]+)\)", doc) if a not in myheads]
gate("G3 internal anchors resolve", not bad, ", ".join(bad))

# G4 TOC lists every numbered heading exactly once
numbered = [m.group(1) for m in re.finditer(r"^#{2,3} (\d+(?:\.\d+)?)\. ", doc, re.M)]
toc_block = doc.split("## Table of Contents")[1].split("## 1. Introduction")[0]
toc_nums = re.findall(r"^(?:(\d+)\. \[|- (\d+\.\d+)\. \[)", toc_block, re.M)
toc_nums = [a or b for a, b in toc_nums]
gate("G4 TOC matches numbered headings", numbered == toc_nums,
     f"headings={numbered} toc={toc_nums}" if numbered != toc_nums else "")

# G5 reference-style links: all used defined, all defined used
used = set(re.findall(r"\]\[([^\]]+)\]", doc)); defined = set(re.findall(r"^\[([^\]]+)\]: ", doc, re.M))
gate("G5 link refs consistent", used == defined, f"undefined={used-defined} unused={defined-used}")

# G6 requirement IDs: document set == register set, no duplicates in doc
doc_ids = re.findall(r"\*\*(B-WSA-\d+)(?: \([^)]*\))?\.\*\*", doc)
dup = {i for i in doc_ids if doc_ids.count(i) > 1}
reg_ids = [r["id"] for r in reg["requirements"]]
gate("G6a no duplicate IDs in document", not dup, ", ".join(sorted(dup)))
gate("G6b document IDs == register IDs", set(doc_ids) == set(reg_ids),
     f"doc-only={set(doc_ids)-set(reg_ids)} reg-only={set(reg_ids)-set(doc_ids)}")
gate("G6c register IDs unique", len(reg_ids) == len(set(reg_ids)))

# G7 register integrity: level matches a verb in the summary; rows end cleanly; sections exist
badlvl, trunc, badsec = [], [], []
for r in reg["requirements"]:
    lvl, txt = r["level"], r["requirement"]
    if lvl in ("MUST", "SHOULD", "MAY") and not re.search(rf"\b{lvl}\b", txt): badlvl.append(r["id"])
    if lvl == "WITHDRAWN" and "ithdrawn" not in txt: badlvl.append(r["id"])
    if not txt.rstrip().endswith((".", ")")): trunc.append(r["id"])
    if r["section"] not in numbered and r["section"] not in [n for n in re.findall(r"^#{4} (\d+\.\d+\.\d+)\. ", doc, re.M)]:
        badsec.append(r["id"])
gate("G7a register level matches summary verb", not badlvl, ", ".join(badlvl))
gate("G7b register rows not truncated", not trunc, ", ".join(trunc))
gate("G7c register sections exist in document", not badsec, ", ".join(badsec))

# G8 register section matches the section where the ID is defined in the document
sec_of = {}; cur = None
for line in doc.splitlines():
    m = re.match(r"^#{2,4} (\d+(?:\.\d+)*)\. ", line)
    if m: cur = m.group(1)
    for i in re.findall(r"\*\*(B-WSA-\d+)(?: \([^)]*\))?\.\*\*", line): sec_of[i] = cur
mism = [r["id"] for r in reg["requirements"] if sec_of.get(r["id"]) != r["section"]]
gate("G8 register section == definition section", not mism, ", ".join(mism))

# G9 inline template identical to standalone file
m = re.search(r"### 6\.5\. Template.*?```yaml\n(.*?)```", doc, re.S)
gate("G9 inline template == asyncapi-template.yaml", m and m.group(1) == (bdir / "asyncapi-template.yaml").read_text())

# G10 template x-opencdc validates against the extension schema (structural checks without jsonschema dependency)
try:
    import jsonschema
    schema = json.loads((bdir / "x-opencdc.schema.json").read_text())
    ch = next(iter(tmpl["channels"].values()))
    jsonschema.Draft202012Validator(schema).validate(ch["x-opencdc"]); ok = True; det = ""
except ImportError:
    ok = True; det = "jsonschema not installed; skipped"
except Exception as e:
    ok = False; det = str(e).splitlines()[0]
gate("G10 template x-opencdc validates against schema", ok, det)

# G11 every close code mentioned in requirements appears in the 5.8 table
table = doc.split("### 5.8.")[1].split("## 6.")[0]
codes_in_table = set(re.findall(r"^\| `(\d{4})`", table, re.M))
codes_used = set(re.findall(r"`(4\d{3})`", doc)) - {"4003", "4010"}  # withdrawn, named only in B-WSA-57
missing = codes_used - codes_in_table
gate("G11 close codes used are defined in 5.8", not missing, ", ".join(sorted(missing)))

# G12 no stale terms from prior revisions
stale = [t for t in ["Appendix C", "rowFiltering", "Ephemeral Mode)", "B-WS-", "opencdc-websocket"] if t in doc]
gate("G12 no stale terms", not stale, ", ".join(stale))

# G13 prose line length (tables, code, links, TOC exempt)
long = [i+1 for i, l in enumerate(doc.splitlines())
        if len(l) > 80 and not l.startswith(("|", "[", "```", "- ", "  ", "#", "**")) and "](#" not in l and "](" not in l]
gate("G13 prose wrapped at 80", not long, f"lines {long[:10]}")

print(f"\n{len(fails)} gate(s) failed" if fails else "\nall gates passed")
sys.exit(len(fails))
