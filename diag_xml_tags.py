"""
diag_xml_tags.py — tag census for the two Datatypes files: which tags exist,
their counts, and how many instances of each would YIELD fields (attributes or
text-bearing children). Explains why 4.2 parsed and 4.4 didn't.

Run on the Mac:  python3 diag_xml_tags.py
"""
import sys
from pathlib import Path
import xml.etree.ElementTree as ET

SRC = Path("/Volumes/raedsync/Documents/OmniVista/FIX Dict")
for name in ("Datatypes_FIX42", "Datatypes_FIX44"):
    hits = sorted(SRC.rglob(f"{name}*"))
    if not hits:
        print(f"{name}: not found")
        continue
    fp = hits[0]
    root = ET.parse(fp).getroot()
    stats = {}
    for el in root.iter():
        if el.tag == root.tag:
            continue
        n_fields = len(el.attrib) + sum(
            1 for c in el if c.text and c.text.strip())
        cnt, yld = stats.get(el.tag, (0, 0))
        stats[el.tag] = (cnt + 1, yld + (1 if n_fields else 0))
    print(f"\n{fp.name}  (root: {root.tag})")
    print(f"{'tag':<25} {'count':>6} {'yield':>6}")
    for tag, (cnt, yld) in sorted(stats.items(), key=lambda x: -x[1][0])[:12]:
        print(f"{tag:<25} {cnt:>6} {yld:>6}")
