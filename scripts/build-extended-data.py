"""Build agency-extended.json from psn-scrape/Extended/<slug>/meta.json + CSVs.

Each agency directory under Extended/ contains:
  - meta.json: agency-level metadata (title, badges, overview prose, list of sections)
  - <table>.csv: one CSV per tabular section (referenced from meta.json `csv` fields)

Cell markup: cells may contain {pill:variant:label} or {b:text} tokens which the
front-end renderer translates into spans/strong tags. Plain text is HTML-escaped
client-side.

Run from the site root: python scripts/build-extended-data.py
"""
import csv
import json
from pathlib import Path

SOURCE = Path("data/Extended").resolve()
OUTPUT = Path("agency-extended.json")


def read_csv(path: Path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = [row for row in reader]
    if not rows:
        return {"headers": [], "rows": []}
    return {"headers": rows[0], "rows": rows[1:]}


def resolve_section(section: dict, agency_dir: Path):
    """Replace `csv` references with parsed CSV data so the front-end gets ready-to-render rows."""
    s = dict(section)
    if "csv" in s:
        s["table"] = read_csv(agency_dir / s["csv"])
        del s["csv"]
    if "groups" in s:
        resolved_groups = []
        for g in s["groups"]:
            gg = dict(g)
            if "csv" in gg:
                gg["table"] = read_csv(agency_dir / gg["csv"])
                del gg["csv"]
            resolved_groups.append(gg)
        s["groups"] = resolved_groups
    return s


def main():
    if not SOURCE.exists():
        print(f"Source dir not found: {SOURCE}")
        return
    out = {"agencies": {}}
    for agency_dir in sorted(SOURCE.iterdir()):
        if not agency_dir.is_dir():
            continue
        meta_path = agency_dir / "meta.json"
        if not meta_path.exists():
            print(f"  skip {agency_dir.name}: no meta.json")
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            sections = [resolve_section(s, agency_dir) for s in meta.get("sections", [])]
        except Exception as exc:
            print(f"  WARNING: skip {agency_dir.name}: {exc}")
            continue
        out["agencies"][agency_dir.name] = {
            "title": meta.get("title", ""),
            "tag": meta.get("tag", ""),
            "badges": meta.get("badges", []),
            "overview": meta.get("overview", []),
            "sections": sections,
        }
        print(f"  {agency_dir.name}: {len(sections)} section(s)")
    OUTPUT.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"\nWrote {OUTPUT}: {OUTPUT.stat().st_size} bytes, {len(out['agencies'])} agency/ies")


if __name__ == "__main__":
    main()
