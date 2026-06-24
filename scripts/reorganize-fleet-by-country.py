"""Re-organize the firefighting fleet CSVs from per-type into per-(country, type).

Reads the existing per-type CSVs in psn-scrape/Extended/nsw-aviation/, groups
rows by Country, and writes new files named `fleet-<country>-<type>.csv`.
Removes the original per-type files. Also writes an updated meta.json that
declares one collapsible grouped-table section per country with one sub-table
per type within that country.

Run from the site root: python scripts/reorganize-fleet-by-country.py
"""
import csv
import json
import re
from pathlib import Path
from collections import defaultdict

AVIATION_DIR = Path("data/Extended/nsw-aviation").resolve()

# Source per-type files → display label and emoji
TYPE_INFO = {
    "rw-type-1-heavy": ("Rotary-Wing — Type 1 (Heavy)", "🚁"),
    "rw-type-2-medium": ("Rotary-Wing — Type 2 (Medium)", "🚁"),
    "rw-type-3-light": ("Rotary-Wing — Type 3 (Light)", "🚁"),
    "fw-type-1-meat": ("Fixed-Wing — Type 1 (MEAT)", "🛩"),
    "fw-type-4-seat": ("Fixed-Wing — Type 4 (SEAT)", "🛩"),
    "fw-type-5-seat": ("Fixed-Wing — Type 5 (SEAT)", "🛩"),
    "fw-aas-recce": ("Fixed-Wing — AAS / Recce", "🛩"),
    "fw-other": ("Fixed-Wing — Other", "🛩"),
    "uav": ("Unmanned Aerial Vehicles", "🛸"),
    "unspecified": ("Unspecified", "❓"),
}

# Display order for countries on the page (others fall in alphabetically after).
COUNTRY_ORDER = [
    "Australia",
    "New Zealand",
    "United States",
    "Canada",
    "Papua New Guinea",
    "Philippines",
]
COUNTRY_FLAG = {
    "Australia": "🇦🇺",
    "New Zealand": "🇳🇿",
    "United States": "🇺🇸",
    "Canada": "🇨🇦",
    "Papua New Guinea": "🇵🇬",
    "Philippines": "🇵🇭",
}


def country_slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "unknown"


def main():
    # Read all source per-type CSVs and group their rows by country.
    by_country_type = defaultdict(lambda: defaultdict(list))  # country → type_key → [row, ...]
    headers_seen = None
    source_files = []
    for type_key in TYPE_INFO:
        src = AVIATION_DIR / f"{type_key}.csv"
        if not src.exists():
            continue
        source_files.append(src)
        with open(src, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if headers_seen is None:
                headers_seen = reader.fieldnames
            for row in reader:
                country = (row.get("Country") or "").strip() or "Other"
                by_country_type[country][type_key].append(row)

    headers = headers_seen or ["Reg", "Callsign", "Country", "Make", "Model"]

    # Read+parse meta.json up-front, before deleting anything: if it's
    # missing or malformed we want to fail *before* unlinking the originals.
    meta_path = AVIATION_DIR / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    # Write per-(country, type) CSVs.
    written = 0
    for country, by_type in by_country_type.items():
        cs = country_slug(country)
        for type_key, rows in by_type.items():
            out = AVIATION_DIR / f"fleet-{cs}-{type_key}.csv"
            with open(out, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
                writer.writeheader()
                for r in rows:
                    writer.writerow({h: r.get(h, "") for h in headers})
            written += 1

    # Build the country sections list for meta.json (preserve curated order).
    ordered_countries = []
    for c in COUNTRY_ORDER:
        if c in by_country_type:
            ordered_countries.append(c)
    for c in sorted(by_country_type.keys()):
        if c not in ordered_countries:
            ordered_countries.append(c)

    # Keep operator-based sections (they don't have a `csv` matching fleet-* prefix).
    kept = []
    for s in meta.get("sections", []):
        csv_ref = s.get("csv") or ""
        # Drop any old per-type fleet sections (rw-type-*, fw-type-*, fw-aas-recce, fw-other, uav, unspecified)
        if any(csv_ref.startswith(k + ".csv") or csv_ref == k + ".csv" for k in TYPE_INFO):
            continue
        kept.append(s)

    fleet_sections = []
    for country in ordered_countries:
        types_present = list(by_country_type[country].keys())
        # Order sub-tables by canonical TYPE_INFO ordering
        ordered_types = [k for k in TYPE_INFO if k in types_present]
        groups = []
        for type_key in ordered_types:
            label, emoji = TYPE_INFO[type_key]
            groups.append({
                "heading": f"{emoji} {label}",
                "csv": f"fleet-{country_slug(country)}-{type_key}.csv",
            })
        flag = COUNTRY_FLAG.get(country, "🌐")
        total = sum(len(by_country_type[country][t]) for t in types_present)
        fleet_sections.append({
            "type": "grouped-table",
            "title": f"{flag} {country}",
            "tag": f"{total} aircraft",
            "wide": True,
            "collapsible": True,
            "open": False,   # all country cards start collapsed
            "intro": f"Firefighting fleet aircraft registered in {country}.",
            "table_class": "aircraft-table",
            "groups": groups,
        })

    meta["sections"] = kept + fleet_sections
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # Only now that all new CSVs *and* the new meta.json are on disk is it
    # safe to remove the old per-type files.
    for src in source_files:
        src.unlink()

    print(f"Wrote {written} per-(country, type) CSVs.")
    print(f"Removed {len(source_files)} old per-type CSVs.")
    print(f"Countries: {ordered_countries}")
    print(f"Total sections: {len(meta['sections'])} ({len(kept)} operator + {len(fleet_sections)} country)")


if __name__ == "__main__":
    main()
