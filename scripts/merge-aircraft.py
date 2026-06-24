"""Merge external aircraft.csv into psn-scrape/Extended/nsw-aviation/.

Reads tab-separated source at D:/working-dir/(radio-stuff)/aircraft.csv,
skips any registration that already exists in the current aviation CSVs,
groups the remainder by source category, and writes new CSVs under
nsw-aviation/. Country is derived from the registration prefix.

Run from the site root: python scripts/merge-aircraft.py
"""
import csv
import os
import re
from pathlib import Path

SOURCE = Path("../../(radio-stuff)/aircraft.csv").resolve()
AVIATION_DIR = Path("data/Extended/nsw-aviation").resolve()

# Registration-prefix → country, longest-match-first (multi-letter prefixes
# checked before single-letter ones). Subset of ICAO common prefixes.
PREFIX_TABLE = [
    # Multi-character first
    ("RDPL", "Laos"),
    ("VP-B", "Bermuda"),
    ("XA", "Mexico"), ("XB", "Mexico"), ("XC", "Mexico"),
    ("PP", "Brazil"), ("PR", "Brazil"), ("PS", "Brazil"), ("PT", "Brazil"), ("PU", "Brazil"),
    ("OE", "Austria"), ("OO", "Belgium"), ("OK", "Czech Republic"),
    ("OY", "Denmark"), ("OH", "Finland"),
    ("LY", "Lithuania"), ("LZ", "Bulgaria"), ("LN", "Norway"), ("LV", "Argentina"),
    ("CS", "Portugal"), ("CR", "Portugal (Azores)"),
    ("SP", "Poland"), ("SE", "Sweden"), ("SU", "Egypt"), ("ST", "Sudan"),
    ("EC", "Spain"), ("ET", "Ethiopia"),
    ("HB", "Switzerland"), ("HK", "Colombia"), ("HC", "Ecuador"),
    ("HP", "Panama"), ("HR", "Honduras"), ("HZ", "Saudi Arabia"),
    ("HI", "Dominican Republic"), ("HL", "South Korea"),
    ("TC", "Turkey"),
    ("3A", "Monaco"),
    ("CP", "Bolivia"), ("CC", "Chile"), ("CU", "Cuba"),
    ("OB", "Peru"),
    ("VH", "Australia"), ("VT", "India"), ("VP", "British Overseas Territories"),
    ("ZK", "New Zealand"),
    ("JY", "Jordan"), ("JA", "Japan"),
    ("4X", "Israel"),
    ("5A", "Libya"), ("5Y", "Kenya"), ("5X", "Uganda"),
    ("9V", "Singapore"), ("9M", "Malaysia"),
    ("A6", "United Arab Emirates"), ("A7", "Qatar"),
    ("AP", "Pakistan"),
    ("PK", "Indonesia"), ("P2", "Papua New Guinea"),
    ("UN", "Kazakhstan"), ("UR", "Ukraine"),
    ("RA", "Russia"), ("RP", "Philippines"),
    # Single-letter last
    ("N", "United States"),
    ("G", "United Kingdom"),
    ("F", "France"),
    ("D", "Germany"),
    ("I", "Italy"),
    ("B", "China"),
    ("C", "Canada"),
    ("M", "Isle of Man"),
    ("P", "North Korea"),
]


def country_for_reg(reg: str) -> str:
    if not reg:
        return ""
    r = reg.strip().upper()
    for prefix, country in PREFIX_TABLE:
        if not r.startswith(prefix):
            continue
        # Single-letter prefixes need a digit/dash after to avoid matching e.g.
        # "C-FXYZ" accidentally hitting a multi-letter country that happens to
        # share the first letter. Multi-char prefixes use plain startswith.
        if len(prefix) == 1:
            nxt = r[len(prefix):len(prefix) + 1]
            if not (nxt.isdigit() or nxt == "-"):
                continue
        return country
    return ""


# Map source category → filename used inside nsw-aviation/.
CATEGORY_FILE = {
    "RW - Type 1 Heavy": "rw-type-1-heavy.csv",
    "RW - Type 2 Medium": "rw-type-2-medium.csv",
    "RW - Type 3 Light": "rw-type-3-light.csv",
    "FW - Type 1 MEAT": "fw-type-1-meat.csv",
    "FW - Type 4 SEAT": "fw-type-4-seat.csv",
    "FW - Type 5 SEAT": "fw-type-5-seat.csv",
    "FW - AAS / Recce": "fw-aas-recce.csv",
    "FW - Other": "fw-other.csv",
    "Unspecified": "unspecified.csv",
    "Unmanned Aerial Vehicle": "uav.csv",
}


def existing_registrations() -> set:
    seen = set()
    for csv_file in AVIATION_DIR.glob("*.csv"):
        with open(csv_file, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            if not headers:
                continue
            # Find the Reg column (case-insensitive)
            reg_idx = None
            for i, h in enumerate(headers):
                if h.strip().lower() == "reg":
                    reg_idx = i
                    break
            if reg_idx is None:
                continue
            for row in reader:
                if reg_idx < len(row):
                    reg = row[reg_idx].strip().upper()
                    if reg:
                        seen.add(reg)
    return seen


def main():
    if not SOURCE.exists():
        print(f"Source not found: {SOURCE}")
        return
    seen = existing_registrations()
    print(f"Existing registrations across nsw-aviation/: {len(seen)}")

    # Bucket new entries by category
    buckets: dict[str, list[dict]] = {}
    skipped = 0
    new_count = 0
    with open(SOURCE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            reg = (row.get("registration") or "").strip()
            if not reg:
                continue
            if reg.upper() in seen:
                skipped += 1
                continue
            cat = (row.get("category") or "Unspecified").strip()
            entry = {
                "Reg": reg,
                "Callsign": (row.get("callsign") or "").strip(),
                "Country": country_for_reg(reg),
                "Make": (row.get("make") or "").strip(),
                "Model": (row.get("model") or "").strip(),
            }
            buckets.setdefault(cat, []).append(entry)
            seen.add(reg.upper())  # de-dupe within source too
            new_count += 1

    print(f"Skipped (already present): {skipped}")
    print(f"New entries to write: {new_count}")

    # Write each bucket. Keep existing files intact: if a bucket file already
    # exists, append the new rows; otherwise create with headers.
    HEADERS = ["Reg", "Callsign", "Country", "Make", "Model"]
    for cat, entries in buckets.items():
        fname = CATEGORY_FILE.get(cat, "fleet-" + re.sub(r"[^a-z0-9]+", "-", cat.lower()).strip("-") + ".csv")
        path = AVIATION_DIR / fname
        existing_rows = []
        existing_regs_in_file = set()
        if path.exists():
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    existing_rows.append(r)
                    if r.get("Reg"):
                        existing_regs_in_file.add(r["Reg"].strip().upper())
        appended = 0
        # Write atomically: a crash mid-write must not clobber the existing
        # file. Write to a temp file in the same dir, then os.replace() it
        # over the target (atomic on the same filesystem).
        tmp_path = path.with_name(path.name + ".tmp")
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS, extrasaction="ignore")
            writer.writeheader()
            for r in existing_rows:
                writer.writerow({h: r.get(h, "") for h in HEADERS})
            for e in entries:
                if e["Reg"].strip().upper() in existing_regs_in_file:
                    continue
                writer.writerow(e)
                appended += 1
        os.replace(tmp_path, path)
        print(f"  {fname}: +{appended} ({len(existing_rows)} kept)")


if __name__ == "__main__":
    main()
