"""Build agency-data.json from D:/working-dir/psn-scrape CSVs.

Each agency entry includes name, slug, category, count, optional extendedHref,
a Font Awesome icon class, and the full TGID/Alias/Description rows.

Run from the site root: python scripts/build-agency-data.py
"""
import csv
import json
import re
from pathlib import Path

# Source CSVs ship with the site under data/ so the build is self-contained
# (the original D:\working-dir\psn-scrape\ folder is no longer required at deploy time).
PSN_SCRAPE = Path("data").resolve()

CAT_DISPLAY = {
    "Emergency-Services": "Emergency Services",
    "Enforcement": "Enforcement",
    "Transport": "Transport",
    "Utilities": "Utilities",
    "Miscellaneous": "Miscellaneous",
    "Aviation": "Aviation",
}

SLUG_OVERRIDE = {
    "Fire & Rescue NSW": "fire-and-rescue-nsw",
    "NSW Ambulance": "nsw-ambulance",
    "NSW Rural Fire Service": "nsw-rural-fire-service",
}

EXTENDED_HREF = {
    "fire-and-rescue-nsw": "fire-and-rescue.html",
    "nsw-ambulance": "ambulance.html",
    "nsw-rural-fire-service": "rural-fire-service.html",
}

# Per-agency Font Awesome 6 icon classes (free tier)
ICON_BY_SLUG = {
    # Emergency Services
    "fire-and-rescue-nsw": "fa-solid fa-fire-extinguisher",
    "marine-rescue-nsw": "fa-solid fa-life-ring",
    "nsw-ambulance": "fa-solid fa-truck-medical",
    "nsw-police-force": "fa-solid fa-shield-halved",
    "nsw-rural-fire-service": "fa-solid fa-fire",
    "nsw-state-emergency-service": "fa-solid fa-cloud-showers-heavy",
    "surf-life-saving-nsw": "fa-solid fa-water",
    "vra-rescue-nsw": "fa-solid fa-helmet-safety",

    # Enforcement
    "australian-federal-police": "fa-solid fa-shield",
    "corrective-services-nsw": "fa-solid fa-handcuffs",
    "nsw-department-of-primary-industries": "fa-solid fa-tractor",
    "nsw-environmental-protection-agency": "fa-solid fa-leaf",
    "office-of-the-sheriff-of-nsw": "fa-solid fa-gavel",
    "rspca-nsw": "fa-solid fa-paw",
    "special-constables": "fa-solid fa-user-shield",
    "youth-justice-nsw": "fa-solid fa-scale-balanced",

    # Transport
    "australian-rail-track-corporation": "fa-solid fa-train-track",
    "transport-for-nsw": "fa-solid fa-bus",
    "transport-for-nsw-ferries": "fa-solid fa-ship",
    "transport-for-nsw-maritime": "fa-solid fa-anchor",
    "transport-for-nsw-rail": "fa-solid fa-train-subway",
    "transport-for-nsw-roads": "fa-solid fa-road",

    # Utilities
    "ausgrid": "fa-solid fa-bolt",
    "blue-mountains-city-council": "fa-solid fa-building-columns",
    "central-tablelands-water": "fa-solid fa-droplet",
    "endeavour-energy": "fa-solid fa-plug",
    "forestry-corporation-of-nsw": "fa-solid fa-tree",
    "hunter-water": "fa-solid fa-faucet",
    "nsw-mining-and-rescue": "fa-solid fa-hammer",
    "nsw-national-parks-and-wildlife-service": "fa-solid fa-mountain",
    "nsw-telco-authority": "fa-solid fa-tower-cell",
    "property-nsw": "fa-solid fa-building",
    "sutherland-shire-council": "fa-solid fa-building-columns",
    "sydney-water": "fa-solid fa-droplet",
    "water-nsw": "fa-solid fa-water",
    "western-sydney-parklands": "fa-solid fa-tree-city",

    # Miscellaneous
    "abc-news": "fa-solid fa-newspaper",
    "act-government": "fa-solid fa-landmark",
    "hatzolah-medical-sydney": "fa-solid fa-star-of-david",
    "newborn-and-paediatric-emergency-transport-service-nets": "fa-solid fa-baby",
    "rental-groups": "fa-solid fa-walkie-talkie",
    "shared-liaison-groups": "fa-solid fa-people-arrows",
    "st-john-ambulance-nsw": "fa-solid fa-cross",
    "sydney-opera-house": "fa-solid fa-masks-theater",
    "unconfirmed-groups": "fa-solid fa-circle-question",

    # Aviation
    "nsw-aviation": "fa-solid fa-helicopter",
}

DEFAULT_ICON = "fa-solid fa-tower-broadcast"


def slugify(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[&]", "and", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


SKIP_DIRS = {"Extended", ".waze-browser-profile"}


def main():
    out = {"categories": [], "agencies": {}}
    for cat_dir in sorted(PSN_SCRAPE.iterdir()):
        if not cat_dir.is_dir() or cat_dir.name in SKIP_DIRS:
            continue
        cat = CAT_DISPLAY.get(cat_dir.name, cat_dir.name)
        cat_slug = slugify(cat)
        cat_entry = {"name": cat, "slug": cat_slug, "agencies": []}
        for csv_file in sorted(cat_dir.glob("*.csv")):
            name = csv_file.stem
            slug = SLUG_OVERRIDE.get(name, slugify(name))
            rows = []
            with open(csv_file, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    tgid = (row.get("TGID") or "").strip()
                    if not tgid:
                        continue
                    rows.append({
                        "tgid": tgid,
                        "alias": (row.get("Alias") or "").strip(),
                        "description": (row.get("Description") or "").strip(),
                    })
            icon = ICON_BY_SLUG.get(slug, DEFAULT_ICON)
            out["agencies"][slug] = {
                "name": name,
                "slug": slug,
                "category": cat,
                "count": len(rows),
                "icon": icon,
                "extendedHref": EXTENDED_HREF.get(slug),
                "tgids": rows,
            }
            cat_entry["agencies"].append({"name": name, "slug": slug, "icon": icon})
        out["categories"].append(cat_entry)

    target = Path("agency-data.json")
    target.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {target}: {target.stat().st_size} bytes, {len(out['agencies'])} agencies, {len(out['categories'])} categories")
    # warn about missing icon mappings
    missing = [s for s in out["agencies"] if out["agencies"][s]["icon"] == DEFAULT_ICON and s not in {sl for sl in ICON_BY_SLUG if ICON_BY_SLUG[sl] == DEFAULT_ICON}]
    if missing:
        print("Slugs using default icon:", missing)


if __name__ == "__main__":
    main()
