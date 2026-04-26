#!/usr/bin/env python3
"""
Migrate alert_presets.alert_types to the canonical, singular alert_type names.

WHEN TO RUN:
    Run this once after deploying the bot with the renamed alert_type
    strings (see ALERT_TYPES in bot.py). Existing presets will still
    carry the old keys ('waze_hazards', 'traffic_incidents',
    'traffic_major', 'power_ausgrid', 'user_incidents', 'bom',
    'power_endeavour'); this script rewrites each row's alert_types
    array to the new canonical form.

IDEMPOTENT:
    Re-running on already-canonical data is a no-op — the renames map
    only matches the legacy keys, and dedup is applied at write time so
    nothing duplicates if a row was already migrated.

USAGE:
    python migrate_canonical_alert_types.py            # apply migration
    python migrate_canonical_alert_types.py --dry-run  # preview only

REQUIRES:
    psycopg2-binary, python-dotenv. BOT_DATABASE_URL must be set in
    discord-bot/.env (or in the environment) — same value the bot uses.
"""

import argparse
import os
import sys
from typing import List

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("psycopg2 is required. pip install psycopg2-binary", file=sys.stderr)
    sys.exit(2)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv is optional — env-via-shell still works.
    pass


# Map legacy -> list of canonical replacements.
# - Renames map old key -> [new_key]
# - Splits map old key -> [new_key_a, new_key_b]
# Order in the lists is irrelevant; the migrator dedupes per-row.
RENAMES = {
    'waze_hazards':      ['waze_hazard', 'waze_jam'],
    'traffic_incidents': ['traffic_incident'],
    'traffic_major':     ['traffic_majorevent'],
    'bom':               ['bom_land', 'bom_marine'],
    'power_endeavour':   ['endeavour_current', 'endeavour_planned'],
    'power_ausgrid':     ['ausgrid'],
    'user_incidents':    ['user_incident'],
}


def expand_alert_types(types: List[str]) -> List[str]:
    """Apply RENAMES to a single row's alert_types array, dedupe, and
    preserve a stable order: keys not in RENAMES first (in original order),
    then expanded keys (also dedup'd, in the order they were produced).
    """
    if not types:
        return []
    seen = set()
    out: List[str] = []
    for t in types:
        if t in RENAMES:
            for new_t in RENAMES[t]:
                if new_t not in seen:
                    seen.add(new_t)
                    out.append(new_t)
        else:
            if t not in seen:
                seen.add(t)
                out.append(t)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview rewrites without committing')
    args = parser.parse_args()

    db_url = os.environ.get('BOT_DATABASE_URL')
    if not db_url:
        print("BOT_DATABASE_URL is not set. Add it to discord-bot/.env or "
              "export it in your shell.", file=sys.stderr)
        return 2

    try:
        conn = psycopg2.connect(db_url)
    except Exception as exc:
        print(f"Failed to connect to Postgres: {exc}", file=sys.stderr)
        return 2

    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, guild_id, channel_id, name, alert_types "
                "FROM alert_presets ORDER BY id"
            )
            rows = cur.fetchall()

        total_rows = len(rows)
        changed_rows = 0
        before_total = 0
        after_total = 0
        legacy_hits: dict = {}

        with conn.cursor() as cur_w:
            for row in rows:
                old = list(row.get('alert_types') or [])
                new = expand_alert_types(old)
                before_total += len(old)
                after_total += len(new)
                for t in old:
                    if t in RENAMES:
                        legacy_hits[t] = legacy_hits.get(t, 0) + 1
                if old != new:
                    changed_rows += 1
                    print(
                        f"preset id={row['id']} guild={row['guild_id']} "
                        f"channel={row['channel_id']} name={row.get('name')!r}"
                    )
                    print(f"   before: {old}")
                    print(f"   after : {new}")
                    if not args.dry_run:
                        cur_w.execute(
                            "UPDATE alert_presets "
                            "SET alert_types = %s, updated_at = now() "
                            "WHERE id = %s",
                            (new, row['id']),
                        )

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print()
    print("Summary")
    print("-------")
    print(f"presets scanned        : {total_rows}")
    print(f"presets that changed   : {changed_rows}")
    print(f"alert_types before     : {before_total}")
    print(f"alert_types after      : {after_total}")
    if legacy_hits:
        print("legacy keys encountered:")
        for k in sorted(legacy_hits):
            print(f"   {k:24s} {legacy_hits[k]}")
    else:
        print("legacy keys encountered: none (already canonical)")
    if args.dry_run:
        print()
        print("DRY RUN — no rows were modified.")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
