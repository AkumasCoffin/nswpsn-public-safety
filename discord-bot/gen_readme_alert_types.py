#!/usr/bin/env python3
"""Regenerate the Alert Types table in README.md from the catalog.

The table used to be typed by hand and described as the source of truth
alongside bot.py's dict — a tenth copy of a list that already had nine, and
it drifted like the rest. Run this after changing shared/alert-catalog.json.

    python gen_readme_alert_types.py            # rewrite the table
    python gen_readme_alert_types.py --check    # exit 1 if stale (for CI)
"""

import argparse
import io
import os
import sys

import alert_catalog

START = '<!-- BEGIN GENERATED ALERT TYPES -->'
END = '<!-- END GENERATED ALERT TYPES -->'
README = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'README.md')


def build_table() -> str:
    lines = [
        START,
        '',
        '_Generated from `shared/alert-catalog.json` by `gen_readme_alert_types.py`'
        ' — do not edit by hand._',
        '',
        '| Provider | Type | Agency | Description |',
        '|----------|------|--------|-------------|',
    ]
    for prov in alert_catalog.PROVIDERS:
        for t in alert_catalog.types_for_provider(prov['key']):
            note = ' _(not yet live)_' if t.get('soon') else ''
            lines.append('| %s | `%s` | %s | %s%s |' % (
                prov['label'], t['key'], t['agencyLabel'], t['botLabel'], note))
    lines += ['', END]
    return '\n'.join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--check', action='store_true',
                    help='exit 1 if the README is out of date')
    args = ap.parse_args()

    src = io.open(README, encoding='utf-8').read()
    table = build_table()

    if START in src and END in src:
        head = src[:src.index(START)]
        tail = src[src.index(END) + len(END):]
        new = head + table + tail
    else:
        # First run: replace the hand-written table under "## Alert Types".
        marker = '## Alert Types'
        if marker not in src:
            print('Could not find "## Alert Types" in README.md', file=sys.stderr)
            return 2
        head = src[:src.index(marker)] + marker + '\n\n'
        rest = src[src.index(marker) + len(marker):]
        nxt = rest.index('\n## ', 1)
        new = head + table + '\n' + rest[nxt:]

    if args.check:
        if new != src:
            print('README.md alert-type table is stale — run '
                  'python gen_readme_alert_types.py', file=sys.stderr)
            return 1
        print('README.md alert-type table is up to date.')
        return 0

    io.open(README, 'w', encoding='utf-8', newline='').write(new)
    print('Wrote %d alert types to README.md' % len(alert_catalog.TYPE_DEFS))
    return 0


if __name__ == '__main__':
    sys.exit(main())
