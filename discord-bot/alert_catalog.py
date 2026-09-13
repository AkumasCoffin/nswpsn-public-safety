# -*- coding: utf-8 -*-
"""The alert-type catalog — loaded from shared/alert-catalog.json at runtime.

This is the same file the backend reads (backends/node/src/services/
alertCatalog.ts) and the same one the dashboard renders from, via
GET /api/dashboard/alert-catalog. One definition, three consumers.

It used to be nine hand-maintained copies across three languages — ALERT_TYPES
in bot.py, three maps in embeds.py, three ladders in alert_poller.py, the
backend whitelist, the dashboard's PROVIDERS literal and the README table —
with nothing checking them against each other. That is how Victoria's SES, EMV
and ESTA records came to be ingested but unalertable, and how the backend came
to reject keys the dashboard was offering.
"""

import json
import os
from typing import Any, Dict, List, Optional

_CATALOG_RELATIVE = os.path.join('shared', 'alert-catalog.json')


def _locate() -> str:
    """Walk up from this file looking for shared/alert-catalog.json."""
    override = os.environ.get('ALERT_CATALOG_PATH')
    if override:
        return os.path.abspath(override)

    here = os.path.dirname(os.path.abspath(__file__))
    for _ in range(8):
        candidate = os.path.join(here, _CATALOG_RELATIVE)
        if os.path.isfile(candidate):
            return candidate
        parent = os.path.dirname(here)
        if parent == here:
            break
        here = parent
    raise FileNotFoundError(
        'alert-catalog.json not found (looked for %s above %s). '
        'Set ALERT_CATALOG_PATH to override.'
        % (_CATALOG_RELATIVE, os.path.dirname(os.path.abspath(__file__)))
    )


def _load() -> Dict[str, Any]:
    with open(_locate(), encoding='utf-8') as fh:
        cat = json.load(fh)

    providers = {p['key'] for p in cat['providers']}
    seen = set()
    for t in cat['types']:
        if t['key'] in seen:
            raise ValueError('duplicate alert type key: %s' % t['key'])
        seen.add(t['key'])
        if t['provider'] not in providers:
            raise ValueError(
                'alert type %s names unknown provider %s' % (t['key'], t['provider']))
    return cat


CATALOG: Dict[str, Any] = _load()

PROVIDERS: List[Dict[str, Any]] = CATALOG['providers']
TYPE_DEFS: List[Dict[str, Any]] = CATALOG['types']
SEVERITY_SCALES: Dict[str, List[str]] = CATALOG['severityScales']

BY_KEY: Dict[str, Dict[str, Any]] = {t['key']: t for t in TYPE_DEFS}
PROVIDER_BY_KEY: Dict[str, Dict[str, Any]] = {p['key']: p for p in PROVIDERS}

# Retired key -> canonical key. Presets saved before a rename keep working.
ALIASES: Dict[str, str] = {
    a: t['key'] for t in TYPE_DEFS for a in t.get('aliases', [])
}

# key -> standalone label, the bot's ALERT_TYPES replacement.
LABELS: Dict[str, str] = {t['key']: t['botLabel'] for t in TYPE_DEFS}

# key -> endpoint, for every type that is polled generically. Types with a
# bespoke checker (user_incident, radio_summary) have endpoint null and are
# deliberately absent.
ENDPOINTS: Dict[str, str] = {
    t['key']: t['endpoint'] for t in TYPE_DEFS if t.get('endpoint')
}


# feedShape -> set of keys. The poller's id and timestamp ladders branch on the
# SHAPE a record arrives in, not on the key; deriving the key sets from the
# catalog is what stops a newly added agency silently missing a branch.
SHAPES: Dict[str, set] = {}
for _t in TYPE_DEFS:
    SHAPES.setdefault(_t['feedShape'], set()).add(_t['key'])


def shape(key: str) -> Optional[str]:
    t = type_def(key)
    return t['feedShape'] if t else None


def keys_with_shape(*names: str) -> set:
    """Every alert type whose records arrive in one of these shapes."""
    out = set()
    for n in names:
        out |= SHAPES.get(n, set())
    return out


def canonical(key: str) -> Optional[str]:
    """Fold a possibly-retired key to its canonical form, or None if unknown."""
    if key in BY_KEY:
        return key
    return ALIASES.get(key)


def accepted_keys(key: str) -> List[str]:
    """Every spelling a stored preset might use for this type.

    A preset saved before a rename still holds the old string, and the row is
    only rewritten when migrate_canonical_alert_types.py runs. Matching on the
    canonical key alone would silently drop those subscribers in the meantime.
    """
    canon = canonical(key)
    if not canon:
        return [key]
    t = BY_KEY[canon]
    return [canon] + list(t.get('aliases', []))


def type_def(key: str) -> Optional[Dict[str, Any]]:
    canon = canonical(key)
    return BY_KEY.get(canon) if canon else None


def label(key: str) -> str:
    t = type_def(key)
    return t['botLabel'] if t else key


def color(key: str) -> Optional[int]:
    """Embed accent as an int, parsed from the catalog's hex string."""
    t = type_def(key)
    if not t or not t.get('color'):
        return None
    return int(str(t['color']).lstrip('#'), 16)


def icon(key: str) -> str:
    t = type_def(key)
    return t.get('icon', '') if t else ''


def general_picker_types() -> Dict[str, str]:
    """Types offered in the generic setup picker and by "Enable All".

    Radio summary opts out via generalPicker:false — it has its own setup flow.
    `soon` types are excluded for the same reason the dashboard excludes them:
    The Wire has not launched, so it must not be enableable yet.
    """
    return {
        t['key']: t['botLabel']
        for t in TYPE_DEFS
        if t.get('generalPicker', True) and not t.get('soon')
    }


def types_for_provider(provider_key: str) -> List[Dict[str, Any]]:
    return [t for t in TYPE_DEFS if t['provider'] == provider_key]


def providers_with_types(include_soon: bool = False) -> List[Dict[str, Any]]:
    """Providers that have at least one selectable type, in catalog order."""
    out = []
    for p in PROVIDERS:
        types = [
            t for t in types_for_provider(p['key'])
            if (include_soon or not t.get('soon'))
            and t.get('generalPicker', True)
        ]
        if types:
            out.append({**p, 'types': types})
    return out


def _get_field(item: Any, path: str) -> Any:
    """Read a dotted path such as 'properties.agency' off a feed record."""
    cur = item
    for part in path.split('.'):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def matches(item: Any, type_key: str) -> bool:
    """Does this feed record belong to this alert type?

    Types that share an upstream feed are told apart by their discriminator —
    the five Victorian agencies off one VicEmergency feed, NT Fire & Rescue vs
    Bushfires NT, BOM land vs marine. A type with no discriminator owns its
    whole feed.
    """
    t = type_def(type_key)
    if not t:
        return False
    disc = t.get('discriminator')
    if not disc:
        return True

    value = _get_field(item, disc['field'])
    value = '' if value is None else str(value)
    if disc.get('caseInsensitive'):
        value = value.upper()

        def norm(x):
            return str(x).upper()
    else:
        def norm(x):
            return str(x)

    if 'equals' in disc:
        return value == norm(disc['equals'])
    if 'notEquals' in disc:
        return value != norm(disc['notEquals'])
    if 'in' in disc:
        return value in {norm(x) for x in disc['in']}
    return True


def items_from(data: Any, type_key: str) -> List[Any]:
    """Pull this type's records out of its endpoint's response body.

    itemsPath names the wrapping key ('features', 'warnings', 'outages'), is ''
    for a bare list, or a list of candidate keys where upstream is inconsistent
    about capitalisation. The discriminator then narrows a shared feed.
    """
    t = type_def(type_key)
    if not t:
        return []

    path = t.get('itemsPath', '')
    if not path:
        items = data if isinstance(data, list) else []
    elif isinstance(path, list):
        items = []
        for candidate in path:
            if isinstance(data, dict) and isinstance(data.get(candidate), list):
                items = data[candidate]
                break
    else:
        items = data.get(path) if isinstance(data, dict) else None
        if not isinstance(items, list):
            items = []

    if t.get('discriminator'):
        items = [i for i in items if matches(i, type_key)]
    return items
