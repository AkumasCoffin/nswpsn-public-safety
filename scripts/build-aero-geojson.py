# -*- coding: utf-8 -*-
"""Turn openAIP exports into something a web map can actually load.

The raw exports are 11.3 MB across three files, most of it fields a map has no
use for (`_id`, `createdBy`, `updatedAt`, `dataIngestion`) and coordinates at
14 decimal places — roughly a nanometre, for airspace boundaries drawn as a
line a few pixels wide.

This keeps the fields the map shows, rounds coordinates to a resolution that
survives any zoom the map offers, and writes minified JSON.

THE NUMERIC ENUMS WERE VERIFIED AGAINST THE DATA, not assumed, because a
mislabelled airspace class on an aviation map is worse than no label:

  icaoClass  0/2/3/4/6 carry names reading "CTA A", "CTA C", "CTA D", "CTA E"
             and "UNCR" respectively, so A/C/D/E/G. 8 is everything with no
             class — the restricted and danger areas.
  airspace   1 is every "R###" name, 2 every "D###", 4 every "CONTROL ZONE",
    type     26 every "CTA", 0 the "EFREQUENCY" sectors.
  altitude   unit 6 appears only with values like 245/180 (flight levels),
             unit 1 with 1500/2500/4500 (feet) and with 0 at datum 0 (ground).
  navaid     type 2 is the only one on kHz — NDBs. 1 and 5 land exclusively on
    type     military fields (Amberley, Darwin, Nowra, Tindal), i.e. TACAN and
             VORTAC. 3 has no channel, 4 does: VOR and VOR/DME.
  runway     length unit 0 is metres — Sydney 16R/34L comes out 3962, which is
    length   its published length in metres.
  traffic    YBBN carries [0] and openAIP shows it as VFR; YSSY carries [0, 1].
    type     So 0 is VFR and 1 is IFR.
  navaid     the only unit present is 2, so it cannot be cross-checked against
    range    a second one, but the values (20-170, clustering on 50/90/100) are
             a service volume in nautical miles; in kilometres they would be
             implausibly short for a VOR.

Airport frequency TYPE is not mapped at all, and deliberately. Each frequency
carries its own `name` — "TOWER RWY 16L/34R", "CTAF", "BRISBANE CENTRE" — which
is both more specific than any enum label and already written the way a pilot
would say it. The type is used only to ORDER them, so that the handful shown on
a busy field are the ones you would actually want: CTAF and tower before the
fuel company's apron frequency.

Runway SURFACE is dropped. mainComposite 0 is asphalt (Sydney), but 22 accounts
for 2016 of 4240 runways and nothing in the data pins it down; a wrong surface
under a real runway is the sort of error this file must not make.

Airport `type` was left unlabelled at first because 3 and 9 both hold
capital-city airports (Sydney is 3, Brisbane is 9). openAIP's own site supplied
the distinction — 3 "International Airport", 9 "Airport resp. Airfield IFR",
2 "Airfield Civil" — and the rest of the enum then agreed with the data on
every code present:

  0  the joint civil/military fields (Curtin, Darwin, Townsville, Wagga)
  1  gliding sites (Bunyan, Bathurst Pipersfield)
  2  1655 ordinary civil airfields, Archerfield among them
  5  Amberley, Richmond, Williamtown, East Sale — RAAF
  7  helipads: Batman Park, and the Bass Strait platforms
  8  Katoomba and Yagga Yagga, both closed
  10 Rose Bay, Elizabeth Quay, Melville — water
  11 farm strips (Midway Farm Stall, Whorouly)

    python scripts/build-aero-geojson.py <src-dir>
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(os.path.dirname(HERE), 'assets', 'geo')

# Airspace boundaries are drawn as a line; 4dp is ~11m, far finer than the
# stroke. Points get 5dp (~1m) so an airport sits on its runway.
ASP_DP = 4
PT_DP = 5

ICAO_CLASS = {0: 'A', 1: 'B', 2: 'C', 3: 'D', 4: 'E', 5: 'F', 6: 'G'}
ASP_TYPE = {
    0: 'Radio frequency sector',
    1: 'Restricted',
    2: 'Danger',
    4: 'Control zone',
    6: 'Aerodrome (certified)',
    13: 'Aerodrome (uncertified)',
    26: 'Control area',
}
NAV_TYPE = {0: 'DME', 1: 'TACAN', 2: 'NDB', 3: 'VOR', 4: 'VOR/DME', 5: 'VORTAC'}
TRAFFIC_TYPE = {0: 'VFR', 1: 'IFR'}

# Which services matter on a crowded field, best first. Everything unlisted
# sorts after these, in the order the source gives — area, met and fuel
# frequencies, which are the ones worth losing to the cap.
FREQ_RANK = {
    4: 0,    # CTAF — the only frequency at most of these aerodromes
    14: 1,   # tower
    9: 2,    # ground
    15: 3,   # ATIS / AWIS
    24: 4,   # AWIS
    0: 5,    # approach
    6: 6,    # departure
    5: 7,    # clearance delivery
    13: 8,   # radar
    3: 9,    # centre
    20: 10,  # pilot-activated lighting
}
MAX_FREQS = 6

# Long official names carry the field's own name and a runway list; on a map
# label that is noise around the one token you are looking for.
FREQ_TAGS = (
    ('CTAF', 'CTAF'), ('TOWER', 'TWR'), ('GROUND', 'GND'), ('SMC', 'GND'),
    ('GND', 'GND'), ('ATIS', 'ATIS'), ('AWIS', 'AWIS'), ('APP', 'APP'),
    ('DEP', 'DEP'), ('CENTRE', 'CTR'), ('CENTER', 'CTR'), ('RADAR', 'RADAR'),
    ('DELIVERY', 'DEL'), ('CLNC', 'DEL'), ('CLEARANCE', 'DEL'),
    ('UNICOM', 'UNICOM'), ('INFORMATION', 'INFO'), ('PAL', 'PAL'),
    ('RAMP', 'RAMP'), ('TRAFFIC', 'TRAFFIC'), ('RADIO', 'RADIO'),
)


def freq_tag(name):
    """Shorten a frequency's name to the service it is."""
    up = (name or '').upper()
    for needle, tag in FREQ_TAGS:
        if needle in up:
            return tag
    # Nothing recognised: first word, which is usually the operator's name.
    first = up.split()
    return first[0][:12] if first else ''


def frequencies(raw):
    """[tag, MHz] pairs, ranked, deduplicated, capped."""
    rows = []
    for i, f in enumerate(raw or []):
        val = (f.get('value') or '').strip()
        if not val:
            continue
        # Every airport frequency in the export is unit 2 (MHz); anything
        # else would need its own label, so it is skipped not mislabelled.
        if f.get('unit') != 2:
            continue
        rank = FREQ_RANK.get(f.get('type'), 50)
        # A field's primary frequency comes first within its service.
        rows.append((rank, 0 if f.get('primary') else 1, i,
                     freq_tag(f.get('name')), val))
    rows.sort()
    out, seen = [], set()
    for _, _, _, tag, val in rows:
        key = (tag, val)
        if key in seen:
            continue
        seen.add(key)
        out.append([tag, val])
        if len(out) >= MAX_FREQS:
            break
    return out


def runways(raw):
    """[designator, length-in-metres], longest first."""
    rows = []
    for r in raw or []:
        d = (r.get('designator') or '').strip()
        if not d:
            continue
        dim = ((r.get('dimension') or {}).get('length') or {})
        ln = dim.get('value') if dim.get('unit') == 0 else None
        rows.append((-(ln or 0), d, ln))
    rows.sort()
    out, seen = [], set()
    for _, d, ln in rows:
        # 16R and 34L are the two ends of one strip; the reciprocal adds
        # nothing but length.
        head = d[:2]
        recip = str((int(head) + 17) % 36 + 1).zfill(2) if head.isdigit() else None
        if recip and any(k.startswith(recip) for k in seen):
            continue
        seen.add(d)
        out.append([d, ln] if ln else [d])
        if len(out) >= 4:
            break
    return out
APT_KIND = {
    0: 'Airport (civil/military)',
    1: 'Glider site',
    2: 'Civil airfield',
    3: 'International airport',
    4: 'Heliport (military)',
    5: 'Military aerodrome',
    6: 'Ultralight strip',
    7: 'Heliport',
    8: 'Closed',
    9: 'Airport (IFR)',
    10: 'Water aerodrome',
    11: 'Landing strip',
    12: 'Agricultural strip',
    13: 'Altiport',
}
# Drawn larger: the ones an airliner uses.
APT_MAJOR = {0, 3, 9}
APT_MIL = {4, 5}


def limit(v):
    """An altitude limit as it is written on a chart."""
    if not isinstance(v, dict):
        return None
    val, unit, datum = v.get('value'), v.get('unit'), v.get('referenceDatum')
    if val is None:
        return None
    if unit == 6:
        return 'FL%g' % val
    if datum == 0:
        return 'GND' if not val else '%g ft AGL' % val
    return '%g ft AMSL' % val


# Most airspace is a circle written out point by point — 950 of the 1911
# features carry 100-199 vertices, and the largest carries 1323. Douglas-Peucker
# drops the ones that sit on a line their neighbours already describe.
#
# 0.0005 degrees is ~55m. For a 5 NM aerodrome circle that leaves roughly 28
# segments, which still reads as a circle at any zoom the map offers; a coarser
# tolerance starts showing corners.
SIMPLIFY_DEG = 0.0005


def _perp(p, a, b):
    """Distance from p to the segment ab, in degrees."""
    (px, py), (ax, ay), (bx, by) = p[:2], a[:2], b[:2]
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return ((px - ax) ** 2 + (py - ay) ** 2) ** 0.5
    t = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    return ((px - (ax + t * dx)) ** 2 + (py - (ay + t * dy)) ** 2) ** 0.5


def simplify(pts, tol):
    if len(pts) < 3:
        return pts
    keep = [False] * len(pts)
    keep[0] = keep[-1] = True
    stack = [(0, len(pts) - 1)]
    while stack:
        lo, hi = stack.pop()
        worst, idx = 0.0, -1
        for i in range(lo + 1, hi):
            d = _perp(pts[i], pts[lo], pts[hi])
            if d > worst:
                worst, idx = d, i
        if idx >= 0 and worst > tol:
            keep[idx] = True
            stack.append((lo, idx))
            stack.append((idx, hi))
    return [p for p, k in zip(pts, keep) if k]


def simplify_ring(ring, tol):
    """Simplify a closed ring, keeping it closed and never below a triangle."""
    if len(ring) < 5:
        return ring
    out = simplify(ring, tol)
    if len(out) < 4:
        return ring
    if out[0] != out[-1]:
        out.append(out[0])
    return out


def round_coords(c, dp):
    if isinstance(c, list):
        if c and isinstance(c[0], (int, float)):
            return [round(x, dp) for x in c]
        return [round_coords(x, dp) for x in c]
    return c


def fc(features):
    return {'type': 'FeatureCollection', 'features': features}


def write(name, features):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(fc(features), f, ensure_ascii=False, separators=(',', ':'))
    return path, os.path.getsize(path)


def build(src):
    def load(n):
        with open(os.path.join(src, n), encoding='utf-8') as f:
            return json.load(f)['features']

    # ---- airspace ----
    out = []
    for x in load('au_asp.geojson'):
        p = x['properties']
        out.append({
            'type': 'Feature',
            'geometry': {'type': x['geometry']['type'],
                         'coordinates': [simplify_ring(r, SIMPLIFY_DEG) for r in
                                         round_coords(x['geometry']['coordinates'], ASP_DP)]},
            'properties': {
                'n': p.get('name'),
                'c': ICAO_CLASS.get(p.get('icaoClass')),
                't': ASP_TYPE.get(p.get('type')),
                'u': limit(p.get('upperLimit')),
                'l': limit(p.get('lowerLimit')),
            },
        })
    asp = write('au-airspace.geojson', out)

    # ---- airports ----
    out = []
    for x in load('au_apt.geojson'):
        p = x['properties']
        el = (p.get('elevation') or {}).get('value')
        out.append({
            'type': 'Feature',
            'geometry': {'type': 'Point',
                         'coordinates': round_coords(x['geometry']['coordinates'], PT_DP)},
            'properties': {
                'n': p.get('name'),
                'i': p.get('icaoCode'),
                'a': p.get('iataCode'),
                'k': APT_KIND.get(p.get('type')),
                'maj': 1 if p.get('type') in APT_MAJOR else 0,
                'mil': 1 if p.get('type') in APT_MIL else 0,
                'tt': [TRAFFIC_TYPE[t] for t in (p.get('trafficType') or [])
                       if t in TRAFFIC_TYPE] or None,
                'f': frequencies(p.get('frequencies')) or None,
                'r': runways(p.get('runways')) or None,
                'e': round(el) if isinstance(el, (int, float)) else None,
                'pr': 1 if p.get('private') else 0,
                'ppr': 1 if p.get('ppr') else 0,
            },
        })
    apt = write('au-airports.geojson', out)

    # ---- navaids ----
    out = []
    for x in load('au_nav.geojson'):
        p = x['properties']
        el = (p.get('elevation') or {}).get('value')
        f = p.get('frequency') or {}
        val, unit = f.get('value'), f.get('unit')
        out.append({
            'type': 'Feature',
            'geometry': {'type': 'Point',
                         'coordinates': round_coords(x['geometry']['coordinates'], PT_DP)},
            'properties': {
                'n': p.get('name'),
                'id': p.get('identifier'),
                't': NAV_TYPE.get(p.get('type')),
                # unit 1 is kHz (NDBs), 2 is MHz.
                'f': ('%s %s' % (val, 'kHz' if unit == 1 else 'MHz')) if val else None,
                'ch': p.get('channel'),
                # Service volume, in nautical miles — see the header.
                'rng': (p.get('range') or {}).get('value'),
                'e': round(el) if isinstance(el, (int, float)) else None,
            },
        })
    nav = write('au-navaids.geojson', out)

    total_in = sum(os.path.getsize(os.path.join(src, n)) for n in
                   ('au_asp.geojson', 'au_apt.geojson', 'au_nav.geojson'))
    total_out = asp[1] + apt[1] + nav[1]
    for path, size in (asp, apt, nav):
        print('%-46s %7.0f KB' % (os.path.relpath(path, os.path.dirname(HERE)), size / 1024))
    print('total %.1f MB -> %.1f MB (%.0f%% smaller)'
          % (total_in / 1e6, total_out / 1e6, 100 * (1 - total_out / total_in)))


if __name__ == '__main__':
    build(sys.argv[1] if len(sys.argv) > 1 else '.')
