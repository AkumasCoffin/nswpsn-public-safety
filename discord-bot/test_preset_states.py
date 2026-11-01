"""
Checks for a preset's States filter and the poller's re-baseline guard.

The States filter is alert_catalog.passes_states, which preset_alert_matches
in bot.py calls; bot.py itself needs discord.py, so this tests the helpers
directly and checks the gate is wired to them. The guard is exercised through
AlertPoller with a fake database: aiohttp and python-dotenv are stubbed when
they are not installed, and nothing touches a real database or network.

Run: python test_preset_states.py
"""
import asyncio
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# alert_poller imports these at module level; neither is needed here.
sys.modules.setdefault('dotenv', types.SimpleNamespace(load_dotenv=lambda *a, **k: None))
try:
    import aiohttp  # noqa: F401
except ImportError:
    sys.modules['aiohttp'] = types.SimpleNamespace(ClientSession=object, ClientTimeout=object)

import alert_catalog  # noqa: E402
import alert_poller  # noqa: E402

FAILS = []


def check(cond, msg):
    print(('ok   ' if cond else 'FAIL ') + msg)
    if not cond:
        FAILS.append(msg)


def road(state):
    return {'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [150.0, -33.0]},
            'properties': {'title': 'Roadwork', 'state': state}}


# --- state of an alert -----------------------------------------------------
check(alert_catalog.state_code('nsw') == 'NSW', 'a code in any case')
check(alert_catalog.state_code('Western Australia') == 'WA', 'a full name')
check(alert_catalog.state_code('Aotearoa') is None and alert_catalog.state_code('') is None, 'anything else is unknown')
check(alert_catalog.home_state('rfs') == 'NSW' and alert_catalog.home_state('cfa') == 'VIC', 'home states from the catalog')
check(alert_catalog.home_state('bom_land') is None and alert_catalog.home_state('firms') is None, 'no home state for types spanning states')
check(alert_catalog.alert_state('traffic_roadwork', road('QLD')) == 'QLD', "a record's own properties.state")
check(alert_catalog.alert_state('rfs', {'properties': {'title': 'Fire'}}) == 'NSW', 'the home state when the record has none')
check(alert_catalog.alert_state('rfs', {'properties': {'state': 'VIC'}}) == 'VIC', 'the record wins over the home state')
check(alert_catalog.alert_state('wire_article', {'state': 'SA'}) == 'SA', "a flat record's top-level state")
check(alert_catalog.alert_state('pager', {'capcode': '123'}) is None, 'unknown when neither is there')

# --- the States filter -----------------------------------------------------
P = alert_catalog.passes_states
check(P(None, 'traffic_roadwork', road('QLD')) and P([], 'traffic_roadwork', road('QLD')), 'no list is no filter')
check(P(['NSW', 'ACT'], 'traffic_roadwork', road('ACT')), 'a state in the list passes')
check(not P(['NSW'], 'traffic_roadwork', road('QLD')), 'a state not in the list is dropped')
check(not P(['NSW'], 'qfd', {'properties': {'title': 'Grass fire'}}), "the type's home state is used when the record has none")
check(P(['QLD'], 'qfd', {'properties': {'title': 'Grass fire'}}), '...and passes when it is in the list')
check(P(['NSW'], 'bom_land', {'title': 'Severe weather'}), 'an alert whose state is unknown passes')
check(P(['NSW'], 'pager', {'capcode': '123'}), '...pager included')
check(P(['nsw'], 'traffic_roadwork', road('NSW')), 'the list is read in any case')
check(P(['nope'], 'traffic_roadwork', road('QLD')), 'a list of nothing known is no filter')

# --- the gate is wired -------------------------------------------------------
with open(os.path.join(HERE, 'bot.py'), encoding='utf-8') as fh:
    BOT = fh.read()
gate = BOT[BOT.index('def preset_alert_matches'):BOT.index('# Default-preset helpers')]
check("alert_catalog.passes_states(f.get('states'), alert_type, alert_data)" in gate,
      'preset_alert_matches applies the States filter')


# --- the re-baseline guard -------------------------------------------------
class FakeDB:
    def __init__(self):
        self.seen = set()

    def get_presets_for_alert_type(self, t):
        return [{'id': 1}]

    def mark_alerts_seen_batch(self, batch):
        self.seen.update(batch)

    def filter_unseen_alerts(self, alerts):
        return [a for a in alerts if a not in self.seen]


async def cycle_with(total, new):
    """Bootstrap a feed of `total` items, then swap `new` of them for unseen ids."""
    db = FakeDB()
    p = alert_poller.AlertPoller('http://x', 'k', db)
    p._extract_items = lambda t, d: [{'id': x} for x in d['ids']]
    p._get_alert_id = lambda t, item: item['id']
    p._get_alert_timestamp = lambda t, item: alert_poller.datetime.now(alert_poller.timezone.utc)
    loop = asyncio.get_running_loop()
    ids = [f'old-{i}' for i in range(total)]
    await p._new_alerts_for(loop, 'traffic_roadwork', {'ids': ids})
    ids = [f'new-{i}' for i in range(new)] + ids[new:]
    out = await p._new_alerts_for(loop, 'traffic_roadwork', {'ids': ids})
    again = await p._new_alerts_for(loop, 'traffic_roadwork', {'ids': ids})
    return out, again, db


out, again, db = asyncio.run(cycle_with(300, 201))
check(out == [] and again == [], '201 of 300 new at once: re-baselined, nothing posted, nothing next cycle')
check(('traffic_roadwork', 'new-0') in db.seen, '...and the new ids are marked seen')
out, _, _ = asyncio.run(cycle_with(1000, 150))
check(len(out) == 150, '150 of 1000 new: an ordinary burst, all alerted')
out, _, _ = asyncio.run(cycle_with(300, 200))
check(len(out) == 200, 'exactly 200 new is under the bar: alerted')
out, _, _ = asyncio.run(cycle_with(220, 210))
check(out == [], '210 of 220 new: re-baselined')

print('\nFAILED:' if FAILS else '\nALL PASSED', *FAILS, sep='\n  ')
sys.exit(1 if FAILS else 0)
