"""
seen_alerts retention: an alert is forgotten once it has been OUT of its feed
for the retention window, not that long after it first appeared.

Pruned on first_seen, a roadwork listed for months re-alerted 7 days after it
first appeared (71 of 402 roadworks counted as new on 2026-11-03/04 had been in
the feed since before 10-27), and a work that leaves the feed outside its
working hours and returns each night would re-alert weekly.

Runs against a throwaway SQLite file. Run: python test_seen_retention.py
"""
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.pop('BOT_DATABASE_URL', None)

import database  # noqa: E402

FAILS = []


def check(cond, msg):
    print(('ok   ' if cond else 'FAIL ') + msg)
    if not cond:
        FAILS.append(msg)


def stamp(days_ago):
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()


tmp = tempfile.mkdtemp()
db = database.Database(os.path.join(tmp, 'seen.db'))
db.init_db()
db.init_db()  # the column add must be safe to repeat

conn = db._connect()
cols = [r[1] for r in conn.execute('PRAGMA table_info(seen_alerts)').fetchall()]
check('last_seen' in cols, 'init adds seen_alerts.last_seen')
# Three alerts first seen 10 days ago: one still in the feed, one gone, one
# confirmed yesterday by an earlier pass.
conn.executemany(
    'INSERT INTO seen_alerts (alert_type, alert_id, first_seen, last_seen) VALUES (?, ?, ?, ?)',
    [('traffic_roadwork', 'still-listed', stamp(10), None),
     ('traffic_roadwork', 'gone', stamp(10), None),
     ('traffic_roadwork', 'nightly', stamp(10), stamp(0.5))],
)
conn.commit()
conn.close()

unseen = db.filter_unseen_alerts([
    ('traffic_roadwork', 'still-listed'),
    ('traffic_roadwork', 'nightly'),
    ('traffic_roadwork', 'brand-new'),
])
check(unseen == [('traffic_roadwork', 'brand-new')], 'only the never-seen id is new')

conn = db._connect()
rows = dict(conn.execute('SELECT alert_id, last_seen FROM seen_alerts').fetchall())
conn.close()
check(rows['still-listed'] is not None, 'an id still in the feed is confirmed')
check(not database._seen_before(rows['nightly'], datetime.now(timezone.utc) - timedelta(days=1)),
      'an id confirmed within the day is left alone')
check(rows['gone'] is None, 'an id no longer in the feed is not touched')

db.cleanup_old_seen(7)
conn = db._connect()
left = {r[0] for r in conn.execute('SELECT alert_id FROM seen_alerts').fetchall()}
conn.close()
check('still-listed' in left, 'a long-running item survives retention while it is listed')
check('nightly' in left, 'an item that comes and goes survives while it keeps coming back')
check('gone' not in left, 'an item out of its feed for the window is forgotten')

check(database._seen_before('not a date', datetime.now(timezone.utc)), 'an unparsable stamp counts as old')
check(database._seen_before('2026-01-01T00:00:00', datetime(2026, 1, 2, tzinfo=timezone.utc)),
      'a stamp without an offset is read as UTC')

print('\n' + ('FAILED: %d' % len(FAILS) if FAILS else 'all passed'))
sys.exit(1 if FAILS else 0)
