# Feeding your scanner's calls to NSW PSN

This is for a contributor running their **own rdio-scanner** (from a desktop
scanner or any other receiver) who wants their calls counted on
nswpsn.forcequit.xyz.

There is **nothing to install**. rdio can already forward every call it receives
to another server — that is what a *downstream* is — so the whole setup is one
entry in your rdio admin page.

---

## Setup

1. Open your rdio-scanner admin page → **Config** → **Downstreams**.
2. Add a downstream:

   | Field | Value |
   |---|---|
   | **URL** | `https://nswpsn.forcequit.xyz/api/scanner-ingest` |
   | **API key** | *(the key you were sent)* |
   | **Systems** | `*` (all) — or just the systems you want to share |
   | **Disabled** | unchecked |

3. Save. That's it — rdio starts forwarding on the next call.

> **If you already have a downstream pointing at NSW PSN**, change that one's URL
> rather than adding a second. Two downstreams to the same place would upload
> every call twice.

You can confirm it's working from your own rdio's log — a failed downstream is
logged with the response status.

---

## What gets sent

Exactly what rdio sends any downstream: the call audio, its **talkgroup**, the
**radio ID** that transmitted, the frequency, the start time, and the talker
alias if the radio sent one.

## What we do with it

- **Your talkgroup and radio IDs are the point.** They're the same network, so
  they line up with everything else.
- **Your labels are ignored** — deliberately. Names are resolved from the site's
  own talkgroup and unit lists, so you don't have to match anyone else's naming,
  and renaming things on your end changes nothing here.
- Your calls appear in the Data tab's call and radio views, and count toward
  talkgroup and radio activity.

## What it can't do

A scanner has no control-channel view, so there's no site, signal or decode
information, and your feed doesn't appear in the Live tab. It contributes
**calls only** — which is the whole intent.

## Turning it off

Disable or delete the downstream in your rdio admin, and it stops immediately.
Nothing needs doing at our end. (We can also switch the feed off from our side
without you having to change your key.)

---

## Notes

- **Your calls still reach the main rdio.** We pass every upload straight
  through, which is why you repoint your existing downstream instead of adding
  another one. Nothing you can see changes.
- **Timestamps are shifted by one second** on the way through, to line up with
  the network nodes' clock basis — the two sources stamp different moments of
  the same transmission (your recorder stamps when audio starts; a node stamps
  when the call is set up, about a second earlier).

  This matters more than it sounds. rdio recognises a **patch** — two or more
  talkgroups carrying one conversation — by the copies sharing an *exact*
  timestamp. A second out and the copies never match, so a patched
  transmission is stored as several separate calls and the patch is never
  recognised. Aligning the clocks is what keeps patches working once your feed
  and the nodes are both contributing.
- Calls that both you and a node hear merge into one call with two
  contributors, rather than being counted twice.
- Retries are safe: if rdio re-sends a call after a failure, it is recognised as
  the same call and not stored twice.
