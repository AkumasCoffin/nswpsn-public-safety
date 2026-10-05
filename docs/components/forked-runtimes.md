# Forked runtimes

AusAware runs five repositories it does not own the upstream of. A **radio feeder
node downloads and runs two of them**; the other three run centrally.

None of this code lives in this repository. What lives here is the manifest that
pins it: **`backends/node/assets/node-versions.json`**.

Read [`../../AGENTS.md`](../../AGENTS.md) before changing anything here.

---

## Which forks, at a glance

| Repository | Upstream | Language | Branch | Who runs it |
|---|---|---|---|---|
| `AkumasCoffin/sdrtrunk` | `DSheirer/sdrtrunk` | Java | `feature/node-control` | nobody directly — it is where the headless control server was written |
| `AkumasCoffin/sdrtrunk-vce` | `tylerwatt12/sdrtrunk-vce` | Java | `feature/node-control` | **every radio node** |
| `AkumasCoffin/rdio-scanner` | `chuot/rdio-scanner` | Go + Angular | `master` | **every radio node**, and the central instance |
| `AkumasCoffin/rdio-scanner-plugins` | **not a fork** | JavaScript | `main` | the central rdio-scanner |
| `AkumasCoffin/pagermon` | `pagermon/pagermon` | JavaScript | `master` | the central Pagermon |

The two a node downloads are the two in the manifest: `sdrtrunk` and `rdio`.

## How a node gets them

`backends/node/assets/node-versions.json` declares five entries — `agent`,
`pager-agent`, `adsb-agent`, `sdrtrunk`, `rdio` — each with a per-platform URL and
a per-platform sha256. Everything is served as static files; the agent binaries
come off the site webroot (`NODE_DOWNLOADS_BASE`, default
`https://nswpsn.forcequit.xyz/downloads`), and the two big forked runtimes come
straight off their GitHub release assets.

The agent (`feeder-nodes/radio-node/internal/update`) checks the manifest:

- at start;
- every 6 hours;
- on a `cmd{action:'update'}` frame over the control WebSocket.

Then: download to `.pending`, **sha256-verify**, swap the component directory (or
self-replace via a detached helper, for its own binary), restart.

**An empty sha256 means "nothing to do".** The agent treats it as "no update" and
skips, so a partially filled manifest is safe and a manifest fetch failure is a
graceful no-op — components resolve from whatever is already installed.

The backend serves the manifest at `GET /api/node-updates/manifest`,
node-token authenticated, and the mapping is **kind-aware**: a pager node is
served the `pager-agent` entry *as* its `agent` component, an ADS-B node gets
`adsb-agent`, and neither is offered `sdrtrunk` or `rdio` at all.

### Version comparison ignores pre-release suffixes

The agent compares three numeric parts. `6.14.1-beta.7` therefore reads as
`6.14.1`, and that is deliberate on both pinned entries:

- **`rdio`** — the suffix is ignored so `6.14.1-beta.7` wins over upstream
  `6.13.1`.
- **`sdrtrunk`** — the manifest version line is kept **independent of VCE's own
  version string**. VCE calls itself `0.6.2-alpha-N`; with suffixes ignored that
  would read as `0.6.2` and *lose* to the old fork's `0.6.10`, so the component
  would never install. The manifest uses its own `0.7.x` line for the node-runtime
  releases instead (`node-versions.json:41`).

### Pinned versions as committed

| Entry | Version | sha256 present |
|---|---|---|
| `sdrtrunk` | `0.7.17` | yes, `windows-amd64` and `linux-amd64` |
| `rdio` | `6.14.1-beta.7` | yes, `windows-amd64` and `linux-amd64` |
| `agent` | `0.2.39` | empty — the agent binaries are rebuilt by the deploy and the installer verifies against a published `.sha256` sidecar |
| `pager-agent` | `0.1.22` | empty, same reason |
| `adsb-agent` | `0.1.12` | empty, same reason |

**Bumping an agent version in this manifest is how an agent change reaches
running nodes.** `backends/node/scripts/deploy.sh` skips the Go rebuild entirely
when the built binary already reports the manifest version, and the built binary's
version is stamped from the manifest (`-ldflags -X …/version.Version`) so
self-update compares equal and does not loop.

The two big forked runtimes are **placed in the downloads directory once, by
hand** — the deploy does not rebuild them. Refreshing one means rebuilding its
release on its own repository, then bumping the version and the sha256 values
here.

---

## `AkumasCoffin/sdrtrunk` — branch `feature/node-control`

Fork of `DSheirer/sdrtrunk`, the Java application that actually decodes P25.
**32 commits ahead** of upstream `master`.

No node runs this build. It is where the headless control server was written,
before being ported onto the VCE line. It matters because it is the origin of the
design and still carries the CI workflow that builds a node runtime.

Two unrelated bodies of work share the branch:

**1. The headless control server** — the reason the fork exists. Upstream
SDR-Trunk is a GUI application; an agent cannot drive it. Added:

```
src/main/java/io/github/dsheirer/control/ControlServer.java
src/main/java/io/github/dsheirer/control/ControlWebSocketServer.java
src/main/java/io/github/dsheirer/control/EventBuffer.java
src/main/java/io/github/dsheirer/control/SpectrumStreamer.java
.github/workflows/build-node-runtime.yml
```

Plus the surrounding work needed to make SDR-Trunk survive with no display and no
human: P25 tuner controls (gain, sample rate, autoppm), call and alias enrichment,
honouring requested spectrum bins up to 4096, per-tuner capabilities on
`GET /tuners`, headless first-run CPU calibration and JMBE auto-install, gating
channel start on both of those being ready, **never persisting the playlist in
headless** (the agent owns it), and stopping an `AudioPlaybackManager` NPE flood on
a box with no audio output.

**2. A theming line** — a dark mode and preset themes under User Preferences,
`ThemeManager.java`, `Theme.java`, `AppearancePreferenceEditor.java`,
`sdrtrunk_dark.css`, a GUI scale slider, and a long tail of fixes for JIDE and
Windows-look-and-feel crashes on non-Windows JDKs. Unrelated to node control; same
branch.

## `AkumasCoffin/sdrtrunk-vce` — branch `feature/node-control`

Fork of `tylerwatt12/sdrtrunk-vce`, itself a fork of SDR-Trunk with extra
features and optimisations. **21 commits ahead, 482 behind** its upstream `main`.

**This is the build every radio node actually runs.** Its first commit on the
branch is *"node-control: port headless control server + node-runtime build from
sdrtrunk fork"* — the control server above, moved onto the VCE line.

What the fork added beyond the port, and why each matters here:

| Change | Why AusAware needs it |
|---|---|
| `GET /activity/events` — cursor-paged feed of reception, data and page activity | **This is the source of `node_radio_events`.** `internal/activityship` polls it every ~4s. It carries the real P25 identity — system id, talkgroup, source radio, site, encryption flag — which is how a reception is recorded even when the talkgroup is encrypted and there is no audio |
| `GET /site/snapshots` — per-P25-site metadata, including active patch groups | `internal/siteship` ships it every ~60s. It is what `/api/radio/monitored-sites` and the map's repeater badges are built on |
| `systemName` + talker `sourceAlias` on the activity feed | identity that would otherwise have to be re-derived |
| Patched-reception member talkgroups on the activity feed | a patched transmission reports which talkgroups actually carried it |
| Per-channel decode health and signal level (`syncPercent`, `signalDbfs`) | the measurement `internal/chanmgr` and `internal/decodeprobe` judge a channel on. Without it, automatic channel management has nothing to act on |
| Tuners `autoppm` action and real autoPpm readback | per-dongle crystal correction without a human |
| Self-healing channel auto-start timer, with **agent-stop suppression** | a channel the agent deliberately stopped must stay down. Without the suppression, SDR-Trunk's own 30s self-heal sweep restarts it and wins the race |
| Waiting for tuner discovery before channel auto-start | a fresh node otherwise tries to start channels before it knows what hardware it has |
| Not starting channels during a config import or reload | a config push would otherwise race the start |
| Recovering `@JsonIgnore` alias fields on import; decoupling auto-start from JMBE | an import used to silently lose alias data, and auto-start used to depend on JMBE being installed |
| Fixes: a JDBC connection leak, DMR/P25-2 timeslot state aggregation, an HTTP server left parked, a bounded site query, DB failures no longer swallowed | stability on a box nobody is watching |
| "Acquisition is not a decode failure" | a channel mid-acquisition was being scored as failing, which made the channel manager act on noise |

Node runtimes are published as release assets on the `node-runtime` tag:
`sdrtrunk-windows-amd64.zip` and `sdrtrunk-linux-amd64.zip`.

One implementation detail the agent has to tolerate: the app image ships its
launcher as either `bin/sdr-trunk` or `bin/sdrtrunk-vce` (plus `.bat` on Windows),
so `internal/update` tries both in preference order.

## `AkumasCoffin/rdio-scanner` — branch `master`

Fork of `chuot/rdio-scanner`, the Go-plus-Angular scanner that stores reception
audio and serves the listening interface. **431 commits ahead, 26 behind**
upstream `main`. A radio node runs it locally; there is also a central instance.

### Why the fork is not optional

The manifest says it plainly (`node-versions.json:53`): nodes must run the **same
rdio the central instance does**, because that is where **talkgroup patches** live
(`rdioScannerPatches`, added after upstream 6.13.1), and the backend reads them
from the central instance to group patched receptions.

A patch is a set of talkgroups on one system that carry the same conversation.
When one transmission arrives once per member talkgroup — same audio, same
timestamp — only one is kept. Copies are recognised by timestamp, independent of
duplicate-detection settings, and each patch carries a **delay** for the case
where separate recorders cover the members and keep their own clocks. Members are
listed in display order and that order is the ranking: the surviving reception
files under the highest-listed talkgroup that **actually received a copy**, and
moves up if a copy later arrives on a higher one — so a talkgroup never shows
traffic it did not carry.

The live display cannot get that right first time, because the first copy is sent
on the moment it lands while the others are still in flight. So the reception goes
out at once and the display is corrected when its siblings arrive, rather than
every listener paying a delay on every patched reception. Downstreams get the
finished article instead: a forward is an upload, not a record the upstream can
revise later, so a patched reception waits out its delay before being sent on.

That last sentence is the one that matters for a node: **the node's local rdio is
an upstream, and the backend is its downstream.**

### What else the fork changed

From the fork's own `CHANGELOG.md` (6.14.1 and the 6.14.2 line):

- **A rebuilt Search panel** — filters down the side rather than across the top,
  talkgroups grouped by system, picks shown as droppable chips; date ranges across
  several systems and talkgroups at once with presets (today, last 24 hours, this
  week, this month); results as one growing list rather than page numbers; a
  **Live** toggle that joins newly received receptions to the top of an open list,
  holding off while you are scrolled away; receptions on the same talkgroup within
  thirty seconds kept together so one exchange reads as one exchange.
- **Patches** as above, plus CSV export and import from Tools, one row per patch
  with members as a list in one cell because their order is the ranking.
- **A plugin system** — see the plugins repository below. Transcription and the
  OBS overlay moved out of the server and into plugins at 6.14.
- Bulk editing across talkgroups and units.
- Database work for a large install staying responsive: the primary key put back
  on the calls table, an index matching the order search actually asks for,
  bounded walks behind plugin-table search filters, filter options no longer
  rebuilt on every talkgroup click.
- Plugins given somewhere to draw besides the plugin manager, an admin session
  that outlives the tab it was opened in, and a way to stay off their own event
  loop.
- An **Android client** (`android/`, Kotlin and Compose) — not used by AusAware,
  but it is in the fork.

### How a node's local rdio is wired

`internal/configapply` PUTs a full config carrying stable per-agency API keys
(`internal/keys`) and **exactly one downstream**, pointing at the agent's own
localhost relay listener. That downstream is the entire mechanism by which audio
leaves the local rdio.

`internal/rdioctl` speaks its admin API. Login exchanges the admin password for a
session token, and authenticated requests send that token in a **raw
`Authorization` header, not `Bearer <token>`** — matching
`rdio-scanner/server/command.go`.

## `AkumasCoffin/rdio-scanner-plugins` — branch `main`

**Not a fork.** An original repository, JavaScript: the official plugin repository
for the rdio-scanner fork above. Plugins are browsed and installed from the rdio
admin panel, stored alongside the server, and take effect **after a restart**.
They can be enabled and disabled without uninstalling, and uninstalling keeps
their settings and data — removing the data is a separate explicit purge.

Three plugins:

| Plugin | What it does |
|---|---|
| **`transcripts`** | Transcribes reception audio with Whisper and shows it in the scanner. Was part of the rdio server until 6.14; renders identically. Announces transcripts to other plugins |
| **`stream`** | The OBS overlay at `/stream` — a configurable canvas of readouts, borders and the transcript, with no application chrome. Also part of the server until 6.14 |
| **`hello-world`** | Reference plugin — config, a table, watching a reception, changing one, a WebSocket command, an HTTP endpoint and a frontend |

### `transcripts` is the one AusAware depends on

`plugin.json`: version `1.0.12`, `minServerVersion` `6.14.0-beta.1`. It offers
three providers — Groq, OpenAI, and **self-hosted Whisper**. The self-hosted
option is the one AusAware uses: its base URL is pointed at
`<backend>/api/whisper/v1`, permanently, and the backend's router chooses a server
per reception. See [`transcription.md`](transcription.md).

Each provider keeps its own base URL, key and model, and switching provider hides
the others rather than clearing them. Several API keys may be given separated by
commas, semicolons or newlines; they are used in rotation and one that comes back
rate-limited is set aside until its retry window passes.

Upgrading from a server with transcription built in needs nothing: existing
`calls.transcript` rows are migrated into the plugin's tables on first start,
verified by count, the plugin is installed and enabled automatically, and the old
settings — API keys included — are carried across under the plugin's own names.

Other plugins consume transcripts over the plugin bus rather than by reading the
plugin's tables, which is deliberate: the schema is the plugin's business to
change. Every transcript is published on a `transcript` topic as it becomes final,
and `has` / `get` / `transcribe` are answerable over RPC. Publishing never waits
and never reports who listened, so a subscriber cannot slow transcription down or
make it fail.

Each plugin gets its own database tables, created on install and namespaced to it.

Backend plugin code is JavaScript run in-process by an embedded interpreter — one
artifact works on every platform rdio supports, nothing to compile. Frontend code
is plain JavaScript loaded by the webapp at runtime; it is not Angular and does
not require rebuilding the webapp.

The admin panel lists **every branch** of the plugin repository, not just `main`.
Branches other than `main` may hold untested work.

## `AkumasCoffin/pagermon` — branch `master`

Fork of `pagermon/pagermon`, a multimon-ng pager message parser and viewer,
JavaScript. **3 commits ahead, 0 behind** — 10 files, and that is the whole fork:

- restyle the default theme to match the AusAware dark design;
- remove browser notifications, add a one-hour inactivity timeout;
- revise the client setup instructions in the README.

Everything touched is a stylesheet, a template, a view or the README. **No
decoding or protocol changes at all.** If you need to know how Pagermon parses a
page, read upstream — this fork does not change it.

This is the self-hosted Pagermon that sits on both sides of the pager path:

- the backend **polls** it for the map (`PAGERMON_URL` →
  `backends/node/src/sources/pager.ts` → `/api/pager/hits`);
- the backend **forwards into** it from pager nodes (`PAGERMON_INGEST_URL`, plus
  per-state pairs such as `PAGERMON_INGEST_URL_QLD`).

`backends/node/src/services/capcodeAliasSync.ts` additionally harvests capcode
aliases from it, falling back to feed harvest when its capcode list is
unavailable.

**Its ingest credentials never leave the server.** A pager node authenticates with
its own node token; the backend substitutes the Pagermon key when it forwards.
Never write anything that implies a node holds them.

## Changing a pinned version

1. Rebuild the release on the fork's own repository.
2. Update `version` **and** the matching `sha256` values in
   `backends/node/assets/node-versions.json`.
3. For a runtime, place the new artifact in the downloads directory. For an
   agent, the deploy builds it — but only because the version changed.
4. Commit on `dev-beta`.
5. **Stop there.** The owner deploys. Do not restart any node, and do not push an
   `update` command to the fleet yourself.

An empty sha256 disables that entry rather than shipping an unverified artifact.
Leave it empty rather than guessing.

## See also

- [`radio-node.md`](radio-node.md) — the agent that downloads and supervises
  these.
- [`transcription.md`](transcription.md) — where the `transcripts` plugin sends
  audio.
- [`../architecture.md`](../architecture.md) — the full air-to-pin trace.
