# Radio feeder node — `feeder-nodes/radio-node`

**Entry point: `feeder-nodes/radio-node/cmd/nodeagent/main.go`.**

Go 1.26, module `github.com/AkumasCoffin/nswpsn-node/radio-node`. A separate Go
module from the other two agents, with its own `go.mod` — build it from its own
directory.

What it decodes: **P25 trunked voice** (plus DMR, and AM/airband) — whatever
SDR-Trunk can decode from the channels the backend pushes it. It is the only
agent that downloads and runs external forked software.

Read [`../../OPERATING-RULES.md`](../../OPERATING-RULES.md) before changing anything here.

---

## What it is

A supervisor. The agent does not decode anything itself. It:

1. launches and babysits a **forked, headless SDR-Trunk** and a **forked local
   rdio-scanner**, both downloaded and version-pinned by the agent;
2. runs a localhost HTTP listener impersonating rdio's upload endpoint;
3. buffers every reception to a disk-backed queue;
4. drains that queue to the backend;
5. holds a control WebSocket so the backend can configure and command it.

```
   antenna ─► RTL-SDR ─► SDR-Trunk (forked, headless)
                              │                 │
                   audio ─────┘                 └───── metadata
                     │                            (control server REST)
                     ▼                                    │
              local rdio-scanner                 activityship (~4s)
            (downstream → the agent)             siteship (~60s)
                     │                                    │
                     ▼                                    │
          internal/relay  (localhost HTTP, always 200)     │
                     │                                    │
                     ▼                                    │
          internal/queue  (disk FIFO, one file per reception)
                     │                                    │
                     ▼                                    ▼
      POST /api/node-ingest/call-upload   POST /api/node-ingest/activity
                                          POST /api/node-ingest/site-snapshots
```

## Build and run

```bash
cd feeder-nodes/radio-node
go build ./cmd/nodeagent
go test ./...
```

```
nodeagent [--config <path>] <run|version|install|uninstall|start|stop>
```

`run` is the default and works both under a service manager and as a plain
foreground process. `install`/`uninstall`/`start`/`stop` manage an OS service
named **`nswpsn-node`** via `github.com/kardianos/service` — systemd on Linux, a
Windows service on Windows. `--config` may appear anywhere in the arguments and
defaults to a path beside the executable.

Configuration is YAML (`internal/agentcfg`). `agent.example.yaml` is the
template. Installers are in `installers/` — `installers/linux/install.sh`,
`nodeagent.service`, a `99-nswpsn-sdr.rules` udev rule, and
`installers/windows/setup.iss` for Inno Setup.

Platforms built by `backends/node/scripts/deploy.sh`: `linux-amd64`,
`linux-arm64`, `windows-amd64`.

## Enrolment

A fresh install has no credential. The installer writes a single-use **enrolment
code** and an empty `node_token`. On first run `internal/enrol` presents the code
to `POST /api/node-enrol` and receives the node token, which it persists.

From then on every request carries `X-Node-Token` + `X-Node-Install`, and the
agent sends `User-Agent: NSWPSN-NodeAgent/<version>` so the backend's request log
tags it `[node]`.

## The managed components

`internal/update` fetches the manifest from the backend
(`/api/node-updates/manifest`, served from
`backends/node/assets/node-versions.json`) and resolves two managed components:

| Component | What it is | Source |
|---|---|---|
| `sdrtrunk` | headless SDR-Trunk node runtime | `AkumasCoffin/sdrtrunk-vce`, branch `feature/node-control`, release tag `node-runtime` |
| `rdio` | rdio-scanner | `AkumasCoffin/rdio-scanner` |

Both carry a version and a per-platform sha256 in the manifest. The agent checks
at start, every 6 hours, and on a `cmd{action:'update'}` frame: download to
`.pending`, sha256-verify, swap the component directory (or self-replace via a
detached helper for its own binary), restart. An **empty sha256 means "nothing to
do"** — not an error, so a partially-filled manifest is safe.

Why forks at all, and what each changed:
[`forked-runtimes.md`](forked-runtimes.md).

A manifest fetch failure is a graceful no-op: components resolve from whatever is
already installed, which may be nothing, in which case they stay skipped.

**A change to this agent's Go code does not reach any node until `agent.version`
is bumped in `backends/node/assets/node-versions.json`.** `deploy.sh` skips the
Go rebuild entirely when the built binary already reports the manifest version.

Bumping the version is necessary but **not currently sufficient**: the `agent`
entry's sha256 is empty, and an empty sha256 is treated as nothing to do
(`internal/update/update.go:138`), so agent self-update is dormant and a
changed agent reaches a node only via a fresh install. Self-update does work
for `sdrtrunk` and `rdio`, whose digests are filled. Details:
[`forked-runtimes.md`](forked-runtimes.md#self-update-is-dormant-for-the-agents).

## Supervision

`internal/supervise` runs the two children and restarts them on exit.

Before launching, `supervise.KillStale` reaps an SDR-Trunk left behind by a
previous agent — a self-update re-exec keeps the PID and cgroup, so the old JVM
survives and would keep holding the control port. It matches **only** the precise
SDR-Trunk main class `io.github.dsheirer`, never the operator-configured command:
that could be something as generic as `java`, and matching it would SIGKILL
unrelated JVMs belonging to other users.

The local rdio-scanner binds `127.0.0.1:17391` — admin API and upload on the same
port (`main.go:54-60`). SDR-Trunk's state lives under `--app-root`, with its
config database at `<app-root>/database/sdrtrunk.sqlite`.

A fresh per-boot bearer token (32 random hex bytes) is generated and shared
between the launched control server and the agent's own REST and spectrum
clients.

## Config apply

`internal/configapply` turns one backend config push into local state:

1. **Stable per-agency rdio API keys.** `internal/keys` generates a UUIDv4 once
   per system id and persists it to `data/keys.json`, so a key survives agent
   restarts. It is injected into both the rdio config (`apiKeys[].key`) and the
   SDR-Trunk playlist (`<stream> api_key`) — both sides have to agree.
2. **A full rdio-scanner config PUT**, with **one downstream** pointing at the
   agent's own relay listener. That downstream is the whole mechanism by which
   audio leaves the local rdio.
3. **A full SDR-Trunk configuration import** — channels, aliases, streams.

**SDR-Trunk's config is applied first**, deliberately: a flaky rdio must not
prevent the channel list from landing.

`internal/rdioctl` speaks the local rdio admin API. Its login exchanges the admin
password for a session token, and authenticated requests send that token in a
**raw `Authorization` header — not `Bearer <token>`**, matching
`rdio-scanner/server/command.go`. The admin password is persisted to
`data/rdio-admin.secret`; if it is missing the agent falls back to rdio's default.

`internal/sdrctl` speaks the SDR-Trunk control server: REST on
`http://127.0.0.1:<P>` for status, tuners, channels and playlist, and a WebSocket
on `ws://127.0.0.1:<P+1>` for the live spectrum stream.

`internal/presets` embeds the base SDR-Trunk playlist (`presets/default.xml`) and
rdio config (`presets/rdio-scanner.json`) into the binary with `go:embed`. These
are the **same no-secret presets the backend serves** — key fields are empty —
and exist purely as a fallback so the agent can render a playlist when the
backend is unreachable.

On startup, `bootImportConfig` re-imports the last-applied config once the
control server answers, so SDR-Trunk always runs the agent's current config
rather than whatever its SQLite database held from the last session — and never
auto-starts a channel the operator disabled. On a first-ever boot there is
nothing persisted, so SDR-Trunk runs its imported preset database.

## Getting receptions out

`internal/relay` is a localhost HTTP listener impersonating rdio-scanner's
`call-upload`. It **must always answer 200** — the local rdio drops the recording
on any other response — so it accepts, enqueues, and returns.

`internal/queue` is a disk-backed, bounded FIFO. One file per reception, named
`<20-digit-zero-padded-unixnano>-<rand4>.call`, so the filename sorts
chronologically. That queue is what makes a node survive its own internet: a
dropped link delays receptions, it does not lose them.

The sender drains the queue to `POST /api/node-ingest/call-upload`, enriching each
with P25 site headers from the control server when available.

Receptions lost between rdio and the queue — a full queue, a write failure — are
counted and reported in the status frame, so a node shedding traffic shows on the
fleet page instead of failing silently.

### Metadata ships separately

| Shipper | Cadence | Reads | Posts to |
|---|---|---|---|
| `internal/activityship` | ~4s | control server `/activity/events`, after a persisted cursor | `/api/node-ingest/activity` |
| `internal/siteship` | ~60s | control server `/site/snapshots` | `/api/node-ingest/site-snapshots` |

**The activity feed is what creates the permanent record.** Rows in
`node_radio_events` come only from it (`backends/node/src/services/nodeEvents.ts`,
migration 044); the audio upload merely calls `markRecorded()` on the closest
matching event row. So a reception on an encrypted talkgroup — no audio to
recover — is still recorded with its full P25 identity, site and encryption flag.
That is the right outcome, and it is why the two paths exist.

`activityship` runs **regardless of whether the audio feed is enabled**: the
metadata is wanted even when a node's feed is off.

`siteship` replaces the full snapshot each time — no cursor — and **pauses while a
site survey is running**, because a survey locks onto sites all over the council
area and none of them is a site this node monitors.

## The control WebSocket

`internal/wsclient` holds a persistent outbound connection to
`/api/node-ws/agent`:

- a `hello` on connect;
- a status heartbeat every **15s**;
- answers to `cmd` frames with `cmdResult`;
- a WebSocket ping every **30s**, because Cloudflare Tunnel kills an idle
  WebSocket at around 100s;
- reconnection with backoff.

Frames are JSON text wrapping an envelope:
`{ "t": type, "id"?: correlation, "data"?: payload }` (`internal/protocol`;
the backend's half is `backends/node/src/services/nodes/protocol.ts`).

The WS client also bridges the SDR-Trunk control server on the same port and
token the agent used to launch it, which is how staff spectrum views work.

## Automatic channel management

`internal/chanmgr` stops a channel that is burning a tuner for nothing, retests
it, and restores it when it recovers. The thresholds are two-tier
(`chanmgr.go:48-78`):

| Rule | Threshold | Dwell |
|---|---|---|
| Marginal | decode health below **60%** | **10 minutes** continuous |
| Dead air | decode health below **40%** | **5 minutes** |
| Never locked | a running control channel that never acquires | **5 minutes** |

The two decode clocks run alongside each other: a sample in [40, 60) resets only
the severe clock; a sample at or above 60 resets both. A stopped channel is
retested after **10 minutes** the first time — a channel stopped by a passing
interference spike should not sit dead for half an hour on that evidence — and
every **30 minutes** after a retest has already failed. Recovery needs several
consecutive measured samples at or above threshold.

The never-locked rule is deliberately separate from the decode thresholds,
because a channel that never locks reports no decode figure at all, and the
figure is what the dwells are measured on. "No number" means "nothing to judge"
for a channel whose runtime has no monitor — and exactly the wrong thing for one
sitting on a dead frequency burning a tuner. The distinction is the lock, not the
absence of data.

Design rules, learned the hard way — two earlier agent-autonomy attempts were
reverted for churning whole nodes:

- **Stateless across restarts.** Nothing is persisted. Every tick reconciles
  against the live `/channels` list, and the one fact that must survive a restart
  — which channels it stopped on purpose — is recovered from SDR-Trunk's own
  `suppressed` flag. A world that changed underneath it (config push, operator
  click, SDR-Trunk restart) always wins.
- **State keys on the trimmed channel name.** SDR-Trunk channel ids are
  process-local counters reassigned on every reload, so the id is re-resolved
  from a fresh fetch immediately before every start or stop.
- **Never touches tuner assignment, sample rates, or config import/reload.** Only
  per-channel start/stop/suppress/unsuppress.
- **Suppress strictly before stop.** SDR-Trunk's 30s self-heal sweep restarts any
  non-processing auto-start channel and would otherwise win the race.
- **An unmeasured channel is never judged.** A measured sample is `SyncPercent`
  non-nil and greater than zero; null and 0 both mean unmeasured. So analog
  (NBFM/AM) channels, and channels on an older SDR-Trunk runtime, are never
  touched.
- **Every decision is logged loudly**, and the full manager state rides the
  status frame so staff can see exactly what it is doing and why.
- It **pauses** while a site survey runs, and idles by itself on a runtime that
  predates the suppression API.

`internal/decodeprobe` is the one place that turns SDR-Trunk's live channel state
into a verdict, shared by the channel manager and the site survey.

## Site survey

`internal/sitesurvey` measures how well this node can hear a list of candidate
P25 GRN sites, so the backend can decide which belong in its channel list. The
backend picks the candidates (sites in the node's LGA neighbourhood) and owns the
pass threshold and the resulting channel-list change — the agent reports raw
numbers and nothing else. Results go to
`POST /api/node-ingest/site-survey`.

Staff-triggered, plus one automatic run at first install. While a survey runs it
owns the node's channel set, which is why both the channel manager and the site
shipper pause.

## Internal packages

| Package | Does |
|---|---|
| `activityship` | ships decode-activity events to the backend |
| `agentcfg` | loads, defaults and persists the YAML config |
| `chanmgr` | automatic channel manager |
| `configapply` | backend config push → local rdio + SDR-Trunk state |
| `decodeprobe` | turns live channel state into a decode verdict |
| `enrol` | single-use code → node token |
| `keys` | stable per-agency rdio API keys |
| `presets` | `go:embed` of the base playlist and rdio config |
| `protocol` | the JSON WebSocket envelope |
| `queue` | disk-backed bounded FIFO |
| `rdioctl` | local rdio-scanner admin client |
| `relay` | localhost listener impersonating rdio's upload |
| `sdrctl` | SDR-Trunk control-server client (REST + spectrum WS) |
| `siteship` | ships P25 site snapshots |
| `sitesurvey` | measures candidate GRN sites |
| `supervise` | child-process supervision, stale-process reaping |
| `update` | manifest fetch, component install, self-update |
| `version` | build version, set with `-ldflags -X …/version.Version` |
| `wsclient` | the persistent control WebSocket |

## Honest limits

- **Encrypted traffic carries no audio.** Most police traffic on the network is
  encrypted. The reception is observed and recorded, but there is nothing to
  listen to and nothing to transcribe. No change here can alter that.
- **A node hears what its antenna hears.** Coverage is wherever contributors
  happen to live.
- The agent needs Java (for SDR-Trunk) and the disk space the runtime takes.
- SDR-Trunk's first run does a CPU calibration and installs JMBE before channels
  can start, so a fresh node is not productive immediately.

## Further reference

Two config references live beside the code, written from the fork's sources:

- `docs/sdrtrunk-config-reference.md`
- `docs/alias-rdio-config-reference.md`
- `docs/sdrtrunk-gui-layout-reference.md`

## See also

- [`forked-runtimes.md`](forked-runtimes.md) — what the two forks changed, why,
  and how they are pinned.
- [`transcription.md`](transcription.md) — what happens to the audio afterwards.
- [`../architecture.md`](../architecture.md) — the full trace from a transmission
  in the air to where it surfaces.
- [`../scanner-feed-setup.md`](../scanner-feed-setup.md) — contributing from an
  existing rdio-scanner with nothing to install.
