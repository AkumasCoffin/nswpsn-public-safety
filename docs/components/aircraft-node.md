# Aircraft (ADS-B) feeder node — `feeder-nodes/aircraft-node`

**Entry point: `feeder-nodes/aircraft-node/cmd/nodeagent/main.go`.**

Go 1.26, module `github.com/AkumasCoffin/nswpsn-node/aircraft-node`. A separate
Go module with its own `go.mod` — build it from its own directory.

What it decodes: **ADS-B Mode S at 1090 MHz**, via a supervised `dump1090`
decoder. It reads the decoder's JSON output files rather than a network socket.

**Install `dump1090-fa`.** That is the default and the preferred build —
`dump1090_bin: "dump1090-fa"` in `feeder-nodes/aircraft-node/agent.example.yaml:51`,
and the installer sets it up. `dump1090-mutability` speaks the same
`--write-json` contract and works too (`agent.example.yaml:44-45`). This doc
says "`dump1090`" generically where either will do, and names `dump1090-fa`
where the exact binary matters.

Read [`../../OPERATING-RULES.md`](../../OPERATING-RULES.md) before changing anything here.

---

## Why it exists when public aggregators already have ADS-B

AusAware already fans out across adsb.lol, adsb.fi and airplanes.live
(`backends/node/src/sources/adsb.ts`, 8s). The node fleet is a different thing:
its own hardware, three times fresher than the aggregators' roughly 15s effective
cadence, and with coverage and range statistics AusAware owns.

Both exist side by side. Do not conflate them — `/api/adsb/aircraft` is the
aggregator fan-out; the node path is `/api/node-ingest/adsb-upload` and the
`services/nodes/adsb*.ts` family.

## What it is

```
   antenna ─► RTL-SDR ─► dump1090-fa  (--write-json <dir>)
                               │
                   aircraft.json + stats.json, rewritten ~1×/s
                               │
                internal/decoderjson reads both files
                               │
                  snapshot loop, every 5 seconds
                               │
                internal/queue  (disk FIFO, 90s age bound)
                               │
                               ▼
                POST /api/node-ingest/adsb-upload
                               │
            services/nodes/adsb*.ts — per-node stats, coverage
            folds, range, tracks, daily counters
```

Like the pager agent and unlike the radio agent, it **manages no external
installs**: the decoder is a system package, so the agent only ever self-updates
its own binary.

`cmd/nodeagent/aircraftmgr.go` owns the receiver — the supervised child, the
adaptive gain loop, and the snapshots. It is the counterpart of the pager agent's
`readerManager` and plays the same three roles (config applier, status provider,
stats provider), so the shared WebSocket client needs no knowledge of which kind
of node it is running on.

## Build and run

```bash
cd feeder-nodes/aircraft-node
go build ./cmd/nodeagent
go test ./...
```

```
nodeagent [--config <path>] <run|version|install|uninstall|start|stop>
```

OS service integration via `github.com/kardianos/service`, service name
**`nswpsn-node`** — shared with the other agents, which is fine because an
aircraft node runs on its own machine.

Configuration is YAML (`internal/agentcfg`); `agent.example.yaml` is the
template.

**Linux only**, because of the `dump1090` stack. `deploy.sh` builds
`linux-amd64` and `linux-arm64` — the arm64 build is for a Raspberry Pi.

## Enrolment

Identical to the other agents: the installer writes a single-use enrolment code
and an empty token, `internal/enrol` trades it at `POST /api/node-enrol` on first
run, and every later request carries `X-Node-Token` + `X-Node-Install` with
`User-Agent: NSWPSN-NodeAgent/<version>`.

**An exact antenna position is mandatory for this node kind.** Without lat/lon
`dump1090` reports no range statistics at all and cannot decode surface
positions, which is why the backend makes the map pin required here and optional
elsewhere.

## The decoder

`internal/decoder` renders a small `dump1090` launch script into the agent's own
data directory and supervises `bash <script>`, mirroring the pager agent's
`reader.sh` pattern. The supervised child is named **`dump1090`** — the staff UI
keys its restart button on that exact string.

The script path is unique to this agent, and that is what makes `KillStale` safe
at startup: matching a bare `dump1090` would kill a decoder the operator is
running for their own purposes.

Everything interpolated into the script is validated first. The values arrive
from a server config push, so numbers are rendered from parsed floats and never
from the received text — a launcher that pastes remote strings into a shell
command is the kind of thing that becomes a problem later.

Swapping the decoder waits **1500 ms** after the kill before starting the
replacement, so the USB device is released first. Without the settle the new
process hits `usb_claim_interface error` and crash-loops — the same rationale as
the pager agent's reader swap.

## Why JSON files, not the SBS socket

`internal/decoderjson` reads the two files `dump1090 --write-json <dir>` rewrites
about once a second:

- `aircraft.json` — every aircraft currently tracked;
- `stats.json` — decoder counters over several time windows.

This is the same contract `tar1090` and `graphs1090` consume, and it was chosen
over the SBS-1 network output on port 30003 deliberately. SBS delivers a stream of
partial messages a client has to reassemble into aircraft state itself, and
carries neither category nor signal strength. `aircraft.json` **is** the assembled
state, already deduplicated and aged by the decoder that owns the radio.

It also means **the decoder needs no network stack at all**: no `--net`, no
listening sockets, nothing to firewall. There is a test asserting no `--net`,
`--net-only` or `--net-bind-address` flag creeps back in
(`internal/decoder/decoder_test.go`), with the reason written into the test.

Both files are written to a temp name and renamed into place, so a reader never
sees a half-written file — but it *can* see `ENOENT` while the decoder is
starting, which callers treat as "not ready", never as an error.

## Snapshots

The loop in `cmd/nodeagent/snapshot.go` reads what the decoder can currently see,
queues it, and keeps the heartbeat figures current.

**Every upload supersedes the previous one.** A snapshot is a complete restatement
of this receiver's view, not a delta — which is what lets the queue drop stale
entries on age alone, with no reconciliation.

`snapshotInterval` is **5 seconds**, and the number is chosen against three
constraints at once:

- three times fresher than the public aggregators' ~15s effective cadence, which
  is much of the point of running the hardware;
- well inside the queue's **90s age bound**, so a network blip of ~18 snapshots
  still drains rather than expiring;
- 12 uploads a minute, comfortably under the server's 20-per-minute limiter.

## Adaptive gain

`internal/autogain` adapts gain to the site. There is no single correct gain for
ADS-B: the right value depends on the antenna, the feedline loss, and what else
is radiating nearby.

The loop watches the share of messages arriving **strong** — loud enough that the
front end is being over-driven — and walks gain down when there are too many, up
when there are too few.

| Constant | Value | Meaning |
|---|---|---|
| `StrongHigh` | 0.10 | above this share, the front end is over-driven: gain down |
| `StrongLow` | 0.02 | below this, there is headroom to hear further: gain up |

The gap between the two is a hysteresis band — inside it nothing happens, which
is what stops the loop oscillating around a threshold. The loop is deliberately
**asymmetric**, reacting sooner to overload than to quiet, because overload is the
more damaging failure: a saturated front end loses distant aircraft entirely,
while slightly low gain merely trims the fringe.

This is **not** the dongle's hardware AGC (`--gain -10`). That responds to total
band energy on millisecond timescales, which for short bursty 1090 transmissions
means it rides down on interference and misses the packets that matter. Every
serious ADS-B setup uses a fixed gain; the question is only which one, and this
answers it per site.

From `dump1090`'s point of view gain is **always fixed** — the agent supplies a
concrete value on every restart, so the adaptation lives in the agent, not in the
decoder. Each change restarts the decoder (about 2s of lost reception plus a USB
settle), so the loop runs on a multi-minute cadence and logs every step. Cheap to
get right slowly; expensive to thrash.

## Crystal correction

`internal/sdrppm` measures the dongle's crystal error by running `rtl_test` and
reading its cumulative figure. A cheap RTL dongle's oscillator is typically tens
of ppm off its nominal 28.8 MHz and drifts further as the board warms up; at
1090 MHz, 30 ppm is a meaningful offset.

**Waiting for the reading to settle is the whole job.** `rtl_test` prints a
cumulative figure every ten seconds and its own banner says to "press ^C after a
few minutes" — early readings are dominated by USB transfer jitter and startup
transients. An earlier version ran for twenty seconds and took the last line,
which is one or two readings of noise: one node measured −1, 28, −94 and 10 ppm
on four consecutive boots of the same dongle, and −94 ppm is about 102 kHz at
1090 MHz. Applying it would have been far worse than no correction.

So a reading is accepted only once consecutive ones agree, and the run stops as
soon as they do — about thirty seconds on a healthy dongle rather than the full
budget. A dongle that never settles yields an error, and the caller keeps its
previous correction or runs without one.

A **staff-pushed ppm always wins.** The measurement is used only when the backend
has not pushed an explicit value: someone who typed a number meant it.

## The control WebSocket

`internal/wsclient` holds a persistent outbound connection to
`/api/node-ws/agent`: `hello` on connect, a status heartbeat every **15s**,
answers to `cmd` frames, **applies pushed decoder config**, and a ping every
**30s** because Cloudflare Tunnel kills an idle WebSocket at around 100s.
Reconnects with backoff.

Envelope: `{ "t": type, "id"?: correlation, "data"?: payload }`
(`internal/protocol`).

## Self-update

`internal/update` fetches the manifest. No managed external components, so only
the `agent` component — its own binary. `/api/node-updates/manifest` is
kind-aware: an ADS-B node is served the `adsb-agent` entry from
`backends/node/assets/node-versions.json` *as* its `agent` component. Download to
`.pending`, sha256-verify, self-replace, restart. An empty URL or sha256 means
"nothing to do".

**A change to this agent's Go code does not reach any node until
`adsb-agent.version` is bumped in that manifest** — `node-versions.json` says so
in its own comment, because `deploy.sh` skips the Go rebuild when the built
binary already reports the manifest version.

Bumping the version is necessary but **not currently sufficient**: the
`adsb-agent` entry's sha256 is empty on both platforms, and an empty sha256 is
treated as nothing to do, so self-update is dormant and a changed agent reaches
a node only via a fresh install. Details:
[`forked-runtimes.md`](forked-runtimes.md#self-update-is-dormant-for-the-agents).

## What the backend does with it

`backends/node/src/services/nodes/adsb*.ts`:

| Module | Keeps |
|---|---|
| `adsbNodeStore.ts` | per-node receptions, snapshot range, daily counters (flushed every minute) |
| `adsbCoverage.ts` | coverage folds |
| `adsbSeries.ts` | time series for the staff graphs |
| `adsbNodeView.ts` | the staff-facing shape |
| `nodeTrackArchive.ts` | archived tracks |

The daily-counter flush is **awaited** at shutdown, so the partial minute is
written out rather than discarded on every restart.

Receiver identity and location are **role-gated** (`/api/adsb/receivers`). The
public surface never reveals which receivers exist or where they are.

## Internal packages

| Package | Does |
|---|---|
| `agentcfg` | loads, defaults and persists the YAML config |
| `autogain` | adapts receiver gain to the site |
| `decoder` | renders the `dump1090` launch script |
| `decoderjson` | reads `aircraft.json` / `stats.json` |
| `enrol` | single-use code → node token |
| `protocol` | the JSON WebSocket envelope |
| `queue` | disk-backed bounded FIFO, 90s age bound |
| `sdrppm` | measures crystal error |
| `supervise` | child-process supervision, stale-process reaping |
| `update` | manifest fetch and self-update |
| `version` | build version, set with `-ldflags -X …/version.Version` |
| `wsclient` | the persistent control WebSocket |

## Honest limits

- A receiver hears what its antenna hears. 1090 MHz is line-of-sight, so range is
  mostly a question of height and horizon.
- An exact antenna position is mandatory, not optional.
- Linux only.
- `dump1090-fa` must already be installed — the agent does not install it.
- Gain adaptation costs about 2s of reception per step, so it moves slowly by
  design.

## See also

- [`../architecture.md`](../architecture.md) — the ADS-B layer, and why it is two
  things.
- [`backend.md`](backend.md) — `api/node-ingest.ts`, `sources/adsb.ts` and the
  `services/nodes/adsb*.ts` family.
