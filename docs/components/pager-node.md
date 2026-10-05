# Pager feeder node — `feeder-nodes/pager-node`

**Entry point: `feeder-nodes/pager-node/cmd/nodeagent/main.go`.**

Go 1.26, module `github.com/AkumasCoffin/nswpsn-node/pager-node`. A separate Go
module with its own `go.mod` — build it from its own directory.

What it decodes: **POCSAG pager messages** — 512, 1200 and 2400 baud — off NBFM,
using the system's `rtl_fm` and `multimon-ng`.

This is the node kind that produces **pins on the public map**: a page carries an
address, the backend parses coordinates out of the message body, and
`/api/pager/hits` becomes markers on `map.html`.

Read [`../../AGENTS.md`](../../AGENTS.md) before changing anything here.

---

## What it is

Smaller than the radio agent, and deliberately so: it **manages no external
installs**. The readers drive system-installed `rtl_fm`, `multimon-ng` and `curl`,
so the agent only ever self-updates its own binary.

```
   antenna ─► RTL-SDR (addressed by USB serial)
                   │
                   ▼
        reader.sh:  rtl_fm ──► multimon-ng ──► curl
                   (NBFM)      (POCSAG)         │
                                                ▼
                   internal/relay  (localhost, POST /pager, always 200)
                                                │
                                   internal/pagerdecode parses the line
                                                ▼
                   internal/queue  (disk FIFO, one file per message)
                                                │
                                                ▼
                   POST /api/node-ingest/pager-upload
                                                │
                   backend substitutes PAGERMON_INGEST_API_KEY
                                                ▼
                         central Pagermon  (self-hosted)
                                                │
                     src/sources/pager.ts polls PAGERMON_URL every 60s
                                                ▼
                     /api/pager/hits  ──►  a pin on map.html
```

## Build and run

```bash
cd feeder-nodes/pager-node
go build ./cmd/nodeagent
go test ./...
```

```
nodeagent [--config <path>] <run|version|install|uninstall|start|stop>
```

Same shape as the radio agent: OS service integration via
`github.com/kardianos/service`, service name **`nswpsn-node`**, and `run` working
both under a service manager and in the foreground. Sharing the service name with
the radio agent is fine — a pager node runs on its own machine.

Configuration is YAML (`internal/agentcfg`); `agent.example.yaml` is the
template.

**Linux only**, because of the `rtl_fm | multimon-ng` stack. `deploy.sh` builds
`linux-amd64` and `linux-arm64` — the arm64 build is for a Raspberry Pi.

## Enrolment

Identical to the other agents. The installer writes a single-use enrolment code
and an empty token; on first run `internal/enrol` trades the code at
`POST /api/node-enrol` for a node token, which it persists. Every later request
carries `X-Node-Token` + `X-Node-Install`, with
`User-Agent: NSWPSN-NodeAgent/<version>`.

## The readers

`internal/reader` renders one `reader.sh` per frequency from an embedded template
(`reader.sh.tmpl`, `go:embed`). Each is a self-contained bash pipeline:

```
rtl_fm -d <serial> -f <MHz> -p <ppm> -g <gain>  →  multimon-ng -a POCSAG512 -a POCSAG1200 -a POCSAG2400  →  curl → the agent's loopback relay
```

`internal/supervise` launches each rendered script as a component and restarts it
on crash.

Everything interpolated into the script is rendered from validated values —
numbers from parsed floats, never from received text — because the settings
arrive from a server config push.

### Frequencies

`cmd/nodeagent/readermgr.go` holds the locked default plan, used until a
`configPush` supplies one, in priority order:

| Label | Frequency | Service |
|---|---|---|
| `NSWRFS` | 148.5875 MHz | NSW Rural Fire Service |
| `FRNSW` | 148.9875 MHz | Fire & Rescue NSW |

**At most two readers run** (`maxReaders = 2`), because the locked plan is two
frequencies. One dongle means the first frequency only; two or more means the
first two, and extra dongles are idle spares.

### Why dongles are addressed by serial

`internal/pagersdr` enumerates dongles by shelling out to `rtl_test` and
`rtl_eeprom` (installed on the host by the node installer), and its key job is
guaranteeing each has a **unique, stable USB serial** so a reader can be pinned
to a specific antenna and frequency with `rtl_fm -d <serial>`.

Cheap RTL-SDR clones ship with identical serials (`00000001`), which makes
per-device addressing ambiguous. The agent rewrites an EEPROM serial **only when
one actually collides** — an EEPROM write is a permanent hardware change, so a
dongle that already has a distinct serial is left untouched.

## Decoding and relaying

`internal/pagerdecode` parses `multimon-ng` stdout. One decoded page per line:

```
POCSAG1200: Address:  1234567  Function: 0  Alpha:   SOME MESSAGE TEXT
POCSAG512: Address:  0987654  Function: 3  Numeric:  123 456
```

It captures the capcode, the function bits (0–3), whether the payload is
alphanumeric or numeric, and the text. Anything that is not a POCSAG line —
multimon's banner, stderr noise, blanks — is ignored.

`internal/relay` is the loopback listener. Each reader POSTs one raw line to
`POST /pager` with `X-Pager-Source` and `X-Pager-Freq` headers. The listener
parses it and, on a decodable page, enqueues a normalised JSON message. It
**must always answer 200** so the fire-and-forget reader `curl`s never block or
retry. A single inbound line is capped at 64 KiB as a sanity bound.

The reader can also tee `rtl_fm`'s 22050 Hz s16le mono PCM to a `/audio`
endpoint on the same listener, for a staff audio monitor. Off unless configured.

`internal/queue` is the same disk-backed, bounded FIFO the radio agent uses — one
file per message, named `<20-digit-zero-padded-unixnano>-<rand4>.call`. It is
what makes the node survive its own internet.

## Credentials stay on the server

The agent relays to `POST /api/node-ingest/pager-upload`
(`backends/node/src/api/node-ingest.ts`). The **backend** then forwards into the
central Pagermon with the server-held `PAGERMON_INGEST_API_KEY`.

**A pager node never holds a Pagermon credential.** It authenticates with its own
node token and nothing else. That is the point of the indirection: cutting a
node's feed is toggling its flag in the staff panel, not rotating a shared key.
Never write code or documentation that implies otherwise.

Routing is **per state**. A node's messages go to the Pagermon for `nodes.state`:
the unsuffixed `PAGERMON_INGEST_URL` / `PAGERMON_INGEST_API_KEY` pair is NSW (the
legacy default, overridable from the staff-set `feeder_global_config` row), and
every other state is env-only — `PAGERMON_INGEST_URL_QLD` and so on. Bringing a
new state online is: stand up its Pagermon, add its pair, done. When neither a
DB value nor an env pair is set, `pager-upload` returns 503.

`PAGERMON_URL` is a **different thing** — the read path the backend polls to draw
pages on the map. Do not confuse the two.

### Blocked capcodes

Some capcodes are non-message transmitters that fire encoded or binary data
frames rather than readable pages — FRNSW capcode `521839` emits roughly one
base64 frame a minute. `PAGER_BLOCKED_CAPCODES` (default `521839`) drops them
before the Pagermon forward **and** before the staff drawer buffer.

## The control WebSocket

`internal/wsclient` holds a persistent outbound connection to
`/api/node-ws/agent`: a `hello` on connect, a status heartbeat every **15s**,
answers to `cmd` frames, **applies pushed pager config**, and a ping every **30s**
because Cloudflare Tunnel kills an idle WebSocket at around 100s. Reconnects with
backoff.

Envelope: `{ "t": type, "id"?: correlation, "data"?: payload }`
(`internal/protocol`).

## Self-update

`internal/update` fetches the manifest from the backend. The pager agent has **no
managed external components**, so unlike the radio agent it only ever
self-updates the `agent` component — its own binary.

`/api/node-updates/manifest` is kind-aware: a pager node is served the
`pager-agent` entry from `backends/node/assets/node-versions.json` *as* its
`agent` component. The flow is download to `.pending`, sha256-verify, self-replace
(an in-place rename and re-exec on Unix), restart. An empty URL or empty sha256
is treated as "nothing to do" — not an error.

**A change to this agent's Go code does not reach any node until
`pager-agent.version` is bumped in that manifest.** `deploy.sh` skips the rebuild
when the built binary already reports the manifest version.

## Internal packages

| Package | Does |
|---|---|
| `agentcfg` | loads, defaults and persists the YAML config |
| `enrol` | single-use code → node token |
| `pagerdecode` | parses multimon-ng POCSAG lines |
| `pagersdr` | enumerates dongles, guarantees unique USB serials |
| `protocol` | the JSON WebSocket envelope |
| `queue` | disk-backed bounded FIFO |
| `reader` | renders `reader.sh` per frequency |
| `relay` | loopback listener for decoded lines (and optional PCM tee) |
| `supervise` | child-process supervision, stale-process reaping |
| `update` | manifest fetch and self-update |
| `version` | build version, set with `-ldflags -X …/version.Version` |
| `wsclient` | the persistent control WebSocket |

## Honest limits

- Pager traffic is unencrypted by nature, so unlike radio there is no
  encryption ceiling here. The ceiling is **coverage**: a node hears what its
  antenna hears, and there is none where nobody has put one up.
- At most two frequencies per node.
- **A page without coordinates is never mapped.** An FRNSW `FRINC` turnout, for
  instance, has no location in the body. `lat`/`lon` are nullable in
  `backends/node/src/sources/pager.ts` precisely so the distinction survives:
  coordinate-less pages are archived and appear in `/logs`, but get no pin.
- Linux only.

## See also

- [`../architecture.md`](../architecture.md) — the full trace, including the
  pager path that ends in a pin.
- [`backend.md`](backend.md) — `api/node-ingest.ts` and `sources/pager.ts`.
- [`forked-runtimes.md`](forked-runtimes.md) — the Pagermon fork the backend
  forwards into.
