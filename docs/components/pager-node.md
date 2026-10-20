# Pager feeder node — `feeder-nodes/pager-node`

**Entry point: `feeder-nodes/pager-node/cmd/nodeagent/main.go`.**

Go 1.26, module `github.com/AkumasCoffin/nswpsn-node/pager-node`. A separate Go
module with its own `go.mod` — build it from its own directory.

What it decodes: **POCSAG pager messages** — 512, 1200 and 2400 baud — off NBFM,
using the system's `rtl_fm` (or `rtl_sdr`, when one dongle carries several
frequencies) and `multimon-ng`.

This is the node kind that produces **pins on the public map**: a page carries an
address, the backend parses coordinates out of the message body, and
`/api/pager/hits` becomes markers on `map.html`.

Read [`../../OPERATING-RULES.md`](../../OPERATING-RULES.md) before changing anything here.

---

## What it is

Smaller than the radio agent, and deliberately so: it **manages no external
installs**. The readers drive system-installed `rtl_fm`, `rtl_sdr` (both from the
`rtl-sdr` package the installer already pulls), `multimon-ng` and `curl`, so the
agent only ever self-updates its own binary.

```
   antenna ─► RTL-SDR (identified by USB serial, opened by index)
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

`internal/reader` renders one `reader.sh` per dongle in use, from an embedded
template (`go:embed`). Each is a self-contained bash pipeline. A dongle carrying
one frequency gets `reader.sh.tmpl`:

```
rtl_fm -d <index> -f <MHz> -p <ppm> -g <gain>  →  multimon-ng -a POCSAG512 -a POCSAG1200 -a POCSAG2400  →  curl → the agent's loopback relay
```

A dongle carrying several gets `reader-multi.sh.tmpl` (see
[Several frequencies on one dongle](#several-frequencies-on-one-dongle)):

```
rtl_sdr -d <index> -f <centre Hz> -s <rate> -p <ppm> -g <gain>
  → nodeagent-pager channelize <rate> <centre> <Hz> >(multimon-ng … → curl) <Hz> >(multimon-ng … → curl) …
```

`internal/supervise` launches each rendered script as a component and restarts it
on crash.

`-p` is per dongle, measured for each dongle in use at startup and again on every rescan by
`pagersdr.MeasurePPM`. It runs `rtl_test -p` — which compares the RTL's actual
sample rate against the host clock, so no antenna or signal is needed — and waits
for the reading to **settle**: three consecutive cumulative readings agreeing to
within 2 ppm. `PPMMeasureDur` is a **one-minute ceiling with an early exit**, not
a fixed run; a healthy dongle settles in about thirty seconds and the run is
interrupted (with SIGINT, so the USB device is released cleanly) the moment it
does. Settling is the whole job: taking whatever the last line said after a fixed
run is two or three readings still dominated by USB transfer jitter, and a bogus
94 ppm at 148.5875 MHz is ~14 kHz of detune, which walks the POCSAG carrier out of
`rtl_fm`'s passband at `-s 22050`. The reader then decodes nothing while the
heartbeat still reports it running. A reading that never settles falls back to
`ppm=0`, and so does one outside **±100 ppm** (`maxPlausiblePPM`): a clock that far
out is faulty, and writing an absurd correction is worse than writing none. Zero
is close enough for wide NBFM.

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

The backend pushes each state's plan (`PAGER_PLANS` in
`backends/node/src/services/nodes/configMerge.ts`), which overrides this default.

`channelize.Assign` decides which dongle carries which frequency, and the answer
is normally **one reader on dongle 0**:

- **The first dongle carries every frequency it can fit**, packed first-fit in
  priority order, so the first one is always placed. Both NSW frequencies share
  it; a state with one frequency runs it on the plain `rtl_fm` reader.
- **A further dongle is only taken for a frequency that fits on none in use**
  (too far from the others, or past `MaxChannelsPerDongle` = 8). Dongles beyond
  that stay idle, however many are plugged in.
- A frequency no dongle can take is **not received**, and the agent logs it by
  name. The priority order (the staff drawer's "Priority frequency") decides
  what keeps its place.
- If the agent cannot resolve its own path it has no `channelize`, and falls back
  to one frequency per dongle.

Detection follows the same plan (`detectDevices`). Only the dongles that will
carry a reader are measured for ppm, because each measurement holds its dongle
for up to a minute with every reader waiting, so measuring an idle spare only
lengthens the deaf spell on every boot. Serials are only made unique when two or
more dongles are in use (see below). A dongle that later picks up a reader
without having been measured runs at `ppm=0`, and the agent says so, until the
next "Recheck SDRs".

### Several frequencies on one dongle

`rtl_fm` demodulates one frequency. Given several `-f` flags it becomes a
**scanner**: it sits on one frequency until the squelch closes, then hops. That
hears one channel at a time, so a page on one frequency is lost while the other
is busy — and NSW RFS and Fire & Rescue NSW often page for the same job at the
same moment. It also refuses to start without a squelch level (which depends on
gain and site noise), and it never says which frequency a page came from, so
every page would carry the same label.

So a shared dongle runs `rtl_sdr` instead, which hands over the raw I/Q around a
centre frequency, and the agent's own `channelize` subcommand
(`internal/channelize`) splits it. For each frequency it mixes the channel down
to zero, low-pass filters and decimates in two stages to 22050 Hz, FM-discriminates
and removes DC. That is the audio `rtl_fm -s 22050 -E dc` produced — same rate,
same scaling, same discriminator polarity — so each stream feeds its own
`multimon-ng` exactly as before, through a bash process substitution, and its
decoded lines go to the relay under its own label and frequency.

How a shared dongle is tuned (`channelize.fit`):

- **Sample rate**: the lowest of 1,058,400 / 1,411,200 / 2,116,800 / 2,381,400 S/s
  that spans the frequencies. Each is a whole multiple of 22050, so there is no
  resampling. The NSW pair, 375 kHz apart, takes the lowest; the top rate spans
  about 1.88 MHz between the outermost channels.
- **Every channel inside ±40% of the rate**, where the RTL2832's own filter is
  still flat.
- **Every channel at least 50 kHz from the centre**, clear of the DC spike, which
  is why `rtl_fm` never tunes onto the frequency it demodulates either.
- **No channel on another's image** (mirrored about the centre). R820T dongles
  have no image to speak of; E4000-style zero-IF tuners do.

The cost is CPU, which scales with the sample rate. On one desktop core the NSW
pair splits at about 46× real time (`BenchmarkTwoChannels`), and each channel
gets a core of its own. A Raspberry Pi core is several times slower than that;
it has not been measured on one yet.

`channelize` lives in the agent binary (`nodeagent-pager channelize …`), so a
node needs nothing installed beyond what the plain reader uses, and it updates
with the agent. If any channel's `multimon-ng` dies, `channelize` exits and the
supervisor restarts the whole reader.

In the staff drawer nothing changes shape: the heartbeat still reports one
`reader:<label>` per frequency (the shared component is
`reader:<label>+<label>`), Listen taps that one frequency's audio, and Restart
on any of them restarts the dongle's reader, so its other frequencies drop for
the same couple of seconds.

How it was verified: unit tests synthesise two FSK transmitters keyed at the same
time and check every bit comes out on the right channel and none on the other
(512, 1200 and 2400 baud; a 5 kHz mistune; a 3× stronger neighbour 25 kHz away),
and that read sizes never change an output sample. `multimon_test.go` encodes
real POCSAG pages, runs them through `channelize` into `multimon-ng`, and checks
each decodes on its own frequency; it skips where `multimon-ng` is not on `PATH`.
The rendered script was also run end to end under bash against multimon-ng 1.3.0,
with a stand-in `rtl_sdr` replaying recorded I/Q. It has **not** yet run on real
hardware.

### Why dongles are identified by serial but opened by index

`internal/pagersdr` enumerates dongles by shelling out to `rtl_test` and
`rtl_eeprom` (installed on the host by the node installer), and its key job is
guaranteeing each dongle in use has a **unique, stable USB serial**. The serial is
the dongle's identity, and what the EEPROM work exists to protect.

Cheap RTL-SDR clones ship with identical serials (`00000001`), which makes that
identity ambiguous. The agent rewrites an EEPROM serial **only when one actually
collides and two or more dongles are in use** — an EEPROM write is a permanent
hardware change, so a dongle that already has a distinct serial is left
untouched, and so is every dongle on a node running one reader, spare or not.

In practice readers take dongles in **index order** (the reader for the first
group goes on index 0), not by serial, so the serial pins nothing by itself; it
is how a log line or a script on disk names the physical dongle.

What `rtl_fm -d` (or `rtl_sdr -d`) receives, though, is the **librtlsdr device index** the agent
resolved for that serial, not the serial itself. librtlsdr's
`verbose_device_search()` runs `strtol` over the `-d` argument *first* and, when
the whole string parses to a number below the device count, treats it as an
index — only otherwise does it compare serials. The factory serial `00000001`
parses (base 0, so the leading zeros make it octal) to 1, so on a two-dongle node
`rtl_fm -d 00000001` opened device 1: both readers resolved to the same dongle,
the second died on `usb_claim_interface` after five restarts, and one of the two
locked frequencies went permanently silent. A serial carrying a dash or an
underscore had the opposite failure — sanitised into a string no device reports,
so that reader could never start at all.

So the agent resolves serial → index itself, freshly at every detect and rescan,
and hands `rtl_fm` the index, which has exactly one meaning
(`internal/reader/reader.go`). The serial is still recorded in a comment at the
top of each generated `reader.sh`, so a script on disk says which physical dongle
it was written for.

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

The reader can also tee one frequency's 22050 Hz s16le mono PCM (`rtl_fm`'s, or
that channel's stream out of `channelize`) to a `/audio` endpoint on the same
listener, for a staff audio monitor. Off unless configured.

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
`pager-agent.version` is bumped in that manifest**, and that bump is **also all
that is needed**. `deploy.sh` skips the rebuild when the built binary already
reports the manifest version, so the bump is what triggers the cross-build into
`<webroot>/downloads` — and the backend fills the entry's sha256 at serve time by
hashing the binary sitting there, so the empty digests committed in the manifest
are never the ones a pager node is served. A node running an older version picks
the new binary up on its next check and self-replaces, so treat the bump as a
deploy to the live pager fleet. Details:
[`forked-runtimes.md`](forked-runtimes.md#how-a-version-bump-reaches-a-running-node).

## Internal packages

| Package | Does |
|---|---|
| `agentcfg` | loads, defaults and persists the YAML config |
| `channelize` | decides which dongle carries which frequencies, and splits one dongle's I/Q into a 22050 Hz audio stream per frequency |
| `enrol` | single-use code → node token |
| `pagerdecode` | parses multimon-ng POCSAG lines |
| `pagersdr` | enumerates dongles, guarantees unique USB serials |
| `protocol` | the JSON WebSocket envelope |
| `queue` | disk-backed bounded FIFO |
| `reader` | renders `reader.sh` per dongle (rtl_fm for one frequency, channelize for several) |
| `relay` | loopback listener for decoded lines (and optional PCM tee) |
| `supervise` | child-process supervision, stale-process reaping |
| `update` | manifest fetch and self-update |
| `version` | build version, set with `-ldflags -X …/version.Version` |
| `wsclient` | the persistent control WebSocket |

## Honest limits

- Pager traffic is unencrypted by nature, so unlike radio there is no
  encryption ceiling here. The ceiling is **coverage**: a node hears what its
  antenna hears, and there is none where nobody has put one up.
- A dongle shares only frequencies within about 1.88 MHz of each other, and at
  most eight. Anything beyond what the node's dongles can take is not received
  (it is logged by name, not dropped silently).
- Frequencies sharing a dongle share its antenna and gain: one gain setting
  covers all of them.
- **A page is left off the map only if the backend's coordinate parser found
  nothing at all.** An FRNSW `FRINC` turnout, for instance, has no
  comma-separated number pair anywhere in the body, so it archives and appears
  in `/logs` with no pin — `lat`/`lon` are nullable in
  `backends/node/src/sources/pager.ts` precisely so that distinction survives.
  But the parser's fallback pattern treats *any* bare comma-separated number
  pair in the body as coordinates, bracketed or not, with no range check on
  the result — a unit number next to a street number can parse as a
  coordinate and get a pin. A page without real coordinates is not guaranteed
  to be mapless.
- Linux only.

## See also

- [`../architecture.md`](../architecture.md) — the full trace, including the
  pager path that ends in a pin.
- [`backend.md`](backend.md) — `api/node-ingest.ts` and `sources/pager.ts`.
- [`forked-runtimes.md`](forked-runtimes.md) — the Pagermon fork the backend
  forwards into.
