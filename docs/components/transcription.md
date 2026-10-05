# Transcription tier — the Whisper router and its two servers

**Entry point: `backends/node/src/services/whisperRouter.ts`,** with the HTTP
surface in `backends/node/src/api/whisper.ts`.

Radio reception audio is transcribed by faster-whisper. There are **two whisper
servers** and rdio-scanner's transcripts plugin accepts exactly **one** base URL,
so something has to choose between them per reception. That something is this
router, inside the backend.

Read [`../../AGENTS.md`](../../AGENTS.md) before changing anything here.

---

## The shape of it

```
   central rdio-scanner
   (transcripts plugin, provider = "Whisper (self-hosted)")
            │
            │  POST <backend>/api/whisper/v1/audio/transcriptions
            ▼
   api/whisper.ts  ──►  services/whisperRouter.ts
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
          pc  (preferred)            vm  (always on)
     up only while nobody            the safety net
       is using the machine
```

**Neither whisper server's source is in this repository.** They are external
hosts. The code refers to `whisper_openai_server.py` (the server) and
`whisper_watch.ps1` (the PC's idle watcher), which live on those machines.

The router lives inside the backend rather than being its own service because
everything is on one LAN, so the extra hop costs nothing on the network and one
fewer supervised process is worth more than the isolation. The cost that buys is
real and worth knowing: **a backend restart now interrupts transcription**, which
is why the deploy passes `--kill-timeout 30000` and `shutdown()` in
`src/index.ts` lets in-flight requests finish.

## Why the router exists at all

rdio's transcripts plugin takes one base URL. Point it at
`<backend>/api/whisper/v1` once and it never needs reconfiguring again,
whichever server happens to be up.

## Configuration

```
WHISPER_BACKENDS=pc=http://10.1.0.50:8000,vm=http://10.1.0.118:8000
WHISPER_ADMIN_TOKEN=<openssl rand -hex 32>
```

`"name=url,name=url"`, and **order is preference**: the first healthy,
non-draining, non-quarantined backend with a free slot takes the reception. Fast
one first, dependable one last. If that request fails outright the next backend
gets it.

A URL may carry an optional `@N` concurrency ceiling — `pc=http://x:8000@2`. The
`@N` is matched at the **end** only, so it cannot eat an `@` inside credentials.
Omitting it is preferred: the backend then follows whatever it reports as its own
`num_workers`, which keeps one source of truth, on the box.

**Not configured is not an error.** With `WHISPER_BACKENDS` unset, the whole
feature is off and `/api/whisper/status` answers 200 with `configured: false`, so
the staff panel hides its card rather than showing a failure for something nobody
turned on.

## Endpoints

| Route | Caller |
|---|---|
| `POST /api/whisper/v1/audio/transcriptions` | rdio's transcripts plugin |
| `GET /api/whisper/v1/models` | rdio's probe — answered locally |
| `GET /api/whisper/status` | the staff panel, and the PC's watcher |
| `POST /api/whisper/drain` | the PC's watcher, before a stop |
| `GET /api/whisper/history` | the staff throughput dashboard |

### Authentication is two different things here, on purpose

- **The transcription path keeps the ordinary site API key gate.** rdio has no
  login, but it does have an API key field for its whisper provider, so
  `NSWPSN_API_KEY` goes in there and no new secret has to exist. This route is
  deliberately **not** in the public-endpoint list — it is the one that spends GPU
  time.
- **`/status` and `/drain` are public to that gate**, because the PC's watcher is
  a headless script with no session and no reason to hold the site key. They
  verify their own credential instead: the panel authenticates as a user with a
  role, the watcher carries `WHISPER_ADMIN_TOKEN` in `X-Whisper-Token`. Either is
  accepted for status; **drain takes the token only**, because it is a machine
  action.

A request body is capped at 25 MB — a sanity ceiling, not a working limit. rdio
sends a few seconds of narrowband audio.

## Health

The probe is `GET /v1/models` on each backend, every **5 seconds**, with a
**3 second** timeout.

That is the right probe precisely because `whisper_openai_server.py` **loads its
model at import, before uvicorn binds**. A backend that answers at all has a
model in memory and is genuinely ready, so there is no warm-up state to guess
about.

`FAIL_THRESHOLD = 2` consecutive probe failures before a backend is taken out. One
is too eager — a single dropped packet would flap the whole feed to the other
server.

The probe also pulls the server's own `GET /v1/stats`, which reports model,
device, compute type, `waiting`, `active`, totals, average and p95, uptime and
`num_workers`. `waiting` is the reason that matters: a reception queued *inside*
whisper looks in-flight to this router, so queue depth — the "is this server
keeping up" signal — is structurally unknowable from here. The server has to say.

Each backend tracks `stateSince`, when `healthy` last flipped, because a backend
flapping every 90 seconds otherwise looks identical to a steady one.

## Quarantine — the failure the probe cannot see

Measured on this deployment: a whisper whose CUDA libraries were missing **loaded
its model, answered `/v1/models`, and 500'd every single transcription**. It
stayed "healthy", stayed preferred, and taxed every reception with a failed
attempt before the retry landed on the other server.

So consecutive *transcription* failures are counted separately:
`WORK_FAIL_THRESHOLD = 3` failures quarantines a backend for
`QUARANTINE_MS = 60_000`.

Quarantine is **deliberately not folded into `healthy`**: `probe()` marks a
backend healthy on every good probe, so a quarantine expressed that way would be
undone within 5 seconds by the very check that cannot see the problem. It is
time-boxed rather than permanent because the fault is usually fixed by a restart,
and the first reception after expiry is the re-test.

## Concurrency

A backend's ceiling comes from its own reported `num_workers`, overridden by an
explicit `@N`, falling back to `DEFAULT_MAX_IN_FLIGHT = 2`.

The default is deliberately small. Over-committing is the failure this whole
mechanism exists to prevent, and a backend that can take more will say so on its
next 5 second probe — whereas guessing high re-creates the
queue-inside-whisper problem for the seconds before the first probe lands.

When every slot is taken, a reception **queues rather than being shed**, for up to
`SLOT_WAIT_MS = 120_000`. That tradeoff is explicit: a dropped transcript is gone
for good, and a reception that waits is only slow. The wait is bounded so a total
stall surfaces as an error instead of holding rdio's connection open
indefinitely.

A single transcription may take up to `REQUEST_TIMEOUT_MS = 180_000`. Generous on
purpose — a long reception on a busy CPU backend is slow, and cutting it off loses
that transcript for good. An honest one measured on this deployment took 16.9s.

`RELAY_TIMEOUT_MS` (default 30s, `src/config.ts`) is the separate ceiling on
outbound relays generally. It exists because relays used to run with no deadline
at all, so a stalled upstream held *our* inbound request — and the sender's
connection — for as long as the socket stayed open. Measured once: four transcript
pushes held 70s, 57s, 47s and 40s before the far end dropped them all together.

## Draining — what makes the PC safe to stop

The PC can only run whisper while nobody is using it, so something has to stop the
service when its owner comes back.

**Killing whisper mid-transcription loses that reception's transcript, and rdio
does not come back for it.** So the stop sequence is:

1. the watcher (`whisper_watch.ps1`) calls `POST /api/whisper/drain` with
   `X-Whisper-Token`;
2. the router sets `draining` on that backend — *finish what you have, take
   nothing new*;
3. the watcher polls `/api/whisper/status` until `inFlight` reaches 0;
4. only then does it stop the service.

A draining backend is skipped for new work but is not unhealthy. The distinction
matters: unhealthy is a fault, draining is a plan.

The watcher identifies itself with `X-Client-Type: whisper-node`, so the backend's
request log tags it `[whisper-node]`. Without the header its
`Invoke-RestMethod` user agent reads as a browser, and its status polls
masqueraded as dashboard traffic (`src/server.ts:115`).

## Durable statistics

`backends/node/src/services/whisperStats.ts` writes `whisper_hourly`
(migration 098) — one row per (hour, backend), incremented **per attempt**, not
per reception. A reception that fails on the PC and succeeds on the VM writes a
failure row for `pc` and a success row for `vm`; one that no backend could take
writes to the reserved backend name `none`.

`ms` is recorded only for a successful attempt. Failed-attempt latency is mostly
timeouts and would poison the average.

Recording is fire-and-forget. Transcription volume is a few receptions a minute,
so a direct upsert per attempt is cheap, and a statistics failure must never fail
or even delay the transcription itself.

The router's own counters are in memory and reset on restart; `whisper_hourly` is
the persistent record the staff dashboard graphs, via `/api/whisper/history`.

## What becomes of a transcript

The transcript is stored by **rdio's `transcripts` plugin**, in the central
rdio-scanner's database — not by AusAware. The backend reads that database
**read-only** (`RDIO_DATABASE_URL`, `src/services/rdio.ts`) for:

- `/api/rdio/calls/:id` — one reception by id, with system, talkgroup and unit
  labels resolved;
- the **hourly summaries** (`src/services/llm.ts`, Gemini, prompt in
  `backends/prompts/rdio_hourly.txt`), surfaced at `/api/summaries/latest` and
  shown on `/live` and in the bot's `/summary`;
- `src/services/rdioIncidentAlerts.ts` — a burst of transcribed receptions on one
  talkgroup becomes one ntfy push, with a cooldown so one incident is one push.
  Off unless `RDIO_INCIDENT_ALERTS_ENABLED=true`.

**Never add a write path against that database.**

### Transcript search was removed

`/api/rdio/transcripts/search` is **gone** (2026-09). Its browse mode ran an
unbounded `COUNT(*)` plus a leading-wildcard `ILIKE` over the whole
calls-joined-transcripts set, every 30 seconds per open staff tab — which blew the
rdio pool's 30 second statement timeout and starved its five connections.

The staff Transcripts view is now a whisper **throughput dashboard**
(`/api/whisper/history`) that never touches the rdio database, and the Discord
bot's `/ts` command was retired with it. Do not reinstate either without solving
the query first.

## The plugin side

The transcripts plugin lives in `AkumasCoffin/rdio-scanner-plugins`
(`plugins/transcripts`) — version `1.0.12`, `minServerVersion`
`6.14.0-beta.1`. It offers Groq, OpenAI and **self-hosted Whisper**; the
self-hosted option is the one pointed at this backend. Each provider keeps its own
base URL, key and model, and the server appends `/audio/transcriptions` itself —
which is why the configured value is `<backend>/api/whisper/v1` and not the full
path.

Transcription moved out of the rdio server and into this plugin at 6.14. Full
detail: [`forked-runtimes.md`](forked-runtimes.md).

## Honest limits

- **An encrypted transmission has no audio**, so there is nothing to transcribe.
  Most police traffic on the network is encrypted. The reception is still recorded
  with its full identity — see [`../architecture.md`](../architecture.md) — but it
  will never have a transcript.
- **One of the two servers is only up part of the day.** The PC runs whisper when
  nobody is using it. Capacity is genuinely variable.
- A transcript lost to a mid-transcription kill is lost permanently. rdio does not
  retry.
- Whisper is wrong sometimes. A transcript is a lead, not a record of what was
  said.

## See also

- [`forked-runtimes.md`](forked-runtimes.md) — the plugin and the rdio fork.
- [`radio-node.md`](radio-node.md) — where the audio came from.
- [`backend.md`](backend.md) — the config and the rdio pool.
- [`../architecture.md`](../architecture.md) — the full air-to-pin trace.
