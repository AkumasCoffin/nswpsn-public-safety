# NSW PSN feeder-node installers

Packaging for the `nodeagent` binary (built from `../cmd/nodeagent`). Each
installer places the agent, writes an `agent.yaml` carrying the volunteer's node
token + server URL, and registers a background service. The heavy runtimes
(headless SDR-Trunk, rdio-scanner) are **not** bundled — the agent downloads
them on first run from the release manifest, keeping the installers small.

```
installers/
  windows/setup.iss          # Inno Setup 6 script -> NSWPSN-Node-Setup.exe
  linux/install.sh           # self-contained root installer
  linux/nodeagent.service    # canonical systemd unit (mirrored inside install.sh)
  linux/99-nswpsn-sdr.rules  # canonical udev rules  (mirrored inside install.sh)
```

## Token delivery

Both platforms deliver the personalised token without asking the volunteer to
type anything:

- **Windows** — the setup EXE parses **its own filename** for `npsn_` + 40 hex
  chars. The site serves the download named
  `NSWPSN-Node-Setup_<npsn_...token...>.exe`, so double-clicking is enough.
  If the filename has no token (e.g. the browser renamed it), a wizard page
  prompts for it, plus an optional server URL.
- **Linux** — the download endpoint returns a copy of `install.sh` with the
  token prefilled into the `INLINE_TOKEN=""` line near the top. The token can
  also come from `NODE_TOKEN=` / `--token`, and `SERVER_URL=` / `--server`.

The token is validated against `^npsn_[0-9a-f]{40}$` on both platforms.

## Windows — building `NSWPSN-Node-Setup.exe`

1. Build the agent for Windows and drop `nodeagent.exe` next to `setup.iss`
   (or pass its path with `/DAgentExe=...`):
   ```
   GOOS=windows GOARCH=amd64 go build -o nodeagent.exe \
     -ldflags "-X github.com/AkumasCoffin/nswpsn-node/radio-node/internal/version.Version=0.1.0" \
     ./cmd/nodeagent
   ```
2. Compile with Inno Setup 6:
   ```
   iscc windows\setup.iss
   # or, agent elsewhere:
   iscc /DAgentExe="C:\build\nodeagent.exe" windows\setup.iss
   ```
   Output: `windows\Output\NSWPSN-Node-Setup.exe`.
3. The website renames each download per volunteer to
   `NSWPSN-Node-Setup_<token>.exe`. (Renaming does **not** invalidate the
   Authenticode signature — sign the base EXE once; see below.)

What the installer does:

- Installs `nodeagent.exe` to `%ProgramFiles%\NSWPSN Node\`.
- Writes `%ProgramData%\NSWPSN Node\agent.yaml` (token + server + a `data_dir`
  under ProgramData). An existing `install_id` is **preserved** across
  re-installs / upgrades.
- Runs `nodeagent.exe install --config "<ProgramData>\NSWPSN Node\agent.yaml"`
  to register + start the Windows service; `nodeagent.exe uninstall` on removal.
- Finish page + an optional post-install checkbox link to **Zadig**.

## Linux — how `install.sh` works

Run as root (it re-execs itself with `sudo -E` if needed):
```
sudo bash install-nswpsn-node.sh
```

It is idempotent — re-run it to update the token or pull a newer binary. Steps:

- Detects arch (`amd64` / `arm64`) and downloads
  `${RELEASE_BASE}/nodeagent-linux-<arch>` to `/opt/nswpsn-node/nodeagent`.
- Creates the `nswpsn-node` system user (nologin) in the `plugdev` group.
- Writes `/etc/nswpsn-node/agent.yaml` (`data_dir=/var/lib/nswpsn-node`),
  preserving an existing `install_id`; config is `root:nswpsn-node 0640`.
- Installs the udev rules to `/etc/udev/rules.d/99-nswpsn-sdr.rules` and reloads
  them (`udevadm control --reload-rules && udevadm trigger`).
- Installs the systemd unit, `systemctl daemon-reload`, then `enable` + start.

The `.service` and `.rules` files in `linux/` are the canonical sources; their
contents are mirrored inside `install.sh` so the single-file download the
backend generates is fully self-contained.

## RTL-SDR driver note (Windows)

RTL-SDR dongles need the **WinUSB** driver on Windows. Volunteers install it
with [Zadig](https://zadig.akeo.ie/): plug in the dongle, select it, choose
**WinUSB**, and click *Replace Driver*. On Linux the udev rules above handle
device access; no driver swap is needed.

## What a maintainer must fill in before first release

- **`RELEASE_BASE`** in `install.sh` — set to the real release download base so
  `nodeagent-linux-amd64` / `-arm64` resolve. (Currently a GitHub
  `releases/latest/download` placeholder.)
- **Windows code signing** — Authenticate the base `NSWPSN-Node-Setup.exe` (and
  ideally `nodeagent.exe`) with an EV/OV cert so SmartScreen doesn't block it;
  the per-volunteer filename rename preserves the signature.
- **Agent first-run download manifest** — the URLs/manifest the agent uses to
  fetch the SDR-Trunk runtime + rdio binary (owned by the Go/agent side).
- **Version bumps** — `AppVersion` in `setup.iss` and the `-ldflags` version
  should track the agent release tag.
- Optionally publish a Linux one-liner (`curl ... | sudo bash`) once the release
  URL is stable.
