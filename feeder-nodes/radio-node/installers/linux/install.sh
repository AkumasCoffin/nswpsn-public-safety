#!/usr/bin/env bash
# ============================================================================
#  NSW PSN radio feeder node - Linux installer
# ----------------------------------------------------------------------------
#  Installs the nodeagent binary, writes its config, registers the systemd
#  service, and installs udev rules for SDR USB access. Safe to re-run (updates
#  the token / binary in place).
#
#  Token + server can be supplied (in precedence order, highest first):
#    1. flags:   --token npsn_...   --server https://api.forcequit.xyz
#    2. env:     NODE_TOKEN=npsn_...  SERVER_URL=https://api.forcequit.xyz
#    3. inlined: the download page prefills INLINE_TOKEN below.
#
#  Usage:
#    sudo bash install-nswpsn-node.sh
#    sudo NODE_TOKEN=npsn_... bash install.sh
#    sudo bash install.sh --token npsn_... --server https://api.forcequit.xyz
# ============================================================================
set -euo pipefail

# --- The feeder download endpoint replaces the value below with your token. ---
INLINE_TOKEN=""
# ------------------------------------------------------------------------------

# Base URL the release artifacts (nodeagent binaries) are served from.
# MAINTAINER: point this at the published release before first use.
RELEASE_BASE="${RELEASE_BASE:-https://github.com/AkumasCoffin/nswpsn-node/releases/latest/download}"

DEFAULT_SERVER="https://api.forcequit.xyz"

INSTALL_DIR="/opt/nswpsn-node"
CONFIG_DIR="/etc/nswpsn-node"
CONFIG_FILE="${CONFIG_DIR}/agent.yaml"
DATA_DIR="/var/lib/nswpsn-node"
SERVICE_FILE="/etc/systemd/system/nodeagent.service"
UDEV_FILE="/etc/udev/rules.d/99-nswpsn-sdr.rules"
SERVICE_USER="nswpsn-node"

log()  { printf '\033[36m[nswpsn]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[nswpsn] WARN:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31m[nswpsn] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# --- Parse flags (override env / inline) ------------------------------------
CLI_TOKEN=""
CLI_SERVER=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --token)  CLI_TOKEN="${2:-}"; shift 2 ;;
    --token=*) CLI_TOKEN="${1#*=}"; shift ;;
    --server) CLI_SERVER="${2:-}"; shift 2 ;;
    --server=*) CLI_SERVER="${1#*=}"; shift ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) warn "ignoring unknown argument: $1"; shift ;;
  esac
done

# --- Must run as root (re-exec via sudo, preserving env) --------------------
if [ "$(id -u)" -ne 0 ]; then
  if command -v sudo >/dev/null 2>&1; then
    log "Re-running with sudo..."
    exec sudo -E bash "$0" \
      ${CLI_TOKEN:+--token "$CLI_TOKEN"} \
      ${CLI_SERVER:+--server "$CLI_SERVER"}
  fi
  die "This installer must run as root. Try: sudo bash $0"
fi

# --- Resolve token + server (flags > env > inline) --------------------------
NODE_TOKEN="${CLI_TOKEN:-${NODE_TOKEN:-$INLINE_TOKEN}}"
SERVER_URL="${CLI_SERVER:-${SERVER_URL:-$DEFAULT_SERVER}}"

if ! printf '%s' "$NODE_TOKEN" | grep -Eq '^npsn_[0-9a-f]{40}$'; then
  die "No valid node token. Expected npsn_ followed by 40 hex chars.
     Re-download your personalised installer from the feeder page, or pass
       sudo bash $0 --token npsn_..."
fi

# --- Detect architecture ----------------------------------------------------
case "$(uname -m)" in
  x86_64|amd64)   ARCH="amd64" ;;
  aarch64|arm64)  ARCH="arm64" ;;
  *) die "Unsupported architecture: $(uname -m) (need x86_64 or aarch64)." ;;
esac
BIN_URL="${RELEASE_BASE}/nodeagent-linux-${ARCH}"

# --- Download / update the nodeagent binary ---------------------------------
install -d -m 0755 "$INSTALL_DIR"
TMP_BIN="$(mktemp)"
trap 'rm -f "$TMP_BIN"' EXIT
log "Downloading nodeagent (${ARCH}) from ${BIN_URL} ..."
if command -v curl >/dev/null 2>&1; then
  curl -fSL --retry 3 -o "$TMP_BIN" "$BIN_URL" || die "Download failed (curl). Check RELEASE_BASE / network."
elif command -v wget >/dev/null 2>&1; then
  wget -q -O "$TMP_BIN" "$BIN_URL" || die "Download failed (wget). Check RELEASE_BASE / network."
else
  die "Need curl or wget to download the agent."
fi
[ -s "$TMP_BIN" ] || die "Downloaded agent is empty."
install -m 0755 "$TMP_BIN" "${INSTALL_DIR}/nodeagent"
log "Installed ${INSTALL_DIR}/nodeagent"

# --- Service user + groups --------------------------------------------------
if ! getent group plugdev >/dev/null 2>&1; then
  groupadd --system plugdev
fi
if ! getent passwd "$SERVICE_USER" >/dev/null 2>&1; then
  log "Creating system user ${SERVICE_USER}"
  useradd --system --no-create-home --home-dir "$DATA_DIR" \
          --shell /usr/sbin/nologin --groups plugdev "$SERVICE_USER"
else
  usermod --append --groups plugdev "$SERVICE_USER" || true
fi

# --- Directories ------------------------------------------------------------
install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_USER" "$DATA_DIR"
install -d -m 0755 "$CONFIG_DIR"

# --- Preserve an existing install_id ----------------------------------------
INSTALL_ID=""
if [ -f "$CONFIG_FILE" ]; then
  INSTALL_ID="$(sed -n 's/^[[:space:]]*install_id:[[:space:]]*//p' "$CONFIG_FILE" \
                 | head -n1 | tr -d '"'"'"' ' | tr -d '[:space:]')"
  [ -n "$INSTALL_ID" ] && log "Preserving existing install_id"
fi

# --- Write agent.yaml -------------------------------------------------------
umask 077
cat > "$CONFIG_FILE" <<YAML
# NSW PSN radio feeder node - generated by install.sh.
# Do not share this file: node_token authenticates this node.

server_url: "${SERVER_URL}"
ws_url: ""
node_token: "${NODE_TOKEN}"
install_id: "${INSTALL_ID}"
data_dir: "${DATA_DIR}"
relay_addr: "127.0.0.1:17390"
sdrtrunk_control_port: 17392

# SDR-Trunk + rdio-scanner runtimes are downloaded by the agent on first run;
# it fills in the command paths below automatically.
sdrtrunk:
  enabled: true
  command: ""
  args: []
rdio:
  enabled: true
  command: ""
  args: []
YAML
umask 022
# The service user must read its config (and rewrite install_id on first run).
chown root:"$SERVICE_USER" "$CONFIG_FILE"
chmod 0640 "$CONFIG_FILE"
chown "$SERVICE_USER":"$SERVICE_USER" "$CONFIG_DIR"
log "Wrote ${CONFIG_FILE}"

# --- Install udev rules -----------------------------------------------------
cat > "$UDEV_FILE" <<'RULES'
# NSW PSN radio feeder node - udev rules for common SDR hardware.
# Grants the "plugdev" group access to the USB device node.

# RTL2832U based RTL-SDR (most common)
SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2838", MODE="0660", GROUP="plugdev", TAG+="uaccess"
SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2832", MODE="0660", GROUP="plugdev", TAG+="uaccess"
# Airspy R2 / Mini
SUBSYSTEM=="usb", ATTRS{idVendor}=="1d50", ATTRS{idProduct}=="60a1", MODE="0660", GROUP="plugdev", TAG+="uaccess"
# Airspy HF+
SUBSYSTEM=="usb", ATTRS{idVendor}=="03eb", ATTRS{idProduct}=="800c", MODE="0660", GROUP="plugdev", TAG+="uaccess"
# HackRF One / Jawbreaker / rad1o
SUBSYSTEM=="usb", ATTRS{idVendor}=="1d50", ATTRS{idProduct}=="6089", MODE="0660", GROUP="plugdev", TAG+="uaccess"
SUBSYSTEM=="usb", ATTRS{idVendor}=="1d50", ATTRS{idProduct}=="604b", MODE="0660", GROUP="plugdev", TAG+="uaccess"
SUBSYSTEM=="usb", ATTRS{idVendor}=="1d50", ATTRS{idProduct}=="cc15", MODE="0660", GROUP="plugdev", TAG+="uaccess"
# SDRplay (RSP1/1A/2/duo/dx)
SUBSYSTEM=="usb", ATTRS{idVendor}=="1df7", MODE="0660", GROUP="plugdev", TAG+="uaccess"
RULES
log "Wrote ${UDEV_FILE}"
if command -v udevadm >/dev/null 2>&1; then
  udevadm control --reload-rules && udevadm trigger || warn "udevadm reload/trigger failed (non-fatal)."
fi

# --- Install systemd unit ---------------------------------------------------
cat > "$SERVICE_FILE" <<'UNIT'
# NSW PSN radio feeder node - systemd service.
[Unit]
Description=NSW PSN radio feeder node agent
Documentation=https://nswpsn.forcequit.xyz
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=nswpsn-node
Group=nswpsn-node
SupplementaryGroups=plugdev

ExecStart=/opt/nswpsn-node/nodeagent run --config /etc/nswpsn-node/agent.yaml
Restart=always
RestartSec=5

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ProtectControlGroups=true
ProtectKernelModules=true
ProtectKernelTunables=true
ProtectKernelLogs=true
RestrictSUIDSGID=true
RestrictRealtime=true
LockPersonality=true
# /opt/nswpsn-node writable so the agent can self-update its binary in place.
ReadWritePaths=/opt/nswpsn-node /var/lib/nswpsn-node /etc/nswpsn-node
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK

[Install]
WantedBy=multi-user.target
UNIT
log "Wrote ${SERVICE_FILE}"

# --- Enable + (re)start -----------------------------------------------------
if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload
  systemctl enable nodeagent.service >/dev/null 2>&1 || true
  systemctl restart nodeagent.service
  log "Service enabled and started."
  echo
  systemctl --no-pager --lines=0 status nodeagent.service || true
  echo
  log "Follow logs with:  journalctl -u nodeagent -f"
else
  warn "systemctl not found. Start the agent manually:"
  warn "  ${INSTALL_DIR}/nodeagent run --config ${CONFIG_FILE}"
fi

log "Done. Your feeder node status will appear on the website's feeder page."
