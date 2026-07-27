// Package wsclient maintains a persistent outbound WebSocket to the backend:
// it sends a hello on connect, a status heartbeat every 15s, answers cmd
// messages, applies pushed pager config, sends WS pings every 30s (Cloudflare
// Tunnel kills idle WS ~100s), and reconnects with backoff.
package wsclient

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/agentcfg"
	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/protocol"
	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/update"
	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/version"
)

const (
	statusInterval     = 15 * time.Second
	pingInterval       = 30 * time.Second
	readDeadline       = 90 * time.Second
	writeWait          = 10 * time.Second
	backoffInitial     = 1 * time.Second
	backoffMax         = 30 * time.Second // transient drops recover fast
	backoffDisabled    = 30 * time.Second // 401/403 (disabled / role removed): retry every ~30s so re-enabling recovers quickly
	backoffStableReset = 15 * time.Second // a session must stay up this long before it resets the network backoff

	updateInitialDelay = 30 * time.Second // let the WS settle before the first update check
	updateInterval     = 6 * time.Hour    // periodic update check cadence
)

// PagerFrequency is one frequency in a pushed pager config, in priority order.
type PagerFrequency struct {
	Label string  `json:"label"`
	MHz   float64 `json:"mhz"`
}

// PagerConfig is the applied pager configuration handed to the ConfigApplier.
type PagerConfig struct {
	ConfigVersion  string
	CaptureEnabled bool
	FeedEnabled    bool
	Frequencies    []PagerFrequency
	Protocols      []string
	// Gain overrides the reader tuner gain (dB) for ALL readers. "" = agent
	// default; "auto" = hardware AGC; a number = that fixed gain.
	Gain string
	// Ppm, when non-nil, overrides the per-dongle measured ppm for ALL readers
	// (nil = keep the auto-measured value).
	Ppm *int
}

// ConfigApplier applies a pushed pager config to the reader/supervisor manager
// and services component-restart commands. Implemented by the main package's
// reader manager.
type ConfigApplier interface {
	// Apply (re)computes and (re)starts the readers for cfg. captureEnabled=false
	// stops the readers without disconnecting the agent.
	Apply(cfg PagerConfig) error
	// Restart restarts a single named reader component (e.g. "reader:NSWRFS").
	Restart(component string) error
	// Rescan stops the readers, re-detects the attached SDRs, and replays the last
	// config (staff "Recheck SDRs"). Slow (re-measures ppm) — call in a goroutine.
	Rescan() error
	// AudioStart begins tapping the named reader's rtl_fm audio (rebuilds that
	// reader with a tee to the loopback relay). AudioStop clears the tap. Both
	// trigger a brief reader restart, so call in a goroutine.
	AudioStart(label string) error
	AudioStop() error
}

// StatusProvider returns the current reader component states (name -> status)
// for the heartbeat.
type StatusProvider func() map[string]string

// depthProvider is the subset of the queue the client needs.
type depthProvider interface {
	Depth() int
}

// Client owns the WS connection lifecycle.
type Client struct {
	cfg     *agentcfg.Config
	q       depthProvider
	applier ConfigApplier
	status  StatusProvider

	applyMu  sync.Mutex // serializes config applies so two pushes can't race
	updateMu sync.Mutex // serializes update checks (manifest + self-update)

	swapScheduled atomic.Bool // one-shot guard: at most one self-update swap+restart in flight

	verMu          sync.Mutex // guards appliedVersion
	appliedVersion string     // config version last successfully applied (persisted)

	writeMu sync.Mutex // serializes all conn writes (gorilla forbids concurrent writers)
	conn    *websocket.Conn
}

// New builds a WS client. q supplies the queue depth; applier applies pushed
// config + services restart cmds; status supplies the reader component states.
func New(cfg *agentcfg.Config, q depthProvider, applier ConfigApplier, status StatusProvider) *Client {
	c := &Client{cfg: cfg, q: q, applier: applier, status: status}
	c.loadAppliedVersion()
	return c
}

// Run connects and services the WS until ctx is cancelled, reconnecting with
// backoff. It never returns except on ctx cancel.
func (c *Client) Run(ctx context.Context) {
	// Periodic self-update checks run independently of the WS session so they
	// survive reconnects. Best-effort and placeholder-safe.
	go c.updateLoop(ctx)

	backoff := backoffInitial
	for {
		if ctx.Err() != nil {
			return
		}

		sessStart := time.Now()
		connected, rejected, err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("wsclient: session ended: %v", err)
		}

		// A healthy session that STAYED UP resets the network backoff so a
		// long-lived connection doesn't inherit a huge delay from earlier flaps.
		// Require a minimum stable duration: a server that accepts the handshake
		// then immediately drops must NOT keep resetting backoff to 1s (that would
		// be a ~1s reconnect storm), so only a genuinely stable session resets it.
		if connected && time.Since(sessStart) >= backoffStableReset {
			backoff = backoffInitial
		}

		// Choose backoff cadence: slower if the server rejected the handshake
		// (e.g. node disabled -> 403), faster for transient network drops.
		var wait time.Duration
		if rejected {
			wait = jitter(backoffDisabled)
		} else {
			wait = jitter(backoff)
			backoff *= 2
			if backoff > backoffMax {
				backoff = backoffMax
			}
		}
		log.Printf("wsclient: reconnecting in %s", wait.Round(time.Millisecond))
		if !sleepCtx(ctx, wait) {
			return
		}
	}
}

// session performs one full connect->serve cycle. It returns connected=true if
// the WS handshake succeeded (so the caller can reset backoff), and rejected=true
// if the handshake was rejected by the server (so the caller backs off at a
// slower cadence).
func (c *Client) session(ctx context.Context) (connected bool, rejected bool, err error) {
	url := c.cfg.WSURL + "/api/node-ws/agent"
	hdr := http.Header{
		"X-Node-Token":   {c.cfg.NodeToken},
		"X-Node-Install": {c.cfg.InstallID},
		"User-Agent":     {version.UserAgent()},
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
	}
	conn, resp, dialErr := dialer.DialContext(ctx, url, hdr)
	if dialErr != nil {
		if resp != nil {
			// Only a genuine auth rejection (401 bad token / 403 disabled or
			// role removed) is a persistent condition worth the slow backoff.
			// 5xx/502 etc. are transient (a backend restart / deploy behind
			// Cloudflare) — treat those like a network drop so the node
			// reconnects promptly instead of sitting out the slow cadence.
			rejected := resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden
			log.Printf("wsclient: dial %s failed: status=%d (rejected=%t)", url, resp.StatusCode, rejected)
			return false, rejected, dialErr
		}
		return false, false, dialErr
	}
	log.Printf("wsclient: connected to %s", url)

	c.writeMu.Lock()
	c.conn = conn
	c.writeMu.Unlock()

	defer func() {
		c.writeMu.Lock()
		c.conn = nil
		c.writeMu.Unlock()
		_ = conn.Close()
	}()

	// Read deadline + pong handler: a dead peer trips the read loop.
	_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readDeadline))
	})

	// Send hello.
	if err := c.sendHello(conn); err != nil {
		return true, false, err
	}

	sessCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go c.statusLoop(sessCtx, conn)
	go c.pingLoop(sessCtx, conn)

	// Read loop (blocks until error/close/ctx).
	return true, false, c.readLoop(sessCtx, conn)
}

func (c *Client) sendHello(conn *websocket.Conn) error {
	hostname, _ := os.Hostname()
	h := protocol.Hello{
		ProtocolVersion: protocol.ProtocolVersion,
		AgentVersion:    version.Version,
		// The pager agent runs no managed SDR-Trunk/rdio components; these serialize
		// empty and are ignored by the backend for a pager node.
		SDRTrunkVersion:      "",
		RdioVersion:          "",
		OS:                   runtime.GOOS,
		Arch:                 runtime.GOARCH,
		Hostname:             hostname,
		AppliedConfigVersion: c.getAppliedVersion(),
		Kind:                 c.cfg.Kind,
	}
	return c.writeType(conn, protocol.TypeHello, h, "")
}

func (c *Client) statusLoop(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(statusInterval)
	defer t.Stop()
	// Send one immediately so the server has fresh state.
	if err := c.sendStatus(conn); err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.sendStatus(conn); err != nil {
				return
			}
		}
	}
}

// sendStatus builds and sends the heartbeat: reader component states + queue
// depth + applied config version. The radio-specific arrays (tuners/channels/
// activeCalls/events) serialize empty for a pager node.
func (c *Client) sendStatus(conn *websocket.Conn) error {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	comps := c.status()
	if comps == nil {
		comps = map[string]string{}
	}

	st := protocol.Status{
		Tuners:        []any{},
		Channels:      []any{},
		ActiveCalls:   []any{},
		Events:        []any{},
		Components:    comps,
		QueueDepth:    c.q.Depth(),
		CPUPct:        0, // best-effort; not computed
		MemMB:         int(ms.Alloc / (1024 * 1024)),
		DiskFreeMB:    0, // best-effort; not computed
		ConfigVersion: c.appliedVersionPtr(),
	}
	return c.writeType(conn, protocol.TypeStatus, st, "")
}

func (c *Client) pingLoop(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.writeMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(writeWait))
			c.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		env, perr := protocol.ParseEnvelope(msg)
		if perr != nil {
			log.Printf("wsclient: bad frame: %v", perr)
			continue
		}
		c.handle(conn, env)
	}
}

func (c *Client) handle(conn *websocket.Conn, env *protocol.Envelope) {
	switch env.T {
	case protocol.TypeHelloAck:
		var ack protocol.HelloAck
		_ = json.Unmarshal(env.Data, &ack)
		log.Printf("wsclient: helloAck ok=%t serverProto=%d updateAvailable=%t", ack.OK, ack.ServerProtocolVersion, ack.UpdateAvailable)

	case protocol.TypeCmd:
		c.handleCmd(conn, env)

	case protocol.TypeConfigPush:
		c.handleConfigPush(env)

	case protocol.TypeAudioStart:
		var a struct {
			Label string `json:"label"`
		}
		_ = json.Unmarshal(env.Data, &a)
		go func() {
			if err := c.applier.AudioStart(a.Label); err != nil {
				log.Printf("wsclient: audioStart failed: %v", err)
			}
		}()

	case protocol.TypeAudioStop:
		go func() {
			if err := c.applier.AudioStop(); err != nil {
				log.Printf("wsclient: audioStop failed: %v", err)
			}
		}()

	case protocol.TypeDisabled:
		log.Printf("wsclient: server reports node disabled; will let socket close and back off")

	default:
		log.Printf("wsclient: ignoring unknown message type %q", env.T)
	}
}

// handleConfigPush applies a pushed pager config. The apply runs on a background
// goroutine so it never blocks the WS read loop, and applies are serialized by
// applyMu so two overlapping pushes can't race. On success it records + persists
// the applied config version and sends configApplied; on failure it sends
// configError with the failing stage.
func (c *Client) handleConfigPush(env *protocol.Envelope) {
	var raw struct {
		ConfigVersion  string `json:"configVersion"`
		CaptureEnabled bool   `json:"captureEnabled"`
		FeedEnabled    bool   `json:"feedEnabled"`
		Pager          struct {
			Frequencies []PagerFrequency `json:"frequencies"`
			Protocols   []string         `json:"protocols"`
			Gain        string           `json:"gain"`
			Ppm         *int             `json:"ppm"`
		} `json:"pager"`
	}
	if err := json.Unmarshal(env.Data, &raw); err != nil {
		_ = c.sendMessage(protocol.TypeConfigError, protocol.ConfigError{
			Stage:   "parse",
			Message: "malformed configPush: " + err.Error(),
		})
		return
	}

	cfg := PagerConfig{
		ConfigVersion:  raw.ConfigVersion,
		CaptureEnabled: raw.CaptureEnabled,
		FeedEnabled:    raw.FeedEnabled,
		Frequencies:    raw.Pager.Frequencies,
		Protocols:      raw.Pager.Protocols,
		Gain:           raw.Pager.Gain,
		Ppm:            raw.Pager.Ppm,
	}

	go func() {
		c.applyMu.Lock()
		defer c.applyMu.Unlock()

		log.Printf("wsclient: applying config version %s (capture=%t feed=%t freqs=%d)",
			cfg.ConfigVersion, cfg.CaptureEnabled, cfg.FeedEnabled, len(cfg.Frequencies))

		if err := c.applier.Apply(cfg); err != nil {
			log.Printf("wsclient: config apply failed: %v", err)
			_ = c.sendMessage(protocol.TypeConfigError, protocol.ConfigError{Stage: "apply", Message: err.Error()})
			return
		}

		// Persist the FULL config BEFORE the version so the two files can never end
		// up with a newer version than config: if the config write fails we skip the
		// version write, leaving the old (matching) version so the backend re-pushes
		// on the next connect. On boot main.go replays this exact config, so the node
		// resumes the chosen frequency rather than the NSW-RFS-first default.
		if err := c.persistAppliedConfig(cfg); err != nil {
			log.Printf("wsclient: persist applied config failed (%v); leaving version stale so backend re-pushes", err)
		} else {
			c.setAppliedVersion(cfg.ConfigVersion)
			if err := c.persistAppliedVersion(cfg.ConfigVersion); err != nil {
				log.Printf("wsclient: persist applied config version failed: %v", err)
			}
		}
		log.Printf("wsclient: config version %s applied", cfg.ConfigVersion)
		_ = c.sendMessage(protocol.TypeConfigApplied, protocol.ConfigApplied{ConfigVersion: cfg.ConfigVersion})
	}()
}

func (c *Client) handleCmd(conn *websocket.Conn, env *protocol.Envelope) {
	var cmd protocol.Command
	if err := json.Unmarshal(env.Data, &cmd); err != nil {
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: "malformed cmd"})
		return
	}

	switch cmd.Action {
	case "restartComponent":
		var a struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(cmd.Args, &a)
		if a.Name == "" {
			c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: "missing component name"})
			return
		}
		if err := c.applier.Restart(a.Name); err != nil {
			c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: err.Error()})
			return
		}
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: true, Message: "restarting " + a.Name})

	case "rescanSdr":
		// ACK immediately — a rescan stops the readers, frees the dongles, and
		// re-measures ppm (~30s per SDR), which exceeds the staff command timeout.
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: true, Message: "rescan started"})
		go func() {
			if err := c.applier.Rescan(); err != nil {
				log.Printf("wsclient: rescan failed: %v", err)
			}
		}()

	case "rebootAgent":
		log.Printf("wsclient: rebootAgent requested — exiting for service manager to relaunch")
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: true, Message: "rebooting agent"})
		// Give the reply a moment to flush, then exit cleanly.
		go func() {
			time.Sleep(500 * time.Millisecond)
			os.Exit(0)
		}()

	case "update":
		// ACK IMMEDIATELY, then run the update in the background. A real update
		// downloads the new binary and then self-updates + re-execs (which drops
		// this WS) — that can exceed the staff command timeout, so we confirm
		// "started" now; the outcome is visible via the node's reported version
		// (and the agent log).
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: true, Message: "update started"})
		go func() {
			msg, ok := c.runUpdateCheck("cmd")
			log.Printf("wsclient: manual update finished: ok=%v — %s", ok, msg)
		}()

	default:
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: "unknown action: " + cmd.Action})
	}
}

// reply sends a typed response echoing the incoming correlation id.
func (c *Client) reply(conn *websocket.Conn, id, t string, data any) {
	if err := c.writeType(conn, t, data, id); err != nil {
		log.Printf("wsclient: reply write failed: %v", err)
	}
}

// updateLoop runs an update check shortly after start and then every
// updateInterval, for the process lifetime. Best-effort and placeholder-safe.
func (c *Client) updateLoop(ctx context.Context) {
	if !sleepCtx(ctx, updateInitialDelay) {
		return
	}
	c.runUpdateCheck("startup")

	t := time.NewTicker(updateInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.runUpdateCheck("periodic")
		}
	}
}

// runUpdateCheck fetches the update manifest and self-updates the agent when a
// newer build is available. It returns a human-readable summary (used as the
// cmdResult message for an "update" cmd). Checks are serialized by updateMu.
//
// PLACEHOLDER-SAFE: with the live placeholder manifest (empty sha256 / stub
// urls) StageAgentUpdate returns ErrNothingToDo, so this reports "no update
// available" and does no downloads.
func (c *Client) runUpdateCheck(reason string) (string, bool) {
	c.updateMu.Lock()
	defer c.updateMu.Unlock()

	m, err := update.FetchManifest(c.cfg.ServerURL, c.cfg.NodeToken)
	if err != nil {
		log.Printf("wsclient: update(%s): manifest fetch failed: %v", reason, err)
		return "update check failed: " + err.Error(), false
	}

	// The manual "update" command always applies; the AUTOMATIC triggers
	// (startup / the 6h periodic ticker) must respect the server's global
	// auto-update switch.
	manual := reason == "cmd"
	if !manual && m.AutoUpdate != nil && !*m.AutoUpdate {
		log.Printf("wsclient: update(%s): auto-update paused by server; skipping", reason)
		return "auto-update paused by server; skipping", true
	}

	pending, newVer, serr := update.StageAgentUpdate(m.Agent, c.cfg.DataDir)
	switch {
	case errors.Is(serr, update.ErrNothingToDo):
		log.Printf("wsclient: update(%s): agent: no update available", reason)
		return "agent: no update available", true
	case serr != nil:
		log.Printf("wsclient: update(%s): stage agent: %v", reason, serr)
		return "agent: update error", false
	default:
		log.Printf("wsclient: update(%s): staged agent v%s; swapping + restarting", reason, newVer)
		// Swap on a short delay so any pending cmd ack flushes before this process
		// re-execs / exits. Guard with a one-shot CAS: two updates within the delay
		// window must not each spawn a SwapAndRestart (the second helper's
		// move-into-place would spin forever on an already-consumed pending file).
		if c.swapScheduled.CompareAndSwap(false, true) {
			go func() {
				time.Sleep(1 * time.Second)
				if swerr := update.SwapAndRestart(pending); swerr != nil {
					log.Printf("wsclient: update: swap + restart failed: %v", swerr)
					c.swapScheduled.Store(false) // allow a retry if the swap didn't take
				}
			}()
		} else {
			log.Printf("wsclient: update(%s): swap already scheduled; skipping duplicate", reason)
		}
		return "agent: updating to v" + newVer + ", restarting", true
	}
}

// SendBinary writes a raw binary frame (pager monitor audio) to the current
// connection under the write mutex. The backend relays binary frames verbatim to
// subscribed staff; the frame's own first byte discriminates audio from other
// binary streams on the staff side. A no-op error when no socket is up (the tap
// just drops until reconnect).
func (c *Client) SendBinary(b []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn := c.conn
	if conn == nil {
		return errors.New("wsclient: no active connection")
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
	return conn.WriteMessage(websocket.BinaryMessage, b)
}

// sendMessage writes a typed, id-less frame to the current connection under the
// write mutex. Unlike writeType it targets c.conn (not a captured conn), so an
// async config ack sent after a reconnect lands on the live socket (or fails
// harmlessly if none is up).
func (c *Client) sendMessage(t string, data any) error {
	b, err := protocol.Marshal(t, data, "")
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn := c.conn
	if conn == nil {
		return errors.New("wsclient: no active connection")
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
	return conn.WriteMessage(websocket.TextMessage, b)
}

// loadAppliedVersion seeds the reported config version from the persisted FULL
// config, which is the single source of truth: the version is only trustworthy
// if we can actually replay the config it names on boot. A node upgraded from an
// older agent has the legacy version file but no full config — reporting that
// stale version would make the backend skip the push (version matches) while the
// node runs the default plan, stranding it on the wrong frequency. Reporting ""
// instead forces a push that both corrects the readers and writes the full
// config, so subsequent restarts resume cleanly.
func (c *Client) loadAppliedVersion() {
	pc, ok := LoadPersistedConfig(c.cfg)
	if !ok {
		return // no trustworthy config → report "" and let the backend push
	}
	c.setAppliedVersion(strings.TrimSpace(pc.ConfigVersion))
}

func (c *Client) setAppliedVersion(v string) {
	c.verMu.Lock()
	c.appliedVersion = v
	c.verMu.Unlock()
}

func (c *Client) getAppliedVersion() string {
	c.verMu.Lock()
	defer c.verMu.Unlock()
	return c.appliedVersion
}

// appliedVersionPtr returns a pointer to the applied version for the status
// frame, or nil when nothing has been applied yet.
func (c *Client) appliedVersionPtr() *string {
	v := c.getAppliedVersion()
	if v == "" {
		return nil
	}
	return &v
}

// persistAppliedVersion atomically writes the applied config version so it
// survives restarts and is reported in the next hello.
func (c *Client) persistAppliedVersion(v string) error {
	path := c.cfg.AppliedConfigVersionPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(v), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// persistAppliedConfig atomically writes the full applied config (frequency plan
// + toggles) so the agent can replay the exact config on boot rather than the
// hardcoded default. See agentcfg.AppliedConfigPath for why this matters.
func (c *Client) persistAppliedConfig(cfg PagerConfig) error {
	b, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	path := c.cfg.AppliedConfigPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadPersistedConfig reads the full config last applied by this agent, so boot
// can replay it (resuming e.g. a Fire & Rescue primary) instead of falling back
// to the default frequency order. Returns ok=false when nothing is persisted yet
// (fresh install) or the file is unreadable/corrupt, in which case the caller
// uses its boot default.
func LoadPersistedConfig(cfg *agentcfg.Config) (PagerConfig, bool) {
	b, err := os.ReadFile(cfg.AppliedConfigPath())
	if err != nil {
		return PagerConfig{}, false
	}
	var pc PagerConfig
	if err := json.Unmarshal(b, &pc); err != nil {
		log.Printf("wsclient: persisted config unreadable (%v); using boot default", err)
		return PagerConfig{}, false
	}
	return pc, true
}

// writeType serializes and writes a frame under the write mutex.
func (c *Client) writeType(conn *websocket.Conn, t string, data any, id string) error {
	b, err := protocol.Marshal(t, data, id)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
	return conn.WriteMessage(websocket.TextMessage, b)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func jitter(d time.Duration) time.Duration {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return d
	}
	frac := float64(uint16(b[0])<<8|uint16(b[1])) / 65535.0
	return d + time.Duration(float64(d)*0.25*frac)
}
