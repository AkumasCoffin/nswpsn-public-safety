// Package wsclient maintains a persistent outbound WebSocket to the backend:
// it sends a hello on connect, a status heartbeat every 15s, answers cmd
// messages, sends WS pings every 30s (Cloudflare Tunnel kills idle WS ~100s),
// and reconnects with backoff.
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

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/agentcfg"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/configapply"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/protocol"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/rdioctl"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/supervise"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/update"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/version"
)

const (
	statusInterval         = 15 * time.Second
	spectrumStatusInterval = 3 * time.Second // faster status cadence while a spectrum stream is live
	pingInterval           = 30 * time.Second
	readDeadline           = 90 * time.Second
	writeWait              = 10 * time.Second
	backoffInitial         = 1 * time.Second
	backoffMax             = 30 * time.Second // transient drops recover fast
	backoffDisabled        = 30 * time.Second // 401/403 (disabled / role removed): retry every ~30s so re-enabling recovers quickly
	backoffStableReset     = 15 * time.Second // a session must stay up this long before it resets the network backoff

	updateInitialDelay = 30 * time.Second // let the WS settle before the first update check
	updateInterval     = 6 * time.Hour    // periodic update check cadence
)

// depthProvider is the subset of the queue the client needs.
type depthProvider interface {
	Depth() int
}

// Client owns the WS connection lifecycle.
type Client struct {
	cfg *agentcfg.Config
	sup *supervise.Supervisor
	q   depthProvider

	sdr  *sdrctl.Client       // REST client for the SDR-Trunk control server
	spec *sdrctl.SpectrumConn // shared spectrum WS (ref-counted per tuner)

	rdio         *rdioctl.Client // local rdio-scanner admin client (config apply)
	rdioPassword string          // resolved local rdio admin password

	applyMu     sync.Mutex // serializes config applies so two pushes can't race
	lastApplyAt time.Time  // when the last apply finished (guarded by applyMu) — burst dedup only
	updateMu    sync.Mutex // serializes update checks (manifest + component ensure + self-update)

	swapScheduled atomic.Bool // one-shot guard: at most one self-update swap+restart in flight

	// Component versions the RUNNING processes were launched with (snapshot of the
	// current.txt pointers at startup, i.e. what resolveSDRTrunk/resolveRdio
	// actually exec'd). A component self-update only downloads + flips the pointer;
	// the live JVM keeps running the old build until the agent process restarts and
	// re-resolves the version-pinned path. We compare against these to know when a
	// download requires a restart to actually take effect.
	runningComp map[string]string

	verMu          sync.Mutex // guards appliedVersion
	appliedVersion string     // config version last successfully applied (persisted)

	writeMu sync.Mutex // serializes all conn writes (gorilla forbids concurrent writers)
	conn    *websocket.Conn
}

// New builds a WS client. q must expose Depth(). controlPort/controlToken are
// the SDR-Trunk control server's REST port and per-boot bearer token (the
// spectrum WS is controlPort+1); both the REST client and the spectrum WS are
// built here so they share the same port/token as the launched sdrtrunk.
func New(cfg *agentcfg.Config, sup *supervise.Supervisor, q depthProvider, controlPort int, controlToken string, rdio *rdioctl.Client, rdioPassword string) *Client {
	c := &Client{cfg: cfg, sup: sup, q: q, rdio: rdio, rdioPassword: rdioPassword}
	c.sdr = sdrctl.New(controlPort, controlToken)
	// Snapshot the versions the running processes were launched with. New() runs
	// after resolveSDRTrunk/resolveRdio have already exec'd the components, so the
	// current.txt pointers reflect exactly what is live right now.
	c.runningComp = map[string]string{
		"sdrtrunk": update.InstalledVersion("sdrtrunk", cfg.DataDir),
		"rdio":     update.InstalledVersion("rdio", cfg.DataDir),
	}
	c.loadAppliedVersion()
	// Each spectrum binary frame is relayed verbatim up the node WS.
	c.spec = sdrctl.NewSpectrumConn(controlPort, controlToken, func(b []byte) {
		if err := c.SendBinary(b); err != nil {
			log.Printf("wsclient: spectrum frame relay failed: %v", err)
		}
	})
	return c
}

// SendBinary writes a binary frame up the node WS under the shared write mutex.
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

// Run connects and services the WS until ctx is cancelled, reconnecting with
// backoff. It never returns except on ctx cancel.
func (c *Client) Run(ctx context.Context) {
	// Periodic self/component update checks run independently of the WS session
	// so they survive reconnects. Best-effort and placeholder-safe.
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
		// Report the versions actually RUNNING (snapshot of what was launched), not
		// the on-disk current.txt pointer — a component download flips the pointer
		// before the process restarts, so the pointer would falsely advertise the
		// new version while the old build is still live. runningComp reflects reality.
		SDRTrunkVersion:      c.runningComp["sdrtrunk"],
		RdioVersion:          c.runningComp["rdio"],
		OS:                   runtime.GOOS,
		Arch:                 runtime.GOARCH,
		Hostname:             hostname,
		AppliedConfigVersion: c.getAppliedVersion(),
		Kind:                 c.cfg.Kind,
	}
	return c.writeType(conn, protocol.TypeHello, h, "")
}

func (c *Client) statusLoop(ctx context.Context, conn *websocket.Conn) {
	// Tick at the fast cadence and decide per-tick whether enough time has
	// elapsed for the currently-desired interval: statusInterval normally, but
	// spectrumStatusInterval while ≥1 spectrum stream is live so the staff
	// drawer sees near-live channel/tuner state.
	t := time.NewTicker(spectrumStatusInterval)
	defer t.Stop()
	// Send one immediately so the server has fresh state.
	if err := c.sendStatus(conn); err != nil {
		return
	}
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			interval := statusInterval
			if c.spec.ActiveCount() > 0 {
				interval = spectrumStatusInterval
			}
			// Small slack so ticker jitter doesn't push a send one tick late.
			if now.Sub(last) < interval-50*time.Millisecond {
				continue
			}
			if err := c.sendStatus(conn); err != nil {
				return
			}
			last = now
		}
	}
}

// sendStatus builds and sends the heartbeat, enriching tuners/channels/events
// from the SDR-Trunk control server. If the control server is unreachable
// (sdrtrunk down or still starting) those lists stay empty and, when the
// supervisor believes the process is running, components["sdrtrunk"] is
// downgraded to "unreachable" so staff can tell a live process from a
// responsive control API.
func (c *Client) sendStatus(conn *websocket.Conn) error {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	comps := c.sup.Status()
	tuners := []any{}
	channels := []any{}
	activeCalls := []any{}
	events := []any{}
	var calibrated, jmbeInstalled *bool

	// Node readiness (calibration + JMBE) from the control server /status.
	if ss, err := c.sdr.Status(); err == nil {
		calibrated = ss.Calibrated
		jmbeInstalled = ss.JmbeInstalled
	}

	if ts, err := c.sdr.Tuners(); err != nil {
		// Control API unreachable: only reflect it if the process is up.
		if comps["sdrtrunk"] == supervise.StatusRunning {
			comps["sdrtrunk"] = "unreachable"
		}
	} else {
		for _, tuner := range ts {
			tuners = append(tuners, tuner)
		}
		if cs, acs, err := c.sdr.Channels(); err == nil {
			for _, ch := range cs {
				channels = append(channels, ch)
			}
			for _, ac := range acs {
				activeCalls = append(activeCalls, ac)
			}
		}
		if evs, err := c.sdr.Events(20); err == nil {
			for _, e := range evs {
				events = append(events, e)
			}
		}
	}

	st := protocol.Status{
		Tuners:        tuners,
		Channels:      channels,
		ActiveCalls:   activeCalls,
		Events:        events,
		Components:    comps,
		QueueDepth:    c.q.Depth(),
		CPUPct:        0, // best-effort; not computed in Phase 2
		MemMB:         int(ms.Alloc / (1024 * 1024)),
		DiskFreeMB:    0, // best-effort; not computed in Phase 2
		ConfigVersion: c.appliedVersionPtr(),
		Calibrated:    calibrated,
		JmbeInstalled: jmbeInstalled,
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

	case protocol.TypeSpectrumStart:
		c.handleSpectrumStart(env)

	case protocol.TypeSpectrumStop:
		c.handleSpectrumStop(env)

	case protocol.TypeConfigPush:
		c.handleConfigPush(env)

	case protocol.TypeDisabled:
		log.Printf("wsclient: server reports node disabled; will let socket close and back off")

	default:
		log.Printf("wsclient: ignoring unknown message type %q", env.T)
	}
}

// handleSpectrumStart opens/reuses the SDR-Trunk spectrum WS and starts the
// requested tuner's stream; binary frames are relayed via the onBinary
// callback wired in New. No cmdResult is expected for spectrum control.
func (c *Client) handleSpectrumStart(env *protocol.Envelope) {
	var d struct {
		TunerID string `json:"tunerId"`
		FPS     int    `json:"fps"`
		Bins    int    `json:"bins"`
	}
	if err := json.Unmarshal(env.Data, &d); err != nil || d.TunerID == "" {
		log.Printf("wsclient: spectrumStart: bad data")
		return
	}
	if err := c.spec.Start(d.TunerID, d.FPS, d.Bins); err != nil {
		log.Printf("wsclient: spectrumStart(%s) failed: %v", d.TunerID, err)
	}
}

// handleSpectrumStop stops the requested tuner's spectrum stream; the shared WS
// closes automatically when no streams remain.
func (c *Client) handleSpectrumStop(env *protocol.Envelope) {
	var d struct {
		TunerID string `json:"tunerId"`
	}
	if err := json.Unmarshal(env.Data, &d); err != nil || d.TunerID == "" {
		log.Printf("wsclient: spectrumStop: bad data")
		return
	}
	if err := c.spec.Stop(d.TunerID); err != nil {
		log.Printf("wsclient: spectrumStop(%s) failed: %v", d.TunerID, err)
	}
}

// configReapplyDedupWindow is how recently the SAME config version must have been
// applied for a re-push to be skipped. Long enough to absorb a backend fan-out burst,
// short enough that a deliberate re-apply (boot, manual Save & sync, a later edit that
// happens to hash the same) still runs.
const configReapplyDedupWindow = 15 * time.Second

// handleConfigPush applies a pushed ConfigPayload. The apply runs on a
// background goroutine so it never blocks the WS read loop, and applies are
// serialized by applyMu so two overlapping pushes can't race. On success it
// records + persists the applied config version and sends configApplied; on
// failure it sends configError with the failing stage.
func (c *Client) handleConfigPush(env *protocol.Envelope) {
	var payload configapply.ConfigPayload
	if err := json.Unmarshal(env.Data, &payload); err != nil {
		_ = c.sendMessage(protocol.TypeConfigError, protocol.ConfigError{
			Stage:   "parse",
			Message: "malformed configPush: " + err.Error(),
		})
		return
	}

	go func() {
		c.applyMu.Lock()
		defer c.applyMu.Unlock()

		// Drop a rapid RE-PUSH of the version we just applied. The backend can fan the
		// same config out several times in a burst (e.g. after unrelated node edits),
		// and re-applying each time churns sdrtrunk + bounces rdio. Only dedup within a
		// short window so a DELIBERATE re-apply still runs — on boot (to (re)apply tuner
		// ppm/gain + restart channels), on a manual Save & sync, or any change after a
		// pause. A failed apply never records the version, so retries always go through.
		if payload.ConfigVersion != "" && payload.ConfigVersion == c.getAppliedVersion() &&
			!c.lastApplyAt.IsZero() && time.Since(c.lastApplyAt) < configReapplyDedupWindow {
			log.Printf("wsclient: config version %s re-pushed within %s; skipping", payload.ConfigVersion, configReapplyDedupWindow)
			_ = c.sendMessage(protocol.TypeConfigApplied, protocol.ConfigApplied{ConfigVersion: payload.ConfigVersion})
			return
		}

		log.Printf("wsclient: applying config version %s", payload.ConfigVersion)
		deps := configapply.Deps{
			DataDir:         c.cfg.DataDir,
			PresetsDir:      c.cfg.PresetsDir,
			SDRTrunkAppRoot: c.cfg.SDRTrunkAppRoot,
			Rdio:            c.rdio,
			SDR:             c.sdr,
			Supervisor:      c.sup,
			RdioPassword:    c.rdioPassword,
		}

		applyErr := configapply.Apply(payload, deps)

		// Persist the payload for the startup pre-launch playlist render whenever the
		// PLAYLIST stage succeeded — even if rdio failed — so SDR-Trunk always boots
		// from the current config regardless of a flaky local rdio.
		if !configapply.HasStage(applyErr, "playlist") && !configapply.HasStage(applyErr, "reload") {
			if perr := c.persistAppliedPayload(env.Data); perr != nil {
				log.Printf("wsclient: persist applied config payload failed: %v", perr)
			}
		}

		if applyErr != nil {
			stage, msg := "apply", applyErr.Error()
			var se *configapply.StageError
			if errors.As(applyErr, &se) {
				stage = se.Stage
				msg = se.Message
				if se.Err != nil {
					msg = se.Message + ": " + se.Err.Error()
				}
			}
			log.Printf("wsclient: config apply failed at stage %q: %s", stage, msg)
			_ = c.sendMessage(protocol.TypeConfigError, protocol.ConfigError{Stage: stage, Message: msg})
			return
		}

		c.setAppliedVersion(payload.ConfigVersion)
		c.lastApplyAt = time.Now() // held under applyMu; gates the burst-dedup window
		if err := c.persistAppliedVersion(payload.ConfigVersion); err != nil {
			log.Printf("wsclient: persist applied config version failed: %v", err)
		}
		log.Printf("wsclient: config version %s applied", payload.ConfigVersion)
		_ = c.sendMessage(protocol.TypeConfigApplied, protocol.ConfigApplied{ConfigVersion: payload.ConfigVersion})
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
		if err := c.sup.Restart(a.Name); err != nil {
			c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: err.Error()})
			return
		}
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: true, Message: "restarting " + a.Name})

	case "rebootAgent":
		log.Printf("wsclient: rebootAgent requested — exiting for service manager to relaunch")
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: true, Message: "rebooting agent"})
		// Give the reply a moment to flush, then exit cleanly.
		go func() {
			time.Sleep(500 * time.Millisecond)
			os.Exit(0)
		}()

	case "tunerSet":
		var a struct {
			TunerID    string          `json:"tunerId"`
			Frequency  *int64          `json:"frequency"`
			PPM        *float64        `json:"ppm"`
			Gain       *int            `json:"gain"`
			GainParams json.RawMessage `json:"gainParams"`
			SampleRate *float64        `json:"sampleRate"`
			AutoPpm    *bool           `json:"autoPpm"`
		}
		_ = json.Unmarshal(cmd.Args, &a)
		if a.TunerID == "" {
			c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: "missing tunerId"})
			return
		}
		var done []string
		if a.SampleRate != nil {
			if err := c.sdr.SetSampleRate(a.TunerID, *a.SampleRate); err != nil {
				c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: err.Error()})
				return
			}
			done = append(done, "sampleRate")
		}
		if a.AutoPpm != nil {
			if err := c.sdr.SetAutoPPM(a.TunerID, *a.AutoPpm); err != nil {
				c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: err.Error()})
				return
			}
			done = append(done, "autoPpm")
		}
		if a.Frequency != nil {
			if err := c.sdr.SetFrequency(a.TunerID, *a.Frequency); err != nil {
				c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: err.Error()})
				return
			}
			done = append(done, "frequency")
		}
		if a.PPM != nil {
			if err := c.sdr.SetPPM(a.TunerID, *a.PPM); err != nil {
				c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: err.Error()})
				return
			}
			done = append(done, "ppm")
		}
		// Gain: a device-shaped gainParams object takes precedence (multi-axis
		// devices); otherwise the scalar gain path is used (back-compat).
		if len(a.GainParams) > 0 {
			var gp map[string]any
			if err := json.Unmarshal(a.GainParams, &gp); err != nil {
				c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: "bad gainParams: " + err.Error()})
				return
			}
			if len(gp) > 0 {
				if err := c.sdr.SetGainParams(a.TunerID, gp); err != nil {
					c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: err.Error()})
					return
				}
				done = append(done, "gain")
			}
		} else if a.Gain != nil {
			if err := c.sdr.SetGain(a.TunerID, *a.Gain); err != nil {
				c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: err.Error()})
				return
			}
			done = append(done, "gain")
		}
		if len(done) == 0 {
			c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: "no tuner parameters provided"})
			return
		}
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: true, Message: "set " + strings.Join(done, ", ") + " on " + a.TunerID})

	case "startChannel", "stopChannel":
		var a struct {
			ChannelID   int    `json:"channelId"`
			ChannelName string `json:"channelName"`
		}
		_ = json.Unmarshal(cmd.Args, &a)

		// SDR-Trunk reassigns channel ids on every playlist reload, so an id the UI
		// cached before a Save & sync is stale ("channel not found"). Resolve the
		// CURRENT id against the live channel list — by name first (stable across
		// reloads), then the given id — before starting/stopping.
		id := a.ChannelID
		if chans, _, cerr := c.sdr.Channels(); cerr == nil {
			byName, byID := -1, -1
			for _, ch := range chans {
				if a.ChannelName != "" && ch.Name == a.ChannelName {
					byName = ch.ID
				}
				if ch.ID == a.ChannelID {
					byID = ch.ID
				}
			}
			if byName >= 0 {
				id = byName
			} else if byID >= 0 {
				id = byID
			}
		}

		var err error
		if cmd.Action == "startChannel" {
			err = c.sdr.StartChannel(id)
		} else {
			err = c.sdr.StopChannel(id)
		}
		if err != nil {
			c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: err.Error()})
			return
		}
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: true, Message: cmd.Action + " ok"})

	case "reloadPlaylist":
		if err := c.sdr.ReloadPlaylist(); err != nil {
			c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: err.Error()})
			return
		}
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: true, Message: "playlist reloaded"})

	case "update":
		// ACK IMMEDIATELY, then run the update in the background. A real update
		// downloads ~90MB of components and then self-updates + re-execs (which
		// drops this WS) — that far exceeds the staff command timeout, so waiting
		// to reply made the UI report "update failed" even though it succeeded.
		// Confirm "started" now; the outcome is visible via the node's reported
		// versions (and the agent log).
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: true, Message: "update started"})
		go func() {
			msg, ok := c.runUpdateCheck("cmd")
			log.Printf("wsclient: manual update finished: ok=%v — %s", ok, msg)
		}()

	case "pushConfig":
		c.reply(conn, env.ID, protocol.TypeCmdResult, protocol.CmdResult{OK: false, Message: "not implemented in this build"})

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

// sendReply writes a correlated response to the CURRENT connection (not a
// captured conn), for replies produced asynchronously (e.g. after an update
// check) that may complete after a reconnect. It no-ops if no socket is up.
func (c *Client) sendReply(id, t string, data any) {
	b, err := protocol.Marshal(t, data, id)
	if err != nil {
		log.Printf("wsclient: sendReply marshal failed: %v", err)
		return
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	conn := c.conn
	if conn == nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
	if werr := conn.WriteMessage(websocket.TextMessage, b); werr != nil {
		log.Printf("wsclient: sendReply write failed: %v", werr)
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

// runUpdateCheck fetches the update manifest, ensures managed components are
// installed at the advertised versions, and self-updates the agent when a newer
// build is available. It returns a human-readable summary (used as the cmdResult
// message for an "update" cmd). Checks are serialized by updateMu.
//
// PLACEHOLDER-SAFE: with the live placeholder manifest (empty sha256 / stub
// urls) EnsureComponent installs nothing and StageAgentUpdate returns
// ErrNothingToDo, so this reports "no update available" and does no downloads.
func (c *Client) runUpdateCheck(reason string) (string, bool) {
	c.updateMu.Lock()
	defer c.updateMu.Unlock()

	hadError := false

	m, err := update.FetchManifest(c.cfg.ServerURL, c.cfg.NodeToken)
	if err != nil {
		log.Printf("wsclient: update(%s): manifest fetch failed: %v", reason, err)
		return "update check failed: " + err.Error(), false
	}

	// The manual "update" command always applies; the AUTOMATIC triggers
	// (startup / the 6h periodic ticker) must respect the server's global
	// auto-update switch. We always FETCH the manifest above (so the flag is
	// read), but when auto-update is paused we don't APPLY component/agent
	// updates on an automatic pass.
	manual := reason == "cmd"
	if !manual && m.AutoUpdate != nil && !*m.AutoUpdate {
		log.Printf("wsclient: update(%s): auto-update paused by server; skipping", reason)
		return "auto-update paused by server; skipping", true
	}

	var parts []string
	componentChanged := false // a component was updated on disk but the live process is still the old build

	// Managed components (best-effort; a failure never aborts the check).
	for _, ce := range []struct {
		name string
		spec update.ComponentSpec
	}{
		{"sdrtrunk", m.SDRTrunk},
		{"rdio", m.Rdio},
	} {
		inst, cerr := update.EnsureComponent(ce.name, ce.spec, c.cfg.DataDir)
		switch {
		case cerr != nil:
			log.Printf("wsclient: update(%s): %s: %v", reason, ce.name, cerr)
			parts = append(parts, ce.name+": error")
			hadError = true
		case inst == nil:
			parts = append(parts, ce.name+": not installed")
		default:
			// EnsureComponent only downloads + flips the current.txt pointer; the
			// live process keeps running the version it was launched with. If the
			// installed version now differs from what's RUNNING, a restart is
			// required to actually run the new build (the supervisor's launch path
			// is version-pinned at startup, so only re-resolving it via a process
			// restart picks up the new runtime + reaps the old one via KillStale).
			if inst.Version != c.runningComp[ce.name] {
				log.Printf("wsclient: update(%s): %s updated %q -> %q on disk; restart required to run it",
					reason, ce.name, c.runningComp[ce.name], inst.Version)
				parts = append(parts, ce.name+" v"+inst.Version+" (restart pending)")
				componentChanged = true
			} else {
				parts = append(parts, ce.name+" v"+inst.Version)
			}
		}
	}

	// Agent self-update.
	pending, newVer, serr := update.StageAgentUpdate(m.Agent, c.cfg.DataDir)
	switch {
	case errors.Is(serr, update.ErrNothingToDo):
		parts = append(parts, "agent: no update available")
	case serr != nil:
		log.Printf("wsclient: update(%s): stage agent: %v", reason, serr)
		parts = append(parts, "agent: update error")
		hadError = true
	default:
		log.Printf("wsclient: update(%s): staged agent v%s; swapping + restarting", reason, newVer)
		parts = append(parts, "agent: updating to v"+newVer+", restarting")
		// Swap on a short delay so any pending cmd ack flushes before this
		// process re-execs / exits. Guard with a one-shot CAS: two updates within
		// the delay window must not each spawn a SwapAndRestart (the second helper's
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
	}

	// A component was downloaded but the live process is still the old build.
	// Restart the agent so main() re-resolves the new version-pinned launch path
	// and KillStale reaps the stale JVM (a plain sup.Restart would relaunch the
	// OLD captured path). Skip if the agent self-update above already claimed the
	// restart (it will re-exec and re-resolve everything anyway). Under a service
	// manager (systemd/kardianos — how real nodes run) os.Exit(0) triggers a
	// relaunch; the same mechanism rebootAgent uses.
	if componentChanged {
		if c.swapScheduled.CompareAndSwap(false, true) {
			log.Printf("wsclient: update(%s): restarting agent to launch updated component(s)", reason)
			go func() {
				time.Sleep(1 * time.Second) // let the cmd-ack / summary flush first
				os.Exit(0)
			}()
		} else {
			log.Printf("wsclient: update(%s): restart already scheduled (agent self-update); component will run after it", reason)
		}
	}

	summary := strings.Join(parts, "; ")
	log.Printf("wsclient: update(%s): %s", reason, summary)
	return summary, !hadError
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

// loadAppliedVersion reads the persisted applied config version into memory.
func (c *Client) loadAppliedVersion() {
	b, err := os.ReadFile(c.cfg.AppliedConfigVersionPath())
	if err != nil {
		return
	}
	c.setAppliedVersion(strings.TrimSpace(string(b)))
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

// persistAppliedPayload atomically writes the full config payload so the agent can
// re-render the playlist at startup (before launching SDR-Trunk). Persisted
// whenever the PLAYLIST stage succeeded — even if the rdio stage failed — so the
// pre-launch playlist stays current regardless of a flaky local rdio.
func (c *Client) persistAppliedPayload(raw []byte) error {
	path := c.cfg.AppliedConfigPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
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
