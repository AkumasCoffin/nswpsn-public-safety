// Command nodeagent-adsb is the ADS-B feeder node agent. It supervises a
// dump1090 decoder, reads the aircraft.json / stats.json that decoder writes,
// buffers position snapshots to a disk queue, drains them to the backend, and
// maintains a control WebSocket to the backend.
//
// It shares the radio agent's OS-service integration (github.com/kardianos/
// service): install/uninstall/start/stop manage a "nswpsn-node" system service,
// and "run" works both under a service manager and as a plain foreground process.
// It manages no external installs — the decoder is a system package — so it
// only ever self-updates its own binary.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kardianos/service"

	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/agentcfg"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/queue"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/supervise"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/update"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/version"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/wsclient"
)

// serviceName is the OS service identifier used by install/start/etc. It matches
// the name the self-update helper restarts. The ADS-B node runs on its own
// machine, so sharing the radio agent's service name is fine (no conflict).
const serviceName = "nswpsn-node"

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[nodeagent-adsb] ")

	// Global --config flag can appear anywhere; parse it out manually so we can
	// keep simple subcommand dispatch without cobra.
	args, configPath := extractConfigFlag(os.Args[1:])

	cmd := "run"
	if len(args) > 0 {
		cmd = args[0]
	}

	switch cmd {
	case "run":
		if err := runService(configPath); err != nil {
			log.Fatalf("fatal: %v", err)
		}
	case "version":
		fmt.Println(version.String())
	case "install", "uninstall", "start", "stop":
		if err := controlService(cmd, configPath); err != nil {
			log.Fatalf("%s: %v", cmd, err)
		}
		fmt.Printf("%s: ok (service %q)\n", cmd, serviceName)
	default:
		fmt.Printf("unknown command %q\n", cmd)
		fmt.Println("usage: nodeagent-adsb [--config <path>] <run|version|install|uninstall|start|stop>")
		os.Exit(2)
	}
}

// extractConfigFlag pulls out "--config <path>" / "--config=path" from args,
// returning the remaining args and the resolved config path (default beside exe).
func extractConfigFlag(in []string) ([]string, string) {
	configPath := ""
	var rest []string
	for i := 0; i < len(in); i++ {
		a := in[i]
		switch {
		case a == "--config" || a == "-config":
			if i+1 < len(in) {
				configPath = in[i+1]
				i++
			}
		case len(a) > 9 && a[:9] == "--config=":
			configPath = a[9:]
		case len(a) > 8 && a[:8] == "-config=":
			configPath = a[8:]
		default:
			rest = append(rest, a)
		}
	}
	if configPath == "" {
		configPath = defaultConfigPath()
	}
	return rest, configPath
}

// defaultConfigPath is agent.yaml next to the executable.
func defaultConfigPath() string {
	exe, err := os.Executable()
	if err != nil {
		return "agent.yaml"
	}
	return filepath.Join(filepath.Dir(exe), "agent.yaml")
}

// ---- service integration ----------------------------------------------------

// program implements service.Interface. Start launches the agent's run logic on
// a goroutine (Start must not block); Stop cancels its context and waits briefly
// for a graceful shutdown.
type program struct {
	configPath string
	cancel     context.CancelFunc
	done       chan struct{}
}

func (p *program) Start(s service.Service) error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		if err := runAgent(ctx, p.configPath); err != nil {
			log.Printf("agent run ended with error: %v", err)
		}
	}()
	return nil
}

func (p *program) Stop(s service.Service) error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.done != nil {
		select {
		case <-p.done:
		case <-time.After(12 * time.Second):
			log.Printf("service: graceful stop timed out")
		}
	}
	return nil
}

// newService builds the kardianos service handle and its program for the given
// config path. The service is registered to run "nodeagent-adsb run --config <abs>".
func newService(configPath string) (service.Service, *program, error) {
	cfgArg := configPath
	if abs, err := filepath.Abs(configPath); err == nil {
		cfgArg = abs
	}
	prg := &program{configPath: cfgArg}
	svcConfig := &service.Config{
		Name:        serviceName,
		DisplayName: "AusAware ADS-B Feeder Node",
		Description: "Decodes ADS-B aircraft positions via RTL-SDR and relays them to the AusAware backend.",
		Arguments:   []string{"run", "--config", cfgArg},
	}
	s, err := service.New(prg, svcConfig)
	return s, prg, err
}

// runService serves the "run" command. service.Run handles BOTH modes: under a
// service manager it blocks serving Start/Stop; interactively
// (service.Interactive()==true) it runs Start, waits for an OS interrupt, then
// Stop. If the service infrastructure is unavailable we fall back to running the
// agent inline in the foreground.
func runService(configPath string) error {
	s, _, err := newService(configPath)
	if err != nil {
		log.Printf("service infra unavailable (%v); running inline in foreground", err)
		return runForeground(configPath)
	}
	if service.Interactive() {
		log.Printf("running interactively (foreground); Ctrl+C to stop")
	} else {
		log.Printf("running under service manager as %q", serviceName)
	}
	return s.Run()
}

// runForeground runs the agent directly with signal-based cancellation, used
// only when the service library can't construct a service handle.
func runForeground(configPath string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runAgent(ctx, configPath)
}

// controlService runs install/uninstall/start/stop against the OS service
// manager.
func controlService(action, configPath string) error {
	s, _, err := newService(configPath)
	if err != nil {
		return err
	}
	return service.Control(s, action)
}

// ---- agent run --------------------------------------------------------------

func runAgent(ctx context.Context, configPath string) error {
	cfg, err := agentcfg.Load(configPath)
	if err != nil {
		return err
	}
	log.Printf("config loaded: server=%s ws=%s install=%s data=%s json=%s decoder=%s",
		cfg.ServerURL, cfg.WSURL, cfg.InstallID, cfg.DataDir, cfg.JSONDir, cfg.Dump1090Bin)

	// Disk-backed queue.
	q, err := queue.Open(cfg.QueueDir(), 0, 0)
	if err != nil {
		return fmt.Errorf("open queue: %w", err)
	}
	log.Printf("queue opened at %s (depth=%d)", cfg.QueueDir(), q.Depth())

	// Reap any decoder left running by a previous agent. A re-exec self-update
	// keeps the PID/cgroup, so an orphaned dump1090 survives holding the dongle
	// open and the replacement would crash-loop on "usb_claim_interface error".
	// Match on OUR decoder-script path (unique to this agent's data dir), never
	// a bare "dump1090" — that would kill a decoder the operator runs for their
	// own purposes. KillStale kills the matched process group.
	if n := supervise.KillStale([]string{cfg.DecoderDir()}); n > 0 {
		log.Printf("startup: reaped stale decoder group(s) before launch (%d)", n)
	}

	// Fetch the update manifest best-effort. Informational at startup — the WS
	// client runs the actual self-update loop. Offline / placeholder manifest
	// is a graceful no-op.
	if manifest, merr := update.FetchManifest(cfg.ServerURL, cfg.NodeToken); merr != nil {
		log.Printf("update: manifest fetch failed (%v); self-update will retry on the WS update loop", merr)
	} else {
		log.Printf("update: manifest reachable (agent channel v%q)", manifest.Agent.Version)
	}

	// The aircraft manager owns the supervised decoder, the autogain loop and
	// the heartbeat figures.
	mgr := newAircraftManager(ctx, cfg)

	// Replay the LAST applied config on boot so the receiver comes back on its
	// own gain and antenna position rather than defaults. The backend won't
	// re-push when the reported version matches, so booting on defaults would
	// silently strand the node — notably WITHOUT a position, which would leave
	// it reporting no range at all.
	boot := wsclient.AdsbConfig{CaptureEnabled: true, FeedEnabled: true}
	if persisted, ok := wsclient.LoadPersistedConfig(cfg); ok {
		boot = persisted
		log.Printf("decoder: resuming last applied config (version %q, gain=%q capture=%t feed=%t)",
			persisted.ConfigVersion, persisted.Gain, persisted.CaptureEnabled, persisted.FeedEnabled)
	} else {
		log.Printf("decoder: no persisted config; starting on defaults until the backend pushes")
	}
	// Measure the dongle's crystal error BEFORE the decoder starts: rtl_test
	// needs exclusive access to the device, so doing it afterwards would mean
	// stopping the decoder we just launched. A pushed ppm overrides this; the
	// measurement only fills the gap when staff have set nothing.
	mgr.MeasurePPM()

	if err := mgr.Apply(boot); err != nil {
		log.Printf("decoder: initial apply failed: %v", err)
	}

	// WS control client. The manager is config applier, status provider and
	// stats provider.
	ws := wsclient.New(cfg, q, mgr, mgr.Status, mgr.Stats)

	// The queue sender POSTs each buffered snapshot to the backend.
	sender := newSender(cfg)

	// Launch the long-lived goroutines.
	go runSnapshotLoop(ctx, mgr, q)
	go q.RunSender(ctx, sender.send)
	go ws.Run(ctx)

	log.Printf("nodeagent-adsb running")

	<-ctx.Done()
	log.Printf("shutdown signal received; stopping...")

	// Give in-flight graceful shutdowns a brief moment.
	time.Sleep(300 * time.Millisecond)
	log.Printf("stopped")
	return nil
}

// sender POSTs buffered snapshots to ${server_url}/api/node-ingest/adsb-upload.
type sender struct {
	url        string
	token      string
	installID  string
	httpClient *http.Client
}

func newSender(cfg *agentcfg.Config) *sender {
	return &sender{
		url:       cfg.ServerURL + "/api/node-ingest/adsb-upload",
		token:     cfg.NodeToken,
		installID: cfg.InstallID,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// send forwards one buffered message and maps the HTTP status to a SendResult:
// 200 -> OK (delete); 401/403/404/413 -> Drop (delete, futile to retry);
// anything else / network error -> Retry (keep + backoff).
func (s *sender) send(contentType string, body []byte) queue.SendResult {
	req, err := http.NewRequest(http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		log.Printf("sender: build request failed: %v", err)
		return queue.SendRetry
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Node-Token", s.token)
	req.Header.Set("X-Node-Install", s.installID)
	req.Header.Set("User-Agent", version.UserAgent())

	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("sender: POST failed (will retry): %v", err)
		return queue.SendRetry
	}
	// Drain before close so the keep-alive connection can be reused instead of
	// forcing a fresh TCP+TLS handshake per queued message.
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		return queue.SendOK
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusRequestEntityTooLarge:
		log.Printf("sender: dropping message (server returned %d — retry is futile)", resp.StatusCode)
		return queue.SendDrop
	default:
		log.Printf("sender: server returned %d (will retry)", resp.StatusCode)
		return queue.SendRetry
	}
}
