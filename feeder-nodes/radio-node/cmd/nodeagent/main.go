// Command nodeagent is the radio feeder node agent. It supervises the external
// SDR-Trunk and rdio-scanner processes, runs a localhost HTTP listener
// impersonating rdio's call-upload, buffers calls to a disk queue, drains them
// to the backend relay, and maintains a control WebSocket to the backend.
//
// Phase 4 adds OS service integration (github.com/kardianos/service): install/
// uninstall/start/stop manage a "nswpsn-node" system service, and "run" works
// both under a service manager and as a plain foreground process. It also wires
// managed-component installs (update.EnsureComponent) into the supervisor so
// SDR-Trunk and rdio can be launched from agent-managed versioned installs when
// the operator hasn't provided an explicit command.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/kardianos/service"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/agentcfg"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/queue"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/rdioctl"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/relay"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/supervise"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/update"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/version"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/wsclient"
)

// localRdioAdminURL is the admin API base of the rdio-scanner instance the agent
// supervises locally. Config pushes are applied here.
const localRdioAdminURL = "http://127.0.0.1:17391"

// localRdioListen is the host:port the managed rdio-scanner binds its HTTP
// server to (admin API + call-upload). Must match localRdioAdminURL.
const localRdioListen = "127.0.0.1:17391"

// serviceName is the OS service identifier used by install/start/etc. It matches
// the name the self-update helper restarts.
const serviceName = "nswpsn-node"

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[nodeagent] ")

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
		fmt.Println("usage: nodeagent [--config <path>] <run|version|install|uninstall|start|stop>")
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
// config path. The service is registered to run "nodeagent run --config <abs>".
func newService(configPath string) (service.Service, *program, error) {
	cfgArg := configPath
	if abs, err := filepath.Abs(configPath); err == nil {
		cfgArg = abs
	}
	prg := &program{configPath: cfgArg}
	svcConfig := &service.Config{
		Name:        serviceName,
		DisplayName: "NSW PSN Radio Feeder Node",
		Description: "Supervises SDR-Trunk and rdio-scanner and relays radio calls to the NSW PSN backend.",
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
// manager. It is NOT exercised in this build's verification (needs admin), only
// implemented + compiled.
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
	log.Printf("config loaded: server=%s ws=%s install=%s relay=%s data=%s",
		cfg.ServerURL, cfg.WSURL, cfg.InstallID, cfg.RelayAddr, cfg.DataDir)

	// Disk-backed queue.
	q, err := queue.Open(cfg.QueueDir(), 0, 0)
	if err != nil {
		return fmt.Errorf("open queue: %w", err)
	}
	log.Printf("queue opened at %s (depth=%d)", cfg.QueueDir(), q.Depth())

	// Per-boot bearer token shared between the launched sdrtrunk control server
	// and this agent's control clients (REST + spectrum WS).
	controlToken, err := randomHex(32)
	if err != nil {
		return fmt.Errorf("generate control token: %w", err)
	}

	// Fetch the update manifest best-effort. It drives managed-component
	// resolution (EnsureComponent) below. With today's placeholder manifest, or
	// when offline, this is a graceful no-op: components resolve from whatever is
	// already installed (possibly nothing, in which case they stay skipped).
	manifest, merr := update.FetchManifest(cfg.ServerURL, cfg.NodeToken)
	if merr != nil {
		log.Printf("update: manifest fetch failed (%v); using installed components only", merr)
		manifest = &update.Manifest{}
	}

	// --app-root MUST match where configapply writes the rendered playlist
	// (<app-root>/playlist/default.xml), so sdrtrunk loads the pushed config.
	if err := os.MkdirAll(cfg.SDRTrunkAppRoot, 0o755); err != nil {
		return fmt.Errorf("create sdrtrunk app-root %q: %w", cfg.SDRTrunkAppRoot, err)
	}

	sdrCfg := resolveSDRTrunk(cfg, manifest.SDRTrunk, controlToken)
	rdioCfg := resolveRdio(cfg, manifest.Rdio)

	// Supervisor for external children.
	sup := supervise.New(cfg.DataDir, map[string]agentcfg.ComponentCfg{
		"sdrtrunk": sdrCfg,
		"rdio":     rdioCfg,
	})
	sup.Start(ctx)

	// Localhost listener impersonating rdio call-upload.
	listener := relay.New(cfg.RelayAddr, q)

	// Local rdio-scanner admin client for config apply. The admin password was
	// persisted either by resolveRdio (managed launch) or a P2 bootstrap
	// (data/rdio-admin.secret); fall back to rdio's default if it's missing.
	rdio := rdioctl.New(localRdioAdminURL)
	rdioPassword, haveSecret := rdioctl.ReadAdminPassword(cfg.DataDir)
	if !haveSecret {
		log.Printf("rdio admin secret not found at %s/rdio-admin.secret — using rdio default password", cfg.DataDir)
	}

	// WS control client (also bridges the SDR-Trunk control server on the same
	// port/token used to launch sdrtrunk).
	ws := wsclient.New(cfg, sup, q, cfg.SDRTrunkControlPort, controlToken, rdio, rdioPassword)

	// The queue sender POSTs each buffered call to the backend relay.
	sender := newSender(cfg)

	// Launch the long-lived goroutines.
	errCh := make(chan error, 1)
	go func() {
		if lerr := listener.Run(ctx); lerr != nil {
			errCh <- fmt.Errorf("relay listener: %w", lerr)
		}
	}()
	go q.RunSender(ctx, sender.send)
	go ws.Run(ctx)

	log.Printf("nodeagent running")

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received; stopping...")
	case e := <-errCh:
		log.Printf("component error: %v", e)
	}

	// Give in-flight graceful shutdowns a brief moment.
	time.Sleep(300 * time.Millisecond)
	log.Printf("stopped")
	return nil
}

// resolveSDRTrunk builds the sdrtrunk supervisor component. An operator-provided
// command (cfg.SDRTrunk.Command) always wins. Otherwise, if the component is
// enabled, it resolves an agent-managed install: the jlink runtime's java
// launcher plus the SDRTrunk main class. Either way the P3 launch wiring
// (--headless / --control-port / --app-root + the per-boot control token env) is
// appended. With no command and no managed install the component keeps an empty
// command and the supervisor skips it (status "disabled").
func resolveSDRTrunk(cfg *agentcfg.Config, spec update.ComponentSpec, controlToken string) agentcfg.ComponentCfg {
	c := cfg.SDRTrunk

	if c.Command == "" && c.Enabled {
		inst, err := update.EnsureComponent("sdrtrunk", spec, cfg.DataDir)
		if err != nil {
			log.Printf("sdrtrunk: managed install failed (%v); leaving component skipped", err)
		} else if inst != nil {
			// inst.ExecPath is the jlink app-image launcher (<dir>/bin/sdr-trunk),
			// which sets the classpath + required JVM args (--enable-preview,
			// incubator vector, javafx exports) and forwards CLI args to
			// SDRTrunk.main(). So we launch it directly — no main class, no
			// jvmArgs here. Verified against the built runtime image.
			c.Command = inst.ExecPath
			log.Printf("sdrtrunk: using managed install v%s (%s)", inst.Version, inst.ExecPath)
		}
	}

	// P3 launch wiring: headless control server + app-root, per-boot token env.
	c.Args = append(append([]string{}, c.Args...),
		"--headless",
		"--control-port", strconv.Itoa(cfg.SDRTrunkControlPort),
		"--app-root", cfg.SDRTrunkAppRoot)
	c.Env = map[string]string{"SDRTRUNK_CONTROL_TOKEN": controlToken}
	return c
}

// resolveRdio builds the rdio supervisor component. An operator-provided command
// wins; otherwise, if enabled, it resolves an agent-managed install and launches
// the binary with its P2 args (--base_dir / --listen 127.0.0.1:17391 /
// --admin_password <persisted secret>). The admin password is ensured
// (generated + persisted to data/rdio-admin.secret) so the later config-apply
// admin login uses the same value. With no command and no managed install the
// component stays skipped.
func resolveRdio(cfg *agentcfg.Config, spec update.ComponentSpec) agentcfg.ComponentCfg {
	c := cfg.Rdio

	if c.Command == "" && c.Enabled {
		inst, err := update.EnsureComponent("rdio", spec, cfg.DataDir)
		if err != nil {
			log.Printf("rdio: managed install failed (%v); leaving component skipped", err)
		} else if inst != nil {
			pw, perr := rdioctl.EnsureAdminPassword(cfg.DataDir)
			if perr != nil {
				log.Printf("rdio: %v; leaving managed component skipped", perr)
			} else {
				baseDir := filepath.Join(cfg.DataDir, "rdio")
				if mkerr := os.MkdirAll(baseDir, 0o755); mkerr != nil {
					log.Printf("rdio: create base_dir %q: %v", baseDir, mkerr)
				}
				c.Command = inst.ExecPath
				c.Args = append([]string{
					"--base_dir", baseDir,
					"--listen", localRdioListen,
					"--admin_password", pw,
				}, c.Args...)
				log.Printf("rdio: using managed install v%s (%s)", inst.Version, inst.ExecPath)
			}
		}
	}
	return c
}

// randomHex returns n cryptographically-random bytes hex-encoded (2n chars).
func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// sender POSTs buffered calls to ${server_url}/api/node-ingest/call-upload.
type sender struct {
	url        string
	token      string
	installID  string
	httpClient *http.Client
}

func newSender(cfg *agentcfg.Config) *sender {
	return &sender{
		url:       cfg.ServerURL + "/api/node-ingest/call-upload",
		token:     cfg.NodeToken,
		installID: cfg.InstallID,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// send forwards one buffered call and maps the HTTP status to a SendResult:
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
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return queue.SendOK
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusRequestEntityTooLarge:
		log.Printf("sender: dropping call (server returned %d — retry is futile)", resp.StatusCode)
		return queue.SendDrop
	default:
		log.Printf("sender: server returned %d (will retry)", resp.StatusCode)
		return queue.SendRetry
	}
}
