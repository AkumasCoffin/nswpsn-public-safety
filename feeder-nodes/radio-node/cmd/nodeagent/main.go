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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kardianos/service"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/activityship"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/agentcfg"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/configapply"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/queue"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/rdioctl"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/relay"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/siteship"
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
			// Exit non-zero so the service manager restarts us. Returning
			// quietly leaves a process that is registered and "running" but has
			// no relay listener, so every call rdio hands it is refused and
			// dropped — silently, and forever. Only when we were NOT asked to
			// stop: a cancelled context is a normal shutdown.
			if ctx.Err() == nil {
				os.Exit(1)
			}
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
	// Derived + cancelled on every return path, so a fatal component failure
	// actually stops the supervisor, WS client, queue sender and shippers
	// instead of leaving them running as orphans.
	ctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

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

	// --app-root is where sdrtrunk-vce keeps its state, including its config
	// database at <app-root>/database/sdrtrunk.sqlite (which also drives the
	// --fresh / --upgrade-current bootstrap flag below).
	if err := os.MkdirAll(cfg.SDRTrunkAppRoot, 0o755); err != nil {
		return fmt.Errorf("create sdrtrunk app-root %q: %w", cfg.SDRTrunkAppRoot, err)
	}

	sdrCfg := resolveSDRTrunk(cfg, manifest.SDRTrunk, controlToken)
	rdioCfg := resolveRdio(cfg, manifest.Rdio)

	// Reap any SDR-Trunk left running by a previous agent (a re-exec self-update
	// keeps the PID/cgroup, so the old JVM survives and would keep holding the
	// control port). Match ONLY the precise SDR-Trunk main class — NOT the
	// operator-configured command (which could be a generic term like "java" and
	// would then SIGKILL unrelated processes/other users' JVMs).
	if n := supervise.KillStale([]string{"io.github.dsheirer"}); n > 0 {
		log.Printf("startup: reaped %d stale sdrtrunk process(es) before launch", n)
	}

	// Supervisor for external children.
	sup := supervise.New(cfg.DataDir, map[string]agentcfg.ComponentCfg{
		"sdrtrunk": sdrCfg,
		"rdio":     rdioCfg,
	})
	sup.Start(ctx)

	// Control-server REST client on the same port/token sdrtrunk was launched
	// with, shared by the boot config re-import and the sender's site enrichment
	// (the WS client builds its own equivalent client internally).
	sdr := sdrctl.New(cfg.SDRTrunkControlPort, controlToken)

	// Re-import the last-applied config into the freshly-launched sdrtrunk-vce
	// once its control server answers, so it always runs the agent's current
	// config (not whatever its SQLite database held from the last session) and
	// never auto-starts a channel the operator disabled. No-op on first ever
	// boot (no persisted config yet → sdrtrunk-vce runs its imported/preset DB).
	go bootImportConfig(ctx, cfg, sdr)

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
	// Report calls lost between rdio and the queue in the status frame, so a
	// node shedding traffic is visible on the fleet page instead of silent.
	ws.SetDropProvider(listener)

	// The queue sender POSTs each buffered call to the backend relay, enriching
	// each with P25 site headers from the control server when available.
	sender := newSender(cfg, sdr)

	// Activity-event shipper (radio kind only): polls the control server's
	// /activity/events and forwards decode metadata to the backend. Runs
	// regardless of feedEnabled — the metadata is wanted even when the audio
	// feed is off.
	if cfg.Kind == "radio" {
		shipper := activityship.New(activityship.Options{
			DataDir:   cfg.DataDir,
			DBPath:    filepath.Join(cfg.SDRTrunkAppRoot, "database", "sdrtrunk.sqlite"),
			Fetch:     sdr.ActivityEvents,
			ServerURL: cfg.ServerURL,
			NodeToken: cfg.NodeToken,
			InstallID: cfg.InstallID,
		})
		go shipper.Run(ctx)

		// Site-snapshot shipper (radio kind only): polls the control server's
		// /site/snapshots and forwards the full P25 site metadata set to the
		// backend on a slow cadence (~60s, full-snapshot replace, no cursor).
		siteShipper := siteship.New(siteship.Options{
			Fetch:     sdr.SiteSnapshots,
			ServerURL: cfg.ServerURL,
			NodeToken: cfg.NodeToken,
			InstallID: cfg.InstallID,
		})
		go siteShipper.Run(ctx)
	}

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

	var runErr error
	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received; stopping...")
	case e := <-errCh:
		// FATAL, not informational. The relay listener is how rdio hands us
		// calls; if it never bound (port already in use) or died, rdio's POSTs
		// are refused and it DISCARDS every call. Previously this logged one
		// line and returned nil, so the service manager saw a clean exit and
		// never restarted — a node losing 100% of its traffic while still
		// reporting itself healthy. Surface it so we get restarted.
		log.Printf("component error (fatal): %v", e)
		runErr = e
	}
	cancelAll()

	// Give in-flight graceful shutdowns a brief moment.
	time.Sleep(300 * time.Millisecond)
	log.Printf("stopped")
	return runErr
}

// bootImportConfig re-imports the last-applied config payload (persisted by the
// WS client on each successful apply) into a freshly-launched sdrtrunk-vce. It
// waits for the control server to answer /status (polling with backoff up to
// ~90s — JVM start + calibration can be slow on a cold node), then POSTs the
// rebuilt ConfigurationState to /config/import (idempotent full overwrite).
// This guarantees sdrtrunk-vce runs the agent's current config rather than a
// stale last-session database — closing the window where it would auto-start a
// just-disabled channel on every reboot.
func bootImportConfig(ctx context.Context, cfg *agentcfg.Config, sdr *sdrctl.Client) {
	raw, err := os.ReadFile(cfg.AppliedConfigPath())
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("startup: read last-applied config failed: %v", err)
		}
		return // first boot / nothing applied yet — sdrtrunk-vce uses its own DB
	}
	var payload configapply.ConfigPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		log.Printf("startup: last-applied config unreadable (%v); skipping boot config import", err)
		return
	}

	deadline := time.Now().Add(90 * time.Second)
	for {
		if ctx.Err() != nil {
			return
		}
		if _, serr := sdr.Status(); serr == nil {
			break
		}
		if time.Now().After(deadline) {
			log.Printf("startup: sdrtrunk control server not ready after 90s; skipping boot config import")
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}

	deps := configapply.Deps{
		DataDir:         cfg.DataDir,
		PresetsDir:      cfg.PresetsDir,
		SDRTrunkAppRoot: cfg.SDRTrunkAppRoot,
		SDR:             sdr,
	}
	if err := configapply.ImportOnBoot(payload, deps); err != nil {
		log.Printf("startup: boot config import failed: %v", err)
		return
	}
	log.Printf("startup: re-imported last-applied config into sdrtrunk-vce")
}

// resolveSDRTrunk builds the sdrtrunk supervisor component. SDR-Trunk is core to
// a radio feeder node, so it is always enabled — there is no yaml opt-in. An
// operator-provided command (cfg.SDRTrunk.Command) always wins; otherwise it
// resolves an agent-managed install (the jlink launcher). Either way the P3
// launch wiring (--headless / --control-port / --app-root + the per-boot control
// token env) is appended. With no command and no managed install (e.g. a failed
// download) the command stays empty and the supervisor skips it gracefully.
func resolveSDRTrunk(cfg *agentcfg.Config, spec update.ComponentSpec, controlToken string) agentcfg.ComponentCfg {
	c := cfg.SDRTrunk
	c.Enabled = true // a radio feeder node always runs SDR-Trunk

	if c.Command == "" {
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

	// Launch wiring: headless control server + app-root, per-boot token env,
	// plus the sdrtrunk-vce stats flags and its database bootstrap flag.
	//
	// vce requires exactly ONE bootstrap flag and errors on the wrong one:
	// --fresh when its SQLite DB (<appRoot>/database/sdrtrunk.sqlite) does not
	// exist yet, --upgrade-current when it does. Computed at launch time so a
	// wiped app-root self-heals on the next start.
	bootstrapFlag := "--fresh"
	if _, err := os.Stat(filepath.Join(cfg.SDRTrunkAppRoot, "database", "sdrtrunk.sqlite")); err == nil {
		bootstrapFlag = "--upgrade-current"
	}
	c.Args = append(append([]string{}, c.Args...),
		"--headless",
		"--control-port", strconv.Itoa(cfg.SDRTrunkControlPort),
		"--app-root", cfg.SDRTrunkAppRoot,
		bootstrapFlag,
		"--stats-logging", "on",
		"--stats-detailed-history", "on",
		"--stats-retention-days", "7")
	// SDR-Trunk's SettingsManager (SystemProperties -> ~/SDRTrunk) and Java's
	// user-preferences store (~/.java, where the calibration "done" flag and the
	// JMBE library path persist) both derive from the JVM's user.home. The service
	// user's real home (/home/nswpsn-node) is not writable under systemd, so
	// settings + prefs fail with AccessDenied / "Could not lock User prefs" and
	// calibration re-runs + JMBE re-installs every start. Point user.home at the
	// managed, writable app root. We set BOTH $HOME (which the JVM reads for
	// user.home on Unix) AND -Duser.home via JDK_JAVA_OPTIONS (honored by the java
	// launcher) — the latter is authoritative and immune to the systemd "User=
	// overrides Environment=HOME=" quirk, so this holds regardless of the unit.
	c.Env = map[string]string{
		"SDRTRUNK_CONTROL_TOKEN": controlToken,
		"HOME":                   cfg.SDRTrunkAppRoot,
		"JDK_JAVA_OPTIONS":       "-Duser.home=" + cfg.SDRTrunkAppRoot,
	}
	return c
}

// seedRdioAdminPassword runs rdio-scanner's one-shot --admin_password command,
// which writes the admin password into the base_dir database and exits 0. This
// must run BEFORE the server is launched (and the server must not get the flag,
// or it exits on every start). Idempotent: re-seeding just re-sets the same value.
//
// SECURITY NOTE: the password is passed as an argv element, so it is briefly
// visible via /proc/<pid>/cmdline to co-located local users during this one-shot
// run. rdio-scanner's OFFLINE seeding path only accepts the value as the
// --admin_password flag (its RDIO_ADMIN_PASSWORD env var feeds only the
// online `admin-password` subcommand, which needs an already-running,
// authenticated server and so can't seed pre-launch). Fully removing the argv
// exposure therefore requires an rdio-scanner change, which is deliberately kept
// out of scope (the rdio fork is built unmodified). The window is a single
// sub-second process on a single-user volunteer host; accepted risk.
func seedRdioAdminPassword(bin, baseDir, pw string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--base_dir", baseDir, "--admin_password", pw)
	// Bound Wait(): CombinedOutput reads through OS pipes, so the context alone
	// isn't enough — it kills the direct child, but anything that inherited the
	// pipe keeps Wait blocked indefinitely. This runs during startup, before the
	// supervisor and WS client exist, so a hang here leaves the agent alive but
	// completely dark: no components, never online, and nothing in the log to
	// say why.
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// resolveRdio builds the rdio supervisor component. Like SDR-Trunk, the local
// rdio-scanner is core to a feeder node and always enabled — no yaml opt-in. An
// operator-provided command wins; otherwise it resolves an agent-managed install
// and launches the binary with its P2 args (--base_dir / --listen
// 127.0.0.1:17391 / --admin_password <persisted secret>). The admin password is
// ensured (generated + persisted to data/rdio-admin.secret) so the later
// config-apply admin login uses the same value. With no command and no managed
// install (e.g. a failed download) the component stays skipped.
func resolveRdio(cfg *agentcfg.Config, spec update.ComponentSpec) agentcfg.ComponentCfg {
	c := cfg.Rdio
	c.Enabled = true // a radio feeder node always runs its local rdio-scanner

	if c.Command == "" {
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
				// rdio-scanner's --admin_password is a one-shot admin command: it
				// writes the password into the base_dir DB and EXITS. Seed it once
				// here, then launch the server WITHOUT the flag — otherwise the
				// "server" just changes the password, exits 0, and crash-loops.
				if serr := seedRdioAdminPassword(inst.ExecPath, baseDir, pw); serr != nil {
					log.Printf("rdio: seed admin password failed (%v); admin login may fail", serr)
				}
				c.Command = inst.ExecPath
				c.Args = append([]string{
					"--base_dir", baseDir,
					"--listen", localRdioListen,
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
	sdr        *sdrctl.Client // control server, for call-site enrichment (may be nil)
}

func newSender(cfg *agentcfg.Config, sdr *sdrctl.Client) *sender {
	return &sender{
		url:       cfg.ServerURL + "/api/node-ingest/call-upload",
		token:     cfg.NodeToken,
		installID: cfg.InstallID,
		sdr:       sdr,
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

	// Best-effort P25 site enrichment: never delays a failed lookup into an
	// upload failure, and never touches the forwarded body bytes.
	s.addSiteHeaders(req, contentType, body)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("sender: POST failed (will retry): %v", err)
		return queue.SendRetry
	}
	// Drain before close so the keep-alive connection can be reused instead of
	// forcing a fresh TCP+TLS handshake per queued call.
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

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

// ---- P25 call-site enrichment ----------------------------------------------

// enrichDebug gates the (per-call, potentially chatty) enrichment failure logs.
var enrichDebug = os.Getenv("NODEAGENT_DEBUG") != ""

func debugf(format string, args ...any) {
	if enrichDebug {
		log.Printf(format, args...)
	}
}

// callMeta is the metadata parsed from a queued rdio call-upload body.
type callMeta struct {
	talkgroup int
	source    int
	frequency int64 // Hz
	tsMs      int64 // call start, epoch millis
}

// addSiteHeaders best-effort resolves the P25 RFSS/site the call was heard on
// (via the sdrtrunk-vce control server's /activity/call-site) and attaches it
// as X-Call-Site-* headers on the backend upload. The stored multipart body is
// parsed from a read-only view — the forwarded bytes stay byte-identical. ANY
// failure (parse error, control server down, not found) leaves the request
// untouched; enrichment can never fail or retry an upload.
func (s *sender) addSiteHeaders(req *http.Request, contentType string, body []byte) {
	if s.sdr == nil {
		return
	}
	meta, err := extractCallMeta(contentType, body)
	if err != nil {
		debugf("sender: call meta parse failed (no site enrichment): %v", err)
		return
	}
	if meta.talkgroup <= 0 {
		return
	}

	cs, err := s.sdr.CallSite(meta.talkgroup, meta.source, meta.frequency, meta.tsMs, 4000)
	if err != nil {
		debugf("sender: call-site lookup failed (no site enrichment): %v", err)
		return
	}
	// Fresh call not matched yet (the activity log can lag the upload by a
	// beat): wait once briefly and retry.
	if !cs.Found && meta.tsMs > 0 && time.Since(time.UnixMilli(meta.tsMs)) < 30*time.Second {
		time.Sleep(2 * time.Second)
		cs2, err2 := s.sdr.CallSite(meta.talkgroup, meta.source, meta.frequency, meta.tsMs, 4000)
		if err2 != nil {
			debugf("sender: call-site retry failed (no site enrichment): %v", err2)
			return
		}
		cs = cs2
	}
	if !cs.Found {
		return
	}

	if cs.Rfss != nil {
		req.Header.Set("X-Call-Site-Rfss", strconv.Itoa(*cs.Rfss))
	}
	if cs.Site != nil {
		req.Header.Set("X-Call-Site-Id", strconv.Itoa(*cs.Site))
	}
	if cs.Nac != nil {
		req.Header.Set("X-Call-Site-Nac", strconv.Itoa(*cs.Nac))
	}
	if cs.Source != "" {
		req.Header.Set("X-Call-Site-Source", cs.Source)
	}
}

// extractCallMeta parses the rdio call-upload form fields the enrichment needs
// (dateTime / talkgroup / source / frequency) out of a stored multipart body.
// It reads from a fresh reader over the stored bytes — the body itself is
// never modified or re-serialized. Missing/unparseable numeric fields stay 0.
func extractCallMeta(contentType string, body []byte) (callMeta, error) {
	var meta callMeta

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return meta, fmt.Errorf("parse content-type: %w", err)
	}
	if !strings.HasPrefix(mediaType, "multipart/") {
		return meta, fmt.Errorf("not a multipart body: %s", mediaType)
	}
	boundary := params["boundary"]
	if boundary == "" {
		return meta, fmt.Errorf("multipart content-type without boundary")
	}

	mr := multipart.NewReader(bytes.NewReader(body), boundary)
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return meta, fmt.Errorf("read multipart: %w", err)
		}
		name := part.FormName()
		switch name {
		case "dateTime", "talkgroup", "source", "frequency":
			raw, _ := io.ReadAll(io.LimitReader(part, 64))
			v := strings.TrimSpace(string(raw))
			switch name {
			case "dateTime":
				// rdio sends the call start as epoch SECONDS (occasionally
				// fractional); the control server wants millis.
				if f, ferr := strconv.ParseFloat(v, 64); ferr == nil {
					meta.tsMs = int64(f * 1000)
				}
			case "talkgroup":
				meta.talkgroup, _ = strconv.Atoi(v)
			case "source":
				meta.source, _ = strconv.Atoi(v)
			case "frequency":
				meta.frequency, _ = strconv.ParseInt(v, 10, 64)
			}
		}
		_ = part.Close()
	}
	return meta, nil
}
