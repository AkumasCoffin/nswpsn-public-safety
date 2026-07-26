// Package supervise launches and monitors external child processes (the pager
// reader.sh pipelines) as opaque children: launch with the configured command
// line, capture logs, restart on crash, with a crash-loop guard.
package supervise

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/agentcfg"
)

// Component status values reported via Status().
const (
	StatusRunning     = "running"
	StatusStopped     = "stopped"
	StatusDisabled    = "disabled"
	StatusCrashLooped = "crashlooped"
)

const (
	restartBackoff = 3 * time.Second
	crashWindow    = 5 * time.Minute
	crashThreshold = 5
	logMaxBytes    = 10 << 20 // 10 MiB before rotating to .log.1
)

// Supervisor manages a fixed set of named components for the process lifetime.
type Supervisor struct {
	dataDir string

	mu    sync.Mutex
	comps map[string]*component
	ctx   context.Context
}

type component struct {
	name string
	cfg  agentcfg.ComponentCfg

	status     string
	exitTimes  []time.Time // recent exit timestamps for crash-loop detection
	restartNow chan struct{}
	cmd        *exec.Cmd
}

// New builds a Supervisor for the given components keyed by name.
func New(dataDir string, comps map[string]agentcfg.ComponentCfg) *Supervisor {
	s := &Supervisor{
		dataDir: dataDir,
		comps:   make(map[string]*component),
	}
	for name, cfg := range comps {
		st := StatusStopped
		if !cfg.Enabled || cfg.Command == "" {
			st = StatusDisabled
		}
		s.comps[name] = &component{
			name:       name,
			cfg:        cfg,
			status:     st,
			restartNow: make(chan struct{}, 1),
		}
	}
	return s
}

// Start launches all enabled components and monitors them until ctx is done.
func (s *Supervisor) Start(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	s.mu.Unlock()

	for _, c := range s.comps {
		if c.status == StatusDisabled {
			continue
		}
		go s.runComponent(ctx, c)
	}
}

// runComponent is the per-component supervise loop: launch, wait, restart.
func (s *Supervisor) runComponent(ctx context.Context, c *component) {
	logPath := filepath.Join(s.dataDir, "logs", c.name+".log")

	for {
		if ctx.Err() != nil {
			s.setStatus(c, StatusStopped)
			return
		}

		// Crash-loop guard: prune old exits and check threshold.
		if s.crashLooped(c) {
			s.setStatus(c, StatusCrashLooped)
			// Wait until the window clears (or a manual restart / ctx cancel).
			select {
			case <-ctx.Done():
				s.setStatus(c, StatusStopped)
				return
			case <-c.restartNow:
				s.clearExits(c)
			case <-time.After(crashWindow):
				s.clearExits(c)
			}
			continue
		}

		lw, err := newLogWriter(logPath)
		if err != nil {
			// Can't open a log file — still run, dumping to our own stderr.
			fmt.Fprintf(os.Stderr, "supervise: %s: cannot open log %q: %v\n", c.name, logPath, err)
		}

		cmd := exec.CommandContext(ctx, c.cfg.Command, c.cfg.Args...)
		// Run the child in its own process group and, on ctx-cancel (agent stop),
		// kill the WHOLE group — not just the launcher — so the reader.sh pipeline
		// (bash + rtl_fm + multimon-ng + curl) can't leave the rtl_fm holding the
		// SDR device open.
		setProcessGroup(cmd)
		cmd.Cancel = func() error {
			killProcessGroup(cmd)
			return nil
		}
		if c.cfg.WorkDir != "" {
			cmd.Dir = c.cfg.WorkDir
		}
		if len(c.cfg.Env) > 0 {
			// Merge the component env OVER the inherited environment (override, not
			// append) so keys like HOME reliably win — a duplicate "HOME=" entry
			// would otherwise be resolved unpredictably by the OS.
			merged := map[string]string{}
			for _, kv := range os.Environ() {
				if i := strings.IndexByte(kv, '='); i >= 0 {
					merged[kv[:i]] = kv[i+1:]
				}
			}
			for k, v := range c.cfg.Env {
				merged[k] = v
			}
			env := make([]string, 0, len(merged))
			for k, v := range merged {
				env = append(env, k+"="+v)
			}
			cmd.Env = env
		}
		if lw != nil {
			cmd.Stdout = lw
			cmd.Stderr = lw
		}

		s.mu.Lock()
		c.cmd = cmd
		s.mu.Unlock()

		startErr := cmd.Start()
		if startErr != nil {
			fmt.Fprintf(os.Stderr, "supervise: %s: start failed: %v\n", c.name, startErr)
			s.recordExit(c)
			if lw != nil {
				_ = lw.Close()
			}
			if !s.backoffOrRestart(ctx, c) {
				return
			}
			continue
		}

		s.setStatus(c, StatusRunning)
		waitErr := cmd.Wait()
		if lw != nil {
			_ = lw.Close()
		}

		if ctx.Err() != nil {
			s.setStatus(c, StatusStopped)
			return
		}

		fmt.Fprintf(os.Stderr, "supervise: %s exited: %v — restarting\n", c.name, waitErr)
		s.recordExit(c)
		s.setStatus(c, StatusStopped)

		if !s.backoffOrRestart(ctx, c) {
			return
		}
	}
}

// backoffOrRestart waits restartBackoff (or a manual restart signal). Returns
// false if ctx was cancelled.
func (s *Supervisor) backoffOrRestart(ctx context.Context, c *component) bool {
	select {
	case <-ctx.Done():
		s.setStatus(c, StatusStopped)
		return false
	case <-c.restartNow:
		return true
	case <-time.After(restartBackoff):
		return true
	}
}

func (s *Supervisor) setStatus(c *component, st string) {
	s.mu.Lock()
	c.status = st
	s.mu.Unlock()
}

func (s *Supervisor) recordExit(c *component) {
	s.mu.Lock()
	c.exitTimes = append(c.exitTimes, time.Now())
	s.mu.Unlock()
}

func (s *Supervisor) clearExits(c *component) {
	s.mu.Lock()
	c.exitTimes = nil
	s.mu.Unlock()
}

// crashLooped reports whether c has exited >= crashThreshold times within
// crashWindow. It prunes exits older than the window.
func (s *Supervisor) crashLooped(c *component) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().Add(-crashWindow)
	kept := c.exitTimes[:0]
	for _, t := range c.exitTimes {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	c.exitTimes = kept
	return len(c.exitTimes) >= crashThreshold
}

// Status returns a snapshot of name -> status.
func (s *Supervisor) Status() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.comps))
	for name, c := range s.comps {
		out[name] = c.status
	}
	return out
}

// Restart requests a restart of the named component. It kills the current
// process (if any) and clears the crash-loop window so it will relaunch.
func (s *Supervisor) Restart(name string) error {
	s.mu.Lock()
	c, ok := s.comps[name]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("unknown component %q", name)
	}
	if c.status == StatusDisabled {
		s.mu.Unlock()
		return fmt.Errorf("component %q is disabled", name)
	}
	c.exitTimes = nil
	cmd := c.cmd
	s.mu.Unlock()

	// Kill the running process GROUP (bash launcher + rtl_fm/multimon-ng/curl); the
	// supervise loop will observe the exit and restart via the restartNow signal
	// below. Group-kill so rtl_fm doesn't survive and keep holding the SDR device.
	if cmd != nil && cmd.Process != nil {
		killProcessGroup(cmd)
	}
	select {
	case c.restartNow <- struct{}{}:
	default:
	}
	return nil
}
