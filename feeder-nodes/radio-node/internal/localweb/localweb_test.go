package localweb

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeRdio records every path it is asked for and echoes websocket messages,
// standing in for the local rdio-scanner behind the proxy.
type fakeRdio struct {
	mu   sync.Mutex
	seen []string
}

func (f *fakeRdio) handler(t *testing.T) http.Handler {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.seen = append(f.seen, r.Method+" "+r.URL.Path)
		f.mu.Unlock()
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			c, err := up.Upgrade(w, r, nil)
			if err != nil {
				t.Errorf("fake rdio upgrade: %v", err)
				return
			}
			defer c.Close()
			mt, msg, err := c.ReadMessage()
			if err == nil {
				_ = c.WriteMessage(mt, msg)
			}
			return
		}
		_, _ = io.WriteString(w, "rdio:"+r.URL.Path)
	})
}

func (f *fakeRdio) saw() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

// start runs the proxy on a free loopback port in front of a fake rdio.
func start(t *testing.T) (*fakeRdio, string) {
	t.Helper()
	f := &fakeRdio{}
	up := httptest.NewServer(f.handler(t))
	t.Cleanup(up.Close)

	s, err := New("127.0.0.1:0", up.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(2 * time.Second)
	for s.Port() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("proxy never bound")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return f, "127.0.0.1:" + strconv.Itoa(s.Port())
}

func TestListenerPageAndItsFilesAreProxied(t *testing.T) {
	_, addr := start(t)
	for _, p := range []string{
		"/", "/stream", "/main.3f2a1b.js", "/favicon.ico", "/assets/icons/radio.svg",
		"/plugins/demo/plugin.js", "/api/stats", "/api/stats/talkgroup-units?system=1&talkgroup=2",
	} {
		res, err := http.Get("http://" + addr + p)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !strings.HasPrefix(string(body), "rdio:") {
			t.Fatalf("GET %s: want it proxied to rdio, got %d %q", p, res.StatusCode, body)
		}
	}
}

func TestAdminAndUploadNeverReachRdio(t *testing.T) {
	f, addr := start(t)
	blocked := []struct{ method, path string }{
		{"GET", "/admin"},
		{"GET", "/admin/config"},
		{"GET", "/api"},
		{"POST", "/api/admin/login"},
		{"GET", "/api/admin/config"},
		{"GET", "/api/admin/stats"},
		{"POST", "/api/call-upload"},
		{"POST", "/api/trunk-recorder-call-upload"},
		{"GET", "/api/v1/calls"},
		{"GET", "/api/plugin/demo/x"},
		{"GET", "/api/capabilities"},
		{"POST", "/"},
		{"PUT", "/main.js"},
		// A plugin may claim any path under /; only known shapes pass.
		{"GET", "/some/plugin/route"},
		// Paths rdio's mux would clean into something else.
		{"GET", "/assets/../api/admin/config"},
		{"GET", "/assets/..%2Fapi%2Fadmin%2Fconfig"},
		{"GET", "//api/admin/config"},
	}
	for _, b := range blocked {
		req, _ := http.NewRequest(b.method, "http://"+addr+b.path, nil)
		res, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("%s %s: want 404, got %d", b.method, b.path, res.StatusCode)
		}
	}
	if seen := f.saw(); len(seen) != 0 {
		t.Fatalf("blocked requests must not reach rdio at all, it saw %v", seen)
	}
}

func TestWebsocketPassesOnlyOnThePagesOwnURL(t *testing.T) {
	_, addr := start(t)
	for _, p := range []string{"/", "/stream"} {
		c, _, err := websocket.DefaultDialer.Dial("ws://"+addr+p, nil)
		if err != nil {
			t.Fatalf("ws %s: %v", p, err)
		}
		if err := c.WriteMessage(websocket.TextMessage, []byte(`["CFG"]`)); err != nil {
			t.Fatal(err)
		}
		_, msg, err := c.ReadMessage()
		c.Close()
		if err != nil || string(msg) != `["CFG"]` {
			t.Fatalf("ws %s: want the echo back through the proxy, got %q %v", p, msg, err)
		}
	}
	for _, p := range []string{"/admin", "/api/admin/config"} {
		_, res, err := websocket.DefaultDialer.Dial("ws://"+addr+p, nil)
		if err == nil || res == nil || res.StatusCode != http.StatusNotFound {
			t.Fatalf("ws %s: want a 404 refusal, got %v %v", p, res, err)
		}
	}
}

func TestPortIsZeroOnceStopped(t *testing.T) {
	up := httptest.NewServer(http.NotFoundHandler())
	defer up.Close()
	s, err := New("127.0.0.1:0", up.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx); close(done) }()
	for s.Port() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if s.Port() != 0 {
		t.Fatalf("a stopped proxy must not advertise a port, got %d", s.Port())
	}
}
