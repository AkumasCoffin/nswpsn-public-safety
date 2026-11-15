package wsclient

import "testing"

type fakeLocal struct {
	port int
	host string
}

func (f fakeLocal) Port() int    { return f.port }
func (f fakeLocal) Host() string { return f.host }

func TestLocalListenReport(t *testing.T) {
	if (&Client{}).localListenReport() != nil {
		t.Fatal("no listener page: nothing to report")
	}
	if (&Client{localListen: fakeLocal{port: 0, host: "0.0.0.0"}}).localListenReport() != nil {
		t.Fatal("not bound yet: nothing to report")
	}
	for _, h := range []string{"127.0.0.1", "::1", "localhost"} {
		if (&Client{localListen: fakeLocal{port: 17380, host: h}}).localListenReport() != nil {
			t.Fatalf("bound to %s: unreachable from the network, must not be reported", h)
		}
	}
	r := (&Client{localListen: fakeLocal{port: 17380, host: "192.168.1.50"}}).localListenReport()
	if r == nil || r.IP != "192.168.1.50" || r.Port != 17380 {
		t.Fatalf("bound to one interface: report that address, got %+v", r)
	}
	// Every interface: the outbound LAN address, when the machine has a route.
	if localIP() != "" {
		r := (&Client{localListen: fakeLocal{port: 17380, host: "0.0.0.0"}}).localListenReport()
		if r == nil || r.IP != localIP() {
			t.Fatalf("bound to every interface: report the LAN address, got %+v", r)
		}
	}
}
