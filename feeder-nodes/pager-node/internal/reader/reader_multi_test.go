package reader

import (
	"os"
	"strings"
	"testing"
)

func nswGroup() Group {
	return Group{
		Name:        "NSWRFS+FRNSW",
		DeviceIndex: 1,
		Serial:      "00000002",
		CentreHz:    148_800_000,
		SampleRate:  1_058_400,
		PPM:         -3,
		Gain:        "49.6",
		Protocols:   []string{"POCSAG512", "POCSAG1200"},
		Channels: []Channel{
			{Label: "NSWRFS", FreqMHz: 148.5875},
			{Label: "FRNSW", FreqMHz: 148.9625},
		},
		RelayURL: "http://127.0.0.1:17390/pager",
		AudioURL: "http://127.0.0.1:17390/audio",
		Exe:      "/opt/nswpsn/nodeagent-pager",
	}
}

func renderGroup(t *testing.T, g Group) string {
	t.Helper()
	path, err := WriteGroup(t.TempDir(), g)
	if err != nil {
		t.Fatalf("write group script: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read group script: %v", err)
	}
	return string(raw)
}

func TestGroupScriptOpensTheDongleOnce(t *testing.T) {
	script := renderGroup(t, nswGroup())
	for _, want := range []string{
		"rtl_sdr -d 1 -g 49.6 -p -3 -f 148800000 -s 1058400 - ",
		"'/opt/nswpsn/nodeagent-pager' channelize 1058400 148800000",
		"148587500 >(multimon-ng -b1 -c -a POCSAG512 -a POCSAG1200 -f alpha -t raw /dev/stdin | post 'NSWRFS' '148.5875')",
		"148962500 >(multimon-ng -b1 -c -a POCSAG512 -a POCSAG1200 -f alpha -t raw /dev/stdin | post 'FRNSW' '148.9625')",
		"USB serial 00000002",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("group script missing %q:\n%s", want, script)
		}
	}
	if strings.Count(script, "rtl_sdr -d") != 1 {
		t.Fatalf("the dongle must be opened exactly once:\n%s", script)
	}
	if strings.Contains(script, "http://127.0.0.1:17390/audio") {
		t.Fatalf("no channel asked for audio:\n%s", script)
	}
}

func TestGroupScriptAutoGainOmitsTheFlag(t *testing.T) {
	g := nswGroup()
	g.Gain = "auto"
	if script := renderGroup(t, g); !strings.Contains(script, "rtl_sdr -d 1 -p -3 -f ") {
		t.Fatalf("auto gain must leave -g off rtl_sdr:\n%s", script)
	}
}

func TestGroupScriptTapsOnlyTheListenedChannel(t *testing.T) {
	g := nswGroup()
	g.Channels[1].Audio = true
	script := renderGroup(t, g)
	if strings.Count(script, "http://127.0.0.1:17390/audio") != 1 {
		t.Fatalf("want one audio tap:\n%s", script)
	}
	if !strings.Contains(script, "148962500 >(tee --output-error=warn >(curl") {
		t.Fatalf("the tap belongs on FRNSW:\n%s", script)
	}
}

func TestGroupScriptRefusesUnsafeInput(t *testing.T) {
	g := nswGroup()
	g.Exe = "/opt/it's/nodeagent-pager"
	if _, err := WriteGroup(t.TempDir(), g); err == nil {
		t.Fatal("an agent path with a quote must be refused")
	}
	g = nswGroup()
	g.Channels = g.Channels[:1]
	if _, err := WriteGroup(t.TempDir(), g); err == nil {
		t.Fatal("a one-channel group belongs to rtl_fm, not channelize")
	}
	g = nswGroup()
	g.Channels[0].Label = "RFS'; reboot; '"
	if script := renderGroup(t, g); strings.Contains(script, "reboot;") || strings.Contains(script, "RFS'") {
		t.Fatalf("a crafted label survived into the script:\n%s", script)
	}
}
