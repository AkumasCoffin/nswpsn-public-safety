// Package reader renders the pager capture scripts (reader.sh), one per dongle.
//
// Each script is a self-contained bash pipeline. With one frequency on the
// dongle (Write), rtl_fm demodulates it, pipes NBFM audio into multimon-ng
// which decodes POCSAG, and each decoded line is POSTed to the agent's loopback
// relay. With several (WriteGroup), rtl_sdr's raw I/Q goes through the agent's
// channelize command instead, which feeds one multimon-ng per frequency. The
// supervisor launches the rendered script as a component and restarts it on
// crash.
//
// WHY -d TAKES THE INDEX, NOT THE SERIAL. The dongle's identity is still its
// USB serial — that is what pagersdr guarantees unique, and what pairs a
// frequency with an antenna — but librtlsdr's verbose_device_search() runs
// strtol over the -d argument FIRST, and when the whole string parses to a
// number below the device count it uses that number as a device INDEX; only
// otherwise does it compare serials. The RTL-SDR Blog factory serial
// "00000001" parses (base 0, so leading zeros make it octal) to 1, so on a
// two-dongle node `rtl_fm -d 00000001` opened device 1 — and with both readers
// resolving to the same dongle the second died on usb_claim_interface after
// five restarts, leaving one of the two locked frequencies permanently silent.
// A serial carrying a dash or an underscore had the opposite problem: sanitised
// into a string no device reports, and that reader could never start at all.
//
// So the agent resolves the serial to an index itself — fresh at every detect
// and rescan — and hands rtl_fm the index, which has only one meaning.
package reader

import (
	_ "embed"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/channelize"
)

//go:embed reader.sh.tmpl
var readerTmpl string

var tmpl = template.Must(template.New("reader").Parse(readerTmpl))

// defaultProtocols is used when the caller passes no protocol list.
var defaultProtocols = []string{"POCSAG512", "POCSAG1200", "POCSAG2400"}

// tmplData is the render context for reader.sh.tmpl.
type tmplData struct {
	DeviceIndex string // librtlsdr device index for rtl_fm -d
	Serial      string // dongle USB serial, recorded in a script comment
	FreqMHz     string // frequency in MHz, formatted without trailing zeros
	PPM         string // sample-clock correction for rtl_fm -p (measured at startup)
	Gain        string // fixed tuner gain (dB) for rtl_fm -g
	ProtoArgs   string // "-a POCSAG512 -a POCSAG1200 ..." for multimon-ng
	RelayURL    string // loopback relay endpoint the decoded lines POST to
	AudioURL    string // loopback relay /audio endpoint to tee PCM to (empty = off)
	Label       string // human/source label (also the X-Pager-Source header)
}

// Write renders the reader script for one frequency to <dir>/reader-<label>.sh
// (0755) and returns its path. deviceIndex is the librtlsdr index the caller
// resolved for this dongle and is what pins the reader to it (see the package
// comment); deviceSerial is that dongle's serial, recorded in the script for
// diagnosis. ppm is the dongle's measured sample-clock correction (rtl_fm -p,
// 0 = none); gain is the fixed tuner gain in dB (rtl_fm -g); protocols selects
// the multimon-ng POCSAG demodulators (defaulting to all three bauds when
// empty); relayURL is the loopback endpoint decoded lines are POSTed to (e.g.
// http://127.0.0.1:17390/pager). audioURL, when non-empty, is the loopback
// /audio endpoint the raw PCM is tee'd to for the browser monitor; empty leaves
// the audio tap out of the pipeline entirely (no overhead when nobody listens).
func Write(dir, label string, freqMHz float64, deviceIndex int, deviceSerial string, ppm int, gain string, protocols []string, relayURL, audioURL string) (path string, err error) {
	data := tmplData{
		// The script is executed with bash, so every interpolated value is a
		// potential shell-injection sink. The index is numeric-formatted; the
		// serial comes from device output and only lands in a comment, but
		// sanitise it anyway (defense-in-depth against a crafted dongle EEPROM
		// serial, and against it breaking out of the comment line). Freq/PPM are
		// numeric-formatted; Gain is a constant; Label is sanitised by the caller;
		// ProtoArgs is allowlisted below.
		DeviceIndex: strconv.Itoa(deviceIndex),
		Serial:      sanitizeToken(deviceSerial),
		FreqMHz:     strconv.FormatFloat(freqMHz, 'f', -1, 64),
		PPM:         strconv.Itoa(ppm),
		Gain:        normalizeGain(gain),
		ProtoArgs:   protoArgs(protocols),
		RelayURL:    relayURL,
		AudioURL:    audioURL,
		Label:       label,
	}

	var buf strings.Builder
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render reader script: %w", err)
	}
	return writeScript(dir, label, buf.String())
}

//go:embed reader-multi.sh.tmpl
var multiTmplText string

var multiTmpl = template.Must(template.New("reader-multi").Parse(multiTmplText))

// Channel is one frequency on a dongle that receives several at once.
type Channel struct {
	Label   string  // sanitised by the caller: the X-Pager-Source header and the script name
	FreqMHz float64 // the configured frequency
	Audio   bool    // tee this channel's audio to the relay's /audio endpoint
}

// Group describes a dongle that receives several frequencies at once (see
// internal/channelize, which chose CentreHz and SampleRate).
type Group struct {
	Name        string // script and component name, e.g. "NSWRFS+FRNSW"
	DeviceIndex int
	Serial      string
	CentreHz    int64
	SampleRate  int
	PPM         int
	Gain        string
	Protocols   []string
	Channels    []Channel
	RelayURL    string
	AudioURL    string
	// Exe is the agent binary whose "channelize" command splits the I/Q.
	Exe string
}

type multiChannelData struct {
	Label, FreqMHz, Hz string
	Audio              bool
}

// multiTmplData is the render context for reader-multi.sh.tmpl. Every field
// is already formatted or sanitised for bash.
type multiTmplData struct {
	DeviceIndex string
	Serial      string
	CentreHz    string
	SampleRate  string
	PPM         string
	Gain        string
	ProtoArgs   string
	RelayURL    string
	AudioURL    string
	Exe         string
	Channels    []multiChannelData
}

// WriteGroup renders the reader script for a dongle that receives several
// frequencies at once to <dir>/reader-<name>.sh and returns its path. Every
// value reaches bash from numbers formatted here, the allowlisted protocol
// list, sanitised labels, the agent's own URLs, and the agent's own path,
// which is refused if it could break out of its quotes.
func WriteGroup(dir string, g Group) (path string, err error) {
	if g.Exe == "" || strings.ContainsAny(g.Exe, "'\n\r") {
		return "", fmt.Errorf("agent path %q cannot be quoted into a reader script", g.Exe)
	}
	if len(g.Channels) < 2 {
		return "", fmt.Errorf("a multi-frequency reader needs 2+ channels, got %d", len(g.Channels))
	}
	audioURL := ""
	chans := make([]multiChannelData, len(g.Channels))
	for i, c := range g.Channels {
		chans[i] = multiChannelData{
			Label:   sanitizeLabelToken(c.Label),
			FreqMHz: strconv.FormatFloat(c.FreqMHz, 'f', -1, 64),
			Hz:      strconv.FormatInt(channelize.HzFromMHz(c.FreqMHz), 10),
			Audio:   c.Audio && g.AudioURL != "",
		}
		if chans[i].Audio {
			audioURL = g.AudioURL
		}
	}
	data := multiTmplData{
		DeviceIndex: strconv.Itoa(g.DeviceIndex),
		Serial:      sanitizeToken(g.Serial),
		CentreHz:    strconv.FormatInt(g.CentreHz, 10),
		SampleRate:  strconv.Itoa(g.SampleRate),
		PPM:         strconv.Itoa(g.PPM),
		Gain:        normalizeGain(g.Gain),
		ProtoArgs:   protoArgs(g.Protocols),
		RelayURL:    g.RelayURL,
		AudioURL:    audioURL,
		Exe:         g.Exe,
		Channels:    chans,
	}
	var buf strings.Builder
	if err := multiTmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render multi-frequency reader script: %w", err)
	}
	return writeScript(dir, sanitizeLabelToken(g.Name), buf.String())
}

// writeScript writes an executable reader-<name>.sh into dir.
func writeScript(dir, name, body string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create reader dir %q: %w", dir, err)
	}
	path := filepath.Join(dir, "reader-"+name+".sh")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		return "", fmt.Errorf("write reader script %q: %w", path, err)
	}
	// WriteFile doesn't reset the mode of an existing file, so force the exec bit
	// (no-op on Windows, which the readers never run on anyway).
	if err := os.Chmod(path, 0o755); err != nil {
		return "", fmt.Errorf("chmod reader script %q: %w", path, err)
	}
	return path, nil
}

// sanitizeLabelToken keeps a label to the characters the reader manager's own
// sanitising produces (letters, digits, '-', '_') plus the '+' that joins a
// group's labels. A defence in depth: labels are single-quoted in the script.
func sanitizeLabelToken(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '+' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// protoArgs builds the multimon-ng demodulator flags from a protocol list,
// falling back to all three POCSAG bauds when the list is empty. Each protocol
// is interpolated UNQUOTED into the bash reader script (it must expand to
// multiple `-a X` args), so every entry MUST be allowlisted to an alnum token —
// otherwise a protocol string pushed via the backend config (e.g.
// "$(reboot)") would be a command-injection / RCE vector.
var protoRe = regexp.MustCompile(`^[A-Za-z0-9]+$`)

func protoArgs(protocols []string) string {
	ps := protocols
	if len(ps) == 0 {
		ps = defaultProtocols
	}
	parts := make([]string, 0, len(ps)*2)
	for _, p := range ps {
		p = strings.TrimSpace(p)
		if !protoRe.MatchString(p) {
			log.Printf("reader: ignoring invalid protocol %q (must be alphanumeric)", p)
			continue
		}
		parts = append(parts, "-a", p)
	}
	if len(parts) == 0 {
		// Every entry was rejected — fall back to the safe defaults rather than
		// emit a multimon-ng invocation with no demodulators.
		for _, p := range defaultProtocols {
			parts = append(parts, "-a", p)
		}
	}
	return strings.Join(parts, " ")
}

// normalizeGain validates the (backend-supplied) tuner gain before it is
// interpolated UNQUOTED into the reader script's `rtl_fm -g` arg. Returns:
//   - ""            for "auto"/empty/invalid → template omits -g (hardware AGC)
//   - "<number>"    for a plausible numeric gain in dB (formatted canonically)
// An out-of-range or non-numeric value falls back to auto rather than a fixed
// default so a crafted config can never inject shell here, and a bad number
// degrades safely instead of running the wrong gain silently.
func normalizeGain(gain string) string {
	g := strings.TrimSpace(strings.ToLower(gain))
	if g == "" || g == "auto" {
		return ""
	}
	v, err := strconv.ParseFloat(g, 64)
	if err != nil || v < 0 || v > 60 {
		log.Printf("reader: ignoring invalid gain %q (want 0–60 dB or \"auto\"); using auto gain", gain)
		return ""
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// sanitizeToken strips a value to a shell-safe alnum token for interpolation
// into the reader script (used for the dongle serial).
func sanitizeToken(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
