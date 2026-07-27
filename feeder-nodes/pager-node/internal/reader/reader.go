// Package reader renders the per-frequency pager capture script (reader.sh).
//
// Each script is a self-contained bash pipeline: rtl_fm demodulates one
// frequency off a specific dongle (addressed by USB serial via -d), pipes NBFM
// audio into multimon-ng which decodes POCSAG, and each decoded line is POSTed to
// the agent's loopback relay. The supervisor launches the rendered script as a
// component and restarts it on crash.
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
)

//go:embed reader.sh.tmpl
var readerTmpl string

var tmpl = template.Must(template.New("reader").Parse(readerTmpl))

// defaultProtocols is used when the caller passes no protocol list.
var defaultProtocols = []string{"POCSAG512", "POCSAG1200", "POCSAG2400"}

// tmplData is the render context for reader.sh.tmpl.
type tmplData struct {
	Serial    string // dongle USB serial for rtl_fm -d
	FreqMHz   string // frequency in MHz, formatted without trailing zeros
	PPM       string // sample-clock correction for rtl_fm -p (measured at startup)
	Gain      string // fixed tuner gain (dB) for rtl_fm -g
	ProtoArgs string // "-a POCSAG512 -a POCSAG1200 ..." for multimon-ng
	RelayURL  string // loopback relay endpoint the decoded lines POST to
	Label     string // human/source label (also the X-Pager-Source header)
}

// Write renders the reader script for one frequency to <dir>/reader-<label>.sh
// (0755) and returns its path. deviceSerial pins the reader to a specific dongle;
// ppm is that dongle's measured sample-clock correction (rtl_fm -p, 0 = none);
// gain is the fixed tuner gain in dB (rtl_fm -g); protocols selects the
// multimon-ng POCSAG demodulators (defaulting to all three bauds when empty);
// relayURL is the loopback endpoint decoded lines are POSTed to (e.g.
// http://127.0.0.1:17390/pager).
func Write(dir, label string, freqMHz float64, deviceSerial string, ppm int, gain string, protocols []string, relayURL string) (path string, err error) {
	data := tmplData{
		// The script is executed with bash, so every interpolated value is a
		// potential shell-injection sink. Serial comes from device output and is
		// single-quoted in the template, but sanitise it to a safe charset anyway
		// (defense-in-depth against a crafted dongle EEPROM serial). Freq/PPM are
		// numeric-formatted; Gain is a constant; Label is sanitised by the caller;
		// ProtoArgs is allowlisted below.
		Serial:    sanitizeToken(deviceSerial),
		FreqMHz:   strconv.FormatFloat(freqMHz, 'f', -1, 64),
		PPM:       strconv.Itoa(ppm),
		Gain:      normalizeGain(gain),
		ProtoArgs: protoArgs(protocols),
		RelayURL:  relayURL,
		Label:     label,
	}

	var buf strings.Builder
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render reader script: %w", err)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create reader dir %q: %w", dir, err)
	}
	path = filepath.Join(dir, "reader-"+label+".sh")
	if err := os.WriteFile(path, []byte(buf.String()), 0o755); err != nil {
		return "", fmt.Errorf("write reader script %q: %w", path, err)
	}
	// WriteFile doesn't reset the mode of an existing file, so force the exec bit
	// (no-op on Windows, which the readers never run on anyway).
	if err := os.Chmod(path, 0o755); err != nil {
		return "", fmt.Errorf("chmod reader script %q: %w", path, err)
	}
	return path, nil
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
