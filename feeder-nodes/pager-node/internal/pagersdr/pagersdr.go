// Package pagersdr enumerates and prepares RTL-SDR dongles for the pager
// readers. It shells out to the librtlsdr command-line tools (rtl_test,
// rtl_eeprom) installed on the host by the node installer.
//
// The key job is guaranteeing each dongle has a UNIQUE, stable USB serial so a
// reader can be pinned to a specific antenna/frequency by serial (rtl_fm -d
// <serial>). Cheap RTL-SDR clones ship with identical serials ("00000001"), which
// makes per-device addressing ambiguous. We only rewrite EEPROM serials when they
// actually collide — an EEPROM write is a permanent hardware change, so a dongle
// that already has a distinct serial is left untouched.
package pagersdr

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Device is one enumerated RTL-SDR dongle.
type Device struct {
	Index  int    // librtlsdr device index (as reported by rtl_test)
	Serial string // USB serial ("SN:" from rtl_test); may be blank on unprogrammed clones
	PPM    int    // measured sample-clock error (rtl_test -p); 0 if unmeasured
}

const (
	rtlTestBin   = "rtl_test"
	rtlEepromBin = "rtl_eeprom"

	detectTimeout = 30 * time.Second
	eepromTimeout = 30 * time.Second

	// PPMMeasureDur is how long rtl_test -p runs per dongle at startup. It reports
	// a cumulative estimate about every 10s, so this gives a few readings to settle.
	PPMMeasureDur = 30 * time.Second
)

// cumPpmRe captures the integer from rtl_test's "cumulative PPM: <n>" lines.
var cumPpmRe = regexp.MustCompile(`cumulative PPM:\s*(-?\d+)`)

// deviceLineRe matches a device-list entry such as
// "  0:  Realtek, RTL2838UHIDIR, SN: 00000001", capturing the index and the
// trailing descriptor (from which the serial is extracted).
var deviceLineRe = regexp.MustCompile(`^\s*(\d+):\s+(.*)$`)

// foundRe matches the "Found N device(s):" header that precedes the device list.
var foundRe = regexp.MustCompile(`Found\s+\d+\s+device`)

// Detect enumerates attached RTL-SDR dongles by running `rtl_test -t` (which
// prints the device list to stderr, runs a brief tuner test, and exits) and
// parsing its output. It returns a clear error if rtl_test is missing/fails and
// no devices could be parsed.
func Detect() ([]Device, error) {
	ctx, cancel := context.WithTimeout(context.Background(), detectTimeout)
	defer cancel()

	// rtl_test prints to stderr; CombinedOutput captures both streams. A non-zero
	// exit is tolerated as long as the device list parsed (some builds exit non-zero
	// after the tuner test), so we key the error on "did we parse any device?".
	out, err := exec.CommandContext(ctx, rtlTestBin, "-t").CombinedOutput()
	text := string(out)
	devs := parseDevices(text)
	if len(devs) == 0 {
		if err != nil {
			return nil, fmt.Errorf("run %s: %w: %s", rtlTestBin, err, strings.TrimSpace(text))
		}
		return nil, fmt.Errorf("%s reported no RTL-SDR devices", rtlTestBin)
	}
	return devs, nil
}

// parseDevices extracts the device list from rtl_test output. It starts
// collecting at the "Found N device(s):" header and stops at the first line that
// isn't a device entry.
func parseDevices(text string) []Device {
	var devs []Device
	inList := false
	for _, line := range strings.Split(text, "\n") {
		if !inList {
			if foundRe.MatchString(line) {
				inList = true
			}
			continue
		}
		m := deviceLineRe.FindStringSubmatch(strings.TrimRight(line, "\r"))
		if m == nil {
			// End of the list (blank line or "Using device ...").
			if len(devs) > 0 {
				break
			}
			continue
		}
		idx, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		devs = append(devs, Device{Index: idx, Serial: extractSerial(m[2])})
	}
	return devs
}

// extractSerial pulls the value after "SN:" from a device descriptor, or "" if
// none is present (an unprogrammed dongle).
func extractSerial(desc string) string {
	i := strings.LastIndex(desc, "SN:")
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(desc[i+len("SN:"):])
}

// MeasurePPM estimates a dongle's sample-clock error in ppm by running
// `rtl_test -p` on it for `dur`. rtl_test measures the RTL's actual vs nominal
// sample rate against the host clock (no RF signal or antenna needed) and prints
// a cumulative estimate roughly every 10s; we let it run for `dur`, kill it, and
// take the last cumulative reading. The device MUST be free (no reader running).
// Returns 0 + error if no reading was produced (tool missing, device busy, or
// `dur` too short for even one report — the caller falls back to ppm=0).
func MeasurePPM(index int, dur time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	// rtl_test -p runs until interrupted; the context timeout kills it after dur,
	// which makes CombinedOutput return an error — we ignore it and parse whatever
	// cumulative readings were emitted before the kill.
	out, _ := exec.CommandContext(ctx, rtlTestBin, "-d", strconv.Itoa(index), "-p").CombinedOutput()
	text := string(out)
	ms := cumPpmRe.FindAllStringSubmatch(text, -1)
	if len(ms) == 0 {
		return 0, fmt.Errorf("%s -p produced no cumulative PPM reading for device %d: %s",
			rtlTestBin, index, strings.TrimSpace(tail(text, 200)))
	}
	last := ms[len(ms)-1][1]
	ppm, err := strconv.Atoi(last)
	if err != nil {
		return 0, fmt.Errorf("parse ppm %q: %w", last, err)
	}
	return ppm, nil
}

// tail returns up to the last n bytes of s, for compact error context.
func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// EnsureDistinctSerials guarantees every dongle has a unique, non-blank serial.
//
// It ONLY reprograms EEPROMs when serials collide (two or more dongles share a
// serial, or a serial is blank). When all serials are already distinct and
// non-empty it does nothing and returns the devices unchanged — an EEPROM write
// is a permanent hardware change and must not happen gratuitously.
//
// On collision it assigns each device (in index order) a fresh serial
// "0000000N" via `rtl_eeprom -d <index> -s ...`, logs every write, notes that a
// USB replug/reset may be needed for the new serials to take effect, then
// re-detects and returns the updated list.
func EnsureDistinctSerials(devs []Device) ([]Device, error) {
	if len(devs) == 0 {
		return devs, nil
	}
	if !hasCollision(devs) {
		return devs, nil
	}

	log.Printf("pagersdr: SDR serials collide or are blank; reprogramming EEPROM to assign unique serials")
	for i, d := range devs {
		serial := fmt.Sprintf("%08d", i+1) // 0000000N
		if err := writeSerial(d.Index, serial); err != nil {
			return devs, fmt.Errorf("assign serial %q to device %d: %w", serial, d.Index, err)
		}
		log.Printf("pagersdr: wrote EEPROM serial %q to device index %d (PERMANENT hardware change)", serial, d.Index)
	}
	log.Printf("pagersdr: EEPROM serials rewritten; a USB replug/reset may be required for the new serials to take effect")

	updated, err := Detect()
	if err != nil {
		return devs, fmt.Errorf("re-detect after EEPROM rewrite: %w", err)
	}
	return updated, nil
}

// hasCollision reports whether any serial is blank or shared by two or more
// devices.
func hasCollision(devs []Device) bool {
	seen := make(map[string]int, len(devs))
	for _, d := range devs {
		if strings.TrimSpace(d.Serial) == "" {
			return true
		}
		seen[d.Serial]++
	}
	for _, n := range seen {
		if n > 1 {
			return true
		}
	}
	return false
}

// writeSerial runs rtl_eeprom to set a device's USB serial. rtl_eeprom prompts
// for confirmation before writing, so we feed "y" on stdin to auto-confirm.
func writeSerial(index int, serial string) error {
	ctx, cancel := context.WithTimeout(context.Background(), eepromTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, rtlEepromBin, "-d", strconv.Itoa(index), "-s", serial)
	cmd.Stdin = strings.NewReader("y\n") // auto-confirm the interactive write prompt
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w: %s", rtlEepromBin, err, strings.TrimSpace(string(out)))
	}
	return nil
}
