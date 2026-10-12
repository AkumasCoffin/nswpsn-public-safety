package reader

import (
	"os"
	"strings"
	"testing"
)

// render writes a reader script and returns its contents.
func render(t *testing.T, index int, serial string) string {
	t.Helper()
	path, err := Write(t.TempDir(), "NSWRFS", 148.5875, index, serial, -3, "49.6",
		[]string{"POCSAG512"}, "http://127.0.0.1:17390/pager", "")
	if err != nil {
		t.Fatalf("write reader script: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read reader script: %v", err)
	}
	return string(raw)
}

// rtl_fm must be given the device INDEX, never the serial.
//
// librtlsdr's verbose_device_search() runs strtol over -d first and, when the
// whole string parses to a number below the device count, treats it as an
// index. The factory serial "00000001" parses (base 0, so leading zeros make it
// octal) to 1, so on a two-dongle node `-d 00000001` opened device 1 — both
// readers resolved to the same dongle, the second died on usb_claim_interface,
// and one of the two locked frequencies never decoded a page.
func TestReaderAddressesTheDongleByIndex(t *testing.T) {
	script := render(t, 1, "00000001")

	if !strings.Contains(script, "rtl_fm -d 1 ") {
		t.Fatalf("want the dongle addressed by index, got:\n%s", script)
	}
	for _, bad := range []string{"-d '00000001'", "-d 00000001", `-d "00000001"`} {
		if strings.Contains(script, bad) {
			t.Fatalf("the serial reached -d (%q), which librtlsdr may read as an index:\n%s", bad, script)
		}
	}
	// The serial is still recorded, so an operator can tell which dongle a
	// reader is on without re-running rtl_test.
	if !strings.Contains(script, "USB serial 00000001") {
		t.Fatalf("the serial should be recorded for diagnosis:\n%s", script)
	}
}

func TestReaderStillSanitisesTheRecordedSerial(t *testing.T) {
	// It only lands in a comment now, but a serial is device-supplied text in a
	// file that bash executes: it must not be able to end the comment line.
	script := render(t, 0, "A B\n$(reboot)")
	if strings.Contains(script, "$(reboot)") || strings.Contains(script, "reboot)") {
		t.Fatalf("a crafted serial survived into the script:\n%s", script)
	}
	if strings.Count(script, "rtl_fm -d 0 ") != 1 {
		t.Fatalf("want one rtl_fm invocation on index 0, got:\n%s", script)
	}
}
