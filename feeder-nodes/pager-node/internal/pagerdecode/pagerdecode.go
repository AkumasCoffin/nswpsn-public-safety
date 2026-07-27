// Package pagerdecode parses multimon-ng POCSAG stdout lines into structured
// pager messages. multimon-ng prints one decoded page per line, e.g.
//
//	POCSAG1200: Address:  1234567  Function: 0  Alpha:   SOME MESSAGE TEXT
//	POCSAG512: Address:  0987654  Function: 3  Numeric:  123 456
//
// Non-POCSAG output (multimon's banner / stderr noise) is ignored.
package pagerdecode

import (
	"regexp"
	"strconv"
	"strings"
)

// Message is one decoded POCSAG page.
type Message struct {
	Address  string // capcode digits, spaces trimmed
	Function int    // 0-3
	Alpha    bool   // true for an alphanumeric page, false for numeric
	Text     string // the Alpha or Numeric payload, trimmed
}

// pocsagRe matches a decoded POCSAG line across baud prefixes (POCSAG512 /
// POCSAG1200 / POCSAG2400 / ...). It captures the address digits, the function
// bits (0-3), the payload kind (Alpha/Numeric), and the raw payload text.
var pocsagRe = regexp.MustCompile(`^POCSAG\d+:\s*Address:\s*([0-9]+)\s+Function:\s*([0-3])\s+(Alpha|Numeric):(.*)$`)

// ParseLine parses a single multimon-ng output line. It returns (msg, true) for
// a decodable POCSAG page line, or (nil, false) for any line that isn't one
// (banners, blank lines, stderr noise).
func ParseLine(line string) (*Message, bool) {
	m := pocsagRe.FindStringSubmatch(strings.TrimRight(line, "\r\n"))
	if m == nil {
		return nil, false
	}
	fn, err := strconv.Atoi(m[2])
	if err != nil {
		return nil, false
	}
	return &Message{
		Address:  strings.TrimSpace(m[1]),
		Function: fn,
		Alpha:    m[3] == "Alpha",
		Text:     cleanText(m[4]),
	}, true
}

// ctrlMnemonics are ASCII framing/terminator control-code NAMES that some POCSAG
// decoders emit as literal TEXT instead of the raw byte — e.g. a page ends with
// EOT (0x04) shown as "EOT", starts with STX, or pads with NUL. All are message
// framing, never real page content, and none are English words, so stripping
// them at the message edges is safe. (The raw-byte forms are removed separately
// below; this catches the text-mnemonic form.)
var ctrlMnemonics = map[string]bool{
	"NUL": true, // 0x00 padding
	"SOH": true, // 0x01 start of heading
	"STX": true, // 0x02 start of text
	"ETX": true, // 0x03 end of text
	"EOT": true, // 0x04 end of transmission
	"ETB": true, // 0x17 end of block
}

// cleanText strips the framing/padding artifacts a decoded POCSAG page carries.
// Alpha pages are padded/terminated over the air with control codes (NUL 0x00,
// EOT 0x04, …); depending on the decoder these arrive either as raw control
// BYTES or as their literal text mnemonic ("NUL", "EOT"). Left in, they render
// downstream as e.g. "MESSAGE NUL NUL" / "…288] EOT EOT". We (1) drop every
// control byte (< 0x20 and DEL 0x7F), then (2) drop those same codes' text
// mnemonics where they lead/trail the message. Printable content (POCSAG alpha
// is 7-bit ASCII) is otherwise kept verbatim; only edges are touched so a real
// mid-message word is never removed.
func cleanText(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r == '\t':
			b.WriteByte(' ')
		case r < 0x20 || r == 0x7f:
			// Control byte (NUL/EOT/…): emit a space so words it sat between don't
			// fuse; runs are collapsed below.
			b.WriteByte(' ')
		default:
			b.WriteRune(r)
		}
	}
	fields := strings.Fields(b.String())
	for len(fields) > 0 && ctrlMnemonics[fields[0]] {
		fields = fields[1:]
	}
	for len(fields) > 0 && ctrlMnemonics[fields[len(fields)-1]] {
		fields = fields[:len(fields)-1]
	}
	return strings.Join(fields, " ")
}
