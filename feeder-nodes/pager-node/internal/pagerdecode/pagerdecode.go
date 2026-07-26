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
		Text:     strings.TrimSpace(m[4]),
	}, true
}
