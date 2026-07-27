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

// bracketMnemonicRe matches multimon-ng's bracketed control-code mnemonics — how
// it renders unprintable POCSAG characters, e.g. "<EOT>", "<NUL>", "<STX>", and
// 2-letter ones like "<CR>"/"<LF>". These are message framing, never real page
// content. This is the same class Pagermon's own reader.js strips (with
// /<[A-Za-z]{3}>/g); we widen to {2,3} to also catch the 2-letter controls.
var bracketMnemonicRe = regexp.MustCompile(`<[A-Za-z]{2,3}>`)

// natCharReplacer maps the POCSAG 7-bit national characters Ä/Ü to the [ ] they
// stand in for on some paging networks (again mirroring Pagermon's reader.js).
var natCharReplacer = strings.NewReplacer("Ä", "[", "Ü", "]")

// cleanText strips the framing/padding artifacts a decoded POCSAG page carries.
// multimon-ng renders unprintable control codes as BRACKETED mnemonics
// ("<EOT>", "<NUL>", …), so a page terminating on a control char comes out like
// "…288] <EOT>"; some streams also carry raw control bytes. We (1) turn raw
// control bytes into spaces, (2) remove the bracketed mnemonics, (3) map Ä/Ü to
// [ ], then (4) collapse whitespace + trim. This mirrors Pagermon's reference
// reader so our pages match what its normal readers produce. Real content
// (unbracketed words like "CAN") is never touched.
func cleanText(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			b.WriteByte(' ') // raw control byte -> space (defense; runs collapsed below)
		} else {
			b.WriteRune(r)
		}
	}
	out := bracketMnemonicRe.ReplaceAllString(b.String(), "")
	out = natCharReplacer.Replace(out)
	return strings.Join(strings.Fields(out), " ")
}
