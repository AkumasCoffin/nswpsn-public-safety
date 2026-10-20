package channelize

import (
	"bytes"
	"math/bits"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// The end-to-end check: real POCSAG pages, keyed at the same moment on both
// NSW frequencies, through Run, into the real multimon-ng the nodes run, with
// the flags the reader script gives it. Skipped where multimon-ng is not on
// PATH (it is Linux-only, like the readers).

const (
	pocsagSync = 0x7CD215D8
	pocsagIdle = 0x7A89C197
)

// pocsagWord completes 21 data bits (flag + 20) into a 32-bit codeword: the
// BCH(31,21) check bits, then even parity.
func pocsagWord(data21 uint32) uint32 {
	v := data21 << 10
	rem := v
	for i := 30; i >= 10; i-- {
		if rem&(1<<uint(i)) != 0 {
			rem ^= 0x769 << uint(i-10)
		}
	}
	cw := (v | rem) << 1
	if bits.OnesCount32(cw)%2 == 1 {
		cw |= 1
	}
	return cw
}

// pocsagPage encodes one alphanumeric page as transmitted bits: preamble,
// then batches of sync + 16 codewords, with the address in the frame its
// capcode selects. text must be a multiple of 20 characters long, so the
// 7-bit characters fill whole codewords and no padding shows in the decode.
func pocsagPage(capcode uint32, function uint32, text string) []int {
	var msg []int
	for _, c := range []byte(text) {
		for b := 0; b < 7; b++ { // 7-bit characters, least significant bit first
			msg = append(msg, int(c>>b)&1)
		}
	}
	words := []uint32{pocsagWord(((capcode >> 3) << 2) | (function & 3))}
	for i := 0; i < len(msg); i += 20 {
		var v uint32 = 1 << 20 // message flag
		for b := 0; b < 20; b++ {
			v |= uint32(msg[i+b]) << (19 - b)
		}
		words = append(words, pocsagWord(v))
	}

	var out []int
	for i := 0; i < 576; i++ {
		out = append(out, 1-i%2) // preamble: 1010...
	}
	emit := func(w uint32) {
		for b := 31; b >= 0; b-- {
			out = append(out, int(w>>b)&1)
		}
	}
	// Idle up to the capcode's frame, the page, one idle to end it, then
	// idle to the end of the batch.
	frame := int(capcode & 7)
	var cws []uint32
	for len(cws) < 2*frame {
		cws = append(cws, pocsagIdle)
	}
	cws = append(cws, words...)
	cws = append(cws, pocsagIdle)
	for len(cws)%16 != 0 {
		cws = append(cws, pocsagIdle)
	}
	for i, w := range cws {
		if i%16 == 0 {
			emit(pocsagSync)
		}
		emit(w)
	}
	return out
}

func TestPocsagEncoderMakesValidCodewords(t *testing.T) {
	if got := pocsagWord(pocsagIdle >> 11); got != pocsagIdle {
		t.Fatalf("idle codeword: got %08x, want %08x", got, uint32(pocsagIdle))
	}
	if got := pocsagWord(pocsagSync >> 11); got != pocsagSync {
		t.Fatalf("sync codeword: got %08x, want %08x", got, uint32(pocsagSync))
	}
}

func TestBothFrequenciesDecodeThroughMultimon(t *testing.T) {
	mm, err := exec.LookPath("multimon-ng")
	if err != nil {
		t.Skip("multimon-ng not on PATH")
	}

	const (
		rfsCap, frCap   = 1234560, 1654321 // addresses are 21 bits: at most 2097151
		rfsText, frText = "RFS TEST PAGE ALPHA1", "FRNSW TEST PAGE BRVO"
	)
	groups, _ := Assign([]int64{nswRFS, nswFRNSW}, 1, MaxChannelsPerDongle)
	g := groups[0]

	// POCSAG sends a 1 as the lower frequency; synth sends a 1 as the upper,
	// so the bits go over inverted.
	invert := func(b []int) []int {
		out := make([]int, len(b))
		for i, v := range b {
			out[i] = 1 - v
		}
		return out
	}
	rfs := fsk{hz: nswRFS, baud: 512, devHz: 4500, amp: 0.3, mistune: 800, bits: invert(pocsagPage(rfsCap, 3, rfsText))}
	fr := fsk{hz: nswFRNSW, baud: 1200, devHz: 4500, amp: 0.3, mistune: -600, bits: invert(pocsagPage(frCap, 0, frText))}
	seconds := float64(len(rfs.bits))/rfs.baud + 0.5
	in := synth(g.SampleRate, g.CentreHz, seconds, 0.03, rfs, fr)

	audio := split(t, bytes.NewReader(in), g.SampleRate, g.CentreHz, nswRFS, nswFRNSW)
	decode := func(samples []int16) string {
		var pcm bytes.Buffer
		for _, s := range samples {
			pcm.WriteByte(byte(s))
			pcm.WriteByte(byte(uint16(s) >> 8))
		}
		cmd := exec.Command(mm, "-b1", "-c", "-a", "POCSAG512", "-a", "POCSAG1200", "-a", "POCSAG2400",
			"-f", "alpha", "-t", "raw", "/dev/stdin")
		cmd.Stdin = &pcm
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("multimon-ng: %v", err)
		}
		return string(out)
	}

	rfsOut := decode(audio[0])
	frOut := decode(audio[1])
	t.Logf("RFS channel:\n%s", rfsOut)
	t.Logf("FRNSW channel:\n%s", frOut)

	// One line per page: "POCSAG512: Address: 1234560  Function: 3  Alpha:   ...".
	page := func(out, proto string, capcode int, fn, text string) bool {
		for _, line := range strings.Split(out, "\n") {
			f := strings.Fields(line)
			if len(f) >= 6 && f[0] == proto+":" && f[2] == strconv.Itoa(capcode) && f[4] == fn &&
				strings.HasSuffix(strings.TrimSpace(line), text) {
				return true
			}
		}
		return false
	}
	if !page(rfsOut, "POCSAG512", rfsCap, "3", rfsText) {
		t.Fatalf("the RFS page did not decode on the RFS channel:\n%s", rfsOut)
	}
	if !page(frOut, "POCSAG1200", frCap, "0", frText) {
		t.Fatalf("the FRNSW page did not decode on the FRNSW channel:\n%s", frOut)
	}
	if strings.Contains(rfsOut, frText) || strings.Contains(frOut, rfsText) {
		t.Fatalf("a page decoded on the wrong channel")
	}
}
