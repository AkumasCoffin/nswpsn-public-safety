package channelize

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/cmplx"
	"math/rand/v2"
	"testing"
)

// fsk is one simulated paging transmitter: 2-FSK NRZ, as POCSAG is sent.
type fsk struct {
	hz      int64   // carrier
	baud    float64 // 512, 1200 or 2400
	devHz   float64 // deviation, ±
	amp     float64 // amplitude, relative to the 8-bit full scale
	mistune float64 // how far off the dongle's clock puts it, Hz
	bits    []int
}

func randomBits(seed uint64, n int) []int {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	b := make([]int, n)
	for i := range b {
		b[i] = r.IntN(2)
	}
	return b
}

// synth renders what rtl_sdr would hand over: interleaved unsigned 8-bit I/Q
// at rate around centre, carrying every transmitter plus a little noise.
func synth(rate int, centre int64, seconds float64, noise float64, txs ...fsk) []byte {
	n := int(seconds * float64(rate))
	out := make([]byte, 2*n)
	phase := make([]float64, len(txs))
	r := rand.New(rand.NewPCG(7, 11))
	q := func(v float64) byte {
		return byte(math.Max(0, math.Min(255, math.Round(127.5+128*v))))
	}
	for k := range n {
		var s complex128
		for i, tx := range txs {
			sym := int(float64(k) * tx.baud / float64(rate))
			bit := 0
			if sym < len(tx.bits) {
				bit = tx.bits[sym]
			}
			f := float64(tx.hz-centre) + tx.mistune + tx.devHz*float64(2*bit-1)
			phase[i] += 2 * math.Pi * f / float64(rate)
			s += complex(tx.amp, 0) * cmplx.Exp(complex(0, phase[i]))
		}
		s += complex(r.NormFloat64()*noise, r.NormFloat64()*noise)
		out[2*k] = q(real(s))
		out[2*k+1] = q(imag(s))
	}
	return out
}

// split runs Run over in and returns each channel's audio as samples.
func split(t *testing.T, in io.Reader, rate int, centre int64, chans ...int64) [][]int16 {
	t.Helper()
	bufs := make([]*bytes.Buffer, len(chans))
	outs := make([]io.Writer, len(chans))
	for i := range bufs {
		bufs[i] = &bytes.Buffer{}
		outs[i] = bufs[i]
	}
	if err := Run(in, rate, centre, chans, outs); err != nil {
		t.Fatalf("run: %v", err)
	}
	audio := make([][]int16, len(chans))
	for i, b := range bufs {
		raw := b.Bytes()
		audio[i] = make([]int16, len(raw)/2)
		for k := range audio[i] {
			audio[i][k] = int16(binary.LittleEndian.Uint16(raw[2*k:]))
		}
	}
	return audio
}

// bitErrors slices the audio at each symbol centre the way a zero-crossing
// decoder would and returns the error rate against bits, at whichever filter
// delay and polarity line up best. The first skip bits are left out while the
// filters and DC blocker settle.
func bitErrors(audio []int16, baud float64, bits []int, skip int) float64 {
	spb := OutputRate / baud
	best := 1.0
	for lag := 0; lag < 80; lag++ {
		var errs, count int
		for k := skip; k < len(bits); k++ {
			at := lag + int((float64(k)+0.5)*spb)
			if at >= len(audio) {
				break
			}
			got := 0
			if audio[at] > 0 {
				got = 1
			}
			if got != bits[k] {
				errs++
			}
			count++
		}
		if count == 0 {
			continue
		}
		rate := float64(errs) / float64(count)
		best = math.Min(best, math.Min(rate, 1-rate)) // either polarity
	}
	return best
}

// The point of the package: one dongle, two transmitters keying at the same
// time, and each channel's audio carries its own transmitter's bits and not
// the other's.
func TestTwoChannelsAtOnce(t *testing.T) {
	groups, _ := Assign([]int64{nswRFS, nswFRNSW}, 1, MaxChannelsPerDongle)
	g := groups[0]
	rfs := fsk{hz: nswRFS, baud: 1200, devHz: 4500, amp: 0.35, bits: randomBits(1, 1200)}
	fr := fsk{hz: nswFRNSW, baud: 1200, devHz: 4500, amp: 0.35, bits: randomBits(2, 1200)}
	in := synth(g.SampleRate, g.CentreHz, 1.0, 0.02, rfs, fr)

	audio := split(t, bytes.NewReader(in), g.SampleRate, g.CentreHz, nswRFS, nswFRNSW)
	if want := OutputRate; math.Abs(float64(len(audio[0])-want)) > 50 {
		t.Fatalf("want ~%d samples for a second of audio, got %d", want, len(audio[0]))
	}
	if ber := bitErrors(audio[0], 1200, rfs.bits, 20); ber != 0 {
		t.Fatalf("RFS channel: bit error rate %.4f, want 0", ber)
	}
	if ber := bitErrors(audio[1], 1200, fr.bits, 20); ber != 0 {
		t.Fatalf("FRNSW channel: bit error rate %.4f, want 0", ber)
	}
	// Each channel must be blind to the other transmitter: against the wrong
	// bits a channel should score no better than a coin.
	if ber := bitErrors(audio[0], 1200, fr.bits, 20); ber < 0.4 {
		t.Fatalf("FRNSW's bits leaked into the RFS channel (error rate %.3f)", ber)
	}
}

// Every POCSAG speed survives, side by side.
func TestAllBaudRates(t *testing.T) {
	groups, _ := Assign([]int64{nswRFS, nswFRNSW}, 1, MaxChannelsPerDongle)
	g := groups[0]
	slow := fsk{hz: nswRFS, baud: 512, devHz: 4500, amp: 0.35, bits: randomBits(3, 512)}
	fast := fsk{hz: nswFRNSW, baud: 2400, devHz: 4500, amp: 0.35, bits: randomBits(4, 2400)}
	in := synth(g.SampleRate, g.CentreHz, 1.0, 0.02, slow, fast)
	audio := split(t, bytes.NewReader(in), g.SampleRate, g.CentreHz, nswRFS, nswFRNSW)
	if ber := bitErrors(audio[0], 512, slow.bits, 20); ber != 0 {
		t.Fatalf("512 baud: bit error rate %.4f, want 0", ber)
	}
	if ber := bitErrors(audio[1], 2400, fast.bits, 40); ber != 0 {
		t.Fatalf("2400 baud: bit error rate %.4f, want 0", ber)
	}
}

// A dongle whose clock correction is off leaves a frequency offset, which FM
// audio carries as DC. Past the deviation, every bit would land on the same
// side of a zero-crossing slicer; the DC blocker takes the offset out and the
// bits slice cleanly again.
func TestMistunedDongleStillDecodes(t *testing.T) {
	groups, _ := Assign([]int64{nswRFS, nswFRNSW}, 1, MaxChannelsPerDongle)
	g := groups[0]
	// 5 kHz is ~34 ppm at 148 MHz: more than the 4.5 kHz deviation.
	rfs := fsk{hz: nswRFS, baud: 1200, devHz: 4500, amp: 0.35, mistune: 5000, bits: randomBits(5, 1200)}
	in := synth(g.SampleRate, g.CentreHz, 1.0, 0.02, rfs)
	audio := split(t, bytes.NewReader(in), g.SampleRate, g.CentreHz, nswRFS, nswFRNSW)
	if ber := bitErrors(audio[0], 1200, rfs.bits, 400); ber != 0 {
		t.Fatalf("mistuned by 5 kHz: bit error rate %.4f, want 0", ber)
	}
}

// A strong transmitter on the next 25 kHz channel must not swamp ours.
func TestAdjacentChannelIsRejected(t *testing.T) {
	groups, _ := Assign([]int64{nswRFS, nswFRNSW}, 1, MaxChannelsPerDongle)
	g := groups[0]
	ours := fsk{hz: nswFRNSW, baud: 1200, devHz: 4500, amp: 0.2, bits: randomBits(6, 1200)}
	next := fsk{hz: nswFRNSW + 25_000, baud: 1200, devHz: 4500, amp: 0.6, bits: randomBits(7, 1200)}
	in := synth(g.SampleRate, g.CentreHz, 1.0, 0.02, ours, next)
	audio := split(t, bytes.NewReader(in), g.SampleRate, g.CentreHz, nswRFS, nswFRNSW)
	if ber := bitErrors(audio[1], 1200, ours.bits, 20); ber != 0 {
		t.Fatalf("with a 3x stronger neighbour 25 kHz up: bit error rate %.4f, want 0", ber)
	}
}

// How the input happens to arrive through the pipe must not change a single
// output sample: filter and oscillator state carry across reads, and an odd
// byte split between two reads is reassembled.
func TestReadSizesDoNotChangeTheAudio(t *testing.T) {
	groups, _ := Assign([]int64{nswRFS, nswFRNSW}, 1, MaxChannelsPerDongle)
	g := groups[0]
	rfs := fsk{hz: nswRFS, baud: 1200, devHz: 4500, amp: 0.35, bits: randomBits(8, 360)}
	in := synth(g.SampleRate, g.CentreHz, 0.3, 0.02, rfs)

	whole := split(t, bytes.NewReader(in), g.SampleRate, g.CentreHz, nswRFS, nswFRNSW)
	pieces := split(t, &chunkReader{data: in, size: 7777}, g.SampleRate, g.CentreHz, nswRFS, nswFRNSW)
	for c := range whole {
		if len(whole[c]) != len(pieces[c]) {
			t.Fatalf("channel %d: %d samples whole vs %d in pieces", c, len(whole[c]), len(pieces[c]))
		}
		for k := range whole[c] {
			if whole[c][k] != pieces[c][k] {
				t.Fatalf("channel %d differs at sample %d: %d vs %d", c, k, whole[c][k], pieces[c][k])
			}
		}
	}
}

// chunkReader hands data over in fixed (odd) sized pieces, as a pipe might.
type chunkReader struct {
	data []byte
	size int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := min(len(p), r.size, len(r.data))
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

// A channel whose multimon-ng has died must stop the reader, so the
// supervisor restarts the whole pipeline rather than decode on without it.
func TestDeadOutputStopsTheRun(t *testing.T) {
	groups, _ := Assign([]int64{nswRFS, nswFRNSW}, 1, MaxChannelsPerDongle)
	g := groups[0]
	in := synth(g.SampleRate, g.CentreHz, 0.2, 0.02)
	err := Run(bytes.NewReader(in), g.SampleRate, g.CentreHz,
		[]int64{nswRFS, nswFRNSW}, []io.Writer{io.Discard, failingWriter{}})
	if err == nil {
		t.Fatal("want an error when an output fails")
	}
}

func TestRejectsWhatItCannotTune(t *testing.T) {
	if err := Run(bytes.NewReader(nil), 2_048_000, nswRFS, []int64{nswRFS}, []io.Writer{io.Discard}); err == nil {
		t.Fatal("want an error for a rate with no decimation plan")
	}
	if err := Run(bytes.NewReader(nil), 1_058_400, nswRFS, []int64{nswRFS + 600_000}, []io.Writer{io.Discard}); err == nil {
		t.Fatal("want an error for a channel outside the band")
	}
	if err := Run(bytes.NewReader(nil), 1_058_400, nswRFS, []int64{nswRFS}, nil); err == nil {
		t.Fatal("want an error when channels and outputs disagree")
	}
}

// The filters do what their comments say: flat through the signal, deep at
// the stop edge, for every rate plan.
func TestFilterResponse(t *testing.T) {
	gain := func(taps []float32, rate, f float64) float64 {
		var s complex128
		for n, h := range taps {
			s += complex(float64(h), 0) * cmplx.Exp(complex(0, -2*math.Pi*f*float64(n)/rate))
		}
		return cmplx.Abs(s)
	}
	for _, p := range ratePlans {
		rate := float64(p.sampleRate())
		mid := rate / float64(p.d1)
		stages := []struct {
			name             string
			taps             []float32
			rate, pass, stop float64
		}{
			{"first", lowpass(rate, firstPass, mid-audioStop), rate, firstPass, mid - audioStop},
			{"second", lowpass(mid, audioPass, audioStop), mid, audioPass, audioStop},
		}
		for _, s := range stages {
			if g := gain(s.taps, s.rate, s.pass); math.Abs(g-1) > 0.01 {
				t.Fatalf("%d S/s %s stage: gain %.4f at the pass edge, want ~1", p.sampleRate(), s.name, g)
			}
			if g := gain(s.taps, s.rate, s.stop); g > 1e-3 {
				t.Fatalf("%d S/s %s stage: gain %.2g at the stop edge, want under -60 dB", p.sampleRate(), s.name, g)
			}
		}
	}
}

// How much I/Q a core gets through. rtl_sdr delivers 1,058,400 S/s for the
// NSW pair, so the run must comfortably beat that.
func BenchmarkTwoChannels(b *testing.B) {
	groups, _ := Assign([]int64{nswRFS, nswFRNSW}, 1, MaxChannelsPerDongle)
	g := groups[0]
	in := synth(g.SampleRate, g.CentreHz, 0.5, 0.02,
		fsk{hz: nswRFS, baud: 1200, devHz: 4500, amp: 0.35, bits: randomBits(9, 600)})
	b.SetBytes(int64(len(in)))
	b.ResetTimer()
	for range b.N {
		_ = Run(bytes.NewReader(in), g.SampleRate, g.CentreHz,
			[]int64{nswRFS, nswFRNSW}, []io.Writer{io.Discard, io.Discard})
	}
}
