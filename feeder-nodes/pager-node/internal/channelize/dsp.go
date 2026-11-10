package channelize

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
)

// Filter edges. A paging channel's signal (±4.5 kHz deviation, up to 2400
// baud) reaches about ±6 kHz; the extra room is for a dongle whose clock
// correction is a little off.
const (
	// audioPass is flat to here at the final stage.
	audioPass = 9_000
	// audioStop is attenuated from here. Anything above it would fold into the
	// channel when the output is decimated to 22050 Hz (22050 - 9000 = 13050).
	audioStop = OutputRate - audioPass
	// firstPass is what the first stage keeps flat; it only has to protect
	// what the second stage will look at.
	firstPass = 10_000
)

// dcPole is the audio DC blocker's pole: a time constant of about 75 ms at
// 22050 Hz, the same as rtl_fm -E dc's running average, which the proven
// reader used. It takes out the constant offset a slightly mistuned dongle
// leaves in FM audio, which would otherwise shift multimon-ng's zero-crossing
// slicer. A POCSAG preamble is at least 240 ms of alternating bits, so it has
// settled before the first codeword.
const dcPole = 0.9994

// discScale maps the discriminator's radians onto 16-bit samples the same way
// rtl_fm's polar_discriminant does: π becomes 1<<14.
const discScale = float64(1<<14) / math.Pi

// renormEvery is how often the oscillator's phasor is renormalised, so float
// rounding cannot shrink or grow it over hours of samples.
const renormEvery = 1024

// u8ToFloat converts rtl_sdr's unsigned 8-bit samples (centred on 127.5).
var u8ToFloat = func() (t [256]float32) {
	for i := range t {
		t[i] = (float32(i) - 127.5) / 128
	}
	return t
}()

// Run reads rtl_sdr's interleaved unsigned 8-bit I/Q at sampleRate, tuned to
// centreHz, and writes one FM-demodulated audio stream per channel to the
// matching writer: signed 16-bit little-endian mono at OutputRate.
//
// It returns nil when the input ends, and an error as soon as any output
// fails. A failed output means that channel's multimon-ng has gone, and the
// whole reader has to restart to bring it back.
func Run(r io.Reader, sampleRate int, centreHz int64, chansHz []int64, outs []io.Writer) error {
	p, ok := planFor(sampleRate)
	if !ok {
		return fmt.Errorf("unsupported sample rate %d S/s", sampleRate)
	}
	if len(chansHz) == 0 || len(chansHz) != len(outs) {
		return fmt.Errorf("need one output per channel (have %d channels, %d outputs)", len(chansHz), len(outs))
	}
	half := usableFraction * float64(sampleRate)
	taps1 := lowpass(float64(sampleRate), firstPass, float64(sampleRate/p.d1)-audioStop)
	taps2 := lowpass(float64(sampleRate/p.d1), audioPass, audioStop)

	chans := make([]*channel, len(chansHz))
	for i, f := range chansHz {
		off := float64(f - centreHz)
		if math.Abs(off)+chanHalfWidth > half {
			return fmt.Errorf("%d Hz is outside the band around %d Hz at %d S/s", f, centreHz, sampleRate)
		}
		chans[i] = newChannel(off, float64(sampleRate), taps1, p.d1, taps2, p.d2, outs[i])
	}

	// About 50 ms of input per pass: small enough to keep the decode prompt,
	// large enough that the per-pass goroutine handoff is noise.
	buf := make([]byte, 2*(sampleRate/20))
	var iq []float32
	var odd []byte // a trailing byte that arrived without its partner
	errs := make([]error, len(chans))

	for {
		n, rerr := r.Read(buf)
		data := buf[:n]
		if len(odd) > 0 && n > 0 {
			data = append(odd, data...)
			odd = odd[:0]
		}
		if len(data)%2 == 1 {
			odd = append(odd[:0], data[len(data)-1])
			data = data[:len(data)-1]
		}
		if len(data) > 0 {
			iq = grow(iq, len(data))
			for k, b := range data {
				iq[k] = u8ToFloat[b]
			}
			// Channels are independent, so each takes a core of its own.
			var wg sync.WaitGroup
			for i, c := range chans {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = c.process(iq)
				}()
			}
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					return err
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			return fmt.Errorf("read I/Q: %w", rerr)
		}
	}
}

// channel turns the shared wideband I/Q into one channel's audio.
type channel struct {
	// Oscillator: phasor (phRe, phIm) advances by (rotRe, rotIm) every input
	// sample, shifting this channel's frequency down to zero.
	rotRe, rotIm float64
	phRe, phIm   float64
	sinceRenorm  int

	s1, s2 *firDecimator

	// FM discriminator and DC blocker state.
	prevI, prevQ float32
	dcIn, dcOut  float64

	mixI, mixQ   []float32
	midI, midQ   []float32
	baseI, baseQ []float32
	pcm          []byte
	w            io.Writer
}

func newChannel(offsetHz, sampleRate float64, taps1 []float32, d1 int, taps2 []float32, d2 int, w io.Writer) *channel {
	step := -2 * math.Pi * offsetHz / sampleRate
	return &channel{
		rotRe: math.Cos(step), rotIm: math.Sin(step),
		phRe: 1,
		s1:   newFirDecimator(taps1, d1),
		s2:   newFirDecimator(taps2, d2),
		w:    w,
	}
}

// process consumes interleaved I/Q floats and writes the audio they produce.
func (c *channel) process(iq []float32) error {
	n := len(iq) / 2
	c.mixI = grow(c.mixI, n)
	c.mixQ = grow(c.mixQ, n)
	for k := 0; k < n; k++ {
		i, q := iq[2*k], iq[2*k+1]
		pr, pi := float32(c.phRe), float32(c.phIm)
		c.mixI[k] = i*pr - q*pi
		c.mixQ[k] = i*pi + q*pr
		c.phRe, c.phIm = c.phRe*c.rotRe-c.phIm*c.rotIm, c.phRe*c.rotIm+c.phIm*c.rotRe
		c.sinceRenorm++
		if c.sinceRenorm == renormEvery {
			m := math.Hypot(c.phRe, c.phIm)
			c.phRe, c.phIm = c.phRe/m, c.phIm/m
			c.sinceRenorm = 0
		}
	}

	c.midI, c.midQ = c.s1.push(c.mixI, c.mixQ, c.midI[:0], c.midQ[:0])
	c.baseI, c.baseQ = c.s2.push(c.midI, c.midQ, c.baseI[:0], c.baseQ[:0])
	if len(c.baseI) == 0 {
		return nil
	}

	c.pcm = grow8(c.pcm, 2*len(c.baseI))
	for k := range c.baseI {
		i, q := c.baseI[k], c.baseQ[k]
		// Phase step between consecutive samples: arg(z[n] * conj(z[n-1])).
		re := float64(i*c.prevI + q*c.prevQ)
		im := float64(q*c.prevI - i*c.prevQ)
		c.prevI, c.prevQ = i, q
		ang := math.Atan2(im, re)

		y := ang - c.dcIn + dcPole*c.dcOut
		c.dcIn, c.dcOut = ang, y

		v := math.Round(y * discScale)
		v = math.Max(math.MinInt16, math.Min(math.MaxInt16, v))
		binary.LittleEndian.PutUint16(c.pcm[2*k:], uint16(int16(v)))
	}
	if _, err := c.w.Write(c.pcm); err != nil {
		return fmt.Errorf("write audio: %w", err)
	}
	return nil
}

// firDecimator is a streaming FIR low-pass that keeps only every factor'th
// output. Only the outputs it keeps are computed.
type firDecimator struct {
	taps   []float32
	factor int
	// bufI/bufQ hold the last len(taps)-1 inputs followed by the new ones.
	bufI, bufQ []float32
	// next is the buffer index of the newest input the next output uses.
	next int
}

func newFirDecimator(taps []float32, factor int) *firDecimator {
	if len(taps) <= factor {
		panic(fmt.Sprintf("channelize: %d taps cannot decimate by %d", len(taps), factor))
	}
	h := len(taps) - 1
	return &firDecimator{
		taps:   taps,
		factor: factor,
		bufI:   make([]float32, h),
		bufQ:   make([]float32, h),
		next:   h,
	}
}

// push filters inI/inQ, appends the kept outputs to outI/outQ, and returns
// them. The taps are symmetric, so the window is applied oldest-first without
// reversing.
func (d *firDecimator) push(inI, inQ, outI, outQ []float32) ([]float32, []float32) {
	d.bufI = append(d.bufI, inI...)
	d.bufQ = append(d.bufQ, inQ...)
	h := len(d.taps) - 1
	for ; d.next < len(d.bufI); d.next += d.factor {
		wi := d.bufI[d.next-h : d.next+1 : d.next+1]
		wq := d.bufQ[d.next-h : d.next+1 : d.next+1]
		var si, sq float32
		for k, t := range d.taps {
			si += t * wi[k]
			sq += t * wq[k]
		}
		outI = append(outI, si)
		outQ = append(outQ, sq)
	}
	// Keep only the history the next output needs.
	if drop := d.next - h; drop > 0 {
		kept := copy(d.bufI, d.bufI[drop:])
		copy(d.bufQ, d.bufQ[drop:])
		d.bufI = d.bufI[:kept]
		d.bufQ = d.bufQ[:kept]
		d.next -= drop
	}
	return outI, outQ
}

// lowpass designs a linear-phase FIR low-pass for the given sample rate:
// flat to pass, attenuated about 74 dB from stop. It is a Blackman-windowed
// sinc, whose transition band is about 5.5 x rate / taps wide, normalised to
// unity gain at DC.
func lowpass(rate, pass, stop float64) []float32 {
	n := int(math.Ceil(5.5*rate/(stop-pass))) | 1 // odd, so there is a centre tap
	fc := (pass + stop) / 2 / rate
	mid := float64(n-1) / 2
	taps := make([]float64, n)
	var sum float64
	for i := range taps {
		x := float64(i) - mid
		s := 2 * fc
		if x != 0 {
			s = math.Sin(2*math.Pi*fc*x) / (math.Pi * x)
		}
		w := 0.42 - 0.5*math.Cos(2*math.Pi*float64(i)/float64(n-1)) + 0.08*math.Cos(4*math.Pi*float64(i)/float64(n-1))
		taps[i] = s * w
		sum += taps[i]
	}
	out := make([]float32, n)
	for i, t := range taps {
		out[i] = float32(t / sum)
	}
	return out
}

func grow(s []float32, n int) []float32 {
	if cap(s) < n {
		return make([]float32, n)
	}
	return s[:n]
}

func grow8(s []byte, n int) []byte {
	if cap(s) < n {
		return make([]byte, n)
	}
	return s[:n]
}
