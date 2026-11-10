// Package channelize lets one RTL-SDR dongle receive several pager frequencies
// at the same time.
//
// rtl_fm can only demodulate one frequency. Given several -f flags it becomes a
// scanner: it sits on one frequency until the squelch closes and then hops, so
// it hears one channel at a time, cannot start without a squelch level, and
// never says which frequency a decoded page came from. That is a poor fit for
// paging, where NSW RFS and Fire & Rescue NSW often page for the same job at
// the same moment.
//
// Instead, rtl_sdr hands over the dongle's raw I/Q around a centre frequency,
// and Run splits it into one FM audio stream per frequency: mix the channel to
// zero, low-pass and decimate to 22050 Hz, FM-discriminate, remove DC. That is
// the audio rtl_fm -s 22050 -E dc produced, so each stream feeds its own
// multimon-ng exactly as before.
//
// This file decides which frequencies share a dongle and how it is tuned;
// dsp.go does the splitting.
package channelize

import (
	"math"
)

// OutputRate is the audio sample rate every channel is delivered at: what
// rtl_fm -s 22050 produced, and what multimon-ng -t raw expects.
const OutputRate = 22050

// MaxChannelsPerDongle bounds how many frequencies one dongle carries. Each
// channel costs a mixer and two filters at the full sample rate's share of CPU,
// and a Raspberry Pi has to keep up in real time.
const MaxChannelsPerDongle = 8

const (
	// usableFraction is how much of the sample rate, either side of centre, a
	// channel may sit in. The RTL2832's own decimation filter rolls off towards
	// the band edges; staying inside ±40% keeps every channel on the flat part.
	usableFraction = 0.4
	// chanHalfWidth is half a 12.5 kHz paging channel: the most a channel's
	// signal reaches either side of its centre frequency.
	chanHalfWidth = 12_500
	// dcGuard keeps every channel this far from the tuned centre. The dongle
	// leaves a DC spike at the exact centre, which is why rtl_fm itself never
	// tunes onto the frequency it is demodulating.
	dcGuard = 50_000
	// mirrorGuard keeps each channel's image (the frequency mirrored about the
	// centre) clear of every other channel. R820T dongles convert to I/Q
	// digitally and have no image to speak of, but zero-IF tuners such as the
	// E4000 do, and avoiding it costs nothing.
	mirrorGuard = 25_000
	// centreStep is the resolution the centre search moves in.
	centreStep = 5_000
)

// ratePlan is one sample rate the dongle can run at, as the two decimation
// stages that bring it down to OutputRate. Every plan is a whole multiple of
// OutputRate, so the audio comes out at exactly 22050 Hz with no resampling,
// and every rate is inside the range an RTL2832 accepts (900001-3200000 S/s).
type ratePlan struct {
	d1, d2 int
}

func (p ratePlan) sampleRate() int { return OutputRate * p.d1 * p.d2 }

// ratePlans in ascending sample rate. The lowest rate that spans a group's
// frequencies wins, because CPU cost scales with the rate.
var ratePlans = []ratePlan{
	{8, 6},  // 1,058,400 S/s: spans about 820 kHz
	{8, 8},  // 1,411,200 S/s
	{12, 8}, // 2,116,800 S/s
	{12, 9}, // 2,381,400 S/s: spans about 1.88 MHz
}

// planFor returns the decimation plan for a sample rate, or false when the
// rate is not one of ours.
func planFor(sampleRate int) (ratePlan, bool) {
	for _, p := range ratePlans {
		if p.sampleRate() == sampleRate {
			return p, true
		}
	}
	return ratePlan{}, false
}

// Group is what one dongle receives.
type Group struct {
	// Members are indices into the frequency list given to Assign, in
	// priority order.
	Members []int
	// CentreHz and SampleRate tune the dongle for a multi-frequency group.
	// They are zero for a single-frequency group, which keeps the plain
	// rtl_fm reader.
	CentreHz   int64
	SampleRate int
}

// Multi reports whether the group needs splitting (more than one frequency).
func (g Group) Multi() bool { return len(g.Members) > 1 }

// Assign packs freqsHz (in priority order) onto as few dongles as possible
// and returns one Group per dongle used, in dongle order, plus the indices of
// any frequency that found no dongle.
//
// One dongle carries every frequency it can fit, so a node normally runs a
// single reader however many dongles are plugged in, and spare dongles stay
// idle. A further dongle is only taken for a frequency that fits on none
// already in use (too far from the others, or past perDongle). Frequencies
// are placed first-fit in priority order, so the first one is always placed,
// and when the dongles run out it is the lowest-priority ones that go
// unreceived.
//
// perDongle caps the frequencies on one dongle; it is at most
// MaxChannelsPerDongle, and 1 means no sharing (no channelize available).
func Assign(freqsHz []int64, dongles, perDongle int) (groups []Group, unplaced []int) {
	perDongle = max(1, min(perDongle, MaxChannelsPerDongle))
	var packs [][]int
	for i := range freqsHz {
		placed := false
		for g := range packs {
			if len(packs[g]) >= perDongle {
				continue
			}
			if _, _, ok := fit(freqsOf(freqsHz, append(append([]int(nil), packs[g]...), i))); ok {
				packs[g] = append(packs[g], i)
				placed = true
				break
			}
		}
		switch {
		case placed:
		case len(packs) < dongles:
			packs = append(packs, []int{i})
		default:
			unplaced = append(unplaced, i)
		}
	}

	for _, p := range packs {
		g := Group{Members: p}
		if g.Multi() {
			g.CentreHz, g.SampleRate, _ = fit(freqsOf(freqsHz, p))
		}
		groups = append(groups, g)
	}
	return groups, unplaced
}

func freqsOf(all []int64, idx []int) []int64 {
	out := make([]int64, len(idx))
	for k, i := range idx {
		out[k] = all[i]
	}
	return out
}

// fit finds the lowest sample rate, and a centre, at which one dongle can
// receive every frequency in fs: each channel inside the usable band, clear of
// the DC spike, and clear of every other channel's image.
func fit(fs []int64) (centreHz int64, sampleRate int, ok bool) {
	if len(fs) == 0 {
		return 0, 0, false
	}
	lo, hi := fs[0], fs[0]
	for _, f := range fs {
		lo = min(lo, f)
		hi = max(hi, f)
	}
	mid := (lo + hi) / 2
	for _, p := range ratePlans {
		half := int64(usableFraction * float64(p.sampleRate()))
		if (hi-lo)/2+chanHalfWidth > half {
			continue
		}
		// Search outward from the midpoint, so the channels stay as close to
		// the middle of the band as the guards allow.
		for s := int64(0); s <= half; s += centreStep {
			for _, c := range []int64{mid + s, mid - s} {
				if centreWorks(fs, c, half) {
					return c, p.sampleRate(), true
				}
				if s == 0 {
					break
				}
			}
		}
	}
	return 0, 0, false
}

func centreWorks(fs []int64, c, half int64) bool {
	for _, f := range fs {
		d := abs64(f - c)
		if d+chanHalfWidth > half || d < dcGuard {
			return false
		}
	}
	for i := range fs {
		for j := range fs {
			if i != j && abs64(2*c-fs[i]-fs[j]) < mirrorGuard {
				return false
			}
		}
	}
	return true
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// HzFromMHz converts a configured frequency to whole hertz, rounding away the
// float noise in values such as 148.5875.
func HzFromMHz(mhz float64) int64 { return int64(math.Round(mhz * 1e6)) }
