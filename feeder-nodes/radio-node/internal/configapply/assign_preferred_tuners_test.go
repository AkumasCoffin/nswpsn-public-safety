package configapply

import "testing"

// sdrOf collects the resulting SDR (preferredTuner) per channel, keyed by name,
// so assertions read against the intent rather than slice positions.
func sdrOf(chans []ChannelPlan) map[string]string {
	out := make(map[string]string, len(chans))
	for _, c := range chans {
		out[c.Name] = c.SDR
	}
	return out
}

// TestAssignPreferredTuners covers the core spread contract: N frequencies over
// K tuners cluster by frequency into K contiguous groups, one distinct tuner
// each; explicit pins are respected; and single-tuner / single-channel inputs
// are no-ops.
func TestAssignPreferredTuners(t *testing.T) {
	// The operator's case: 6 control channels spanning ~1.37 MHz (all fit one
	// dongle window) + 3 dongles → 3 groups of 2, by frequency, each a distinct
	// serial. Serials are passed unsorted/duplicated to prove dedup + sort.
	t.Run("6 freqs, 3 serials -> 3 groups of 2 by frequency", func(t *testing.T) {
		chans := []ChannelPlan{
			{Name: "f6", Frequency: 142_000_000, Decoder: "p25p1"},
			{Name: "f1", Frequency: 141_000_000, Decoder: "p25p1"},
			{Name: "f4", Frequency: 141_600_000, Decoder: "p25p1"},
			{Name: "f2", Frequency: 141_200_000, Decoder: "p25p1"},
			{Name: "f5", Frequency: 141_800_000, Decoder: "p25p1"},
			{Name: "f3", Frequency: 141_400_000, Decoder: "p25p1"},
		}
		// Unsorted + a duplicate: must sort to [A,B,C] and collapse the dup.
		serials := []string{"C", "A", "B", "A"}

		assignPreferredTuners(chans, serials)
		got := sdrOf(chans)

		// Frequency order f1<f2<f3<f4<f5<f6 → groups (f1,f2)=A (f3,f4)=B (f5,f6)=C.
		want := map[string]string{
			"f1": "A", "f2": "A",
			"f3": "B", "f4": "B",
			"f5": "C", "f6": "C",
		}
		for name, w := range want {
			if got[name] != w {
				t.Errorf("channel %s: preferredTuner = %q, want %q", name, got[name], w)
			}
		}

		// Each of the 3 serials is used, and each group is a single distinct serial.
		used := map[string]int{}
		for _, c := range chans {
			used[c.SDR]++
		}
		if len(used) != 3 || used["A"] != 2 || used["B"] != 2 || used["C"] != 2 {
			t.Errorf("expected an even 2/2/2 spread across A,B,C; got %v", used)
		}
	})

	// An explicitly-pinned channel keeps its pin; only Auto channels are spread.
	t.Run("respects a pre-pinned channel", func(t *testing.T) {
		chans := []ChannelPlan{
			{Name: "pinned", Frequency: 141_100_000, Decoder: "p25p1", SDR: "PINNED"},
			{Name: "a", Frequency: 141_200_000, Decoder: "p25p1"},
			{Name: "b", Frequency: 141_900_000, Decoder: "p25p1"},
		}
		assignPreferredTuners(chans, []string{"A", "B"})
		got := sdrOf(chans)

		if got["pinned"] != "PINNED" {
			t.Errorf("explicit pin overwritten: got %q, want PINNED", got["pinned"])
		}
		// The 2 Auto channels split across the 2 tuners by frequency.
		if got["a"] != "A" || got["b"] != "B" {
			t.Errorf("auto channels not spread: a=%q b=%q, want a=A b=B", got["a"], got["b"])
		}
	})

	// A single tuner cannot spread anything — leave every channel Auto.
	t.Run("no-op with 1 tuner", func(t *testing.T) {
		chans := []ChannelPlan{
			{Name: "a", Frequency: 141_000_000, Decoder: "p25p1"},
			{Name: "b", Frequency: 142_000_000, Decoder: "p25p1"},
		}
		assignPreferredTuners(chans, []string{"only"})
		for _, c := range chans {
			if c.SDR != "" {
				t.Errorf("single tuner should leave %s Auto, got %q", c.Name, c.SDR)
			}
		}
	})

	// A single Auto channel is left Auto even with multiple tuners (nothing to
	// spread — vce's own first-fit is fine for one channel).
	t.Run("no-op with 1 channel", func(t *testing.T) {
		chans := []ChannelPlan{
			{Name: "solo", Frequency: 141_000_000, Decoder: "p25p1"},
		}
		assignPreferredTuners(chans, []string{"A", "B", "C"})
		if chans[0].SDR != "" {
			t.Errorf("single channel should stay Auto, got %q", chans[0].SDR)
		}
	})

	// A channel with no frequency is never assigned a tuner.
	t.Run("channel without frequency stays Auto", func(t *testing.T) {
		chans := []ChannelPlan{
			{Name: "nofreq", Frequency: 0, Decoder: "p25p1"},
			{Name: "a", Frequency: 141_000_000, Decoder: "p25p1"},
			{Name: "b", Frequency: 142_000_000, Decoder: "p25p1"},
		}
		assignPreferredTuners(chans, []string{"A", "B"})
		got := sdrOf(chans)
		if got["nofreq"] != "" {
			t.Errorf("freq-less channel must stay Auto, got %q", got["nofreq"])
		}
		if got["a"] != "A" || got["b"] != "B" {
			t.Errorf("auto channels not spread: a=%q b=%q", got["a"], got["b"])
		}
	})

	// More tuners than Auto channels: K collapses to the channel count, each
	// channel still lands on a distinct tuner (the first serials, sorted).
	t.Run("more tuners than channels", func(t *testing.T) {
		chans := []ChannelPlan{
			{Name: "a", Frequency: 141_000_000, Decoder: "p25p1"},
			{Name: "b", Frequency: 142_000_000, Decoder: "p25p1"},
		}
		assignPreferredTuners(chans, []string{"A", "B", "C", "D"})
		got := sdrOf(chans)
		if got["a"] != "A" || got["b"] != "B" {
			t.Errorf("expected a=A b=B, got a=%q b=%q", got["a"], got["b"])
		}
	})
}
