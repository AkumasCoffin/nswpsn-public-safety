package channelize

import (
	"math/rand/v2"
	"slices"
	"testing"
)

var (
	nswRFS   = HzFromMHz(148.5875)
	nswFRNSW = HzFromMHz(148.9625)
)

// checkGroup fails the test if a multi-frequency group breaks any of the
// tuning guards: every channel inside the usable band, clear of the DC spike,
// and clear of every other channel's image.
func checkGroup(t *testing.T, freqs []int64, g Group) {
	t.Helper()
	if !g.Multi() {
		if g.CentreHz != 0 || g.SampleRate != 0 {
			t.Fatalf("a single-frequency group must keep rtl_fm (no tuning), got %+v", g)
		}
		return
	}
	if _, ok := planFor(g.SampleRate); !ok {
		t.Fatalf("group tuned to an unsupported rate %d", g.SampleRate)
	}
	half := int64(usableFraction * float64(g.SampleRate))
	for _, m := range g.Members {
		d := abs64(freqs[m] - g.CentreHz)
		if d+chanHalfWidth > half {
			t.Fatalf("%d Hz falls outside the band around %d at %d S/s", freqs[m], g.CentreHz, g.SampleRate)
		}
		if d < dcGuard {
			t.Fatalf("%d Hz sits %d Hz from the DC spike at %d", freqs[m], d, g.CentreHz)
		}
		for _, o := range g.Members {
			if o != m && abs64(2*g.CentreHz-freqs[m]-freqs[o]) < mirrorGuard {
				t.Fatalf("the image of %d lands on %d (centre %d)", freqs[m], freqs[o], g.CentreHz)
			}
		}
	}
}

// The case this exists for: one dongle, both NSW pager frequencies, at once.
func TestOneDongleTakesBothNSWFrequencies(t *testing.T) {
	freqs := []int64{nswRFS, nswFRNSW}
	groups, unplaced := Assign(freqs, 1, MaxChannelsPerDongle)
	if len(unplaced) != 0 || len(groups) != 1 {
		t.Fatalf("want both on the one dongle, got groups=%+v unplaced=%v", groups, unplaced)
	}
	g := groups[0]
	if !slices.Equal(g.Members, []int{0, 1}) {
		t.Fatalf("want members [0 1] in priority order, got %v", g.Members)
	}
	// 375 kHz apart fits the lowest rate, which is the cheapest to process.
	if g.SampleRate != 1_058_400 {
		t.Fatalf("want the lowest rate, got %d", g.SampleRate)
	}
	checkGroup(t, freqs, g)

	// The older FRNSW frequency, 400 kHz from RFS, fits the same way.
	freqs = []int64{nswRFS, HzFromMHz(148.9875)}
	groups, unplaced = Assign(freqs, 1, MaxChannelsPerDongle)
	if len(unplaced) != 0 || len(groups) != 1 || len(groups[0].Members) != 2 {
		t.Fatalf("148.9875 should share the dongle too: groups=%+v unplaced=%v", groups, unplaced)
	}
	checkGroup(t, freqs, groups[0])
}

// One reader is all a node needs: extra dongles stay idle rather than each
// taking a frequency.
func TestExtraDonglesStayIdle(t *testing.T) {
	freqs := []int64{nswRFS, nswFRNSW}
	for _, dongles := range []int{2, 3, 5} {
		groups, unplaced := Assign(freqs, dongles, MaxChannelsPerDongle)
		if len(unplaced) != 0 || len(groups) != 1 || !slices.Equal(groups[0].Members, []int{0, 1}) {
			t.Fatalf("%d dongles: want both on the first dongle alone, got %+v unplaced=%v", dongles, groups, unplaced)
		}
	}
	// Three close frequencies on two dongles: still one reader.
	freqs = []int64{nswRFS, nswFRNSW, HzFromMHz(148.7)}
	groups, unplaced := Assign(freqs, 2, MaxChannelsPerDongle)
	if len(unplaced) != 0 || len(groups) != 1 || len(groups[0].Members) != 3 {
		t.Fatalf("want all three on one dongle, got %+v unplaced=%v", groups, unplaced)
	}
	checkGroup(t, freqs, groups[0])
}

// A single frequency keeps the plain rtl_fm reader.
func TestOneFrequencyStaysOnRtlFm(t *testing.T) {
	groups, unplaced := Assign([]int64{HzFromMHz(148.6375)}, 2, MaxChannelsPerDongle)
	if len(unplaced) != 0 || len(groups) != 1 || groups[0].Multi() {
		t.Fatalf("want one single-frequency group, got %+v unplaced=%v", groups, unplaced)
	}
	checkGroup(t, nil, groups[0])
}

// A second dongle is taken only for a frequency the first cannot fit.
func TestTooFarApartTakesASecondDongle(t *testing.T) {
	freqs := []int64{nswRFS, HzFromMHz(152.0)}

	groups, unplaced := Assign(freqs, 1, MaxChannelsPerDongle)
	if len(groups) != 1 || !slices.Equal(groups[0].Members, []int{0}) {
		t.Fatalf("one dongle: want the priority frequency alone, got %+v", groups)
	}
	if !slices.Equal(unplaced, []int{1}) {
		t.Fatalf("one dongle: want 152 MHz reported unplaced, got %v", unplaced)
	}

	groups, unplaced = Assign(freqs, 2, MaxChannelsPerDongle)
	if len(unplaced) != 0 || len(groups) != 2 ||
		!slices.Equal(groups[0].Members, []int{0}) || !slices.Equal(groups[1].Members, []int{1}) {
		t.Fatalf("two dongles: want one each, got %+v unplaced=%v", groups, unplaced)
	}
}

// When a frequency fits nowhere, the ones after it that do fit still go on.
func TestALaterFrequencyThatFitsIsStillPlaced(t *testing.T) {
	freqs := []int64{nswRFS, HzFromMHz(152.0), nswFRNSW}
	groups, unplaced := Assign(freqs, 1, MaxChannelsPerDongle)
	if len(groups) != 1 || !slices.Equal(groups[0].Members, []int{0, 2}) {
		t.Fatalf("want RFS and FRNSW sharing the dongle, got %+v", groups)
	}
	if !slices.Equal(unplaced, []int{1}) {
		t.Fatalf("want only 152 MHz left out, got %v", unplaced)
	}
	checkGroup(t, freqs, groups[0])
}

// A wide pair still shares a dongle, at a higher sample rate.
func TestWideSpanPicksAHigherRate(t *testing.T) {
	freqs := []int64{HzFromMHz(148.0), HzFromMHz(149.5)}
	groups, unplaced := Assign(freqs, 1, MaxChannelsPerDongle)
	if len(unplaced) != 0 || len(groups) != 1 || !groups[0].Multi() {
		t.Fatalf("1.5 MHz apart should still share: groups=%+v unplaced=%v", groups, unplaced)
	}
	if groups[0].SampleRate != 2_116_800 {
		t.Fatalf("want the lowest rate that spans 1.5 MHz, got %d", groups[0].SampleRate)
	}
	checkGroup(t, freqs, groups[0])
}

// With no channelize to split with (perDongle 1), it is one frequency per
// dongle, as the agent always was.
func TestNoSharingMeansOneFrequencyPerDongle(t *testing.T) {
	freqs := []int64{nswRFS, nswFRNSW}
	groups, unplaced := Assign(freqs, 2, 1)
	if len(unplaced) != 0 || len(groups) != 2 || groups[0].Multi() || groups[1].Multi() {
		t.Fatalf("want one each, got %+v unplaced=%v", groups, unplaced)
	}
	groups, unplaced = Assign(freqs, 1, 1)
	if len(groups) != 1 || groups[0].Members[0] != 0 || !slices.Equal(unplaced, []int{1}) {
		t.Fatalf("want the priority frequency only, got %+v unplaced=%v", groups, unplaced)
	}
}

func TestNoDonglesPlacesNothing(t *testing.T) {
	groups, unplaced := Assign([]int64{nswRFS, nswFRNSW}, 0, MaxChannelsPerDongle)
	if len(groups) != 0 || !slices.Equal(unplaced, []int{0, 1}) {
		t.Fatalf("want nothing placed, got %+v unplaced=%v", groups, unplaced)
	}
}

func TestChannelCapPerDongle(t *testing.T) {
	var freqs []int64
	for i := range MaxChannelsPerDongle + 2 {
		freqs = append(freqs, HzFromMHz(148.0)+int64(i)*60_000)
	}
	groups, unplaced := Assign(freqs, 1, MaxChannelsPerDongle)
	if len(groups) != 1 || len(groups[0].Members) != MaxChannelsPerDongle {
		t.Fatalf("want the dongle capped at %d channels, got %+v", MaxChannelsPerDongle, groups)
	}
	if len(unplaced) != 2 {
		t.Fatalf("want the 2 over the cap reported, got %v", unplaced)
	}
	checkGroup(t, freqs, groups[0])

	// A second dongle takes the overflow.
	groups, unplaced = Assign(freqs, 2, MaxChannelsPerDongle)
	if len(unplaced) != 0 || len(groups) != 2 || len(groups[1].Members) != 2 {
		t.Fatalf("want the overflow on the second dongle, got %+v unplaced=%v", groups, unplaced)
	}
}

// Whatever the frequencies, every group obeys the guards, every frequency is
// accounted for exactly once, and no dongle is opened for a frequency an
// earlier one could have taken.
func TestAssignInvariants(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for trial := range 2000 {
		n := 1 + r.IntN(6)
		dongles := r.IntN(4)
		perDongle := 1 + r.IntN(MaxChannelsPerDongle)
		freqs := make([]int64, n)
		for i := range freqs {
			freqs[i] = HzFromMHz(147.5) + int64(r.IntN(3_000))*1_000
		}
		groups, unplaced := Assign(freqs, dongles, perDongle)
		if len(groups) > dongles {
			t.Fatalf("trial %d: %d groups for %d dongles", trial, len(groups), dongles)
		}
		seen := map[int]int{}
		for k, g := range groups {
			if len(g.Members) == 0 || len(g.Members) > perDongle {
				t.Fatalf("trial %d: group size %d (cap %d)", trial, len(g.Members), perDongle)
			}
			checkGroup(t, freqs, g)
			for _, m := range g.Members {
				seen[m]++
			}
			// First-fit: this group's first frequency fit none before it.
			first := g.Members[0]
			for _, earlier := range groups[:k] {
				var prefix []int
				for _, m := range earlier.Members {
					if m < first {
						prefix = append(prefix, m)
					}
				}
				if len(prefix) < perDongle {
					if _, _, ok := fit(freqsOf(freqs, append(prefix, first))); ok {
						t.Fatalf("trial %d: frequency %d opened dongle %d but fit an earlier one", trial, first, k)
					}
				}
			}
		}
		for _, u := range unplaced {
			seen[u]++
		}
		for i := range freqs {
			if seen[i] != 1 {
				t.Fatalf("trial %d: frequency %d accounted for %d times (groups=%+v unplaced=%v)", trial, i, seen[i], groups, unplaced)
			}
		}
		// The first frequency always gets a dongle when there is one.
		if dongles > 0 && (len(groups) == 0 || groups[0].Members[0] != 0) {
			t.Fatalf("trial %d: the priority frequency was not placed first: %+v", trial, groups)
		}
	}
}
