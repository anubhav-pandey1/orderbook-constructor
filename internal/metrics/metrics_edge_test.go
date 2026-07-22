package metrics

import "testing"

func TestPercentileClampAliasAndNegativeSamples(t *testing.T) {
	h := NewHistogram()
	h.Record(-5)
	h.Record(10)
	h.Record(20)
	if h.Min() != -5 || h.Max() != 20 {
		t.Fatalf("min/max=%d/%d", h.Min(), h.Max())
	}
	if h.Percentile(-1) != -5 {
		t.Fatalf("p<0=%d", h.Percentile(-1))
	}
	if h.Percentile(2) != 20 {
		t.Fatalf("p>1=%d", h.Percentile(2))
	}
	if h.P(0.5) != h.Percentile(0.5) {
		t.Fatalf("alias=%d percentile=%d", h.P(0.5), h.Percentile(0.5))
	}
}

func TestFormatNSBoundaries(t *testing.T) {
	for _, tc := range []struct {
		ns   int64
		want string
	}{
		{-5, "-5ns"},
		{999, "999ns"},
		{1000, "1us"},
		{1_000_000, "1ms"},
		{1_000_000_000, "1s"},
	} {
		if got := formatNS(tc.ns); got != tc.want {
			t.Fatalf("formatNS(%d)=%q want %q", tc.ns, got, tc.want)
		}
	}
}
