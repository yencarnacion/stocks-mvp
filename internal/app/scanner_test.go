package app

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"testing"
	"time"
)

func mustLoadTestLoc(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	return loc
}

func TestLiveReplayStartFor(t *testing.T) {
	loc := mustLoadTestLoc(t)
	cfg := defaultConfig()

	beforeOpen := time.Date(2026, 2, 19, 9, 29, 59, 0, loc)
	if got := liveReplayStartFor(beforeOpen, cfg, loc); !got.IsZero() {
		t.Fatalf("expected zero time before open, got %v", got)
	}

	atOpen := time.Date(2026, 2, 19, 9, 30, 0, 0, loc)
	expectedOpen := time.Date(2026, 2, 19, 9, 30, 0, 0, loc)
	if got := liveReplayStartFor(atOpen, cfg, loc); !got.Equal(expectedOpen) {
		t.Fatalf("expected open at 09:30, got %v", got)
	}

	afterOpen := time.Date(2026, 2, 19, 13, 15, 0, 0, loc)
	if got := liveReplayStartFor(afterOpen, cfg, loc); !got.Equal(expectedOpen) {
		t.Fatalf("expected replay start at session open, got %v", got)
	}

	cfg.Market.IncludeExtendedHours = true
	afterExtendedOpen := time.Date(2026, 2, 19, 5, 0, 0, 0, loc)
	expectedExtendedOpen := time.Date(2026, 2, 19, 4, 0, 0, 0, loc)
	if got := liveReplayStartFor(afterExtendedOpen, cfg, loc); !got.Equal(expectedExtendedOpen) {
		t.Fatalf("expected replay start at extended open, got %v", got)
	}
}

func TestTopRVOLCandidatesSortAndLimit(t *testing.T) {
	in := make([]Candidate, 0, 35)
	for i := 0; i < 35; i++ {
		in = append(in, Candidate{
			Symbol:          fmt.Sprintf("SYM%02d", i),
			RVOL:            float64(i),
			DollarVolPerMin: float64(i) * 1000,
		})
	}
	out := topRVOLCandidates(in, 30)
	if len(out) != 30 {
		t.Fatalf("expected top 30 rows, got %d", len(out))
	}
	if out[0].RVOL != 34 {
		t.Fatalf("expected highest RVOL first, got %.2f", out[0].RVOL)
	}
	if out[len(out)-1].RVOL != 5 {
		t.Fatalf("expected cutoff RVOL 5 at row 30, got %.2f", out[len(out)-1].RVOL)
	}
	for i, c := range out {
		if c.Rank != i+1 {
			t.Fatalf("expected sequential rank at %d, got %d", i, c.Rank)
		}
	}

	tied := topRVOLCandidates([]Candidate{
		{Symbol: "BBB", RVOL: 5, DollarVolPerMin: 100},
		{Symbol: "AAA", RVOL: 5, DollarVolPerMin: 100},
		{Symbol: "CCC", RVOL: 5, DollarVolPerMin: 200},
	}, 30)
	if len(tied) != 3 {
		t.Fatalf("expected 3 tied rows, got %d", len(tied))
	}
	if tied[0].Symbol != "CCC" || tied[1].Symbol != "AAA" || tied[2].Symbol != "BBB" {
		t.Fatalf("unexpected tie ordering: %+v", tied)
	}
}

func TestPublishKeepsLiveRVOLFromFirstSnapshotInMinute(t *testing.T) {
	sc := NewScanner(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		defaultConfig(),
		map[string]struct{}{},
		map[string]Baseline{},
	)
	loc := mustLoadTestLoc(t)

	mkSnap := func(minute, sec int, rows []Candidate) TopSnapshot {
		ts := time.Date(2026, 2, 19, 9, minute, sec, 0, loc)
		return TopSnapshot{
			Mode:          "live",
			GeneratedAt:   ts,
			GeneratedAtNY: ts.Format("2006-01-02 15:04:05 MST"),
			AsOfNY:        ts.Format("2006-01-02 15:04:05 MST"),
			CountRVOL:     len(rows),
			RVOLCandidates: func() []Candidate {
				cp := make([]Candidate, len(rows))
				copy(cp, rows)
				return cp
			}(),
		}
	}

	s1 := mkSnap(30, 5, []Candidate{{Rank: 1, Symbol: "AAA", RVOL: 10}})
	s2 := mkSnap(30, 40, []Candidate{{Rank: 1, Symbol: "BBB", RVOL: 20}})
	s3 := mkSnap(31, 4, []Candidate{{Rank: 1, Symbol: "CCC", RVOL: 30}})

	sc.publish(s1, true)
	sc.publish(s2, true)

	got := sc.GetSnapshot()
	if len(got.RVOLCandidates) != 1 || got.RVOLCandidates[0].Symbol != "AAA" {
		t.Fatalf("expected first-minute RVOL rows to stick, got %+v", got.RVOLCandidates)
	}

	sc.publish(s3, true)
	got = sc.GetSnapshot()
	if len(got.RVOLCandidates) != 1 || got.RVOLCandidates[0].Symbol != "CCC" {
		t.Fatalf("expected next-minute RVOL rows to refresh, got %+v", got.RVOLCandidates)
	}
}

func TestLiveEvalNY(t *testing.T) {
	loc := mustLoadTestLoc(t)
	now := time.Date(2026, 2, 19, 11, 0, 0, 0, loc)

	if got := liveEvalNY(now, 0, loc); !got.Equal(now) {
		t.Fatalf("expected now when max event missing, got %v", got)
	}

	pastEvent := now.Add(-20 * time.Minute)
	if got := liveEvalNY(now, pastEvent.UTC().UnixNano(), loc); !got.Equal(pastEvent) {
		t.Fatalf("expected past event time during backfill, got %v", got)
	}

	futureEvent := now.Add(2 * time.Minute)
	if got := liveEvalNY(now, futureEvent.UTC().UnixNano(), loc); !got.Equal(now) {
		t.Fatalf("expected now when event is in future, got %v", got)
	}
}

func TestPassesRVOLTabGates(t *testing.T) {
	cfg := defaultConfig()
	scan := cfg.Scan
	scan.MinDollarVolPerM = 100_000
	scan.MinAvgDollarVol10d = 10_000_000

	if !passesRVOLTabGates(featureRow{
		dollarVolPerMin: 150_000,
		avgDollarVol10d: 20_000_000,
	}, scan) {
		t.Fatalf("expected row with sufficient dollar volume to pass RVOL tab gates")
	}

	if passesRVOLTabGates(featureRow{
		dollarVolPerMin: 90_000,
		avgDollarVol10d: 20_000_000,
	}, scan) {
		t.Fatalf("expected row with low $/min to fail RVOL tab gates")
	}

	if passesRVOLTabGates(featureRow{
		dollarVolPerMin: 150_000,
		avgDollarVol10d: 9_000_000,
	}, scan) {
		t.Fatalf("expected row with low 10d $vol to fail RVOL tab gates")
	}
}

func testIntradayBar(high, low, close float64) intradayBar {
	return intradayBar{
		HighPxN:  int64(math.Round(high * pxScale)),
		LowPxN:   int64(math.Round(low * pxScale)),
		ClosePxN: int64(math.Round(close * pxScale)),
	}
}

func testIntradayBarOHLC(open, high, low, close float64) intradayBar {
	return intradayBar{
		OpenPxN:  int64(math.Round(open * pxScale)),
		HighPxN:  int64(math.Round(high * pxScale)),
		LowPxN:   int64(math.Round(low * pxScale)),
		ClosePxN: int64(math.Round(close * pxScale)),
	}
}

func TestFindBacksideHHIdxRequiresMinRally(t *testing.T) {
	bars := []intradayBar{
		testIntradayBar(9.95, 9.90, 9.92),
		testIntradayBar(9.98, 9.93, 9.95),
		testIntradayBar(9.97, 9.94, 9.96),
		testIntradayBar(9.99, 9.95, 9.98),
	}

	if got := findBacksideHHIdx(bars, 0, 0.01); got != -1 {
		t.Fatalf("expected no HH when rally is below threshold, got idx=%d", got)
	}
}

func TestFindBacksideHHIdxUsesFirstQualifiedPivot(t *testing.T) {
	bars := []intradayBar{
		testIntradayBar(10.00, 9.90, 9.95),
		testIntradayBar(10.05, 9.95, 10.00),
		testIntradayBar(10.02, 9.97, 9.99),
		testIntradayBar(10.20, 10.10, 10.15),
		testIntradayBar(10.15, 10.05, 10.08),
	}

	if got := findBacksideHHIdx(bars, 0, 0.01); got != 1 {
		t.Fatalf("expected first qualified pivot HH at idx=1, got idx=%d", got)
	}
}

func TestFindBacksideHHIdxFallsBackToMaxHighAfterStarterLow(t *testing.T) {
	bars := []intradayBar{
		testIntradayBar(9.95, 9.90, 9.92),
		testIntradayBar(10.00, 9.95, 9.98),
		testIntradayBar(10.10, 10.00, 10.05),
		testIntradayBar(10.20, 10.10, 10.15),
	}

	if got := findBacksideHHIdx(bars, 0, 0.01); got != 3 {
		t.Fatalf("expected fallback max high HH at idx=3, got idx=%d", got)
	}
}

func TestFindBacksideHLAfterHHIdxFindsPivotLow(t *testing.T) {
	bars := []intradayBar{
		testIntradayBar(10.00, 9.90, 9.95),
		testIntradayBar(10.20, 10.00, 10.10),
		testIntradayBar(10.50, 10.20, 10.45),
		testIntradayBar(10.40, 10.15, 10.20),
		testIntradayBar(10.45, 10.05, 10.30),
		testIntradayBar(10.60, 10.10, 10.55),
	}

	if got := findBacksideHLAfterHHIdx(bars, 2); got != 4 {
		t.Fatalf("expected pivot HL at idx=4 after HH idx=2, got idx=%d", got)
	}
}

func TestRubberBandSetupScoreDetectsGreenDoubleBarBreak(t *testing.T) {
	st := &SymbolState{
		Bars: []intradayBar{
			testIntradayBarOHLC(9.96, 10.00, 9.92, 9.98),
			testIntradayBarOHLC(10.00, 10.05, 9.98, 10.01),
			testIntradayBarOHLC(10.01, 10.08, 9.99, 10.02),
		},
		BarMinuteNs: int64(time.Minute),
		BarOpenPxN:  int64(math.Round(10.02 * pxScale)),
		BarHighPxN:  int64(math.Round(10.20 * pxScale)),
		BarLowPxN:   int64(math.Round(10.00 * pxScale)),
		BarClosePxN: int64(math.Round(10.15 * pxScale)),
	}

	score, ok := rubberBandSetupScore(st)
	if !ok {
		t.Fatalf("expected rubber-band setup to pass")
	}
	if score <= 0 {
		t.Fatalf("expected positive score, got %.4f", score)
	}
}

func TestRubberBandSetupScoreRequiresTwoPriorHighBreaks(t *testing.T) {
	st := &SymbolState{
		Bars: []intradayBar{
			testIntradayBarOHLC(9.95, 10.00, 9.90, 9.98),
			testIntradayBarOHLC(10.00, 10.12, 9.99, 10.05),
			testIntradayBarOHLC(10.05, 10.08, 10.00, 10.04),
		},
		BarMinuteNs: int64(time.Minute),
		BarOpenPxN:  int64(math.Round(10.04 * pxScale)),
		BarHighPxN:  int64(math.Round(10.10 * pxScale)),
		BarLowPxN:   int64(math.Round(10.02 * pxScale)),
		BarClosePxN: int64(math.Round(10.09 * pxScale)),
	}

	if score, ok := rubberBandSetupScore(st); ok {
		t.Fatalf("expected setup to fail when only one prior high is cleared, got score %.4f", score)
	}
}

func TestRubberBandSetupScoreRequiresGreenSignalCandle(t *testing.T) {
	st := &SymbolState{
		Bars: []intradayBar{
			testIntradayBarOHLC(9.95, 10.00, 9.90, 9.98),
			testIntradayBarOHLC(9.98, 10.02, 9.95, 10.00),
			testIntradayBarOHLC(10.00, 10.04, 9.98, 10.01),
		},
		BarMinuteNs: int64(time.Minute),
		BarOpenPxN:  int64(math.Round(10.15 * pxScale)),
		BarHighPxN:  int64(math.Round(10.20 * pxScale)),
		BarLowPxN:   int64(math.Round(9.99 * pxScale)),
		BarClosePxN: int64(math.Round(10.05 * pxScale)),
	}

	if score, ok := rubberBandSetupScore(st); ok {
		t.Fatalf("expected setup to fail for non-green signal candle, got score %.4f", score)
	}
}

func TestRubberBandBearishSetupScoreDetectsRedDoubleBarBreak(t *testing.T) {
	st := &SymbolState{
		Bars: []intradayBar{
			testIntradayBarOHLC(10.20, 10.25, 10.10, 10.22),
			testIntradayBarOHLC(10.22, 10.24, 10.05, 10.16),
			testIntradayBarOHLC(10.16, 10.18, 10.00, 10.12),
		},
		BarMinuteNs: int64(time.Minute),
		BarOpenPxN:  int64(math.Round(10.12 * pxScale)),
		BarHighPxN:  int64(math.Round(10.14 * pxScale)),
		BarLowPxN:   int64(math.Round(9.92 * pxScale)),
		BarClosePxN: int64(math.Round(9.98 * pxScale)),
	}

	score, ok := rubberBandBearishSetupScore(st)
	if !ok {
		t.Fatalf("expected bearish rubber-band setup to pass")
	}
	if score <= 0 {
		t.Fatalf("expected positive score, got %.4f", score)
	}
}

func TestRubberBandBearishSetupScoreRequiresTwoPriorLowBreaks(t *testing.T) {
	st := &SymbolState{
		Bars: []intradayBar{
			testIntradayBarOHLC(10.25, 10.30, 10.10, 10.20),
			testIntradayBarOHLC(10.20, 10.24, 9.95, 10.02),
			testIntradayBarOHLC(10.02, 10.05, 10.00, 10.01),
		},
		BarMinuteNs: int64(time.Minute),
		BarOpenPxN:  int64(math.Round(10.01 * pxScale)),
		BarHighPxN:  int64(math.Round(10.03 * pxScale)),
		BarLowPxN:   int64(math.Round(9.98 * pxScale)),
		BarClosePxN: int64(math.Round(9.99 * pxScale)),
	}

	if score, ok := rubberBandBearishSetupScore(st); ok {
		t.Fatalf("expected bearish setup to fail when only one prior low is cleared, got score %.4f", score)
	}
}

func TestRubberBandBearishSetupScoreRequiresRedSignalCandle(t *testing.T) {
	st := &SymbolState{
		Bars: []intradayBar{
			testIntradayBarOHLC(10.20, 10.25, 10.08, 10.12),
			testIntradayBarOHLC(10.12, 10.15, 10.02, 10.05),
			testIntradayBarOHLC(10.05, 10.07, 10.00, 10.02),
		},
		BarMinuteNs: int64(time.Minute),
		BarOpenPxN:  int64(math.Round(9.96 * pxScale)),
		BarHighPxN:  int64(math.Round(10.02 * pxScale)),
		BarLowPxN:   int64(math.Round(9.90 * pxScale)),
		BarClosePxN: int64(math.Round(10.01 * pxScale)),
	}

	if score, ok := rubberBandBearishSetupScore(st); ok {
		t.Fatalf("expected bearish setup to fail for non-red signal candle, got score %.4f", score)
	}
}
