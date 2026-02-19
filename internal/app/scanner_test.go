package app

import (
	"fmt"
	"io"
	"log/slog"
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
