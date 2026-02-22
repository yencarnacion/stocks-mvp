package app

import (
	"testing"
	"time"
)

func TestCollectBacksideHistoryItemsIncludesEachNonEmptySnapshot(t *testing.T) {
	loc := mustLoadTestLoc(t)
	baseDay := time.Date(2026, 2, 19, 9, 30, 0, 0, loc)

	mkSnap := func(minute int, rows []Candidate) TopSnapshot {
		ts := baseDay.Add(time.Duration(minute) * time.Minute)
		return TopSnapshot{
			GeneratedAt:         ts,
			AsOfNY:              ts.Format("2006-01-02 15:04:05 MST"),
			BacksideCandidates:  rows,
			StrongestCandidates: []Candidate{},
			WeakestCandidates:   []Candidate{},
			HardPassCandidates:  []Candidate{},
			Candidates:          []Candidate{},
		}
	}

	history := []TopSnapshot{
		mkSnap(0, nil),
		mkSnap(1, []Candidate{{Rank: 1, Symbol: "ABC"}}),
		mkSnap(2, []Candidate{{Rank: 1, Symbol: "ABC"}}),
		mkSnap(3, nil),
		mkSnap(4, []Candidate{{Rank: 1, Symbol: "XYZ"}}),
	}

	items := collectBacksideHistoryItems(history, "2026-02-19", loc)
	if len(items) != 3 {
		t.Fatalf("expected 3 history items, got %d", len(items))
	}

	if items[0].TimeLabel != "09:31" || items[0].Rows[0].Symbol != "ABC" {
		t.Fatalf("unexpected first item: %+v", items[0])
	}
	if items[1].TimeLabel != "09:32" || items[1].Rows[0].Symbol != "ABC" {
		t.Fatalf("unexpected second item: %+v", items[1])
	}
	if items[2].TimeLabel != "09:34" || items[2].Rows[0].Symbol != "XYZ" {
		t.Fatalf("unexpected third item: %+v", items[2])
	}
}

func TestCollectBacksideHistoryItemsFiltersByDate(t *testing.T) {
	loc := mustLoadTestLoc(t)
	history := []TopSnapshot{
		{
			GeneratedAt:        time.Date(2026, 2, 18, 15, 0, 0, 0, loc),
			AsOfNY:             "2026-02-18 15:00:00 EST",
			BacksideCandidates: []Candidate{{Rank: 1, Symbol: "OLD"}},
		},
	}

	items := collectBacksideHistoryItems(history, "2026-02-19", loc)
	if len(items) != 0 {
		t.Fatalf("expected no items for non-matching date, got %d", len(items))
	}
}

func TestCollectRubberBandHistoryItemsIncludesEachNonEmptySnapshot(t *testing.T) {
	loc := mustLoadTestLoc(t)
	baseDay := time.Date(2026, 2, 19, 9, 30, 0, 0, loc)

	mkSnap := func(minute int, rows []Candidate) TopSnapshot {
		ts := baseDay.Add(time.Duration(minute) * time.Minute)
		return TopSnapshot{
			GeneratedAt:          ts,
			AsOfNY:               ts.Format("2006-01-02 15:04:05 MST"),
			RubberBandCandidates: rows,
			StrongestCandidates:  []Candidate{},
			WeakestCandidates:    []Candidate{},
			HardPassCandidates:   []Candidate{},
			BacksideCandidates:   []Candidate{},
			Candidates:           []Candidate{},
		}
	}

	history := []TopSnapshot{
		mkSnap(0, nil),
		mkSnap(1, []Candidate{{Rank: 1, Symbol: "ABC"}}),
		mkSnap(2, []Candidate{{Rank: 1, Symbol: "ABC"}}),
		mkSnap(3, nil),
		mkSnap(4, []Candidate{{Rank: 1, Symbol: "XYZ"}}),
	}

	items := collectRubberBandHistoryItems(history, "2026-02-19", loc)
	if len(items) != 3 {
		t.Fatalf("expected 3 history items, got %d", len(items))
	}

	if items[0].TimeLabel != "09:31" || items[0].Rows[0].Symbol != "ABC" {
		t.Fatalf("unexpected first item: %+v", items[0])
	}
	if items[1].TimeLabel != "09:32" || items[1].Rows[0].Symbol != "ABC" {
		t.Fatalf("unexpected second item: %+v", items[1])
	}
	if items[2].TimeLabel != "09:34" || items[2].Rows[0].Symbol != "XYZ" {
		t.Fatalf("unexpected third item: %+v", items[2])
	}
}
