package app

import (
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
