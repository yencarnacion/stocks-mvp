package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadWatchlistMissingFileStillIncludesBenchmark(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-watchlist.yaml")

	set, err := LoadWatchlist(path, " qqq ")
	if err != nil {
		t.Fatalf("expected nil error for missing watchlist, got %v", err)
	}
	if len(set) != 1 {
		t.Fatalf("expected only benchmark symbol, got %d symbols", len(set))
	}
	if _, ok := set["QQQ"]; !ok {
		t.Fatalf("expected benchmark symbol QQQ in set, got %+v", set)
	}
}

func TestLoadWatchlistNormalizesAndDeduplicatesSymbols(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchlist.yaml")
	yml := `watchlist:
  - symbol: " aapl "
  - symbol: msft
  - symbol: ""
  - symbol: AAPL
`
	if err := os.WriteFile(path, []byte(yml), 0o644); err != nil {
		t.Fatalf("write watchlist: %v", err)
	}

	set, err := LoadWatchlist(path, "spy")
	if err != nil {
		t.Fatalf("load watchlist: %v", err)
	}

	if len(set) != 3 {
		t.Fatalf("expected 3 unique symbols (AAPL/MSFT/SPY), got %d", len(set))
	}
	if _, ok := set["AAPL"]; !ok {
		t.Fatalf("missing AAPL in set: %+v", set)
	}
	if _, ok := set["MSFT"]; !ok {
		t.Fatalf("missing MSFT in set: %+v", set)
	}
	if _, ok := set["SPY"]; !ok {
		t.Fatalf("missing SPY benchmark in set: %+v", set)
	}
}

func TestLoadWatchlistInvalidYAMLReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken-watchlist.yaml")
	if err := os.WriteFile(path, []byte("watchlist: ["), 0o644); err != nil {
		t.Fatalf("write watchlist: %v", err)
	}

	if _, err := LoadWatchlist(path, "QQQ"); err == nil {
		t.Fatalf("expected error for invalid YAML")
	}
}
