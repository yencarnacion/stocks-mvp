package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadBaselinesCSVSupportsCurrentAndLegacyColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "baselines.csv")
	csv := "symbol,avg_vol_10d,avg_vol_20d,adr_pct_10d,adr_pct_20d,avg_dollar_vol_10d,avg_dollar_vol_20d\n" +
		" aapl ,0,100,0,0.03,0,500\n" +
		"MSFT,200,0,0.04,0,600,0\n" +
		",1,2,3,4,5,6\n"
	if err := os.WriteFile(path, []byte(csv), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	out, err := LoadBaselinesCSV(path)
	if err != nil {
		t.Fatalf("load baselines: %v", err)
	}

	if len(out) != 2 {
		t.Fatalf("expected 2 symbols (blank symbol row ignored), got %d", len(out))
	}

	aapl := out["AAPL"]
	if aapl.AvgVol10d != 100 {
		t.Fatalf("expected AAPL avg vol fallback from avg_vol_20d=100, got %v", aapl.AvgVol10d)
	}
	if aapl.AdrPct10d != 0.03 {
		t.Fatalf("expected AAPL adr fallback from adr_pct_20d=0.03, got %v", aapl.AdrPct10d)
	}
	if aapl.AvgDollarVol10d != 500 {
		t.Fatalf("expected AAPL avg dollar vol fallback from avg_dollar_vol_20d=500, got %v", aapl.AvgDollarVol10d)
	}

	msft := out["MSFT"]
	if msft.AvgVol10d != 200 || msft.AdrPct10d != 0.04 || msft.AvgDollarVol10d != 600 {
		t.Fatalf("expected MSFT to use 10d columns, got %+v", msft)
	}
}

func TestLoadBaselinesCSVMissingRequiredColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-baselines.csv")
	csv := "symbol,avg_vol_10d\nAAPL,100\n"
	if err := os.WriteFile(path, []byte(csv), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	if _, err := LoadBaselinesCSV(path); err == nil {
		t.Fatalf("expected error when adr column is missing")
	}
}
