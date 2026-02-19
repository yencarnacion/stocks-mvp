package app

import (
	"io"
	"log/slog"
	"testing"
)

func TestBuildSymbolChunksAllSymbolsMode(t *testing.T) {
	cfg := defaultConfig()
	cfg.Databento.SymbolsMode = "all_symbols"

	sc := NewScanner(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg,
		map[string]struct{}{"AAPL": {}},
		map[string]Baseline{},
	)

	chunks := sc.buildSymbolChunks()
	if len(chunks) != 1 || len(chunks[0]) != 1 || chunks[0][0] != "ALL_SYMBOLS" {
		t.Fatalf("expected single ALL_SYMBOLS chunk, got %+v", chunks)
	}
}

func TestBuildSymbolChunksSortsAndSplitsByByteBudget(t *testing.T) {
	cfg := defaultConfig()
	cfg.Databento.MaxControlMsgBytes = 9

	sc := NewScanner(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg,
		map[string]struct{}{
			"CCC": {},
			"AAA": {},
			"BB":  {},
		},
		map[string]Baseline{},
	)

	chunks := sc.buildSymbolChunks()
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d (%+v)", len(chunks), chunks)
	}
	if len(chunks[0]) != 2 || chunks[0][0] != "AAA" || chunks[0][1] != "BB" {
		t.Fatalf("expected first chunk [AAA BB], got %+v", chunks[0])
	}
	if len(chunks[1]) != 1 || chunks[1][0] != "CCC" {
		t.Fatalf("expected second chunk [CCC], got %+v", chunks[1])
	}
}

func TestBuildSymbolChunksFallsBackToBenchmarkWhenWatchlistEmpty(t *testing.T) {
	cfg := defaultConfig()
	cfg.Databento.BenchmarkSymbol = "spy"

	sc := NewScanner(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg,
		map[string]struct{}{},
		map[string]Baseline{},
	)

	chunks := sc.buildSymbolChunks()
	if len(chunks) != 1 || len(chunks[0]) != 1 || chunks[0][0] != "SPY" {
		t.Fatalf("expected fallback chunk with benchmark symbol SPY, got %+v", chunks)
	}
}
