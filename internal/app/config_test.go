package app

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigMissingFileReturnsDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("expected nil error for missing config, got %v", err)
	}

	def := defaultConfig()
	if cfg.Server.Port != def.Server.Port {
		t.Fatalf("expected default port %d, got %d", def.Server.Port, cfg.Server.Port)
	}
	if cfg.Market.Timezone != def.Market.Timezone {
		t.Fatalf("expected default timezone %q, got %q", def.Market.Timezone, cfg.Market.Timezone)
	}
	if cfg.Scan.TopK != def.Scan.TopK {
		t.Fatalf("expected default top_k %d, got %d", def.Scan.TopK, cfg.Scan.TopK)
	}
}

func TestLoadConfigAppliesFallbackAliasesAndSortsSpreadCaps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	yml := `server:
  port: 0
databento:
  benchmark_symbol: ""
  max_control_msg_bytes: 0
market:
  timezone: ""
baselines:
  fallback_avg_vol_10d: 0
  fallback_avg_vol_20d: 1234
  fallback_adr_pct_10d: 0
  fallback_adr_pct_20d: 0.031
  fallback_avg_dollar_vol_10d: 0
  fallback_avg_dollar_vol_20d: 4567
scan:
  top_k: 0
  rank_interval: 0s
  max_staleness: 0s
  historical_timeout: 0s
  min_avg_dollar_vol_10d: 0
  min_avg_dollar_vol_20d: 98765
  spread_caps:
    - min_price: 5
      max_spread_pct: 0.003
    - min_price: 50
      max_spread_pct: 0.001
    - min_price: 10
      max_spread_pct: 0.002
`
	if err := os.WriteFile(path, []byte(yml), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Server.Port != 8099 {
		t.Fatalf("expected default port fallback, got %d", cfg.Server.Port)
	}
	if cfg.Databento.BenchmarkSymbol != "QQQ" {
		t.Fatalf("expected benchmark fallback QQQ, got %q", cfg.Databento.BenchmarkSymbol)
	}
	if cfg.Databento.MaxControlMsgBytes != 20000 {
		t.Fatalf("expected max control message bytes fallback 20000, got %d", cfg.Databento.MaxControlMsgBytes)
	}
	if cfg.Scan.TopK != 30 {
		t.Fatalf("expected top_k fallback 30, got %d", cfg.Scan.TopK)
	}
	if cfg.Scan.RankInterval.Duration != 15*time.Second {
		t.Fatalf("expected rank interval fallback 15s, got %v", cfg.Scan.RankInterval.Duration)
	}
	if cfg.Scan.MaxStaleness.Duration != 60*time.Second {
		t.Fatalf("expected max staleness fallback 60s, got %v", cfg.Scan.MaxStaleness.Duration)
	}
	if cfg.Scan.HistoricalTimeout.Duration != 90*time.Second {
		t.Fatalf("expected historical timeout fallback 90s, got %v", cfg.Scan.HistoricalTimeout.Duration)
	}
	if cfg.Market.Timezone != "America/New_York" {
		t.Fatalf("expected timezone fallback, got %q", cfg.Market.Timezone)
	}
	if cfg.Baselines.FallbackAvgVol10d != 1234 {
		t.Fatalf("expected avg vol alias fallback 1234, got %v", cfg.Baselines.FallbackAvgVol10d)
	}
	if cfg.Baselines.FallbackAdrPct10d != 0.031 {
		t.Fatalf("expected adr alias fallback 0.031, got %v", cfg.Baselines.FallbackAdrPct10d)
	}
	if cfg.Baselines.FallbackAvgDollarVol10d != 4567 {
		t.Fatalf("expected avg dollar vol alias fallback 4567, got %v", cfg.Baselines.FallbackAvgDollarVol10d)
	}
	if cfg.Scan.MinAvgDollarVol10d != 98765 {
		t.Fatalf("expected scan alias fallback 98765, got %v", cfg.Scan.MinAvgDollarVol10d)
	}

	if len(cfg.Scan.SpreadCaps) != 3 {
		t.Fatalf("expected 3 spread caps, got %d", len(cfg.Scan.SpreadCaps))
	}
	if cfg.Scan.SpreadCaps[0].MinPrice != 50 || cfg.Scan.SpreadCaps[1].MinPrice != 10 || cfg.Scan.SpreadCaps[2].MinPrice != 5 {
		t.Fatalf("expected spread caps sorted by min_price desc, got %+v", cfg.Scan.SpreadCaps)
	}
}

func TestLoadConfigInvalidYAMLReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.yaml")
	if err := os.WriteFile(path, []byte("scan: ["), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatalf("expected error for invalid YAML")
	}
}
