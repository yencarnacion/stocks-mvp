package app

import (
	"fmt"
	"os"
	"sort"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be scalar")
	}
	dd, err := time.ParseDuration(value.Value)
	if err != nil {
		return err
	}
	d.Duration = dd
	return nil
}

type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Databento DatabentoConfig `yaml:"databento"`
	Market    MarketConfig    `yaml:"market"`
	Baselines BaselinesConfig `yaml:"baselines"`
	Scan      ScanConfig      `yaml:"scan"`
}

type ServerConfig struct {
	Host              string `yaml:"host"`
	Port              int    `yaml:"port"`
	TickerURLTemplate string `yaml:"ticker_url_template"`
}

type DatabentoConfig struct {
	Dataset            string `yaml:"dataset"`
	Schema             string `yaml:"schema"`
	StypeIn            string `yaml:"stype_in"`
	BenchmarkSymbol    string `yaml:"benchmark_symbol"`
	SymbolsMode        string `yaml:"symbols_mode"` // watchlist | all_symbols
	Snapshot           bool   `yaml:"snapshot"`
	Verbose            bool   `yaml:"verbose"`
	MaxControlMsgBytes int    `yaml:"max_control_msg_bytes"`
}

type MarketConfig struct {
	Timezone             string `yaml:"timezone"`
	IncludeExtendedHours bool   `yaml:"include_extended_hours"`
	SessionOpen          string `yaml:"session_open"`
	SessionClose         string `yaml:"session_close"`
	ExtendedOpen         string `yaml:"extended_open"`
	ExtendedClose        string `yaml:"extended_close"`
}

type BaselinesConfig struct {
	CSVPath                 string  `yaml:"csv_path"`
	FallbackAvgVol10d       float64 `yaml:"fallback_avg_vol_10d"`
	FallbackAdrPct10d       float64 `yaml:"fallback_adr_pct_10d"`
	FallbackAvgDollarVol10d float64 `yaml:"fallback_avg_dollar_vol_10d"`
	// Backward-compat aliases.
	FallbackAvgVol20d       float64 `yaml:"fallback_avg_vol_20d"`
	FallbackAdrPct20d       float64 `yaml:"fallback_adr_pct_20d"`
	FallbackAvgDollarVol20d float64 `yaml:"fallback_avg_dollar_vol_20d"`
}

type SpreadCap struct {
	MinPrice     float64 `yaml:"min_price"`
	MaxSpreadPct float64 `yaml:"max_spread_pct"`
}

type ScoreWeights struct {
	RVOL        float64 `yaml:"rvol"`
	RelStrength float64 `yaml:"rel_strength"`
	Range       float64 `yaml:"range"`
	Liquidity   float64 `yaml:"liquidity"`
	Spread      float64 `yaml:"spread"`
}

type ScoringConfig struct {
	Weights               ScoreWeights `yaml:"weights"`
	SpreadBpsCap          float64      `yaml:"spread_bps_cap"`
	RangeExpansionCap     float64      `yaml:"range_expansion_cap"`
	LiquidityTargetPerMin float64      `yaml:"liquidity_target_per_min"`
}

type ScanConfig struct {
	TopK                  int      `yaml:"top_k"`
	RankInterval          Duration `yaml:"rank_interval"`
	MaxStaleness          Duration `yaml:"max_staleness"`
	HistoricalTimeout     Duration `yaml:"historical_timeout"`
	BacksideMinHHRallyPct float64  `yaml:"backside_min_hh_rally_pct"`
	EpisodicMovePct       float64  `yaml:"episodic_move_pct"`
	MinPrice              float64  `yaml:"min_price"`
	MaxPrice              float64  `yaml:"max_price"`
	MinRVOL               float64  `yaml:"min_rvol"`
	MinDollarVolPerM      float64  `yaml:"min_dollar_vol_per_min"`
	MinAdrExpansion       float64  `yaml:"min_adr_expansion"`
	MinAvgDollarVol10d    float64  `yaml:"min_avg_dollar_vol_10d"`
	// Backward-compat alias.
	MinAvgDollarVol20d float64       `yaml:"min_avg_dollar_vol_20d"`
	SpreadCaps         []SpreadCap   `yaml:"spread_caps"`
	Scoring            ScoringConfig `yaml:"scoring"`
}

func defaultConfig() Config {
	return Config{
		Server: ServerConfig{
			Host:              "0.0.0.0",
			Port:              8099,
			TickerURLTemplate: "http://localhost:8081/api/open-chart/{symbol}/{date}",
		},
		Databento: DatabentoConfig{
			Dataset:            "EQUS.MINI",
			Schema:             "mbp-1",
			StypeIn:            "raw_symbol",
			BenchmarkSymbol:    "QQQ",
			SymbolsMode:        "watchlist",
			Snapshot:           false,
			Verbose:            false,
			MaxControlMsgBytes: 20000,
		},
		Market: MarketConfig{
			Timezone:             "America/New_York",
			IncludeExtendedHours: false,
			SessionOpen:          "09:30",
			SessionClose:         "16:00",
			ExtendedOpen:         "04:00",
			ExtendedClose:        "20:00",
		},
		Baselines: BaselinesConfig{
			CSVPath:                 "./baselines.csv",
			FallbackAvgVol10d:       2_000_000,
			FallbackAdrPct10d:       0.025,
			FallbackAvgDollarVol10d: 20_000_000,
		},
		Scan: ScanConfig{
			TopK:                  30,
			RankInterval:          Duration{15 * time.Second},
			MaxStaleness:          Duration{60 * time.Second},
			HistoricalTimeout:     Duration{90 * time.Second},
			BacksideMinHHRallyPct: 0.0005,
			EpisodicMovePct:       0.02,
			MinPrice:              5,
			MaxPrice:              300,
			MinRVOL:               2.0,
			MinDollarVolPerM:      150_000,
			MinAdrExpansion:       1.2,
			MinAvgDollarVol10d:    20_000_000,
			SpreadCaps: []SpreadCap{
				{MinPrice: 50, MaxSpreadPct: 0.0005},
				{MinPrice: 10, MaxSpreadPct: 0.0010},
				{MinPrice: 5, MaxSpreadPct: 0.0015},
			},
			Scoring: ScoringConfig{
				Weights: ScoreWeights{
					RVOL:        0.35,
					RelStrength: 0.25,
					Range:       0.20,
					Liquidity:   0.10,
					Spread:      0.10,
				},
				SpreadBpsCap:          20,
				RangeExpansionCap:     3.0,
				LiquidityTargetPerMin: 1_000_000,
			},
		},
	}
}

func LoadConfig(path string) (Config, error) {
	cfg := defaultConfig()

	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, nil
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, err
	}

	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8099
	}
	if cfg.Databento.BenchmarkSymbol == "" {
		cfg.Databento.BenchmarkSymbol = "QQQ"
	}
	if cfg.Databento.MaxControlMsgBytes <= 0 {
		cfg.Databento.MaxControlMsgBytes = 20000
	}
	if cfg.Scan.TopK <= 0 {
		cfg.Scan.TopK = 30
	}
	if cfg.Scan.RankInterval.Duration <= 0 {
		cfg.Scan.RankInterval = Duration{15 * time.Second}
	}
	if cfg.Scan.MaxStaleness.Duration <= 0 {
		cfg.Scan.MaxStaleness = Duration{60 * time.Second}
	}
	if cfg.Scan.HistoricalTimeout.Duration <= 0 {
		cfg.Scan.HistoricalTimeout = Duration{90 * time.Second}
	}
	if cfg.Scan.BacksideMinHHRallyPct < 0 {
		cfg.Scan.BacksideMinHHRallyPct = 0.0005
	}
	if cfg.Scan.EpisodicMovePct <= 0 {
		cfg.Scan.EpisodicMovePct = 0.02
	}
	if cfg.Market.Timezone == "" {
		cfg.Market.Timezone = "America/New_York"
	}
	if cfg.Baselines.FallbackAvgVol10d <= 0 {
		cfg.Baselines.FallbackAvgVol10d = cfg.Baselines.FallbackAvgVol20d
	}
	if cfg.Baselines.FallbackAdrPct10d <= 0 {
		cfg.Baselines.FallbackAdrPct10d = cfg.Baselines.FallbackAdrPct20d
	}
	if cfg.Baselines.FallbackAvgDollarVol10d <= 0 {
		cfg.Baselines.FallbackAvgDollarVol10d = cfg.Baselines.FallbackAvgDollarVol20d
	}
	if cfg.Baselines.FallbackAvgVol10d <= 0 {
		cfg.Baselines.FallbackAvgVol10d = 2_000_000
	}
	if cfg.Baselines.FallbackAdrPct10d <= 0 {
		cfg.Baselines.FallbackAdrPct10d = 0.025
	}
	if cfg.Baselines.FallbackAvgDollarVol10d <= 0 {
		cfg.Baselines.FallbackAvgDollarVol10d = 20_000_000
	}
	if cfg.Scan.MinAvgDollarVol10d <= 0 {
		cfg.Scan.MinAvgDollarVol10d = cfg.Scan.MinAvgDollarVol20d
	}
	if cfg.Scan.MinAvgDollarVol10d <= 0 {
		cfg.Scan.MinAvgDollarVol10d = 20_000_000
	}

	// Keep spread caps predictable for matching by price.
	sort.Slice(cfg.Scan.SpreadCaps, func(i, j int) bool {
		return cfg.Scan.SpreadCaps[i].MinPrice > cfg.Scan.SpreadCaps[j].MinPrice
	})

	return cfg, nil
}
