package main

import (
	"context"
	"flag"
	"fmt"
	"html"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"databento-scanner/internal/app"
)

type tabSpec struct {
	Title  string
	Signal string
	Action string
	Setup  string
	Pick   func(app.TopSnapshot) []app.Candidate
}

type timedSignals struct {
	TimeLabel string
	AsOfNY    string
	Rows      []app.Candidate
}

func main() {
	var (
		dateRaw      = flag.String("date", "", "Date in YYYYMMDD (required), e.g. 20260218")
		outputPath   = flag.String("output", "output.md", "Output markdown path")
		configPath   = flag.String("config", "config.yaml", "Path to config.yaml")
		watchlist    = flag.String("watchlist", "watchlist.yaml", "Path to watchlist.yaml")
		baselinesCSV = flag.String("baselines", "", "Optional override path to baselines.csv")
		dotenvPath   = flag.String("dotenv", ".env", "Optional .env file path")
		timeoutRaw   = flag.String("timeout", "30m", "Maximum replay/report runtime")
	)
	flag.Parse()

	if strings.TrimSpace(*dateRaw) == "" {
		exitf("missing -date (expected YYYYMMDD)")
	}

	timeout, err := time.ParseDuration(strings.TrimSpace(*timeoutRaw))
	if err != nil || timeout <= 0 {
		exitf("invalid -timeout: %v", err)
	}

	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if err := app.LoadDotEnv(*dotenvPath); err != nil {
		log.Warn("failed loading .env", "path", *dotenvPath, "err", err)
	}
	if _, err := os.Stat(*configPath); err != nil {
		exitf("config file is required and was not found: %s", *configPath)
	}

	cfg, err := app.LoadConfig(*configPath)
	if err != nil {
		exitf("failed to load config: %v", err)
	}
	if *baselinesCSV != "" {
		cfg.Baselines.CSVPath = *baselinesCSV
	}

	wl, err := app.LoadWatchlist(*watchlist, cfg.Databento.BenchmarkSymbol)
	if err != nil {
		exitf("failed to load watchlist: %v", err)
	}
	bl, err := app.LoadBaselinesCSV(cfg.Baselines.CSVPath)
	if err != nil {
		log.Warn("failed to load baselines.csv (using fallbacks)", "path", cfg.Baselines.CSVPath, "err", err)
		bl = map[string]app.Baseline{}
	}

	loc, err := time.LoadLocation(cfg.Market.Timezone)
	if err != nil || loc == nil {
		loc = time.FixedZone("America/New_York", -5*60*60)
	}
	dayNY, err := time.ParseInLocation("20060102", strings.TrimSpace(*dateRaw), loc)
	if err != nil {
		exitf("invalid -date %q: expected YYYYMMDD", *dateRaw)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	snaps, err := app.BuildHistoricalTimelineWithScan(ctx, log, cfg, wl, bl, dayNY, nil)
	if err != nil {
		exitf("failed building historical timeline: %v", err)
	}

	sort.Slice(snaps, func(i, j int) bool { return snaps[i].GeneratedAt.Before(snaps[j].GeneratedAt) })
	snaps = onlyDate(snaps, dayNY, loc)

	md := buildMarkdown(snaps, dayNY, loc, cfg.Server.TickerURLTemplate)
	if err := os.WriteFile(*outputPath, []byte(md), 0o644); err != nil {
		exitf("failed writing %s: %v", *outputPath, err)
	}

	log.Info(
		"report generated",
		"output", *outputPath,
		"date_ny", dayNY.Format("2006-01-02"),
		"snapshots", len(snaps),
	)
}

func buildMarkdown(snaps []app.TopSnapshot, dayNY time.Time, loc *time.Location, tickerTpl string) string {
	var b strings.Builder
	b.WriteString("# Stock Signals Report\n\n")
	b.WriteString(fmt.Sprintf("- Date (NY): `%s`\n", dayNY.In(loc).Format("2006-01-02")))
	b.WriteString(fmt.Sprintf("- Timezone: `%s`\n", loc.String()))
	b.WriteString(fmt.Sprintf("- Generated: `%s`\n", time.Now().In(loc).Format("2006-01-02 15:04:05 MST")))
	if len(snaps) > 0 {
		b.WriteString(fmt.Sprintf("- Benchmark: `%s`\n", snaps[len(snaps)-1].Benchmark))
	}
	b.WriteString(fmt.Sprintf("- Snapshots in report: `%d`\n\n", len(snaps)))

	tabs := []tabSpec{
		{Title: "Strongest", Signal: "buy", Action: "BUY", Setup: "Relative-strength momentum leaders", Pick: func(s app.TopSnapshot) []app.Candidate { return s.StrongestCandidates }},
		{Title: "Weakest", Signal: "sell", Action: "SELL", Setup: "Relative-weakness momentum laggards", Pick: func(s app.TopSnapshot) []app.Candidate { return s.WeakestCandidates }},
		{Title: "Backside", Signal: "buy", Action: "BUY", Setup: "Backside reclaim continuation setup", Pick: func(s app.TopSnapshot) []app.Candidate { return s.BacksideCandidates }},
		{Title: "Rubber Band Bullish", Signal: "buy", Pick: func(s app.TopSnapshot) []app.Candidate {
			if len(s.RubberBandBullishCandidates) > 0 {
				return s.RubberBandBullishCandidates
			}
			return s.RubberBandCandidates
		}},
		{Title: "Rubber Band Bearish", Signal: "sell", Action: "SELL", Setup: "Red candle clears lows of 2+ preceding candles", Pick: func(s app.TopSnapshot) []app.Candidate { return s.RubberBandBearishCandidates }},
	}
	// Fill bullish RB metadata while preserving fallback logic above.
	for i := range tabs {
		if tabs[i].Title == "Rubber Band Bullish" {
			tabs[i].Action = "BUY"
			tabs[i].Setup = "Green candle clears highs of 2+ preceding candles"
		}
	}

	for _, tab := range tabs {
		items := collectTimed(snaps, tab.Pick)
		b.WriteString(fmt.Sprintf("## %s\n\n", tab.Title))
		if strings.TrimSpace(tab.Action) != "" {
			b.WriteString(fmt.Sprintf("- Action bias: `%s`\n", tab.Action))
		}
		if strings.TrimSpace(tab.Setup) != "" {
			b.WriteString(fmt.Sprintf("- Setup: %s\n\n", tab.Setup))
		}
		if len(items) == 0 {
			b.WriteString("_No signals found for this tab on the selected date._\n\n")
			continue
		}
		for _, ts := range items {
			b.WriteString(fmt.Sprintf("### %s ET\n\n", ts.TimeLabel))
			b.WriteString("| # | Action | Symbol | Score | Price | RVOL | Spread bps | $/min | ADR x | RS vs Bench |\n")
			b.WriteString("|---:|:------:|:------|------:|------:|-----:|-----------:|------:|------:|------------:|\n")
			for _, c := range ts.Rows {
				sym := formatSymbolMarkdown(c.Symbol, tickerTpl, ts.AsOfNY, tab.Signal)
				b.WriteString(fmt.Sprintf(
					"| %d | %s | %s | %.4f | %.2f | %.2f | %.2f | %s | %.2f | %.2f%% |\n",
					c.Rank,
					tab.Action,
					sym,
					c.Score,
					c.Price,
					c.RVOL,
					c.SpreadBps,
					formatIntCommas(int64(c.DollarVolPerMin)),
					c.AdrExpansion,
					100*c.RelStrengthVsSpy,
				))
			}
			b.WriteString("\n")
		}
	}

	return b.String()
}

func collectTimed(snaps []app.TopSnapshot, pick func(app.TopSnapshot) []app.Candidate) []timedSignals {
	out := make([]timedSignals, 0, len(snaps))
	for _, snap := range snaps {
		rows := pick(snap)
		if len(rows) == 0 {
			continue
		}
		cp := make([]app.Candidate, len(rows))
		copy(cp, rows)
		out = append(out, timedSignals{
			TimeLabel: snap.GeneratedAt.Format("15:04"),
			AsOfNY:    snap.AsOfNY,
			Rows:      cp,
		})
	}
	return out
}

func onlyDate(snaps []app.TopSnapshot, dayNY time.Time, loc *time.Location) []app.TopSnapshot {
	target := dayNY.In(loc).Format("2006-01-02")
	out := make([]app.TopSnapshot, 0, len(snaps))
	for _, snap := range snaps {
		if snap.GeneratedAt.In(loc).Format("2006-01-02") == target {
			out = append(out, snap)
		}
	}
	return out
}

func formatSymbolMarkdown(symbol, template, asOfNY, signal string) string {
	link := formatTickerURL(template, symbol, asOfNY, signal)
	if link == "" {
		return symbol
	}
	return fmt.Sprintf(
		`<a href="%s" target="_blank" rel="noopener noreferrer">%s</a>`,
		html.EscapeString(link),
		html.EscapeString(symbol),
	)
}

func formatTickerURL(template, symbol, asOfNY, signal string) string {
	template = strings.TrimSpace(template)
	date := ""
	tm := ""
	if len(asOfNY) >= 10 {
		date = asOfNY[:10]
	}
	if len(asOfNY) >= 16 {
		tm = strings.ReplaceAll(asOfNY[11:16], ":", "")
	}

	base := openChartBase(template)
	if base != "" {
		q := url.Values{}
		q.Set("ticker", symbol)
		if date != "" {
			q.Set("date", date)
		}
		if tm != "" {
			q.Set("time", tm)
		}
		if signal != "" {
			q.Set("signal", signal)
		}
		return base + "?" + q.Encode()
	}

	if template == "" {
		return ""
	}

	enc := func(v string) string {
		return strings.ReplaceAll(url.QueryEscape(v), "+", "%20")
	}

	repl := template
	repl = strings.ReplaceAll(repl, "{symbol}", enc(symbol))
	repl = strings.ReplaceAll(repl, "{ticker}", enc(symbol))
	repl = strings.ReplaceAll(repl, "{date}", enc(date))
	repl = strings.ReplaceAll(repl, "{time}", enc(tm))
	repl = strings.ReplaceAll(repl, "{signal}", enc(signal))
	repl = strings.ReplaceAll(repl, "{as_of}", enc(asOfNY))
	return repl
}

func openChartBase(template string) string {
	template = strings.TrimSpace(template)
	if template == "" {
		return "/api/open-chart"
	}
	i := strings.Index(template, "/api/open-chart")
	if i < 0 {
		return ""
	}
	return template[:i] + "/api/open-chart"
}

func formatIntCommas(n int64) string {
	sign := ""
	if n < 0 {
		sign = "-"
		n = -n
	}
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return sign + s
	}
	var b strings.Builder
	b.WriteString(sign)
	rem := len(s) % 3
	if rem > 0 {
		b.WriteString(s[:rem])
		if len(s) > rem {
			b.WriteByte(',')
		}
	}
	for i := rem; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	return b.String()
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
