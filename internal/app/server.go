package app

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed ui/*
var uiFS embed.FS

type Server struct {
	log *slog.Logger
	cfg Config
	sc  *Scanner

	watchlist map[string]struct{}
	baselines map[string]Baseline
	loc       *time.Location

	historyMu    sync.Mutex
	historyCache map[string]TopSnapshot
}

type BacksideHistoryItem struct {
	TimeLabel string      `json:"time_label"`
	AsOfNY    string      `json:"as_of_ny"`
	Rows      []Candidate `json:"rows"`
}

func NewServer(log *slog.Logger, cfg Config, sc *Scanner, watchlist map[string]struct{}, baselines map[string]Baseline) *Server {
	loc, err := time.LoadLocation(cfg.Market.Timezone)
	if err != nil || loc == nil {
		loc = time.FixedZone("America/New_York", -5*60*60)
	}
	return &Server{
		log:          log,
		cfg:          cfg,
		sc:           sc,
		watchlist:    watchlist,
		baselines:    baselines,
		loc:          loc,
		historyCache: make(map[string]TopSnapshot),
	}
}

func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/top", s.handleTop)
	mux.HandleFunc("/api/settings", s.handleSettings)
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/time", s.handleTime)
	mux.HandleFunc("/api/backside-history", s.handleBacksideHistory)
	mux.HandleFunc("/api/rb-history", s.handleRBHistory)

	sub, _ := fs.Sub(uiFS, "ui")
	mux.Handle("/", http.FileServer(http.FS(sub)))

	addr := fmt.Sprintf("%s:%d", s.cfg.Server.Host, s.cfg.Server.Port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx2)
	}()

	s.log.Info("web server listening", "addr", addr)
	err := srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *Server) handleTop(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	mode := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode")))
	if mode == "" {
		mode = "live"
	}
	if mode != "live" && mode != "historical" {
		http.Error(w, "mode must be live or historical", http.StatusBadRequest)
		return
	}

	scan, hasOverrides, err := parseScanOverrides(r, s.cfg.Scan)
	if err != nil {
		http.Error(w, "invalid gate override: "+err.Error(), http.StatusBadRequest)
		return
	}

	asOfNY, hasAsOf, err := parseAsOfFromRequest(r, s.loc)
	if err != nil {
		http.Error(w, "invalid as-of time: "+err.Error(), http.StatusBadRequest)
		return
	}
	asOfLog := "n/a"
	if hasAsOf {
		asOfLog = asOfNY.In(s.loc).Format("2006-01-02 15:04:05 MST")
	}
	s.log.Info(
		"api top request",
		"mode", mode,
		"has_as_of", hasAsOf,
		"as_of_ny", asOfLog,
		"has_gate_overrides", hasOverrides,
		"remote", r.RemoteAddr,
	)

	if mode == "live" {
		if hasAsOf {
			if !hasOverrides {
				if snap, ok := s.sc.GetSnapshotAsOf(asOfNY); ok {
					_ = json.NewEncoder(w).Encode(snap)
					return
				}
			}
			snap, err := s.historicalSnapshot(r.Context(), asOfNY, &scan)
			if err != nil {
				http.Error(w, "historical replay failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(snap)
			return
		}
		if hasOverrides {
			nowNY := time.Now().In(s.loc)
			snap := s.sc.ComputeSnapshotWithScan(nowNY, "live", scan)
			_ = json.NewEncoder(w).Encode(snap)
			return
		}
		_ = json.NewEncoder(w).Encode(s.sc.GetSnapshot())
		return
	}

	if !hasAsOf {
		http.Error(w, "historical mode requires date/time or as_of", http.StatusBadRequest)
		return
	}

	snap, err := s.historicalSnapshot(r.Context(), asOfNY, &scan)
	if err != nil {
		http.Error(w, "historical replay failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(snap)
}

func (s *Server) handleSettings(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	spread50 := spreadBpsForMinPrice(s.cfg.Scan, 50)
	spread10 := spreadBpsForMinPrice(s.cfg.Scan, 10)
	spread5 := spreadBpsForMinPrice(s.cfg.Scan, 5)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"timezone":            s.cfg.Market.Timezone,
		"ticker_url_template": s.cfg.Server.TickerURLTemplate,
		"scan": map[string]any{
			"top_k":                  s.cfg.Scan.TopK,
			"min_price":              s.cfg.Scan.MinPrice,
			"max_price":              s.cfg.Scan.MaxPrice,
			"min_rvol":               s.cfg.Scan.MinRVOL,
			"min_dollar_vol_per_min": s.cfg.Scan.MinDollarVolPerM,
			"min_adr_expansion":      s.cfg.Scan.MinAdrExpansion,
			"min_avg_dollar_vol_10d": s.cfg.Scan.MinAvgDollarVol10d,
			"max_staleness_ms":       s.cfg.Scan.MaxStaleness.Duration.Milliseconds(),
			"spread_cap_bps_50":      spread50,
			"spread_cap_bps_10":      spread10,
			"spread_cap_bps_5":       spread5,
		},
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	now := time.Now().In(s.loc)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":          true,
		"timezone":    s.cfg.Market.Timezone,
		"time_ny":     now.Format(time.RFC3339),
		"time_pretty": now.Format("2006-01-02 15:04:05 MST"),
	})
}

func (s *Server) handleTime(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	now := time.Now().In(s.loc)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"timezone": s.cfg.Market.Timezone,
		"now_ny":   now.Format("2006-01-02 15:04:05 MST"),
		"date":     now.Format("2006-01-02"),
		"time":     now.Format("15:04"),
	})
}

func (s *Server) handleBacksideHistory(w http.ResponseWriter, r *http.Request) {
	s.handleSignalHistory(w, r, func(snap TopSnapshot) []Candidate {
		return snap.BacksideCandidates
	})
}

func (s *Server) handleRBHistory(w http.ResponseWriter, r *http.Request) {
	s.handleSignalHistory(w, r, func(snap TopSnapshot) []Candidate {
		return snap.RubberBandCandidates
	})
}

func (s *Server) handleSignalHistory(w http.ResponseWriter, r *http.Request, pick func(TopSnapshot) []Candidate) {
	w.Header().Set("Content-Type", "application/json")

	mode := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode")))
	if mode == "" {
		mode = "live"
	}
	if mode != "live" && mode != "historical" {
		http.Error(w, "mode must be live or historical", http.StatusBadRequest)
		return
	}

	targetDate := time.Now().In(s.loc).Format("2006-01-02")
	if rawDate := strings.TrimSpace(r.URL.Query().Get("date")); rawDate != "" {
		if _, err := time.ParseInLocation("2006-01-02", rawDate, s.loc); err != nil {
			http.Error(w, "invalid date format (expected YYYY-MM-DD)", http.StatusBadRequest)
			return
		}
		targetDate = rawDate
	}

	// This endpoint is driven from the in-memory live scanner timeline.
	// For non-live mode we return an empty payload to keep UI behavior predictable.
	if mode != "live" {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":     mode,
			"date_ny":  targetDate,
			"timezone": s.cfg.Market.Timezone,
			"count":    0,
			"items":    []BacksideHistoryItem{},
		})
		return
	}

	items := collectSignalHistoryItems(s.sc.GetHistory(), targetDate, s.loc, pick)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"mode":     mode,
		"date_ny":  targetDate,
		"timezone": s.cfg.Market.Timezone,
		"count":    len(items),
		"items":    items,
	})
}

func collectBacksideHistoryItems(history []TopSnapshot, targetDate string, loc *time.Location) []BacksideHistoryItem {
	return collectSignalHistoryItems(history, targetDate, loc, func(snap TopSnapshot) []Candidate {
		return snap.BacksideCandidates
	})
}

func collectRubberBandHistoryItems(history []TopSnapshot, targetDate string, loc *time.Location) []BacksideHistoryItem {
	return collectSignalHistoryItems(history, targetDate, loc, func(snap TopSnapshot) []Candidate {
		return snap.RubberBandCandidates
	})
}

func collectSignalHistoryItems(history []TopSnapshot, targetDate string, loc *time.Location, pick func(TopSnapshot) []Candidate) []BacksideHistoryItem {
	if len(history) == 0 {
		return []BacksideHistoryItem{}
	}

	out := make([]BacksideHistoryItem, 0, len(history))
	for _, snap := range history {
		if snap.GeneratedAt.In(loc).Format("2006-01-02") != targetDate {
			continue
		}
		rowsSrc := pick(snap)
		if len(rowsSrc) == 0 {
			continue
		}

		rows := make([]Candidate, len(rowsSrc))
		copy(rows, rowsSrc)
		out = append(out, BacksideHistoryItem{
			TimeLabel: snap.GeneratedAt.In(loc).Format("15:04"),
			AsOfNY:    snap.AsOfNY,
			Rows:      rows,
		})
	}
	return out
}

func (s *Server) historicalSnapshot(ctx context.Context, asOfNY time.Time, scan *ScanConfig) (TopSnapshot, error) {
	asOfNY = asOfNY.In(s.loc)
	nowNY := time.Now().In(s.loc)
	if asOfNY.After(nowNY) {
		asOfNY = nowNY
	}
	effectiveScan := s.cfg.Scan
	if scan != nil {
		effectiveScan = *scan
	}
	key := asOfNY.Format("2006-01-02T15:04") + "|" + scanSignature(effectiveScan)

	s.historyMu.Lock()
	if snap, ok := s.historyCache[key]; ok {
		s.historyMu.Unlock()
		s.log.Info("historical replay cache hit", "as_of_ny", asOfNY.Format("2006-01-02 15:04:05 MST"))
		return snap, nil
	}
	s.historyMu.Unlock()

	timeout := s.cfg.Scan.HistoricalTimeout.Duration
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	s.log.Info("running historical replay", "as_of_ny", asOfNY.Format("2006-01-02 15:04 MST"), "scan_signature", scanSignature(effectiveScan))
	snap, err := BuildHistoricalSnapshotWithScan(ctx2, s.log, s.cfg, s.watchlist, s.baselines, asOfNY, &effectiveScan)
	if err != nil {
		s.log.Error("historical replay failed", "as_of_ny", asOfNY.Format("2006-01-02 15:04:05 MST"), "err", err)
		return TopSnapshot{}, err
	}
	// Always stamp historical responses with the exact requested as-of time.
	stamp := asOfNY.Format("2006-01-02 15:04:05 MST")
	snap.GeneratedAt = asOfNY
	snap.GeneratedAtNY = stamp
	snap.AsOfNY = stamp
	s.log.Info("historical replay succeeded", "as_of_ny", asOfNY.Format("2006-01-02 15:04:05 MST"), "candidates", len(snap.Candidates))

	s.historyMu.Lock()
	s.historyCache[key] = snap
	if len(s.historyCache) > 200 {
		keys := make([]string, 0, len(s.historyCache))
		for k := range s.historyCache {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, old := range keys[:len(keys)-200] {
			delete(s.historyCache, old)
		}
	}
	s.historyMu.Unlock()

	return snap, nil
}

func parseAsOfFromRequest(r *http.Request, loc *time.Location) (time.Time, bool, error) {
	q := r.URL.Query()

	if raw := strings.TrimSpace(q.Get("as_of")); raw != "" {
		layouts := []string{
			time.RFC3339,
			"2006-01-02T15:04",
			"2006-01-02 15:04",
			"2006-01-02T15:04:05",
		}
		for _, layout := range layouts {
			var t time.Time
			var err error
			if layout == time.RFC3339 {
				t, err = time.Parse(layout, raw)
			} else {
				t, err = time.ParseInLocation(layout, raw, loc)
			}
			if err == nil {
				return t.In(loc), true, nil
			}
		}
		return time.Time{}, true, fmt.Errorf("unsupported as_of format")
	}

	dateStr := strings.TrimSpace(q.Get("date"))
	timeStr := strings.TrimSpace(q.Get("time"))
	if dateStr == "" && timeStr == "" {
		return time.Time{}, false, nil
	}

	now := time.Now().In(loc)
	if dateStr == "" {
		dateStr = now.Format("2006-01-02")
	}
	if timeStr == "" {
		timeStr = now.Format("15:04")
	}

	t, err := time.ParseInLocation("2006-01-02 15:04", dateStr+" "+timeStr, loc)
	if err != nil {
		return time.Time{}, true, err
	}
	return t, true, nil
}

func parseScanOverrides(r *http.Request, base ScanConfig) (ScanConfig, bool, error) {
	scan := base
	scan.SpreadCaps = append([]SpreadCap(nil), base.SpreadCaps...)
	q := r.URL.Query()
	any := false

	parseFloat := func(key string, dst *float64) error {
		raw := strings.TrimSpace(q.Get(key))
		if raw == "" {
			return nil
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return fmt.Errorf("%s must be numeric", key)
		}
		*dst = v
		any = true
		return nil
	}
	parseInt := func(key string, dst *int) error {
		raw := strings.TrimSpace(q.Get(key))
		if raw == "" {
			return nil
		}
		v, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("%s must be integer", key)
		}
		*dst = v
		any = true
		return nil
	}

	if err := parseInt("top_k", &scan.TopK); err != nil {
		return base, false, err
	}
	if err := parseFloat("min_price", &scan.MinPrice); err != nil {
		return base, false, err
	}
	if err := parseFloat("max_price", &scan.MaxPrice); err != nil {
		return base, false, err
	}
	if err := parseFloat("min_rvol", &scan.MinRVOL); err != nil {
		return base, false, err
	}
	if err := parseFloat("min_dollar_vol_per_min", &scan.MinDollarVolPerM); err != nil {
		return base, false, err
	}
	if err := parseFloat("min_adr_expansion", &scan.MinAdrExpansion); err != nil {
		return base, false, err
	}
	if err := parseFloat("min_avg_dollar_vol_10d", &scan.MinAvgDollarVol10d); err != nil {
		return base, false, err
	}
	// Backward-compatible alias.
	if err := parseFloat("min_avg_dollar_vol_20d", &scan.MinAvgDollarVol10d); err != nil {
		return base, false, err
	}
	maxStalenessMS := int(scan.MaxStaleness.Duration / time.Millisecond)
	if err := parseInt("max_staleness_ms", &maxStalenessMS); err != nil {
		return base, false, err
	}
	scan.MaxStaleness = Duration{time.Duration(maxStalenessMS) * time.Millisecond}

	if err := applySpreadBpsOverride(q, "spread_cap_bps_50", 50, &scan, &any); err != nil {
		return base, false, err
	}
	if err := applySpreadBpsOverride(q, "spread_cap_bps_10", 10, &scan, &any); err != nil {
		return base, false, err
	}
	if err := applySpreadBpsOverride(q, "spread_cap_bps_5", 5, &scan, &any); err != nil {
		return base, false, err
	}

	if scan.TopK <= 0 {
		return base, false, fmt.Errorf("top_k must be > 0")
	}
	if scan.MinPrice < 0 || scan.MaxPrice <= 0 || scan.MaxPrice < scan.MinPrice {
		return base, false, fmt.Errorf("price gates invalid")
	}
	if scan.MinRVOL < 0 || scan.MinDollarVolPerM < 0 || scan.MinAdrExpansion < 0 || scan.MinAvgDollarVol10d < 0 {
		return base, false, fmt.Errorf("gates must be >= 0")
	}
	if scan.MaxStaleness.Duration < 0 {
		return base, false, fmt.Errorf("max_staleness_ms must be >= 0")
	}

	return scan, any, nil
}

func applySpreadBpsOverride(q url.Values, key string, minPrice float64, scan *ScanConfig, any *bool) error {
	raw := strings.TrimSpace(q.Get(key))
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fmt.Errorf("%s must be numeric", key)
	}
	if v < 0 {
		return fmt.Errorf("%s must be >= 0", key)
	}
	for i := range scan.SpreadCaps {
		if scan.SpreadCaps[i].MinPrice == minPrice {
			scan.SpreadCaps[i].MaxSpreadPct = v / 10000.0
			*any = true
			return nil
		}
	}
	scan.SpreadCaps = append(scan.SpreadCaps, SpreadCap{
		MinPrice:     minPrice,
		MaxSpreadPct: v / 10000.0,
	})
	sort.Slice(scan.SpreadCaps, func(i, j int) bool { return scan.SpreadCaps[i].MinPrice > scan.SpreadCaps[j].MinPrice })
	*any = true
	return nil
}

func scanSignature(scan ScanConfig) string {
	var s50, s10, s5 float64
	for _, cap := range scan.SpreadCaps {
		if cap.MinPrice == 50 {
			s50 = cap.MaxSpreadPct * 10000.0
		}
		if cap.MinPrice == 10 {
			s10 = cap.MaxSpreadPct * 10000.0
		}
		if cap.MinPrice == 5 {
			s5 = cap.MaxSpreadPct * 10000.0
		}
	}
	return fmt.Sprintf(
		"topk=%d|min=%.4f|max=%.4f|rvol=%.4f|dpm=%.2f|adrx=%.4f|adv10=%.2f|stale_ms=%d|s50_bps=%.2f|s10_bps=%.2f|s5_bps=%.2f",
		scan.TopK,
		scan.MinPrice,
		scan.MaxPrice,
		scan.MinRVOL,
		scan.MinDollarVolPerM,
		scan.MinAdrExpansion,
		scan.MinAvgDollarVol10d,
		scan.MaxStaleness.Duration.Milliseconds(),
		s50,
		s10,
		s5,
	)
}
