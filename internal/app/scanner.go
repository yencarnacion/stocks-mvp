package app

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dbn "github.com/NimbleMarkets/dbn-go"
	dbn_live "github.com/NimbleMarkets/dbn-go/live"
)

const (
	pxScale           = 1_000_000_000.0
	maxIntradayBars   = 180
	rsWindow5m        = 1 * time.Minute
	rsWindow15m       = 15 * time.Minute
	rsWindow30m       = 30 * time.Minute
	vwapSlopeLookback = 1 * time.Minute
	rvolTopK          = 30
)

// SymbolState holds rolling intraday state derived from MBP-1 records.
type SymbolState struct {
	Symbol       string
	InstrumentID uint32

	BidPxN int64
	AskPxN int64
	BidSz  uint32
	AskSz  uint32

	OpenPxN int64
	HighPxN int64
	LowPxN  int64
	LastPxN int64

	CumVol      uint64
	CumNotional float64

	FirstTradeTs uint64
	LastTradeTs  uint64
	LastQuoteTs  uint64

	BarMinuteNs int64
	BarHighPxN  int64
	BarLowPxN   int64
	BarClosePxN int64
	Bars        []intradayBar
}

type intradayBar struct {
	MinuteStartNs int64
	HighPxN       int64
	LowPxN        int64
	ClosePxN      int64
	VWAP          float64
}

func (s *SymbolState) lastUpdateTs() uint64 {
	if s.LastTradeTs > s.LastQuoteTs {
		return s.LastTradeTs
	}
	return s.LastQuoteTs
}

type Candidate struct {
	Rank int `json:"rank"`

	Symbol string  `json:"symbol"`
	Score  float64 `json:"score"`

	Price float64 `json:"price"`
	Bid   float64 `json:"bid"`
	Ask   float64 `json:"ask"`

	SpreadPct float64 `json:"spread_pct"`
	SpreadBps float64 `json:"spread_bps"`

	CumVol           uint64  `json:"cum_vol"`
	RVOL             float64 `json:"rvol"`
	DollarVolPerMin  float64 `json:"dollar_vol_per_min"`
	AvgDollarVol10d  float64 `json:"avg_dollar_vol_10d"`
	RangePct         float64 `json:"range_pct"`
	AdrPct10d        float64 `json:"adr_pct_10d"`
	AdrExpansion     float64 `json:"adr_expansion"`
	RetFromOpen      float64 `json:"ret_from_open"`
	SpyRetFromOpen   float64 `json:"spy_ret_from_open"`
	RelStrengthVsSpy float64 `json:"rel_strength_vs_spy"`
	RS5mVsSpy        float64 `json:"rs_5m_vs_spy"`
	RS15mVsSpy       float64 `json:"rs_15m_vs_spy"`
	RS30mVsSpy       float64 `json:"rs_30m_vs_spy"`
	SessionVWAP      float64 `json:"session_vwap"`
	VWAPSlope5m      float64 `json:"vwap_slope_5m"`

	UpdatedMsAgo int64 `json:"updated_ms_ago"`
}

type TopSnapshot struct {
	Mode           string    `json:"mode"`
	Timezone       string    `json:"timezone"`
	GeneratedAt    time.Time `json:"generated_at"`
	GeneratedAtNY  string    `json:"generated_at_ny"`
	AsOfNY         string    `json:"as_of_ny"`
	Benchmark      string    `json:"benchmark"`
	CountSeen      int       `json:"count_seen"`
	CountRanked    int       `json:"count_ranked"`
	CountStrongest int       `json:"count_strongest"`
	CountWeakest   int       `json:"count_weakest"`
	CountHardPass  int       `json:"count_hard_pass"`
	CountBackside  int       `json:"count_backside"`
	CountRVOL      int       `json:"count_rvol"`
	Message        string    `json:"message,omitempty"`
	GateDebug      GateDebug `json:"gate_debug"`
	// Keep Candidates for backward compatibility with older clients; this mirrors StrongestCandidates.
	Candidates          []Candidate `json:"candidates"`
	StrongestCandidates []Candidate `json:"strongest_candidates"`
	WeakestCandidates   []Candidate `json:"weakest_candidates"`
	HardPassCandidates  []Candidate `json:"hard_pass_candidates"`
	BacksideCandidates  []Candidate `json:"backside_candidates"`
	RVOLCandidates      []Candidate `json:"rvol_candidates"`
}

type GateDebug struct {
	RowsEvaluated             int     `json:"rows_evaluated"`
	FailStale                 int     `json:"fail_stale"`
	FailMinAvgDollarVol10d    int     `json:"fail_min_avg_dollar_vol_10d"`
	FailMinRVOL               int     `json:"fail_min_rvol"`
	FailMinDollarVolPerMin    int     `json:"fail_min_dollar_vol_per_min"`
	FailMinAdrExpansion       int     `json:"fail_min_adr_expansion"`
	FailSpreadCap             int     `json:"fail_spread_cap"`
	FailStrongestTrend        int     `json:"fail_strongest_trend"`
	FailWeakestTrend          int     `json:"fail_weakest_trend"`
	FailStrongestRSPersist    int     `json:"fail_strongest_rs_persist"`
	FailWeakestRSPersist      int     `json:"fail_weakest_rs_persist"`
	PassedAllGates            int     `json:"passed_all_gates"`
	AvgDollarVolFromCSV       int     `json:"avg_dollar_vol_from_csv"`
	AvgDollarVolDerived       int     `json:"avg_dollar_vol_derived"`
	AvgDollarVolFallbackConst int     `json:"avg_dollar_vol_fallback_const"`
	SpreadCapBpsGE50          float64 `json:"spread_cap_bps_ge_50"`
	SpreadCapBpsGE10          float64 `json:"spread_cap_bps_ge_10"`
	SpreadCapBpsGE5           float64 `json:"spread_cap_bps_ge_5"`
	MaxSeenSpreadBps          float64 `json:"max_seen_spread_bps"`
	MaxSeenSpreadSymbol       string  `json:"max_seen_spread_symbol"`
	CandidateSpreadViolations int     `json:"candidate_spread_violations"`
	MaxCumVolSymbol           string  `json:"max_cum_vol_symbol"`
	MaxCumVol                 uint64  `json:"max_cum_vol"`
	MaxDollarVolPerMinSymbol  string  `json:"max_dollar_vol_per_min_symbol"`
	MaxDollarVolPerMin        float64 `json:"max_dollar_vol_per_min"`
	MaxRVOLSymbol             string  `json:"max_rvol_symbol"`
	MaxRVOL                   float64 `json:"max_rvol"`
}

type Scanner struct {
	log *slog.Logger
	cfg Config

	watchlist map[string]struct{}
	baselines map[string]Baseline

	instrumentToSymbol map[uint32]string
	statesBySymbol     map[string]*SymbolState

	loc         *time.Location
	sessionDate string
	startNs     int64
	endNs       int64

	// historical replay control
	stopAtNs                  int64
	maxEvent                  uint64
	captureHistoricalTimeline bool

	snap atomic.Value // TopSnapshot

	historyMu sync.RWMutex
	history   []TopSnapshot

	liveRVOLMinute     string
	liveRVOLCandidates []Candidate

	// per-run telemetry (useful for replay sanity checks)
	runMbp1Messages uint64
	runTradeEvents  uint64
	runQuoteEvents  uint64
	runMappings     uint64
}

func NewScanner(log *slog.Logger, cfg Config, watchlist map[string]struct{}, baselines map[string]Baseline) *Scanner {
	loc, err := time.LoadLocation(cfg.Market.Timezone)
	if err != nil || loc == nil {
		loc = time.FixedZone("America/New_York", -5*60*60)
	}

	s := &Scanner{
		log:                log,
		cfg:                cfg,
		watchlist:          watchlist,
		baselines:          baselines,
		instrumentToSymbol: make(map[uint32]string, 16384),
		statesBySymbol:     make(map[string]*SymbolState, 16384),
		loc:                loc,
		history:            make([]TopSnapshot, 0, 1024),
	}

	nowNY := time.Now().In(loc)
	empty := TopSnapshot{
		Mode:                "live",
		Timezone:            cfg.Market.Timezone,
		GeneratedAt:         nowNY,
		GeneratedAtNY:       nowNY.Format("2006-01-02 15:04:05 MST"),
		AsOfNY:              nowNY.Format("2006-01-02 15:04:05 MST"),
		Benchmark:           strings.ToUpper(cfg.Databento.BenchmarkSymbol),
		Candidates:          []Candidate{},
		StrongestCandidates: []Candidate{},
		WeakestCandidates:   []Candidate{},
		HardPassCandidates:  []Candidate{},
		BacksideCandidates:  []Candidate{},
		RVOLCandidates:      []Candidate{},
	}
	s.snap.Store(empty)
	return s
}

func (s *Scanner) GetSnapshot() TopSnapshot {
	return s.snap.Load().(TopSnapshot)
}

func (s *Scanner) GetSnapshotAsOf(asOfNY time.Time) (TopSnapshot, bool) {
	asOfNY = asOfNY.In(s.loc)
	s.historyMu.RLock()
	defer s.historyMu.RUnlock()

	if len(s.history) == 0 {
		return TopSnapshot{}, false
	}

	targetDay := asOfNY.Format("2006-01-02")
	for i := len(s.history) - 1; i >= 0; i-- {
		snap := s.history[i]
		t := snap.GeneratedAt.In(s.loc)
		if t.Format("2006-01-02") != targetDay {
			continue
		}
		if !t.After(asOfNY) {
			return snap, true
		}
	}
	return TopSnapshot{}, false
}

func (s *Scanner) GetHistory() []TopSnapshot {
	s.historyMu.RLock()
	defer s.historyMu.RUnlock()
	out := make([]TopSnapshot, len(s.history))
	copy(out, s.history)
	return out
}

func (s *Scanner) Run(ctx context.Context) error {
	nowNY := time.Now().In(s.loc)
	return s.runStream(ctx, liveReplayStartFor(nowNY, s.cfg, s.loc), false, time.Time{})
}

func BuildHistoricalSnapshot(ctx context.Context, log *slog.Logger, cfg Config, watchlist map[string]struct{}, baselines map[string]Baseline, asOfNY time.Time) (TopSnapshot, error) {
	return BuildHistoricalSnapshotWithScan(ctx, log, cfg, watchlist, baselines, asOfNY, nil)
}

func BuildHistoricalSnapshotWithScan(ctx context.Context, log *slog.Logger, cfg Config, watchlist map[string]struct{}, baselines map[string]Baseline, asOfNY time.Time, scanOverride *ScanConfig) (TopSnapshot, error) {
	replay := NewScanner(log, cfg, watchlist, baselines)
	if scanOverride != nil {
		replay.cfg.Scan = *scanOverride
	}
	replay.log.Info(
		"historical replay starting",
		"dataset", cfg.Databento.Dataset,
		"schema", cfg.Databento.Schema,
		"benchmark", strings.ToUpper(cfg.Databento.BenchmarkSymbol),
		"as_of_ny", asOfNY.In(replay.loc).Format("2006-01-02 15:04:05 MST"),
	)
	if err := replay.runStream(ctx, sessionOpenFor(asOfNY.In(replay.loc), cfg, replay.loc), true, asOfNY.In(replay.loc)); err != nil {
		return TopSnapshot{}, err
	}
	replay.log.Info(
		"historical replay completed",
		"as_of_ny", asOfNY.In(replay.loc).Format("2006-01-02 15:04:05 MST"),
		"candidates", len(replay.GetSnapshot().Candidates),
	)
	return replay.GetSnapshot(), nil
}

func BuildHistoricalTimelineWithScan(ctx context.Context, log *slog.Logger, cfg Config, watchlist map[string]struct{}, baselines map[string]Baseline, dayNY time.Time, scanOverride *ScanConfig) ([]TopSnapshot, error) {
	replay := NewScanner(log, cfg, watchlist, baselines)
	if scanOverride != nil {
		replay.cfg.Scan = *scanOverride
	}
	replay.captureHistoricalTimeline = true

	dayNY = dayNY.In(replay.loc)
	start := sessionOpenFor(dayNY, cfg, replay.loc)
	end := sessionCloseFor(dayNY, cfg, replay.loc)

	replay.log.Info(
		"historical timeline starting",
		"dataset", cfg.Databento.Dataset,
		"schema", cfg.Databento.Schema,
		"benchmark", strings.ToUpper(cfg.Databento.BenchmarkSymbol),
		"start_ny", start.Format("2006-01-02 15:04:05 MST"),
		"end_ny", end.Format("2006-01-02 15:04:05 MST"),
	)
	if err := replay.runStream(ctx, start, true, end); err != nil {
		return nil, err
	}

	out := replay.GetHistory()
	if len(out) == 0 {
		out = []TopSnapshot{replay.GetSnapshot()}
	}
	replay.log.Info(
		"historical timeline completed",
		"date_ny", dayNY.Format("2006-01-02"),
		"snapshots", len(out),
	)
	return out, nil
}

func (s *Scanner) ComputeSnapshotWithScan(evalNY time.Time, mode string, scan ScanConfig) TopSnapshot {
	return s.computeSnapshotAtWithScan(evalNY, mode, scan)
}

func (s *Scanner) runStream(ctx context.Context, start time.Time, historical bool, asOfNY time.Time) error {
	apiKey := os.Getenv("DATABENTO_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("DATABENTO_API_KEY is not set")
	}

	stypeIn, err := dbn.STypeFromString(s.cfg.Databento.StypeIn)
	if err != nil {
		return fmt.Errorf("invalid databento.stype_in: %w", err)
	}

	client, err := dbn_live.NewLiveClient(dbn_live.LiveConfig{
		ApiKey:               apiKey,
		Dataset:              s.cfg.Databento.Dataset,
		Client:               "databento-scanner",
		Encoding:             dbn.Encoding_Dbn,
		SendTsOut:            false,
		VersionUpgradePolicy: dbn.VersionUpgradePolicy_AsIs,
		Verbose:              s.cfg.Databento.Verbose,
	})
	if err != nil {
		return err
	}
	defer client.Stop()

	if _, err := client.Authenticate(apiKey); err != nil {
		return err
	}

	if historical {
		s.stopAtNs = asOfNY.UTC().UnixNano()
	} else {
		s.stopAtNs = 0
	}
	atomic.StoreUint64(&s.maxEvent, 0)
	s.runMbp1Messages = 0
	s.runTradeEvents = 0
	s.runQuoteEvents = 0
	s.runMappings = 0

	symbolChunks := s.buildSymbolChunks()
	for i, chunk := range symbolChunks {
		sub := dbn_live.SubscriptionRequestMsg{
			Schema:   s.cfg.Databento.Schema,
			StypeIn:  stypeIn,
			Symbols:  chunk,
			Start:    start,
			Snapshot: s.cfg.Databento.Snapshot,
		}
		if err := client.Subscribe(sub); err != nil {
			return fmt.Errorf("subscribe chunk %d/%d failed: %w", i+1, len(symbolChunks), err)
		}
		time.Sleep(125 * time.Millisecond)
	}

	if err := client.Start(); err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = client.Stop()
	}()

	scanner := client.GetDbnScanner()
	if scanner == nil {
		return fmt.Errorf("expected DBN scanner; got nil")
	}

	visitor := &dbVisitor{s: s, historical: historical}
	lastRank := time.Now()
	lastProgressLog := time.Now()
	lastMinutePublish := time.Time{}
	stopReason := "stream_end"
	nextHistoryMinute := start.In(s.loc).Truncate(time.Minute)

	for scanner.Next() {
		if err := scanner.Visit(visitor); err != nil {
			s.log.Warn("scanner visit error", "err", err)
		}

		if historical {
			maxEvent := int64(atomic.LoadUint64(&s.maxEvent))
			if s.captureHistoricalTimeline && maxEvent > 0 {
				eventMinute := time.Unix(0, maxEvent).In(s.loc).Truncate(time.Minute)
				limit := asOfNY.In(s.loc).Truncate(time.Minute)
				for !nextHistoryMinute.After(eventMinute) && !nextHistoryMinute.After(limit) {
					s.publish(s.computeSnapshotAt(nextHistoryMinute, "historical"), true)
					nextHistoryMinute = nextHistoryMinute.Add(time.Minute)
				}
			}
			if maxEvent >= s.stopAtNs {
				stopReason = "as_of_reached"
				break
			}
			if time.Since(lastProgressLog) >= 2*time.Second {
				maxEvent := int64(atomic.LoadUint64(&s.maxEvent))
				var maxEventNY string
				if maxEvent > 0 {
					maxEventNY = time.Unix(0, maxEvent).In(s.loc).Format("2006-01-02 15:04:05 MST")
				} else {
					maxEventNY = "n/a"
				}
				s.log.Info(
					"historical replay progress",
					"as_of_ny", asOfNY.In(s.loc).Format("2006-01-02 15:04:05 MST"),
					"max_event_ny", maxEventNY,
					"mapped_symbols", len(s.statesBySymbol),
				)
				lastProgressLog = time.Now()
			}
			continue
		}

		nowNY := time.Now().In(s.loc)
		currentMinute := nowNY.Truncate(time.Minute)
		if lastMinutePublish.IsZero() || currentMinute.After(lastMinutePublish) {
			maxEvent := int64(atomic.LoadUint64(&s.maxEvent))
			evalNY := liveEvalNY(nowNY, maxEvent, s.loc)
			s.publish(s.computeSnapshotAt(evalNY, "live"), true)
			lastMinutePublish = currentMinute
			lastRank = time.Now()
			continue
		}

		if time.Since(lastRank) >= s.cfg.Scan.RankInterval.Duration {
			maxEvent := int64(atomic.LoadUint64(&s.maxEvent))
			evalNY := liveEvalNY(nowNY, maxEvent, s.loc)
			s.publish(s.computeSnapshotAt(evalNY, "live"), true)
			lastRank = time.Now()
		}
	}

	if err := scanner.Error(); err != nil && !isExpectedScannerStopError(err, historical) {
		return err
	}

	if historical && s.captureHistoricalTimeline {
		limit := asOfNY.In(s.loc).Truncate(time.Minute)
		for !nextHistoryMinute.After(limit) {
			s.publish(s.computeSnapshotAt(nextHistoryMinute, "historical"), true)
			nextHistoryMinute = nextHistoryMinute.Add(time.Minute)
		}
	}

	var finalSnap TopSnapshot
	if historical {
		s.log.Info(
			"historical replay stream ended",
			"reason", stopReason,
			"as_of_ny", asOfNY.In(s.loc).Format("2006-01-02 15:04:05 MST"),
		)
		finalSnap = s.computeSnapshotAt(asOfNY.In(s.loc), "historical")
		s.publish(finalSnap, false)
	} else {
		nowNY := time.Now().In(s.loc)
		maxEvent := int64(atomic.LoadUint64(&s.maxEvent))
		evalNY := liveEvalNY(nowNY, maxEvent, s.loc)
		finalSnap = s.computeSnapshotAt(evalNY, "live")
		s.publish(finalSnap, true)
	}

	if historical {
		s.log.Info(
			"historical replay sanity",
			"as_of_ny", finalSnap.AsOfNY,
			"rows_seen", finalSnap.CountSeen,
			"rows_ranked", finalSnap.CountRanked,
			"message", finalSnap.Message,
			"fail_stale", finalSnap.GateDebug.FailStale,
			"fail_adv10d", finalSnap.GateDebug.FailMinAvgDollarVol10d,
			"fail_rvol", finalSnap.GateDebug.FailMinRVOL,
			"fail_dollar_per_min", finalSnap.GateDebug.FailMinDollarVolPerMin,
			"fail_adr", finalSnap.GateDebug.FailMinAdrExpansion,
			"fail_spread", finalSnap.GateDebug.FailSpreadCap,
			"spread_cap_ge50_bps", finalSnap.GateDebug.SpreadCapBpsGE50,
			"spread_cap_ge10_bps", finalSnap.GateDebug.SpreadCapBpsGE10,
			"spread_cap_ge5_bps", finalSnap.GateDebug.SpreadCapBpsGE5,
			"max_seen_spread_bps", finalSnap.GateDebug.MaxSeenSpreadBps,
			"max_seen_spread_symbol", finalSnap.GateDebug.MaxSeenSpreadSymbol,
			"candidate_spread_violations", finalSnap.GateDebug.CandidateSpreadViolations,
			"dv20_csv", finalSnap.GateDebug.AvgDollarVolFromCSV,
			"dv20_derived", finalSnap.GateDebug.AvgDollarVolDerived,
			"dv20_const", finalSnap.GateDebug.AvgDollarVolFallbackConst,
			"mbp1_messages", s.runMbp1Messages,
			"trade_events", s.runTradeEvents,
			"quote_events", s.runQuoteEvents,
			"mappings", s.runMappings,
		)
	}

	return nil
}

func (s *Scanner) publish(snap TopSnapshot, keepHistory bool) {
	if snap.Mode == "live" {
		minuteKey := snap.GeneratedAt.In(s.loc).Format("2006-01-02 15:04")
		if minuteKey != s.liveRVOLMinute {
			s.liveRVOLMinute = minuteKey
			s.liveRVOLCandidates = cloneCandidates(snap.RVOLCandidates)
		} else {
			snap.RVOLCandidates = cloneCandidates(s.liveRVOLCandidates)
		}
		snap.CountRVOL = len(snap.RVOLCandidates)
	} else {
		s.liveRVOLMinute = ""
		s.liveRVOLCandidates = nil
	}

	s.snap.Store(snap)
	if !keepHistory {
		return
	}

	s.historyMu.Lock()
	defer s.historyMu.Unlock()

	thisMinute := snap.GeneratedAt.In(s.loc).Truncate(time.Minute)
	if n := len(s.history); n > 0 {
		lastMinute := s.history[n-1].GeneratedAt.In(s.loc).Truncate(time.Minute)
		if thisMinute.Equal(lastMinute) {
			s.history[n-1] = snap
			return
		}
	}

	s.history = append(s.history, snap)
	if len(s.history) > 3000 {
		s.history = append([]TopSnapshot(nil), s.history[len(s.history)-3000:]...)
	}
}

func (s *Scanner) buildSymbolChunks() [][]string {
	mode := strings.ToLower(strings.TrimSpace(s.cfg.Databento.SymbolsMode))
	if mode == "all_symbols" {
		return [][]string{{"ALL_SYMBOLS"}}
	}

	symbols := make([]string, 0, len(s.watchlist))
	for sym := range s.watchlist {
		symbols = append(symbols, sym)
	}
	sort.Strings(symbols)

	maxBytes := s.cfg.Databento.MaxControlMsgBytes
	if maxBytes <= 0 {
		maxBytes = 20000
	}

	var chunks [][]string
	var cur []string
	curBytes := 0
	for _, sym := range symbols {
		add := len(sym) + 1
		if len(cur) > 0 && curBytes+add > maxBytes {
			chunks = append(chunks, cur)
			cur = nil
			curBytes = 0
		}
		cur = append(cur, sym)
		curBytes += add
	}
	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}
	if len(chunks) == 0 {
		chunks = append(chunks, []string{strings.ToUpper(s.cfg.Databento.BenchmarkSymbol)})
	}
	return chunks
}

type dbVisitor struct {
	s          *Scanner
	historical bool
}

func (v *dbVisitor) OnSymbolMappingMsg(r *dbn.SymbolMappingMsg) error {
	v.s.runMappings++
	sym := strings.ToUpper(strings.TrimSpace(r.StypeOutSymbol))
	if sym == "" {
		return nil
	}

	if len(v.s.watchlist) > 0 {
		if _, ok := v.s.watchlist[sym]; !ok {
			return nil
		}
	}

	instr := r.Header.InstrumentID
	v.s.instrumentToSymbol[instr] = sym

	st, ok := v.s.statesBySymbol[sym]
	if !ok {
		st = &SymbolState{Symbol: sym, InstrumentID: instr}
		v.s.statesBySymbol[sym] = st
	} else {
		st.InstrumentID = instr
	}
	return nil
}

func (v *dbVisitor) OnMbp1(r *dbn.Mbp1Msg) error {
	v.s.runMbp1Messages++
	ts := int64(r.Header.TsEvent)
	if ts > 0 {
		for {
			cur := atomic.LoadUint64(&v.s.maxEvent)
			if uint64(ts) <= cur || atomic.CompareAndSwapUint64(&v.s.maxEvent, cur, uint64(ts)) {
				break
			}
		}
	}

	if v.historical && v.s.stopAtNs > 0 && ts > v.s.stopAtNs {
		return nil
	}

	sym, ok := v.s.instrumentToSymbol[r.Header.InstrumentID]
	if !ok {
		return nil
	}
	st := v.s.statesBySymbol[sym]
	if st == nil {
		return nil
	}

	if r.Level.BidPx > 0 {
		st.BidPxN = r.Level.BidPx
		st.BidSz = r.Level.BidSz
		v.s.runQuoteEvents++
	}
	if r.Level.AskPx > 0 {
		st.AskPxN = r.Level.AskPx
		st.AskSz = r.Level.AskSz
		v.s.runQuoteEvents++
	}
	st.LastQuoteTs = r.Header.TsEvent

	if r.Action == 'T' && r.Price > 0 && r.Size > 0 {
		v.s.runTradeEvents++
		v.s.onTrade(st, r.Header.TsEvent, r.Price, uint64(r.Size))
	}
	return nil
}

func (s *Scanner) onTrade(st *SymbolState, ts uint64, pxN int64, size uint64) {
	tsTime := time.Unix(0, int64(ts)).In(s.loc)
	s.ensureSessionFor(tsTime)

	st.LastTradeTs = ts
	st.LastPxN = pxN

	if !s.inCountingWindow(int64(ts)) {
		return
	}

	if s.stopAtNs > 0 && int64(ts) > s.stopAtNs {
		return
	}

	minuteNs := minuteBucketNs(int64(ts))
	if st.BarMinuteNs == 0 {
		st.BarMinuteNs = minuteNs
		st.BarHighPxN = pxN
		st.BarLowPxN = pxN
		st.BarClosePxN = pxN
	} else if minuteNs > st.BarMinuteNs {
		s.closeCurrentBar(st)
		st.BarMinuteNs = minuteNs
		st.BarHighPxN = pxN
		st.BarLowPxN = pxN
		st.BarClosePxN = pxN
	} else {
		if pxN > st.BarHighPxN {
			st.BarHighPxN = pxN
		}
		if st.BarLowPxN == 0 || pxN < st.BarLowPxN {
			st.BarLowPxN = pxN
		}
		st.BarClosePxN = pxN
	}

	if st.OpenPxN == 0 {
		st.OpenPxN = pxN
		st.HighPxN = pxN
		st.LowPxN = pxN
		st.FirstTradeTs = ts
	} else {
		if pxN > st.HighPxN {
			st.HighPxN = pxN
		}
		if st.LowPxN == 0 || pxN < st.LowPxN {
			st.LowPxN = pxN
		}
	}

	st.CumVol += size
	st.CumNotional += float64(size) * (float64(pxN) / pxScale)
}

func minuteBucketNs(tsNs int64) int64 {
	return (tsNs / int64(time.Minute)) * int64(time.Minute)
}

func (s *Scanner) closeCurrentBar(st *SymbolState) {
	if st.BarMinuteNs == 0 || st.BarClosePxN <= 0 || st.BarHighPxN <= 0 || st.BarLowPxN <= 0 {
		return
	}
	vwap := 0.0
	if st.CumVol > 0 {
		vwap = st.CumNotional / float64(st.CumVol)
	}
	st.Bars = append(st.Bars, intradayBar{
		MinuteStartNs: st.BarMinuteNs,
		HighPxN:       st.BarHighPxN,
		LowPxN:        st.BarLowPxN,
		ClosePxN:      st.BarClosePxN,
		VWAP:          vwap,
	})
	if len(st.Bars) > maxIntradayBars {
		st.Bars = st.Bars[len(st.Bars)-maxIntradayBars:]
	}
}

func (s *Scanner) ensureSessionFor(t time.Time) {
	if s.loc == nil {
		return
	}
	ny := t.In(s.loc)
	date := ny.Format("2006-01-02")
	if s.sessionDate == date && s.startNs != 0 && s.endNs != 0 {
		return
	}

	s.sessionDate = date
	y, m, d := ny.Date()

	openStr := s.cfg.Market.SessionOpen
	closeStr := s.cfg.Market.SessionClose
	if s.cfg.Market.IncludeExtendedHours {
		openStr = s.cfg.Market.ExtendedOpen
		closeStr = s.cfg.Market.ExtendedClose
	}
	oh, om := parseHHMM(openStr, 9, 30)
	ch, cm := parseHHMM(closeStr, 16, 0)

	start := time.Date(y, m, d, oh, om, 0, 0, s.loc)
	end := time.Date(y, m, d, ch, cm, 0, 0, s.loc)
	if end.Before(start) {
		end = end.Add(24 * time.Hour)
	}

	s.startNs = start.UTC().UnixNano()
	s.endNs = end.UTC().UnixNano()

	for _, st := range s.statesBySymbol {
		st.OpenPxN = 0
		st.HighPxN = 0
		st.LowPxN = 0
		st.LastPxN = 0
		st.CumVol = 0
		st.CumNotional = 0
		st.FirstTradeTs = 0
		st.LastTradeTs = 0
		st.BarMinuteNs = 0
		st.BarHighPxN = 0
		st.BarLowPxN = 0
		st.BarClosePxN = 0
		st.Bars = st.Bars[:0]
	}
}

func sessionOpenFor(asOfNY time.Time, cfg Config, loc *time.Location) time.Time {
	asOfNY = asOfNY.In(loc)
	y, m, d := asOfNY.Date()
	openStr := cfg.Market.SessionOpen
	if cfg.Market.IncludeExtendedHours {
		openStr = cfg.Market.ExtendedOpen
	}
	oh, om := parseHHMM(openStr, 9, 30)
	return time.Date(y, m, d, oh, om, 0, 0, loc)
}

func liveReplayStartFor(nowNY time.Time, cfg Config, loc *time.Location) time.Time {
	nowNY = nowNY.In(loc)
	sessionOpen := sessionOpenFor(nowNY, cfg, loc)
	if !nowNY.Before(sessionOpen) {
		return sessionOpen
	}
	return time.Time{}
}

func sessionCloseFor(asOfNY time.Time, cfg Config, loc *time.Location) time.Time {
	asOfNY = asOfNY.In(loc)
	y, m, d := asOfNY.Date()
	closeStr := cfg.Market.SessionClose
	if cfg.Market.IncludeExtendedHours {
		closeStr = cfg.Market.ExtendedClose
	}
	ch, cm := parseHHMM(closeStr, 16, 0)
	return time.Date(y, m, d, ch, cm, 0, 0, loc)
}

func parseHHMM(v string, defH, defM int) (int, int) {
	v = strings.TrimSpace(v)
	parts := strings.Split(v, ":")
	if len(parts) != 2 {
		return defH, defM
	}
	h, err1 := strconvAtoi(parts[0])
	m, err2 := strconvAtoi(parts[1])
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return defH, defM
	}
	return h, m
}

func strconvAtoi(s string) (int, error) {
	s = strings.TrimSpace(s)
	n := 0
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("not int")
		}
		n = n*10 + int(ch-'0')
	}
	return n, nil
}

func (s *Scanner) inCountingWindow(ts int64) bool {
	return ts >= s.startNs && ts <= s.endNs
}

type featureRow struct {
	symbol string

	price float64
	bid   float64
	ask   float64

	spreadPct float64
	spreadBps float64

	cumVol          uint64
	rvol            float64
	logRvol         float64
	dollarVolPerMin float64

	avgDollarVol10d float64

	open float64
	high float64
	low  float64

	rangePct     float64
	adrPct10d    float64
	adrExpansion float64

	retFromOpen float64
	relStrength float64
	rs5m        float64
	rs15m       float64
	rs30m       float64

	sessionVWAP      float64
	vwapSlope5m      float64
	isAboveVWAP      bool
	isBelowVWAP      bool
	hasHigherHighLow bool
	hasLowerHighLow  bool
	pullbackLong     float64
	failedReclaim    float64
	downsideExtend   float64

	updatedMsAgo int64
}

func (s *Scanner) baselineFor(sym string) Baseline {
	b, ok := s.baselines[sym]
	if !ok {
		return Baseline{
			AvgVol10d:       s.cfg.Baselines.FallbackAvgVol10d,
			AdrPct10d:       s.cfg.Baselines.FallbackAdrPct10d,
			AvgDollarVol10d: 0,
		}
	}
	if b.AvgVol10d <= 0 {
		b.AvgVol10d = s.cfg.Baselines.FallbackAvgVol10d
	}
	if b.AdrPct10d <= 0 {
		b.AdrPct10d = s.cfg.Baselines.FallbackAdrPct10d
	}
	return b
}

func (s *Scanner) maxSpreadPctFor(price float64) float64 {
	return s.maxSpreadPctForScan(price, s.cfg.Scan)
}

func (s *Scanner) maxSpreadPctForScan(price float64, scan ScanConfig) float64 {
	for _, c := range scan.SpreadCaps {
		if price >= c.MinPrice {
			return c.MaxSpreadPct
		}
	}
	return 0.002
}

func spreadBpsForMinPrice(scan ScanConfig, minPrice float64) float64 {
	for _, c := range scan.SpreadCaps {
		if c.MinPrice == minPrice {
			return c.MaxSpreadPct * 10000.0
		}
	}
	return 0
}

func liveEvalNY(nowNY time.Time, maxEventNs int64, loc *time.Location) time.Time {
	nowNY = nowNY.In(loc)
	if maxEventNs <= 0 {
		return nowNY
	}
	eventNY := time.Unix(0, maxEventNs).In(loc)
	if eventNY.After(nowNY) {
		return nowNY
	}
	return eventNY
}

func priceAtOrBefore(st *SymbolState, targetNs int64, nowNs int64) (float64, bool) {
	if st == nil || st.LastPxN <= 0 {
		return 0, false
	}
	if targetNs >= nowNs {
		return float64(st.LastPxN) / pxScale, true
	}
	for i := len(st.Bars) - 1; i >= 0; i-- {
		barCloseNs := st.Bars[i].MinuteStartNs + int64(time.Minute)
		if barCloseNs <= targetNs && st.Bars[i].ClosePxN > 0 {
			return float64(st.Bars[i].ClosePxN) / pxScale, true
		}
	}
	if st.OpenPxN > 0 {
		return float64(st.OpenPxN) / pxScale, true
	}
	return 0, false
}

func vwapAtOrBefore(st *SymbolState, targetNs int64) (float64, bool) {
	if st == nil {
		return 0, false
	}
	for i := len(st.Bars) - 1; i >= 0; i-- {
		barCloseNs := st.Bars[i].MinuteStartNs + int64(time.Minute)
		if barCloseNs <= targetNs && st.Bars[i].VWAP > 0 {
			return st.Bars[i].VWAP, true
		}
	}
	return 0, false
}

func (s *Scanner) relStrengthWindow(stock *SymbolState, benchmark *SymbolState, nowNs int64, lookback time.Duration) (float64, bool) {
	if stock == nil || benchmark == nil || stock.LastPxN <= 0 || benchmark.LastPxN <= 0 {
		return 0, false
	}
	targetNs := nowNs - int64(lookback)
	stockThen, okStock := priceAtOrBefore(stock, targetNs, nowNs)
	benchThen, okBench := priceAtOrBefore(benchmark, targetNs, nowNs)
	if !okStock || !okBench || stockThen <= 0 || benchThen <= 0 {
		return 0, false
	}
	stockNow := float64(stock.LastPxN) / pxScale
	benchNow := float64(benchmark.LastPxN) / pxScale
	stockRet := (stockNow - stockThen) / stockThen
	benchRet := (benchNow - benchThen) / benchThen
	return stockRet - benchRet, true
}

func (s *Scanner) trendState(st *SymbolState, price float64, nowNs int64) (sessionVWAP float64, vwapSlope5m float64, isAboveVWAP bool, isBelowVWAP bool, hasHigherHighLow bool, hasLowerHighLow bool) {
	if st == nil {
		return
	}
	if st.CumVol > 0 {
		sessionVWAP = st.CumNotional / float64(st.CumVol)
	}
	if sessionVWAP > 0 {
		isAboveVWAP = price > sessionVWAP
		isBelowVWAP = price < sessionVWAP
		if pastVWAP, ok := vwapAtOrBefore(st, nowNs-int64(vwapSlopeLookback)); ok {
			vwapSlope5m = sessionVWAP - pastVWAP
		} else if pastVWAP, ok := vwapAtOrBefore(st, nowNs-int64(2*time.Minute)); ok {
			vwapSlope5m = sessionVWAP - pastVWAP
		}
	}
	if st.BarHighPxN <= 0 || st.BarLowPxN <= 0 || len(st.Bars) < 2 {
		return
	}
	n := len(st.Bars)
	h0 := st.BarHighPxN
	h1 := st.Bars[n-1].HighPxN
	h2 := st.Bars[n-2].HighPxN
	l0 := st.BarLowPxN
	l1 := st.Bars[n-1].LowPxN
	l2 := st.Bars[n-2].LowPxN
	hasHigherHighLow = h0 > h1 && h1 > h2 && l0 > l1 && l1 > l2
	hasLowerHighLow = h0 < h1 && h1 < h2 && l0 < l1 && l1 < l2
	return
}

func directionalContext(st *SymbolState, price float64, sessionVWAP float64) (pullbackLong float64, failedReclaim float64, downsideExtend float64) {
	if st == nil || sessionVWAP <= 0 || price <= 0 {
		return 0, 0, 0
	}

	distFromVWAP := (price - sessionVWAP) / sessionVWAP
	aboveVWAP := 0.0
	belowVWAP := 0.0
	if distFromVWAP > 0 {
		aboveVWAP = 1.0
	}
	if distFromVWAP < 0 {
		belowVWAP = 1.0
	}

	prevHigh := 0.0
	prevLow := 0.0
	if len(st.Bars) > 0 {
		last := st.Bars[len(st.Bars)-1]
		if last.HighPxN > 0 {
			prevHigh = float64(last.HighPxN) / pxScale
		}
		if last.LowPxN > 0 {
			prevLow = float64(last.LowPxN) / pxScale
		}
	}
	curHigh := 0.0
	if st.BarHighPxN > 0 {
		curHigh = float64(st.BarHighPxN) / pxScale
	}

	breakoutHold := 0.0
	if prevHigh > 0 {
		if price >= prevHigh {
			breakoutHold = 1.0
		} else if price >= prevHigh*0.997 {
			breakoutHold = 0.5
		}
	}

	pullbackBand := 0.0
	if distFromVWAP > 0 {
		// Prefer names holding above VWAP without being over-extended.
		pullbackBand = clamp01(1.0 - math.Abs(distFromVWAP-0.006)/0.020)
	}
	pullbackLong = clamp01(0.45*aboveVWAP + 0.35*breakoutHold + 0.20*pullbackBand)

	reclaimFail := 0.0
	if curHigh > 0 {
		if curHigh < sessionVWAP {
			reclaimFail = 1.0
		} else if curHigh < sessionVWAP*1.001 {
			reclaimFail = 0.5
		}
	}
	lowBreak := 0.0
	if prevLow > 0 {
		if price <= prevLow {
			lowBreak = 1.0
		} else if price <= prevLow*1.003 {
			lowBreak = 0.5
		}
	}
	failedReclaim = clamp01(0.55*reclaimFail + 0.25*belowVWAP + 0.20*lowBreak)

	downDist := (sessionVWAP - price) / sessionVWAP
	if downDist > 0 {
		downsideExtend = clamp01(1.0 - math.Abs(downDist-0.008)/0.025)
	}
	return pullbackLong, failedReclaim, downsideExtend
}

func liquidityStabilityScore(dollarVolPerMin float64, avgDollarVol10d float64) float64 {
	if dollarVolPerMin <= 0 || avgDollarVol10d <= 0 {
		return 0
	}
	avgPerMin := avgDollarVol10d / 390.0
	if avgPerMin <= 0 {
		return 0
	}
	ratio := dollarVolPerMin / avgPerMin
	return clamp01(ratio / 2.0)
}

func barsWithCurrent(st *SymbolState) []intradayBar {
	if st == nil {
		return nil
	}
	out := make([]intradayBar, 0, len(st.Bars)+1)
	out = append(out, st.Bars...)
	if st.BarMinuteNs > 0 && st.BarHighPxN > 0 && st.BarLowPxN > 0 && st.BarClosePxN > 0 {
		vwap := 0.0
		if st.CumVol > 0 {
			vwap = st.CumNotional / float64(st.CumVol)
		}
		out = append(out, intradayBar{
			MinuteStartNs: st.BarMinuteNs,
			HighPxN:       st.BarHighPxN,
			LowPxN:        st.BarLowPxN,
			ClosePxN:      st.BarClosePxN,
			VWAP:          vwap,
		})
	}
	return out
}

func emaLastClose(bars []intradayBar, period int) (float64, bool) {
	if period <= 1 || len(bars) < period {
		return 0, false
	}
	start := len(bars) - period
	ema := float64(bars[start].ClosePxN) / pxScale
	if ema <= 0 {
		return 0, false
	}
	alpha := 2.0 / float64(period+1)
	for i := start + 1; i < len(bars); i++ {
		closePx := float64(bars[i].ClosePxN) / pxScale
		if closePx <= 0 {
			return 0, false
		}
		ema = alpha*closePx + (1.0-alpha)*ema
	}
	return ema, true
}

func backsideSetupScore(st *SymbolState, price float64, sessionVWAP float64) (float64, bool) {
	if st == nil || st.LowPxN <= 0 || price <= 0 || sessionVWAP <= 0 {
		return 0, false
	}
	bars := barsWithCurrent(st)
	if len(bars) < 10 {
		return 0, false
	}

	lod := float64(st.LowPxN) / pxScale
	if lod <= 0 || sessionVWAP <= lod || price <= lod {
		return 0, false
	}

	// Starter low bar for backside potential: a 1-minute swing-low style bar.
	starterLowIdx := -1
	for i := 0; i < len(bars)-3; i++ {
		low := bars[i].LowPxN
		if low <= 0 {
			continue
		}
		leftOK := i == 0 || low <= bars[i-1].LowPxN
		rightOK := low <= bars[i+1].LowPxN
		if leftOK && rightOK {
			starterLowIdx = i
		}
	}
	if starterLowIdx < 0 {
		return 0, false
	}
	starterLow := float64(bars[starterLowIdx].LowPxN) / pxScale
	if starterLow <= 0 || sessionVWAP <= starterLow {
		return 0, false
	}
	midpoint := starterLow + 0.5*(sessionVWAP-starterLow)
	if price > midpoint {
		return 0, false
	}

	hhCount := 0
	hlCount := 0
	firstHHIdx := -1
	firstHLIdx := -1
	firstHLAfterHHIdx := -1
	for i := starterLowIdx + 1; i < len(bars); i++ {
		if bars[i].HighPxN > bars[i-1].HighPxN {
			hhCount++
			if firstHHIdx < 0 {
				firstHHIdx = i
			}
		}
		if bars[i].LowPxN > bars[i-1].LowPxN {
			hlCount++
			if firstHLIdx < 0 {
				firstHLIdx = i
			}
			if firstHHIdx >= 0 && i > firstHHIdx && firstHLAfterHHIdx < 0 {
				firstHLAfterHHIdx = i
			}
		}
	}
	if hhCount < 1 || hlCount < 1 {
		return 0, false
	}

	if firstHHIdx <= starterLowIdx || firstHLAfterHHIdx <= firstHHIdx {
		return 0, false
	}
	// Require one bar after HH/HL has formed.
	if firstHLAfterHHIdx+1 >= len(bars) {
		return 0, false
	}
	postIdx := firstHLAfterHHIdx + 1
	if postIdx <= starterLowIdx {
		return 0, false
	}

	ema9, okEMA := emaLastClose(bars, 9)
	if !okEMA {
		return 0, false
	}
	aboveEMA := 0
	if firstHLIdx < 0 || firstHLIdx >= len(bars) {
		return 0, false
	}
	for i := firstHLIdx; i < len(bars); i++ {
		closePx := float64(bars[i].ClosePxN) / pxScale
		if closePx >= ema9 {
			aboveEMA++
		}
	}
	totalSinceClosestHL := len(bars) - firstHLIdx
	if totalSinceClosestHL <= 0 {
		return 0, false
	}
	if aboveEMA*2 < totalSinceClosestHL {
		return 0, false
	}

	steps := float64(len(bars) - (starterLowIdx + 1))
	if steps <= 0 {
		return 0, false
	}
	consistency := clamp01(0.5*(float64(hhCount)/steps) + 0.5*(float64(hlCount)/steps))
	emaSupport := clamp01(float64(aboveEMA) / float64(totalSinceClosestHL))
	score := clamp01(0.70*consistency + 0.30*emaSupport)
	return score, true
}

func slopePctScore(vwapSlope5m float64, sessionVWAP float64, dir int) float64 {
	if sessionVWAP <= 0 {
		return 0
	}
	slopePct := vwapSlope5m / sessionVWAP
	if dir < 0 {
		slopePct = -slopePct
	}
	return clamp01(slopePct / 0.0025)
}

func (s *Scanner) computeSnapshotAt(evalNY time.Time, mode string) TopSnapshot {
	return s.computeSnapshotAtWithScan(evalNY, mode, s.cfg.Scan)
}

func (s *Scanner) computeSnapshotAtWithScan(evalNY time.Time, mode string, scan ScanConfig) TopSnapshot {
	evalNY = evalNY.In(s.loc)
	s.ensureSessionFor(evalNY)

	nowNs := evalNY.UTC().UnixNano()
	elapsedNs := float64(nowNs - s.startNs)
	totalNs := float64(s.endNs - s.startNs)
	if totalNs <= 0 {
		totalNs = 6.5 * float64(time.Hour/time.Nanosecond)
	}
	fraction := elapsedNs / totalNs
	if fraction < 0.01 {
		fraction = 0.01
	}
	if fraction > 1.0 {
		fraction = 1.0
	}

	minutesElapsed := elapsedNs / float64(time.Minute/time.Nanosecond)
	if minutesElapsed < 1 {
		minutesElapsed = 1
	}

	bench := strings.ToUpper(strings.TrimSpace(s.cfg.Databento.BenchmarkSymbol))
	benchState := s.statesBySymbol[bench]
	spyRet := 0.0
	if benchState != nil && benchState.OpenPxN > 0 && benchState.LastPxN > 0 {
		spyOpen := float64(benchState.OpenPxN) / pxScale
		spyLast := float64(benchState.LastPxN) / pxScale
		spyRet = (spyLast - spyOpen) / spyOpen
	}

	rows := make([]featureRow, 0, len(s.statesBySymbol))
	logRvols := make([]float64, 0, len(s.statesBySymbol))
	rs5Vals := make([]float64, 0, len(s.statesBySymbol))
	rs15Vals := make([]float64, 0, len(s.statesBySymbol))
	debug := GateDebug{
		SpreadCapBpsGE50: spreadBpsForMinPrice(scan, 50),
		SpreadCapBpsGE10: spreadBpsForMinPrice(scan, 10),
		SpreadCapBpsGE5:  spreadBpsForMinPrice(scan, 5),
	}

	for _, st := range s.statesBySymbol {
		if st.OpenPxN <= 0 || st.LastPxN <= 0 || st.HighPxN <= 0 || st.LowPxN <= 0 {
			continue
		}

		price := float64(st.LastPxN) / pxScale
		if price < scan.MinPrice || price > scan.MaxPrice {
			continue
		}

		bid := float64(st.BidPxN) / pxScale
		ask := float64(st.AskPxN) / pxScale
		mid := (bid + ask) / 2
		spreadPct := 1.0
		if bid > 0 && ask > 0 && ask > bid && mid > 0 {
			spreadPct = (ask - bid) / mid
		}
		spreadBps := spreadPct * 10000

		lastUpd := int64(st.lastUpdateTs())
		updatedMsAgo := int64(1 << 62)
		if lastUpd > 0 {
			updatedMsAgo = (nowNs - lastUpd) / int64(time.Millisecond)
			if updatedMsAgo < 0 {
				updatedMsAgo = 0
			}
		}

		base := s.baselineFor(st.Symbol)
		expVol := base.AvgVol10d * fraction
		if expVol < 1 {
			expVol = 1
		}

		rvol := float64(st.CumVol) / expVol
		logRvol := math.Log(math.Max(rvol, 1e-6))

		open := float64(st.OpenPxN) / pxScale
		high := float64(st.HighPxN) / pxScale
		low := float64(st.LowPxN) / pxScale

		rangePct := 0.0
		if open > 0 {
			rangePct = (high - low) / open
		}

		adrExpansion := 0.0
		if base.AdrPct10d > 0 {
			adrExpansion = rangePct / base.AdrPct10d
		}

		retFromOpen := 0.0
		if open > 0 {
			retFromOpen = (price - open) / open
		}
		relStrength := retFromOpen - spyRet
		rs5m, okRS5 := s.relStrengthWindow(st, benchState, nowNs, rsWindow5m)
		if !okRS5 {
			rs5m = relStrength
		}
		rs15m, okRS15 := s.relStrengthWindow(st, benchState, nowNs, rsWindow15m)
		if !okRS15 {
			rs15m = relStrength
		}
		rs30m, okRS30 := s.relStrengthWindow(st, benchState, nowNs, rsWindow30m)
		if !okRS30 {
			rs30m = relStrength
		}
		sessionVWAP, vwapSlope5m, isAboveVWAP, isBelowVWAP, hasHigherHighLow, hasLowerHighLow := s.trendState(st, price, nowNs)
		pullbackLong, failedReclaim, downsideExtend := directionalContext(st, price, sessionVWAP)
		dollarVolPerMin := st.CumNotional / minutesElapsed
		avgDollarVol10d := base.AvgDollarVol10d
		if avgDollarVol10d > 0 {
			debug.AvgDollarVolFromCSV++
		} else if base.AvgVol10d > 0 && price > 0 {
			avgDollarVol10d = base.AvgVol10d * price
			debug.AvgDollarVolDerived++
		} else {
			avgDollarVol10d = s.cfg.Baselines.FallbackAvgDollarVol10d
			debug.AvgDollarVolFallbackConst++
		}

		row := featureRow{
			symbol:           st.Symbol,
			price:            price,
			bid:              bid,
			ask:              ask,
			spreadPct:        spreadPct,
			spreadBps:        spreadBps,
			cumVol:           st.CumVol,
			rvol:             rvol,
			logRvol:          logRvol,
			dollarVolPerMin:  dollarVolPerMin,
			avgDollarVol10d:  avgDollarVol10d,
			open:             open,
			high:             high,
			low:              low,
			rangePct:         rangePct,
			adrPct10d:        base.AdrPct10d,
			adrExpansion:     adrExpansion,
			retFromOpen:      retFromOpen,
			relStrength:      relStrength,
			rs5m:             rs5m,
			rs15m:            rs15m,
			rs30m:            rs30m,
			sessionVWAP:      sessionVWAP,
			vwapSlope5m:      vwapSlope5m,
			isAboveVWAP:      isAboveVWAP,
			isBelowVWAP:      isBelowVWAP,
			hasHigherHighLow: hasHigherHighLow,
			hasLowerHighLow:  hasLowerHighLow,
			pullbackLong:     pullbackLong,
			failedReclaim:    failedReclaim,
			downsideExtend:   downsideExtend,
			updatedMsAgo:     updatedMsAgo,
		}

		rows = append(rows, row)
		logRvols = append(logRvols, logRvol)
		rs5Vals = append(rs5Vals, rs5m)
		rs15Vals = append(rs15Vals, rs15m)

		if row.cumVol > debug.MaxCumVol {
			debug.MaxCumVol = row.cumVol
			debug.MaxCumVolSymbol = row.symbol
		}
		if row.spreadBps > debug.MaxSeenSpreadBps {
			debug.MaxSeenSpreadBps = row.spreadBps
			debug.MaxSeenSpreadSymbol = row.symbol
		}
		if row.dollarVolPerMin > debug.MaxDollarVolPerMin {
			debug.MaxDollarVolPerMin = row.dollarVolPerMin
			debug.MaxDollarVolPerMinSymbol = row.symbol
		}
		if row.rvol > debug.MaxRVOL {
			debug.MaxRVOL = row.rvol
			debug.MaxRVOLSymbol = row.symbol
		}
	}

	meanLR, stdLR := meanStd(logRvols)
	meanRS5, stdRS5 := meanStd(rs5Vals)
	meanRS15, stdRS15 := meanStd(rs15Vals)

	strongest := &candHeap{}
	weakest := &candHeap{}
	backside := &candHeap{}
	heap.Init(strongest)
	heap.Init(weakest)
	heap.Init(backside)

	ranked := 0
	strongestRanked := 0
	weakestRanked := 0
	hardPassRanked := 0
	backsideRanked := 0
	hardPassOut := make([]Candidate, 0, 256)
	rvolPool := make([]Candidate, 0, 256)
	for _, row := range rows {
		relStrength := row.relStrength
		debug.RowsEvaluated++

		rvolScore := gaussianPercentile(zscore(row.logRvol, meanLR, stdLR))
		rs5Z := zscore(row.rs5m, meanRS5, stdRS5)
		rs15Z := zscore(row.rs15m, meanRS15, stdRS15)
		rsPos15 := gaussianPercentile(rs15Z)
		rsPos5 := gaussianPercentile(rs5Z)
		rsNeg15 := gaussianPercentile(-rs15Z)
		rsNeg5 := gaussianPercentile(-rs5Z)
		liqFlow := clamp01(row.dollarVolPerMin / scan.Scoring.LiquidityTargetPerMin)
		liqStable := liquidityStabilityScore(row.dollarVolPerMin, row.avgDollarVol10d)
		liquidityScore := clamp01(0.60*liqFlow + 0.40*liqStable)
		spreadScore := clamp01(1.0 - (row.spreadBps / scan.Scoring.SpreadBpsCap))
		longVWAPTrend := clamp01(
			0.45*slopePctScore(row.vwapSlope5m, row.sessionVWAP, +1) +
				0.35*row.pullbackLong +
				0.20*boolScore(row.hasHigherHighLow),
		)
		shortVWAPTrend := clamp01(
			0.45*slopePctScore(row.vwapSlope5m, row.sessionVWAP, -1) +
				0.35*row.failedReclaim +
				0.20*boolScore(row.hasLowerHighLow),
		)
		shortSpreadControlled := clamp01(0.70*spreadScore + 0.30*row.downsideExtend)

		// Two-stage directional formulas.
		strongestScore := 0.30*rsPos15 +
			0.20*rsPos5 +
			0.20*rvolScore +
			0.15*longVWAPTrend +
			0.10*liquidityScore +
			0.05*spreadScore
		weakestScore := 0.30*rsNeg15 +
			0.20*rsNeg5 +
			0.20*rvolScore +
			0.15*shortVWAPTrend +
			0.10*liquidityScore +
			0.05*shortSpreadControlled

		cand := Candidate{
			Symbol:           row.symbol,
			Price:            row.price,
			Bid:              row.bid,
			Ask:              row.ask,
			SpreadPct:        row.spreadPct,
			SpreadBps:        row.spreadBps,
			CumVol:           row.cumVol,
			RVOL:             row.rvol,
			DollarVolPerMin:  row.dollarVolPerMin,
			AvgDollarVol10d:  row.avgDollarVol10d,
			RangePct:         row.rangePct,
			AdrPct10d:        row.adrPct10d,
			AdrExpansion:     row.adrExpansion,
			RetFromOpen:      row.retFromOpen,
			SpyRetFromOpen:   spyRet,
			RelStrengthVsSpy: relStrength,
			RS5mVsSpy:        row.rs5m,
			RS15mVsSpy:       row.rs15m,
			RS30mVsSpy:       row.rs30m,
			SessionVWAP:      row.sessionVWAP,
			VWAPSlope5m:      row.vwapSlope5m,
			UpdatedMsAgo:     row.updatedMsAgo,
		}
		if passesRVOLTabGates(row, scan) {
			c := cand
			c.Score = math.Max(strongestScore, weakestScore)
			rvolPool = append(rvolPool, c)
		}

		if row.updatedMsAgo > scan.MaxStaleness.Duration.Milliseconds() {
			debug.FailStale++
			continue
		}
		if row.avgDollarVol10d < scan.MinAvgDollarVol10d {
			debug.FailMinAvgDollarVol10d++
			continue
		}
		if row.rvol < scan.MinRVOL {
			debug.FailMinRVOL++
			continue
		}
		if row.dollarVolPerMin < scan.MinDollarVolPerM {
			debug.FailMinDollarVolPerMin++
			continue
		}
		if row.adrExpansion < scan.MinAdrExpansion {
			debug.FailMinAdrExpansion++
			continue
		}
		if row.spreadPct > s.maxSpreadPctForScan(row.price, scan) {
			debug.FailSpreadCap++
			continue
		}
		// Additional spread stability check: reject names that exceed scoring spread budget.
		if row.spreadBps > scan.Scoring.SpreadBpsCap {
			debug.FailSpreadCap++
			continue
		}
		debug.PassedAllGates++
		cHardPass := cand
		cHardPass.Score = math.Max(strongestScore, weakestScore)
		hardPassOut = append(hardPassOut, cHardPass)
		hardPassRanked++

		st := s.statesBySymbol[row.symbol]
		if backsideScore, ok := backsideSetupScore(st, row.price, row.sessionVWAP); ok {
			c := cand
			c.Score = backsideScore
			pushTopK(backside, c, scan.TopK)
			backsideRanked++
		}

		if relStrength > 0 {
			if !row.isAboveVWAP || row.vwapSlope5m <= 0 || !row.hasHigherHighLow {
				debug.FailStrongestTrend++
				continue
			}
			if row.rs5m <= 0 || row.rs15m <= 0 || row.rs30m <= 0 {
				debug.FailStrongestRSPersist++
				continue
			}
			c := cand
			c.Score = strongestScore
			pushTopK(strongest, c, scan.TopK)
			strongestRanked++
			ranked++
		}
		if relStrength < 0 {
			if !row.isBelowVWAP || row.vwapSlope5m >= 0 || !row.hasLowerHighLow {
				debug.FailWeakestTrend++
				continue
			}
			if row.rs5m >= 0 || row.rs15m >= 0 || row.rs30m >= 0 {
				debug.FailWeakestRSPersist++
				continue
			}
			c := cand
			c.Score = weakestScore
			pushTopK(weakest, c, scan.TopK)
			weakestRanked++
			ranked++
		}
	}

	strongestOut := drainRanked(strongest)
	weakestOut := drainRanked(weakest)
	backsideOut := drainRanked(backside)
	for i := range strongestOut {
		strongestOut[i].Rank = i + 1
		if strongestOut[i].SpreadPct > s.maxSpreadPctForScan(strongestOut[i].Price, scan) {
			debug.CandidateSpreadViolations++
		}
	}
	for i := range weakestOut {
		weakestOut[i].Rank = i + 1
		if weakestOut[i].SpreadPct > s.maxSpreadPctForScan(weakestOut[i].Price, scan) {
			debug.CandidateSpreadViolations++
		}
	}
	for i := range backsideOut {
		backsideOut[i].Rank = i + 1
		if backsideOut[i].SpreadPct > s.maxSpreadPctForScan(backsideOut[i].Price, scan) {
			debug.CandidateSpreadViolations++
		}
	}
	sort.Slice(hardPassOut, func(i, j int) bool { return hardPassOut[i].Score > hardPassOut[j].Score })
	for i := range hardPassOut {
		hardPassOut[i].Rank = i + 1
	}
	rvolOut := topRVOLCandidates(rvolPool, rvolTopK)

	message := ""
	if len(rows) == 0 {
		message = "No symbols had tradable intraday data in the selected replay window. This is expected outside market hours, weekends, or exchange holidays."
	}

	return TopSnapshot{
		Mode:                mode,
		Timezone:            s.cfg.Market.Timezone,
		GeneratedAt:         evalNY,
		GeneratedAtNY:       evalNY.Format("2006-01-02 15:04:05 MST"),
		AsOfNY:              evalNY.Format("2006-01-02 15:04:05 MST"),
		Benchmark:           bench,
		CountSeen:           len(rows),
		CountRanked:         ranked,
		CountStrongest:      strongestRanked,
		CountWeakest:        weakestRanked,
		CountHardPass:       hardPassRanked,
		CountBackside:       backsideRanked,
		CountRVOL:           len(rvolOut),
		Message:             message,
		GateDebug:           debug,
		Candidates:          strongestOut,
		StrongestCandidates: strongestOut,
		WeakestCandidates:   weakestOut,
		HardPassCandidates:  hardPassOut,
		BacksideCandidates:  backsideOut,
		RVOLCandidates:      rvolOut,
	}
}

func meanStd(x []float64) (float64, float64) {
	if len(x) == 0 {
		return 0, 1
	}
	var sum float64
	for _, v := range x {
		sum += v
	}
	mean := sum / float64(len(x))
	var ss float64
	for _, v := range x {
		d := v - mean
		ss += d * d
	}
	std := math.Sqrt(ss / float64(len(x)))
	if std == 0 {
		std = 1
	}
	return mean, std
}

func zscore(x, mean, std float64) float64 {
	if std == 0 {
		return 0
	}
	return (x - mean) / std
}

func gaussianPercentile(z float64) float64 {
	return 0.5 * (1.0 + math.Erf(z/math.Sqrt2))
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

func boolScore(ok bool) float64 {
	if ok {
		return 1.0
	}
	return 0.0
}

type candHeap []Candidate

func (h candHeap) Len() int            { return len(h) }
func (h candHeap) Less(i, j int) bool  { return h[i].Score < h[j].Score }
func (h candHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *candHeap) Push(x interface{}) { *h = append(*h, x.(Candidate)) }
func (h *candHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func cloneCandidates(in []Candidate) []Candidate {
	if len(in) == 0 {
		return []Candidate{}
	}
	out := make([]Candidate, len(in))
	copy(out, in)
	return out
}

func topRVOLCandidates(in []Candidate, k int) []Candidate {
	if len(in) == 0 || k <= 0 {
		return []Candidate{}
	}
	out := cloneCandidates(in)
	sort.Slice(out, func(i, j int) bool {
		if out[i].RVOL == out[j].RVOL {
			if out[i].DollarVolPerMin == out[j].DollarVolPerMin {
				return out[i].Symbol < out[j].Symbol
			}
			return out[i].DollarVolPerMin > out[j].DollarVolPerMin
		}
		return out[i].RVOL > out[j].RVOL
	})
	if len(out) > k {
		out = out[:k]
	}
	for i := range out {
		out[i].Rank = i + 1
	}
	return out
}

func passesRVOLTabGates(row featureRow, scan ScanConfig) bool {
	// RVOL tab intentionally bypasses most hard gates and keeps only dollar-volume constraints.
	return row.avgDollarVol10d >= scan.MinAvgDollarVol10d &&
		row.dollarVolPerMin >= scan.MinDollarVolPerM
}

func pushTopK(h *candHeap, c Candidate, k int) {
	if k <= 0 {
		return
	}
	if h.Len() < k {
		heap.Push(h, c)
		return
	}
	if (*h)[0].Score >= c.Score {
		return
	}
	heap.Pop(h)
	heap.Push(h, c)
}

func drainRanked(h *candHeap) []Candidate {
	out := make([]Candidate, 0, h.Len())
	for h.Len() > 0 {
		out = append(out, heap.Pop(h).(Candidate))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}

func isExpectedScannerStopError(err error, historical bool) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if historical && strings.Contains(strings.ToLower(err.Error()), "use of closed network connection") {
		return true
	}
	return false
}

func (v *dbVisitor) OnMbp0(*dbn.Mbp0Msg) error                      { return nil }
func (v *dbVisitor) OnMbp10(*dbn.Mbp10Msg) error                    { return nil }
func (v *dbVisitor) OnMbo(*dbn.MboMsg) error                        { return nil }
func (v *dbVisitor) OnOhlcv(*dbn.OhlcvMsg) error                    { return nil }
func (v *dbVisitor) OnCmbp1(*dbn.Cmbp1Msg) error                    { return nil }
func (v *dbVisitor) OnBbo(*dbn.BboMsg) error                        { return nil }
func (v *dbVisitor) OnImbalance(*dbn.ImbalanceMsg) error            { return nil }
func (v *dbVisitor) OnStatMsg(*dbn.StatMsg) error                   { return nil }
func (v *dbVisitor) OnStatusMsg(*dbn.StatusMsg) error               { return nil }
func (v *dbVisitor) OnInstrumentDefMsg(*dbn.InstrumentDefMsg) error { return nil }
func (v *dbVisitor) OnErrorMsg(*dbn.ErrorMsg) error                 { return nil }
func (v *dbVisitor) OnSystemMsg(*dbn.SystemMsg) error               { return nil }
func (v *dbVisitor) OnStreamEnd() error                             { return nil }
