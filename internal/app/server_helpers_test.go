package app

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestParseAsOfFromRequestSupportsFormats(t *testing.T) {
	loc := mustLoadTestLoc(t)

	r := httptest.NewRequest("GET", "/api/top?as_of=2026-02-19T15:05:00Z", nil)
	got, has, err := parseAsOfFromRequest(r, loc)
	if err != nil {
		t.Fatalf("parse as_of RFC3339: %v", err)
	}
	if !has {
		t.Fatalf("expected hasAsOf=true")
	}
	expected := time.Date(2026, 2, 19, 10, 5, 0, 0, loc)
	if !got.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, got)
	}

	r = httptest.NewRequest("GET", "/api/top?as_of=2026-02-19T09:35", nil)
	got, has, err = parseAsOfFromRequest(r, loc)
	if err != nil {
		t.Fatalf("parse as_of local layout: %v", err)
	}
	expected = time.Date(2026, 2, 19, 9, 35, 0, 0, loc)
	if !has || !got.Equal(expected) {
		t.Fatalf("expected %v with has=true, got %v has=%v", expected, got, has)
	}

	r = httptest.NewRequest("GET", "/api/top?date=2026-02-19&time=11:15", nil)
	got, has, err = parseAsOfFromRequest(r, loc)
	if err != nil {
		t.Fatalf("parse date/time: %v", err)
	}
	expected = time.Date(2026, 2, 19, 11, 15, 0, 0, loc)
	if !has || !got.Equal(expected) {
		t.Fatalf("expected %v with has=true, got %v has=%v", expected, got, has)
	}

	r = httptest.NewRequest("GET", "/api/top", nil)
	_, has, err = parseAsOfFromRequest(r, loc)
	if err != nil {
		t.Fatalf("expected no error without as_of/date/time, got %v", err)
	}
	if has {
		t.Fatalf("expected hasAsOf=false without query params")
	}
}

func TestParseAsOfFromRequestInvalidFormat(t *testing.T) {
	loc := mustLoadTestLoc(t)
	r := httptest.NewRequest("GET", "/api/top?as_of=not-a-time", nil)

	_, has, err := parseAsOfFromRequest(r, loc)
	if err == nil {
		t.Fatalf("expected invalid as_of error")
	}
	if !has {
		t.Fatalf("expected hasAsOf=true when as_of is supplied")
	}
}

func TestParseScanOverridesAppliesValuesAndKeepsBaseUnchanged(t *testing.T) {
	base := defaultConfig().Scan
	baseSpread10 := spreadBpsForMinPrice(base, 10)

	r := httptest.NewRequest("GET", "/api/top?top_k=40&min_price=7&max_price=250&min_rvol=2.5&min_dollar_vol_per_min=123000&min_adr_expansion=1.5&min_avg_dollar_vol_20d=30000000&max_staleness_ms=15000&spread_cap_bps_10=12.5", nil)
	scan, any, err := parseScanOverrides(r, base)
	if err != nil {
		t.Fatalf("parse scan overrides: %v", err)
	}
	if !any {
		t.Fatalf("expected hasOverrides=true")
	}

	if scan.TopK != 40 || scan.MinPrice != 7 || scan.MaxPrice != 250 {
		t.Fatalf("unexpected numeric gate overrides: %+v", scan)
	}
	if scan.MinRVOL != 2.5 || scan.MinDollarVolPerM != 123000 || scan.MinAdrExpansion != 1.5 {
		t.Fatalf("unexpected float gate overrides: %+v", scan)
	}
	if scan.MinAvgDollarVol10d != 30000000 {
		t.Fatalf("expected legacy min_avg_dollar_vol_20d alias to set 10d value, got %v", scan.MinAvgDollarVol10d)
	}
	if scan.MaxStaleness.Duration != 15*time.Second {
		t.Fatalf("expected max_staleness_ms override to produce 15s, got %v", scan.MaxStaleness.Duration)
	}
	if got := spreadBpsForMinPrice(scan, 10); got != 12.5 {
		t.Fatalf("expected spread_cap_bps_10 override=12.5, got %v", got)
	}
	if got := spreadBpsForMinPrice(base, 10); got != baseSpread10 {
		t.Fatalf("expected base spread caps unchanged, got %v want %v", got, baseSpread10)
	}
}

func TestParseScanOverridesValidationErrors(t *testing.T) {
	base := defaultConfig().Scan

	r := httptest.NewRequest("GET", "/api/top?top_k=abc", nil)
	if _, _, err := parseScanOverrides(r, base); err == nil {
		t.Fatalf("expected integer parse error for top_k")
	}

	r = httptest.NewRequest("GET", "/api/top?min_price=10&max_price=5", nil)
	if _, _, err := parseScanOverrides(r, base); err == nil {
		t.Fatalf("expected invalid price gate error")
	}

	r = httptest.NewRequest("GET", "/api/top?spread_cap_bps_5=-1", nil)
	if _, _, err := parseScanOverrides(r, base); err == nil {
		t.Fatalf("expected negative spread cap validation error")
	}
}

func TestApplySpreadBpsOverrideAddsNewTierAndSorts(t *testing.T) {
	scan := ScanConfig{
		SpreadCaps: []SpreadCap{
			{MinPrice: 10, MaxSpreadPct: 0.001},
		},
	}
	any := false
	q := url.Values{}
	q.Set("spread_cap_bps_50", "7.5")

	if err := applySpreadBpsOverride(q, "spread_cap_bps_50", 50, &scan, &any); err != nil {
		t.Fatalf("apply spread override: %v", err)
	}
	if !any {
		t.Fatalf("expected any=true after override")
	}
	if len(scan.SpreadCaps) != 2 {
		t.Fatalf("expected new spread tier to be appended, got %d tiers", len(scan.SpreadCaps))
	}
	if scan.SpreadCaps[0].MinPrice != 50 || scan.SpreadCaps[1].MinPrice != 10 {
		t.Fatalf("expected spread tiers sorted desc by min price, got %+v", scan.SpreadCaps)
	}
	if got := spreadBpsForMinPrice(scan, 50); got != 7.5 {
		t.Fatalf("expected spread cap at 50 to be 7.5 bps, got %v", got)
	}
}

func TestScanSignatureFormat(t *testing.T) {
	scan := defaultConfig().Scan
	got := scanSignature(scan)
	want := "topk=30|min=5.0000|max=300.0000|rvol=2.0000|dpm=150000.00|adrx=1.2000|adv10=20000000.00|stale_ms=60000|s50_bps=5.00|s10_bps=10.00|s5_bps=15.00"
	if got != want {
		t.Fatalf("unexpected scan signature:\n got: %s\nwant: %s", got, want)
	}
}
