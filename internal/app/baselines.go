package app

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type Baseline struct {
	AvgVol10d       float64 // shares/day
	AdrPct10d       float64 // decimal, e.g. 0.035
	AvgDollarVol10d float64 // dollars/day
}

func LoadBaselinesCSV(path string) (map[string]Baseline, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		return nil, err
	}

	col := map[string]int{}
	for i, h := range header {
		col[strings.ToLower(strings.TrimSpace(h))] = i
	}

	if _, ok := col["symbol"]; !ok {
		return nil, fmt.Errorf("baselines.csv missing required column: symbol")
	}
	_, hasAvg10 := col["avg_vol_10d"]
	_, hasAvg20 := col["avg_vol_20d"]
	if !hasAvg10 && !hasAvg20 {
		return nil, fmt.Errorf("baselines.csv missing required column: avg_vol_10d (or legacy avg_vol_20d)")
	}
	_, hasAdr10 := col["adr_pct_10d"]
	_, hasAdr20 := col["adr_pct_20d"]
	if !hasAdr10 && !hasAdr20 {
		return nil, fmt.Errorf("baselines.csv missing required column: adr_pct_10d (or legacy adr_pct_20d)")
	}

	out := make(map[string]Baseline, 8192)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		sym := strings.ToUpper(strings.TrimSpace(rec[col["symbol"]]))
		if sym == "" {
			continue
		}

		avgVol := 0.0
		if i, ok := col["avg_vol_10d"]; ok && i < len(rec) {
			avgVol, _ = strconv.ParseFloat(strings.TrimSpace(rec[i]), 64)
		}
		if avgVol <= 0 {
			if i, ok := col["avg_vol_20d"]; ok && i < len(rec) {
				avgVol, _ = strconv.ParseFloat(strings.TrimSpace(rec[i]), 64)
			}
		}
		adrPct := 0.0
		if i, ok := col["adr_pct_10d"]; ok && i < len(rec) {
			adrPct, _ = strconv.ParseFloat(strings.TrimSpace(rec[i]), 64)
		}
		if adrPct <= 0 {
			if i, ok := col["adr_pct_20d"]; ok && i < len(rec) {
				adrPct, _ = strconv.ParseFloat(strings.TrimSpace(rec[i]), 64)
			}
		}

		avgDollarVol := 0.0
		if i, ok := col["avg_dollar_vol_10d"]; ok && i < len(rec) {
			avgDollarVol, _ = strconv.ParseFloat(strings.TrimSpace(rec[i]), 64)
		}
		if avgDollarVol <= 0 {
			if i, ok := col["avg_dollar_vol_20d"]; ok && i < len(rec) {
				avgDollarVol, _ = strconv.ParseFloat(strings.TrimSpace(rec[i]), 64)
			}
		}

		out[sym] = Baseline{AvgVol10d: avgVol, AdrPct10d: adrPct, AvgDollarVol10d: avgDollarVol}
	}

	return out, nil
}
