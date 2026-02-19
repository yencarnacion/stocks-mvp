# Databento Stock Ideas Scanner (Go)

A Go service + web UI that ranks day-trading stock ideas using:
- Relative volume (RVOL)
- Relative strength vs benchmark (default `QQQ`)
- Spread quality
- Liquidity ($/min and 10d average dollar volume)
- ADR expansion

All date/time handling is **America/New_York**.

## Run

```bash
cp config.yaml.example config.yaml
cp watchlist.yaml.example watchlist.yaml
cp baselines.csv.example baselines.csv

# .env already supported (DATABENTO_API_KEY=...)
# optional verbose logs
export LOG_LEVEL=debug
go run ./cmd/scanner -config config.yaml -watchlist watchlist.yaml
```

Open: `http://localhost:8099`

You can change hard gates directly in the UI (min RVOL, liquidity, spread caps, etc.) and rerun instantly.
Each response now includes `gate_debug` counters so you can see why symbols were filtered out.
For best `10d $vol`, include `avg_dollar_vol_10d` in `baselines.csv`; if missing, the app derives it from `avg_vol_10d * current price`.
In `config.yaml`, `spread_caps[].max_spread_pct` is a decimal percent (for example 5 bps = `0.0005`, 10 bps = `0.0010`, 15 bps = `0.0015`).
Ticker symbols in the table are clickable and open in a new tab using `server.ticker_url_template` in `config.yaml`.
Supported placeholders in the template: `{symbol}`, `{date}`, `{time}`, `{as_of}`.

## Live vs historical

- `Live` mode uses current feed and keeps intraday snapshots in memory.
- `Historical Replay` mode runs a one-off replay to return the ranked snapshot as-of the requested NY date/time.

API examples:

```bash
# latest live
curl 'http://localhost:8099/api/top?mode=live'

# what you would have seen at 09:30 NY today
curl 'http://localhost:8099/api/top?mode=historical&date=2026-02-19&time=09:30'

# what you would have seen at 09:45 NY today
curl 'http://localhost:8099/api/top?mode=historical&date=2026-02-19&time=09:45'
```

## Daily markdown report

Generate a full-day historical report grouped by tab (`Strongest`, `Weakest`, `Backside`) and by time:

```bash
go run ./cmd/report -date 20260218 -output output.md
```

- `-date` is required in `YYYYMMDD`.
- `-output` defaults to `output.md`.
- Optional flags: `-config`, `-watchlist`, `-baselines`, `-dotenv`, `-timeout`.

The report includes clickable ticker links that call `/api/open-chart` with `ticker`, `date`, `time`, and tab-aligned `signal` (`buy`/`sell`).
