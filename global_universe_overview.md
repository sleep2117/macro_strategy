# Global Universe & Portfolio Overview

## Project Purpose
- Maintain a global investment universe with price history, valuation snapshots, and FX conversions for indices, sectors, themes, and factors.
- Provide tooling to compute local versus USD returns plus risk metrics for reporting.
- Feed a Streamlit portfolio dashboard that tracks performance, holdings, and transactions.

## `global_universe/` Module Outline
- `world_indices.py` - defines `investment_universe`, downloads Yahoo Finance history and valuations, sanitises CSVs, and manages optional KRX backfills.
- `world_returns.py` - reads cached price data to calculate period returns, Sharpe/Sortino ratios, and exports reports under `data/reports/`.
- `fx_rates.py` - fetches Yahoo chart data via `curl` to build USD and KRW conversion factors per currency in `data/fx/`.
- `krx_data.py` - wraps `pykrx` to mirror KRX index OHLCV and fundamentals into the same storage layout used for Yahoo symbols.
- `oecd_cli.py` - retrieves OECD Composite Leading Indicator series, classifies phases, estimates diffusion indices, and renders plots in `figs/` (uses local NanumGothic fonts).
- `cleanup_valuations.py` - drops weekend duplicates or non-matching valuation rows and logs to `data/cleanup_valuations_summary.csv`.
- `audit_yahoo_tickers.py` - audits symbol existence and currency alignment, writing `audit_results.csv` and `audit_summary.txt`.
- `fix_universe_currencies.py` - applies primary currency fixes in `world_indices.py` based on audit output.
- `extract_tickers.py`, `test.py`, `__init__.py` - helper scripts and package convenience exports.

### Cached Data Layout (`global_universe/data/`)
- `daily/` - per-symbol OHLCV CSV files (Date, Open, High, Low, Close, Adj Close, Volume).
- `valuations/` - valuation snapshots (trailingPE, priceToBook, dividend yield, etc.).
- `fx/` - currency conversion tables (USD per local currency, plus KRW conversion).
- `reports/` - return tables produced by `world_returns.py`.
- Root CSVs track housekeeping summaries: `update_summary.csv`, `valuations_update_summary.csv`, `symbols_catalog.csv`, `currency_mismatches.csv`, `cleanup_valuations_summary.csv`.
- `figs/` - CLI diffusion and phase charts.
- `font/` - NanumGothic font assets for matplotlib output.

## Global Universe Data Workflow
1. **Universe definition** - maintain sector/theme mappings and preferred symbols in `investment_universe`.
2. **Price updates** - run `python -m global_universe.world_indices` with environment controls (`PRICE_SCOPE`, `PRICE_MODE`, `SANITIZE_DAILY`, `INCLUDE_KRX`, etc.) to append or backfill prices.
3. **Valuation snapshots** - the same entry point updates ETF/index valuation CSVs unless `SKIP_VALUATIONS=1`.
4. **KRX coverage** - `update_krx_indices()` integrates domestic indices via `pykrx`.
5. **FX cache** - `python -m global_universe.fx_rates --range 10y` refreshes USD/KRW conversion curves for all currencies in the universe.
6. **Audits and hygiene** - `audit_yahoo_tickers.py` validates symbol metadata, `cleanup_valuations.py` tidies valuation CSVs, and `fix_universe_currencies.py` reconciles currency mismatches.
7. **Return analytics** - `python -m global_universe.world_returns --period ytd` (or `1y`, `custom`, etc.) generates local and USD return tables plus Sharpe/Sortino metrics ready for reporting.
8. **Macro overlay** - `python -m global_universe.oecd_cli` updates OECD CLI tables and figures for cycle-aware commentary.

## Supporting References
- `world_indices_project.md` - legacy Korean summary of goals (kept for history).
- Run logs under `data/` provide quick diagnostics on which symbols updated or failed.
- `symbols_catalog.csv` - full inventory with region and sector metadata.

## `Portfolio/` Dashboard Overview
- Streamlit application that consumes the curated CSV data to visualise monthly portfolio performance, holdings, and transactions.
- Designed to operate on private CSV exports while maintaining a historical database for trend analysis.

### Directory Snapshot
- `data/` - fresh broker exports (`account_master.csv`, `daily_performance.csv`, `holdings_snapshot.csv`, `transaction_log.csv`).
- `database/` - consolidated history (`historical_performance.csv`, `historical_holdings.csv`, `historical_transactions.csv`, `account_master.csv`, `metadata.json`) plus dated backups under `backups/YYYY-MM-DD/`.
- `2025년/` - archived monthly PDF summaries (file names remain in Korean).
- `app.py` - Streamlit entry point with four pages (Overview, Performance Analysis, Holdings, Transactions).
- `data_loader.py` - normalises CSVs (handles BOM removal, dtype casting) and exposes helpers such as `get_performance_summary()`, `get_latest_holdings()`, `get_transaction_summary()`.
- `portfolio_analyzer.py` - `PortfolioAnalyzer` class calculating total and annualised returns, volatility, Sharpe, Sortino, drawdowns, monthly aggregates, and account comparisons.
- `update_database.py` - orchestrates CSV merges (with duplicate eviction), creates backups, and refreshes metadata; run via `python update_database.py`.
- `README.md`, `DATABASE_DESIGN.md` - operational guide and schema documentation.

### Update Pipeline
1. Drop the latest four CSV exports into `Portfolio/data/`.
2. Execute `python update_database.py`.
   - Backs up current `database/` to `backups/<YYYY-MM-DD>/`.
   - Merges new rows into historical CSVs (dedupe by date or key, then sort chronologically).
   - Copies the newest `account_master.csv` and rewrites `metadata.json` (last update timestamp, period range, record counts).
3. Launch `streamlit run app.py` to review dashboards with refreshed data.

### Streamlit Pages at a Glance
- **Overview** - headline metrics (total value, total return, investment profit, day count) plus Sharpe, volatility, max drawdown, win rate, and Plotly charts for portfolio value and cumulative return.
- **Performance Analysis** - tabs for daily return bars, monthly aggregates, and risk metrics (Sortino, drawdown series).
- **Holdings** - asset-type pie charts, stock-only breakdown, current holdings table, and account comparison summary.
- **Transactions** - counts, fees, daily average activity, and full transaction log table.

## Integration Ideas
1. **Symbol alignment** - match `Portfolio` holdings with `global_universe/data/daily/` or `data/valuations/` to enrich positions with FX-adjusted returns and valuation metrics.
2. **Joint reporting** - combine `PortfolioAnalyzer.export_summary_report()` output with `global_universe/data/reports/world_returns_*.csv` to automate monthly commentary that blends portfolio and benchmark context.
3. **Macro context** - embed `global_universe/figs/` (OECD CLI visuals) into the Streamlit app or downstream reports to align cycle phases with portfolio changes.
4. **Quality checks** - after ingesting new holdings, consult `global_universe/audit_results.csv` to spot currency or symbol anomalies before publishing dashboards.

## Automation Toolkit
- `python -m global_universe.automation --mode smoke` – runs a quick preflight (default 3 primaries, 2-day lookback) after sanitising CSVs. Use before large runs to surface auth/network issues early.
- `python -m global_universe.automation --mode full` – orchestrates the full pipeline: sanitize → smoke test → price update → valuations → KRX (pykrx required) → summary validation. Flags missing dependencies (`pykrx`, `curl`) before running and fails if summary CSVs contain errors.
- Additional switches:
  - `--skip-krx` to bypass KRX (when pykrx is unavailable locally),
  - `--skip-smoke` or `--no-sanitize` to shorten execution,
  - `--allow-issues` to avoid non-zero exit even if summaries contain warnings.
- Summary validation inspects `data/update_summary.csv`, `data/valuations_update_summary.csv`, and `data/krx_batch_summary_from_world_indices.csv`, listing non-OK rows for immediate triage.

## Continuous Updates (GitHub Actions)
- Dependencies are tracked in the new root `requirements.txt` (pandas, numpy, yfinance, requests, pykrx, matplotlib, sdmxthon).
- `.github/workflows/daily-update.yml` runs daily at 06:30 KST (21:30 UTC) and on manual dispatch:
  1. Checks out the repo and installs requirements.
  2. Calls `python -m global_universe.automation --mode full`.
  3. Commits & pushes any data changes using the default `GITHUB_TOKEN`.
- The automation script will exit non-zero if dependency checks fail or if summary files report errors; the workflow therefore halts on issues, keeping the scheduled updates reliable.
- When extending the pipeline (e.g., adding new dependencies or external APIs), update `requirements.txt` and rerun the automation script locally to ensure the action remains in sync.

## Suggested Next Steps
- Add notebook or script templates that merge holdings with benchmark returns for attribution analysis.
- Configure alerting (e.g., GitHub Action Slack/email notifications) when the daily workflow fails.
- Consider exporting both projects' outputs into a shared data lake (for example SQLite or DuckDB) for downstream analytics.
