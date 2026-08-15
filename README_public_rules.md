# Public-schema data-validation rules

Adds automated data-quality checks for the **`public`** schema (equity daily quotes,
index quotes, financial statements, company master, dividends, commodities, share
ownership) to the existing FinEdgeValidation pipeline.

The existing 32 rules in `rules_for_gsheet.csv` cover **shareholding** analytics only.
These 36 new rules cover the rest of `public` — the high-volume tables that had no
checks (`daily_quotes` alone is ~13.9M rows).

## Files
- **`rules_public_data_quality.csv`** — the 36 rules, in the same 6-column format the
  `Rules` Google Sheet expects (`id, rule_desc, template, cron_expression, last_run, is_active`).
- **`build_public_rules.py`** — the rule catalog + a self-test that runs every rule
  against the live DB (syntax + violation count + timing) and (re)writes the CSV.
  `python build_public_rules.py` to re-validate; `--no-test` to just rewrite the CSV.

## How it plugs into the pipeline (no code changes)
Each rule is a read-only `SELECT` that **returns the offending rows** (0 rows = clean),
exactly like the existing rules. To activate:
1. Append the rows of `rules_public_data_quality.csv` to the `Rules` tab of the
   validation Google Sheet (below the existing rules).
2. `run_validation.py` / the cloud function: checks `is_active` + `cron`, enforces
   read-only SQL (`is_safe_sql`), runs due rules, writes each rule's violation rows
   into a dedicated **`Res <id>`** tab in the same spreadsheet (created once, overwritten
   each run; capped at 500 rows), and logs `row_count` to `Results Log`.

**Results are written to the spreadsheet, not Drive.** A service account has no Drive
storage quota (`files.create` → `403 "Service Accounts do not have storage quota"`), so
the runners write results into per-rule tabs of the same Sheet instead. All cell values
are stringified so date/Timestamp/Decimal columns serialise cleanly. The Drive upload
path was removed for this reason.

All 36 rules are `SELECT`/`WITH` only and pass `is_safe_sql`. Crons: integrity/price
checks **daily** (`0 0 * * *`); slow-moving reference tables **weekly** (`0 2 * * 1`).

## Baseline findings (first run, 2026-07 against nk-data-alchemy)
Counts are current violations — a triage list, not necessarily all bugs (e.g. dual
BSE/NSE listings can legitimately duplicate a symbol+date).

| Rule | Check | Violations |
|---|---|---|
| PUB-DQ-01 | daily_quotes high < low | 2,904 |
| PUB-DQ-02 | open/close outside [low,high] | 29,329 |
| PUB-DQ-03 | non-positive close_price (≤0) | 3,181 |
| PUB-DQ-04 | negative volume / market_cap | 2,972 |
| PUB-DQ-05 | duplicate (symbol, real trading day) | 54,625 |
| PUB-DQ-06 | future quote_date | 0 ✅ |
| PUB-DQ-07 | 52w high < 52w low | 0 ✅ |
| PUB-DQ-08 | >50% 1-day move, active non-SME, 90d | 632 |
| PUB-DQ-09 | active non-SME, no quote in 7d (stale) | 342 |
| PUB-DQ-10 | placeholder quote_date <1990 (per symbol) | 10 |
| PUB-IDX-01 | index OHLC integrity | 2 |
| PUB-IDX-02 | index pe/pb<0 or div_yield∉[0,100] | 3,934 |
| PUB-IDX-03 | duplicate index (symbol, date) | 0 ✅ |
| PUB-CP-01 | duplicate active stock_symbol | 0 ✅ |
| PUB-CP-02 | active company missing name/sector | 0 ✅ |
| PUB-CP-03 | negative market_cap / 52w high<low | 0 ✅ |
| PUB-FIN-01 | statements from_date > to_date | 876 |
| PUB-FIN-02 | statements empty standalone+consolidated | 3,555 |
| PUB-FIN-03 | statement company_id orphan | 3,554 |
| PUB-FIN-04 | profit_losses duplicate period key | 29 |
| PUB-FIN-05 | **coverage gap: missing quarter(s)** in quarterly series | 576 |
| PUB-FIN-06 | **coverage gap: missing fiscal year** in annual series | 233 |
| PUB-FIN-07 | **latest quarter stale/missing** (active non-SME, >7mo) | 139 |
| PUB-BS-05 | balance_sheets: missing quarter(s) — see caveat† | 12,541 |
| PUB-BS-06 | balance_sheets: missing fiscal year | 291 |
| PUB-BS-07 | balance_sheets: latest quarter stale — see caveat† | 4,551 |
| PUB-CF-06 | cash_flows: missing fiscal year | 95 |
| PUB-CF-07 | cash_flows: latest annual stale (>16mo) | 42 |
| PUB-DIV-01 | negative dividend | 0 ✅ |
| PUB-DIV-02 | duplicate dividend period | 60 |
| PUB-COM-01 | commodity_indexes neg/dup | 0 ✅ |
| PUB-IM-01 | index_masters null/dup symbol | 0 ✅ |
| PUB-THC-01 | holiday calendar dup/bad year | 0 ✅ |
| PUB-SOH-01 | ownership pct∉[0,100] / neg shares | 169 |
| PUB-SOH-02 | ownership from_date > to_date | 0 ✅ |
| PUB-SOH-03 | ownership shareholder_id orphan | 31,330 |

**Highest-signal to triage first:** PUB-DQ-05 (54k same-day dup quotes), PUB-SOH-03
(31k orphaned shareholder references), PUB-DQ-02 (29k bad OHLC bars), PUB-FIN-02/03
(~3.5k empty / orphaned financial statements), PUB-IDX-02 (3.9k bad index ratios).

## Notes / knobs
- A null `close_price` is **normal** in `daily_quotes` (~30% of rows are placeholder /
  non-trading / govt-security records) — the price rules deliberately ignore nulls and
  only flag non-positive values / broken bars.
- Large-result rules (DQ-02/05, SOH-03) upload sizable JSONs; scope them tighter (e.g.
  add `AND quote_date > now() - interval '1 year'`) or lower their cron if that's heavy.
- **Coverage-gap rules (FIN/BS/CF-05/06/07):** generated by one shared helper in
  `build_public_rules.py`, parameterized per table + indicator column (profit_losses /
  balance_sheets use `ttm_indicator`; cash_flows uses `ytd_indicator`). cash_flows is
  annual/YTD-reported (~1 quarterly filer) so it only gets the ANNUAL gap + latest-annual
  checks. A "quarterly reporter" is guarded as ≥8 quarterly rows; a gap = a >100-day step
  between consecutive quarter-ends (>460d for annual).
- **†Balance-sheet caveat:** BS-05 (12.5k) and BS-07 (4.5k) are high because SEBI mandates
  a full balance sheet only **half-yearly** — many companies file quarterly balance sheets
  irregularly, so those "gaps" are partly expected cadence rather than ETL misses. The
  annual balance-sheet/cash-flow checks (BS-06, CF-06, CF-07) are the clean, high-signal
  ones. To reduce BS-05/BS-07 noise, raise the gap threshold (e.g. >200 days) or switch
  the freshness check to "latest balance sheet of ANY period older than N months".
