"""
Author + self-test the data-quality validation rules for the `public` schema
(equity quotes, financials, company master, indices, commodities, dividends).

Each rule is a read-only SELECT that RETURNS THE OFFENDING ROWS (0 rows == clean).
This mirrors the existing FinEdgeValidation model: run_validation.py runs due rules,
uploads the returned rows to Drive, and logs the row_count to the Results Log sheet.

This script validates every rule against the live DB (syntax + violation count +
timing) and writes rules_public_data_quality.csv in the exact 6-column Sheet format:
  id, rule_desc, template, cron_expression, last_run, is_active

Run:  python build_public_rules.py            (test + write CSV)
      python build_public_rules.py --no-test  (write CSV without hitting the DB)
"""
import csv
import os
import sys
import time

# Read-only connection for the self-test. Set VALIDATION_DB_URL (or the individual
# DB_* vars the pipeline already uses). No credentials are hardcoded here.
DB_URL = os.environ.get("VALIDATION_DB_URL")
if not DB_URL and all(os.environ.get(k) for k in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD")):
    DB_URL = (f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
              f"@{os.environ['DB_HOST']}:{os.environ.get('DB_PORT', '5432')}/{os.environ['DB_NAME']}")
DAILY = "0 0 * * *"       # every day
HOURLY = "0 * * * *"      # every hour
WEEKLY = "0 2 * * 1"      # Monday 02:00

# ------------------------------------------------------------------ rule catalog
# (id, rule_desc, sql, cron). SQL must be a single SELECT/WITH returning violations.
RULES = [
    # =================== daily_quotes (OHLC price data, 8.5M rows) ===================
    ("PUB-DQ-01", "daily_quotes: high < low (invalid OHLC bar)", """
SELECT dq.id, dq.stock_symbol, dq.quote_date, dq.open_price, dq.high_price, dq.low_price, dq.close_price
FROM public.daily_quotes dq
WHERE dq.deleted_at IS NULL AND dq.high_price IS NOT NULL AND dq.low_price IS NOT NULL
  AND dq.high_price < dq.low_price
""", DAILY),

    ("PUB-DQ-02", "daily_quotes: open/close outside [low, high] range", """
SELECT dq.id, dq.stock_symbol, dq.quote_date, dq.open_price, dq.high_price, dq.low_price, dq.close_price
FROM public.daily_quotes dq
WHERE dq.deleted_at IS NULL AND dq.high_price IS NOT NULL AND dq.low_price IS NOT NULL
  AND (dq.close_price > dq.high_price OR dq.close_price < dq.low_price
       OR dq.open_price  > dq.high_price OR dq.open_price  < dq.low_price)
""", DAILY),

    # NOTE: a null close_price is NORMAL here (~30% of rows are placeholder / non-trading
    # / govt-security records), so we flag only NON-POSITIVE prices, which are always invalid.
    ("PUB-DQ-03", "daily_quotes: non-positive close_price (<= 0)", """
SELECT dq.id, dq.stock_symbol, dq.quote_date, dq.close_price, dq.volume
FROM public.daily_quotes dq
WHERE dq.deleted_at IS NULL AND dq.close_price <= 0
""", DAILY),

    ("PUB-DQ-04", "daily_quotes: negative volume or market_cap", """
SELECT dq.id, dq.stock_symbol, dq.quote_date, dq.volume, dq.market_cap
FROM public.daily_quotes dq
WHERE dq.deleted_at IS NULL AND (dq.volume < 0 OR dq.market_cap < 0)
""", DAILY),

    # Excludes placeholder dates (< 1990) which are covered by PUB-DQ-10; this catches
    # genuine duplicate quotes for the same symbol on the same real trading day.
    ("PUB-DQ-05", "daily_quotes: duplicate (stock_symbol, quote_date) on a real trading day", """
SELECT id, stock_symbol, quote_date, close_price, dup_cnt FROM (
  SELECT id, stock_symbol, quote_date, close_price,
         count(*) OVER (PARTITION BY stock_symbol, quote_date::date) AS dup_cnt
  FROM public.daily_quotes WHERE deleted_at IS NULL AND quote_date >= date '1990-01-01'
) t WHERE dup_cnt > 1
""", DAILY),

    ("PUB-DQ-10", "daily_quotes: placeholder/invalid quote_date (< 1990) per symbol", """
SELECT stock_symbol, count(*) AS bad_rows, min(quote_date) AS min_date, max(quote_date) AS max_date
FROM public.daily_quotes
WHERE deleted_at IS NULL AND quote_date < date '1990-01-01'
GROUP BY stock_symbol
""", DAILY),

    ("PUB-DQ-06", "daily_quotes: quote_date in the future", """
SELECT dq.id, dq.stock_symbol, dq.quote_date, dq.close_price
FROM public.daily_quotes dq
WHERE dq.deleted_at IS NULL AND dq.quote_date > now() + interval '1 day'
""", DAILY),

    ("PUB-DQ-07", "daily_quotes: 52-week high < 52-week low", """
SELECT dq.id, dq.stock_symbol, dq.quote_date, dq.high52, dq.low52
FROM public.daily_quotes dq
WHERE dq.deleted_at IS NULL AND dq.high52 IS NOT NULL AND dq.low52 IS NOT NULL
  AND dq.high52 < dq.low52
""", DAILY),

    ("PUB-DQ-08", "daily_quotes: extreme 1-day move >50% on active non-SME (last 90d, possible bad tick)", """
SELECT dq.id, dq.stock_symbol, dq.quote_date, dq.prev_close, dq.close_price,
       round((dq.close_price/dq.prev_close - 1) * 100, 1) AS pct_move, cp.company_type
FROM public.daily_quotes dq
JOIN public.company_profiles cp ON cp.stock_symbol = dq.stock_symbol
WHERE dq.deleted_at IS NULL AND dq.inactive = false
  AND dq.prev_close > 0 AND dq.close_price > 0
  AND dq.quote_date > now() - interval '90 days'
  AND coalesce(cp.company_type,'') <> 'SME'
  AND abs(dq.close_price/dq.prev_close - 1) > 0.5
""", DAILY),

    ("PUB-DQ-09", "daily_quotes: active non-SME company with no quote in last 7 days (stale/missing)", """
SELECT cp.stock_symbol, cp.company_name, cp.company_type, max(dq.quote_date) AS last_quote
FROM public.company_profiles cp
LEFT JOIN public.daily_quotes dq ON dq.stock_symbol = cp.stock_symbol AND dq.deleted_at IS NULL
WHERE cp.deleted_at IS NULL AND cp.active = true AND coalesce(cp.trading_active,true) = true
  AND coalesce(cp.company_type,'') <> 'SME'
GROUP BY cp.stock_symbol, cp.company_name, cp.company_type
HAVING max(dq.quote_date) IS NULL OR max(dq.quote_date) < now() - interval '7 days'
""", DAILY),

    # =================== index_historical_quotes ===================
    ("PUB-IDX-01", "index_historical_quotes: high < low, or close/open outside range", """
SELECT id, index_symbol, quote_date, open_price, high_price, low_price, close_price
FROM public.index_historical_quotes
WHERE high_price IS NOT NULL AND low_price IS NOT NULL
  AND (high_price < low_price
       OR close_price > high_price OR close_price < low_price
       OR open_price  > high_price OR open_price  < low_price)
""", DAILY),

    ("PUB-IDX-02", "index_historical_quotes: pe<0, pb<0, or div_yield out of [0,100]", """
SELECT id, index_symbol, quote_date, pe, pb, div_yield
FROM public.index_historical_quotes
WHERE (pe < 0) OR (pb < 0) OR (div_yield < 0) OR (div_yield > 100)
""", DAILY),

    ("PUB-IDX-03", "index_historical_quotes: duplicate (index_symbol, quote_date)", """
SELECT id, index_symbol, quote_date, close_price, dup_cnt FROM (
  SELECT id, index_symbol, quote_date, close_price,
         count(*) OVER (PARTITION BY index_symbol, quote_date) AS dup_cnt
  FROM public.index_historical_quotes
) t WHERE dup_cnt > 1
""", DAILY),

    # =================== company_profiles (master data) ===================
    ("PUB-CP-01", "company_profiles: duplicate stock_symbol among active companies", """
SELECT id, stock_symbol, company_name, company_type, dup_cnt FROM (
  SELECT id, stock_symbol, company_name, company_type,
         count(*) OVER (PARTITION BY stock_symbol) AS dup_cnt
  FROM public.company_profiles WHERE deleted_at IS NULL AND active = true
) t WHERE dup_cnt > 1
""", DAILY),

    ("PUB-CP-02", "company_profiles: active+trading company missing name or sector", """
SELECT id, stock_symbol, company_name, macro_sector, sector, industry
FROM public.company_profiles
WHERE deleted_at IS NULL AND active = true AND coalesce(trading_active,true) = true
  AND (company_name IS NULL OR btrim(company_name) = ''
       OR sector IS NULL OR btrim(sector) = '')
""", DAILY),

    ("PUB-CP-03", "company_profiles: negative market_cap or 52w high<low", """
SELECT id, stock_symbol, market_cap, high_52, low_52
FROM public.company_profiles
WHERE deleted_at IS NULL
  AND (market_cap < 0 OR (high_52 IS NOT NULL AND low_52 IS NOT NULL AND high_52 < low_52))
""", DAILY),

    # =================== financial statements (P&L / balance / cash / ratios / segment) ==========
    ("PUB-FIN-01", "financial statements: from_date > to_date (period inverted)", """
SELECT 'profit_losses' AS tbl, id, stock_symbol, period, from_date, to_date FROM public.profit_losses
  WHERE deleted_at IS NULL AND from_date > to_date
UNION ALL SELECT 'balance_sheets', id, stock_symbol, period, from_date, to_date FROM public.balance_sheets
  WHERE deleted_at IS NULL AND from_date > to_date
UNION ALL SELECT 'cash_flows', id, stock_symbol, period, from_date, to_date FROM public.cash_flows
  WHERE deleted_at IS NULL AND from_date > to_date
UNION ALL SELECT 'segment_revenues', id, stock_symbol, period, from_date, to_date FROM public.segment_revenues
  WHERE deleted_at IS NULL AND from_date > to_date
UNION ALL SELECT 'historical_ratios', id, stock_symbol, period, from_date, to_date FROM public.historical_ratios
  WHERE deleted_at IS NULL AND from_date > to_date
""", DAILY),

    ("PUB-FIN-02", "financial statements: both standalone and consolidated JSON empty/null", """
SELECT 'profit_losses' AS tbl, id, stock_symbol, date_header FROM public.profit_losses
  WHERE deleted_at IS NULL AND coalesce(standalone_json::text,'') IN ('','null','{}','[]')
    AND coalesce(consolidated_json::text,'') IN ('','null','{}','[]')
UNION ALL SELECT 'balance_sheets', id, stock_symbol, date_header FROM public.balance_sheets
  WHERE deleted_at IS NULL AND coalesce(standalone_json::text,'') IN ('','null','{}','[]')
    AND coalesce(consolidated_json::text,'') IN ('','null','{}','[]')
UNION ALL SELECT 'cash_flows', id, stock_symbol, date_header FROM public.cash_flows
  WHERE deleted_at IS NULL AND coalesce(standalone_json::text,'') IN ('','null','{}','[]')
    AND coalesce(consolidated_json::text,'') IN ('','null','{}','[]')
""", DAILY),

    ("PUB-FIN-03", "financial statements: company_id not present in company_profiles (orphan)", """
SELECT 'profit_losses' AS tbl, id, stock_symbol, company_id FROM public.profit_losses pl
  WHERE pl.deleted_at IS NULL AND pl.company_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM public.company_profiles cp WHERE cp.id = pl.company_id)
UNION ALL SELECT 'balance_sheets', id, stock_symbol, company_id FROM public.balance_sheets bs
  WHERE bs.deleted_at IS NULL AND bs.company_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM public.company_profiles cp WHERE cp.id = bs.company_id)
UNION ALL SELECT 'cash_flows', id, stock_symbol, company_id FROM public.cash_flows cf
  WHERE cf.deleted_at IS NULL AND cf.company_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM public.company_profiles cp WHERE cp.id = cf.company_id)
""", DAILY),

    ("PUB-FIN-04", "profit_losses: duplicate (company_id, from_date, to_date, period, ttm_indicator)", """
SELECT id, stock_symbol, company_id, period, from_date, to_date, ttm_indicator, dup_cnt FROM (
  SELECT id, stock_symbol, company_id, period, from_date, to_date, ttm_indicator,
         count(*) OVER (PARTITION BY company_id, from_date, to_date, period, ttm_indicator) AS dup_cnt
  FROM public.profit_losses WHERE deleted_at IS NULL AND company_id IS NOT NULL
) t WHERE dup_cnt > 1
""", DAILY),

    # ---- coverage gaps: missing quarters / years in the financial series (profit_losses) ----
    # Guarded to genuine QUARTERLY reporters (>=8 quarterly rows) so half-yearly reporters
    # aren't flagged; a >100-day step between consecutive quarter-ends = >=1 missing quarter.
    ("PUB-FIN-05", "profit_losses: gap in quarterly series (missing quarter[s]) for active co", """
WITH q AS (
  SELECT company_id, stock_symbol, to_date::date AS td
  FROM public.profit_losses
  WHERE deleted_at IS NULL AND period = 'Quarterly' AND ttm_indicator = false AND company_id IS NOT NULL
  GROUP BY company_id, stock_symbol, to_date::date
), qtr_reporters AS (
  SELECT company_id FROM q GROUP BY company_id HAVING count(*) >= 8
), seq AS (
  SELECT q.company_id, q.stock_symbol, q.td,
         lag(q.td) OVER (PARTITION BY q.company_id ORDER BY q.td) AS prev_td
  FROM q JOIN qtr_reporters USING (company_id)
)
SELECT s.stock_symbol, s.company_id, s.prev_td AS gap_after_quarter, s.td AS resumed_quarter,
       round((s.td - s.prev_td)/30.44) AS approx_months_missing
FROM seq s
JOIN public.company_profiles cp ON cp.id = s.company_id AND cp.deleted_at IS NULL AND cp.active = true
WHERE s.prev_td IS NOT NULL AND (s.td - s.prev_td) > 100 AND s.td > now() - interval '4 years'
ORDER BY approx_months_missing DESC
""", DAILY),

    # Annual series: >460-day step between consecutive fiscal-year-ends = a missing year.
    ("PUB-FIN-06", "profit_losses: gap in annual series (missing fiscal year) for active co", """
WITH a AS (
  SELECT company_id, stock_symbol, to_date::date AS td
  FROM public.profit_losses
  WHERE deleted_at IS NULL AND period = 'Annual' AND ttm_indicator = false AND company_id IS NOT NULL
  GROUP BY company_id, stock_symbol, to_date::date
), yr_reporters AS (
  SELECT company_id FROM a GROUP BY company_id HAVING count(*) >= 3
), seq AS (
  SELECT a.company_id, a.stock_symbol, a.td,
         lag(a.td) OVER (PARTITION BY a.company_id ORDER BY a.td) AS prev_td
  FROM a JOIN yr_reporters USING (company_id)
)
SELECT s.stock_symbol, s.company_id, s.prev_td AS gap_after_year, s.td AS resumed_year,
       round((s.td - s.prev_td)/365.0, 1) AS approx_years_missing
FROM seq s
JOIN public.company_profiles cp ON cp.id = s.company_id AND cp.deleted_at IS NULL AND cp.active = true
WHERE s.prev_td IS NOT NULL AND (s.td - s.prev_td) > 460 AND s.td > now() - interval '8 years'
ORDER BY approx_years_missing DESC
""", WEEKLY),

    # Latest quarter missing: active non-SME whose most recent quarterly result is >7 months old.
    ("PUB-FIN-07", "profit_losses: latest quarter stale/missing (active non-SME, >7 months)", """
WITH latest AS (
  SELECT company_id, max(to_date::date) AS last_quarter
  FROM public.profit_losses
  WHERE deleted_at IS NULL AND period = 'Quarterly' AND ttm_indicator = false AND company_id IS NOT NULL
  GROUP BY company_id
)
SELECT cp.stock_symbol, cp.company_name, cp.company_type, l.last_quarter,
       round((now()::date - l.last_quarter)/30.44) AS months_since_last
FROM latest l
JOIN public.company_profiles cp ON cp.id = l.company_id
WHERE cp.deleted_at IS NULL AND cp.active = true AND coalesce(cp.company_type,'') <> 'SME'
  AND l.last_quarter < (now() - interval '7 months')::date
ORDER BY l.last_quarter
""", DAILY),

    # =================== yearly_dividends ===================
    ("PUB-DIV-01", "yearly_dividends: negative dividend amount", """
SELECT id, stock_symbol, date_header, dividend_amount, adj_dividend_amount
FROM public.yearly_dividends
WHERE dividend_amount < 0 OR adj_dividend_amount < 0
""", WEEKLY),

    ("PUB-DIV-02", "yearly_dividends: duplicate (company_id, date_header, ttm_indicator)", """
SELECT id, stock_symbol, company_id, date_header, dividend_amount, dup_cnt FROM (
  SELECT id, stock_symbol, company_id, date_header, dividend_amount,
         count(*) OVER (PARTITION BY company_id, date_header, ttm_indicator) AS dup_cnt
  FROM public.yearly_dividends WHERE company_id IS NOT NULL
) t WHERE dup_cnt > 1
""", WEEKLY),

    # =================== commodity_indexes ===================
    ("PUB-COM-01", "commodity_indexes: negative value or duplicate (code, date_index, index_type)", """
SELECT id, code, date_index, index_type, value, dup_cnt FROM (
  SELECT id, code, date_index, index_type, value,
         count(*) OVER (PARTITION BY code, date_index, index_type) AS dup_cnt
  FROM public.commodity_indexes
) t WHERE dup_cnt > 1 OR value < 0
""", WEEKLY),

    # =================== index_masters ===================
    ("PUB-IM-01", "index_masters: null index_name/index_symbol or duplicate index_symbol", """
SELECT id, index_name, index_symbol, index_type, dup_cnt FROM (
  SELECT id, index_name, index_symbol, index_type,
         count(*) OVER (PARTITION BY index_symbol) AS dup_cnt
  FROM public.index_masters
) t
WHERE index_name IS NULL OR btrim(index_name) = ''
   OR index_symbol IS NULL OR btrim(index_symbol) = ''
   OR (index_symbol IS NOT NULL AND dup_cnt > 1)
""", WEEKLY),

    # =================== trading_holiday_calendars ===================
    ("PUB-THC-01", "trading_holiday_calendars: duplicate trading_date or implausible year", """
SELECT id, trading_date, week_day, year, description, dup_cnt FROM (
  SELECT id, trading_date, week_day, year, description,
         count(*) OVER (PARTITION BY trading_date) AS dup_cnt
  FROM public.trading_holiday_calendars
) t
WHERE dup_cnt > 1 OR year < 2000 OR year > extract(year FROM now())::int + 2
""", WEEKLY),

    # =================== share ownership (numeric sanity beyond existing shareholding rules) =====
    ("PUB-SOH-01", "share_ownership_histories: pct_of_ownership outside [0,100] or negative shares/value", """
SELECT id, stock_symbol, shareholder_id, date_header, pct_of_ownership, number_of_shares, investment_value
FROM public.share_ownership_histories
WHERE excluded_ind = false
  AND (pct_of_ownership < 0 OR pct_of_ownership > 100
       OR number_of_shares < 0 OR investment_value < 0)
""", DAILY),

    ("PUB-SOH-02", "share_ownership_histories: from_date > to_date", """
SELECT id, stock_symbol, shareholder_id, from_date, to_date
FROM public.share_ownership_histories
WHERE from_date IS NOT NULL AND to_date IS NOT NULL AND from_date > to_date
""", DAILY),

    ("PUB-SOH-03", "share_ownership_histories: shareholder_id has no matching shareholders row (orphan)", """
SELECT soh.id, soh.stock_symbol, soh.shareholder_id, soh.date_header
FROM public.share_ownership_histories soh
WHERE soh.shareholder_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM public.shareholders sh WHERE sh.id = soh.shareholder_id)
""", DAILY),
]


# --------------------------------------------------------------------------------------
# Coverage-gap rules generalized to the other statement tables. Same logic as
# PUB-FIN-05/06/07 (validated on profit_losses), parameterized by table + the table's
# period-indicator column (profit_losses/balance_sheets use ttm_indicator; cash_flows
# uses ytd_indicator). cash_flows is reported annually/YTD (only ~1 quarterly reporter),
# so it gets the ANNUAL gap + latest-ANNUAL-stale checks, not the quarterly ones.
def _quarterly_gap_sql(table, ind):
    return f"""
WITH q AS (
  SELECT company_id, stock_symbol, to_date::date AS td
  FROM public.{table}
  WHERE deleted_at IS NULL AND period = 'Quarterly' AND {ind} = false AND company_id IS NOT NULL
  GROUP BY company_id, stock_symbol, to_date::date
), qtr_reporters AS (
  SELECT company_id FROM q GROUP BY company_id HAVING count(*) >= 8
), seq AS (
  SELECT q.company_id, q.stock_symbol, q.td,
         lag(q.td) OVER (PARTITION BY q.company_id ORDER BY q.td) AS prev_td
  FROM q JOIN qtr_reporters USING (company_id)
)
SELECT s.stock_symbol, s.company_id, s.prev_td AS gap_after_quarter, s.td AS resumed_quarter,
       round((s.td - s.prev_td)/30.44) AS approx_months_missing
FROM seq s
JOIN public.company_profiles cp ON cp.id = s.company_id AND cp.deleted_at IS NULL AND cp.active = true
WHERE s.prev_td IS NOT NULL AND (s.td - s.prev_td) > 100 AND s.td > now() - interval '4 years'
ORDER BY approx_months_missing DESC
""".strip()


def _annual_gap_sql(table, ind):
    return f"""
WITH a AS (
  SELECT company_id, stock_symbol, to_date::date AS td
  FROM public.{table}
  WHERE deleted_at IS NULL AND period = 'Annual' AND {ind} = false AND company_id IS NOT NULL
  GROUP BY company_id, stock_symbol, to_date::date
), yr_reporters AS (
  SELECT company_id FROM a GROUP BY company_id HAVING count(*) >= 3
), seq AS (
  SELECT a.company_id, a.stock_symbol, a.td,
         lag(a.td) OVER (PARTITION BY a.company_id ORDER BY a.td) AS prev_td
  FROM a JOIN yr_reporters USING (company_id)
)
SELECT s.stock_symbol, s.company_id, s.prev_td AS gap_after_year, s.td AS resumed_year,
       round((s.td - s.prev_td)/365.0, 1) AS approx_years_missing
FROM seq s
JOIN public.company_profiles cp ON cp.id = s.company_id AND cp.deleted_at IS NULL AND cp.active = true
WHERE s.prev_td IS NOT NULL AND (s.td - s.prev_td) > 460 AND s.td > now() - interval '8 years'
ORDER BY approx_years_missing DESC
""".strip()


def _latest_stale_sql(table, ind, period, months, span_label):
    return f"""
WITH latest AS (
  SELECT company_id, max(to_date::date) AS last_period
  FROM public.{table}
  WHERE deleted_at IS NULL AND period = '{period}' AND {ind} = false AND company_id IS NOT NULL
  GROUP BY company_id
)
SELECT cp.stock_symbol, cp.company_name, cp.company_type, l.last_period AS {span_label},
       round((now()::date - l.last_period)/30.44) AS months_since_last
FROM latest l
JOIN public.company_profiles cp ON cp.id = l.company_id
WHERE cp.deleted_at IS NULL AND cp.active = true AND coalesce(cp.company_type,'') <> 'SME'
  AND l.last_period < (now() - interval '{months} months')::date
ORDER BY l.last_period
""".strip()


# balance_sheets: same cadence as profit_losses (quarterly + annual, ttm_indicator)
RULES += [
    ("PUB-BS-05", "balance_sheets: gap in quarterly series (missing quarter[s]) for active co",
     _quarterly_gap_sql("balance_sheets", "ttm_indicator"), DAILY),
    ("PUB-BS-06", "balance_sheets: gap in annual series (missing fiscal year) for active co",
     _annual_gap_sql("balance_sheets", "ttm_indicator"), WEEKLY),
    ("PUB-BS-07", "balance_sheets: latest quarter stale/missing (active non-SME, >7 months)",
     _latest_stale_sql("balance_sheets", "ttm_indicator", "Quarterly", 7, "last_quarter"), DAILY),
    # cash_flows: annual/YTD-centric -> annual gap + latest-annual-stale (skip quarterly)
    ("PUB-CF-06", "cash_flows: gap in annual series (missing fiscal year) for active co",
     _annual_gap_sql("cash_flows", "ytd_indicator"), WEEKLY),
    ("PUB-CF-07", "cash_flows: latest annual stale/missing (active non-SME, >16 months)",
     _latest_stale_sql("cash_flows", "ytd_indicator", "Annual", 16, "last_annual"), DAILY),
]


def wrap_count(sql):
    return f"SELECT count(*) FROM (\n{sql.strip().rstrip(';')}\n) _dq"


def test_rules():
    if not DB_URL:
        print("VALIDATION_DB_URL (or DB_* env vars) not set -- skipping self-test.")
        return True
    import psycopg2
    conn = psycopg2.connect(DB_URL, connect_timeout=25)
    conn.autocommit = True
    ok = bad = 0
    print(f"{'rule':12} {'status':8} {'viol':>10} {'ms':>7}  desc")
    print("-" * 90)
    for rid, desc, sql, cron in RULES:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '120s'")
            t = time.time()
            try:
                cur.execute(wrap_count(sql))
                n = cur.fetchone()[0]
                ms = int((time.time() - t) * 1000)
                flag = "OK" if n == 0 else "VIOL"
                print(f"{rid:12} {flag:8} {n:>10,} {ms:>7}  {desc[:52]}")
                ok += 1
            except Exception as e:
                conn.rollback() if not conn.autocommit else None
                print(f"{rid:12} {'ERROR':8} {'-':>10} {'-':>7}  {str(e).splitlines()[0][:60]}")
                bad += 1
    conn.close()
    print("-" * 90)
    print(f"{ok} rules valid, {bad} errored")
    return bad == 0


def write_csv(path="rules_public_data_quality.csv"):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "rule_desc", "template", "cron_expression", "last_run", "is_active"])
        for rid, desc, sql, cron in RULES:
            w.writerow([rid, desc, sql.strip(), cron, "", "TRUE"])
    print(f"wrote {len(RULES)} rules -> {path}")


if __name__ == "__main__":
    if "--no-test" not in sys.argv:
        allgood = test_rules()
    else:
        allgood = True
    write_csv()
