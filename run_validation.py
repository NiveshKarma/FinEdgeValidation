import os
import json
import time
import datetime
import pandas as pd
import psycopg2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from croniter import croniter

# --- Configuration ---
# Results are written into the spreadsheet itself (per-rule "Res <id>" tabs), so no
# Google Drive folder is needed -- a service account has no Drive storage quota anyway.
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
GCP_SERVICE_ACCOUNT_JSON = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
# Sheets API allows 60 write requests/min/user. Each rule makes several writes
# (create/clear/fill its result tab + last_run + log), so we pace rules apart to stay
# under the limit. Tune with RULE_DELAY_SECONDS (default 30s per rule).
RULE_DELAY_SECONDS = float(os.environ.get('RULE_DELAY_SECONDS', '30'))


def execute_with_retry(request, retries=6, base_wait=10):
    """Execute a Google API request, backing off and retrying on 429/503 (rate limit /
    transient) so a burst of writes doesn't hard-fail. Exponential: 10s, 20s, 40s, ..."""
    for attempt in range(retries):
        try:
            return request.execute()
        except HttpError as e:
            status = getattr(getattr(e, 'resp', None), 'status', None)
            if status in (429, 503) and attempt < retries - 1:
                wait = base_wait * (2 ** attempt)
                print(f"  Sheets API {status} (rate limit); backing off {wait}s "
                      f"(attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue
            raise

def get_google_service(service_name, version):
    """Authenticates using the service account JSON from env var."""
    try:
        info = json.loads(GCP_SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(info)
        return build(service_name, version, credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"Error authenticating with Google APIs: {e}")
        raise

def get_db_connection():
    """Returns a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=os.environ.get('DB_HOST'),
        database=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        port=os.environ.get('DB_PORT', 5432)
    )

def is_due(cron_string, last_run_str):
    """Checks if a rule is due to run."""
    if not cron_string: return False
    now = datetime.datetime.now(datetime.timezone.utc)
    if not last_run_str or last_run_str == "": return True
    try:
        last_run = datetime.datetime.fromisoformat(last_run_str.replace('Z', '+00:00'))
        iter = croniter(cron_string, last_run)
        next_run = iter.get_next(datetime.datetime)
        return now >= next_run
    except Exception as e:
        return False

def is_safe_sql(sql_query):
    """Basic security check for SELECT/WITH statements."""
    query = sql_query.strip().upper()
    if not query.startswith("SELECT") and not query.startswith("WITH"): return False
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "GRANT", "REVOKE"]
    for keyword in forbidden:
        if f" {keyword} " in f" {query} ": return False
    return True

def log_result(sheets_service, spreadsheet_id, rule_id, rule_desc, status, row_count, error_msg=""):
    """Logs to 'Results Log' sheet."""
    now = datetime.datetime.now().isoformat()
    # Truncate error message if too long for a cell
    if error_msg and len(error_msg) > 5000:
        error_msg = error_msg[:5000] + "...(truncated)"
        
    values = [[now, rule_id, rule_desc, status, row_count, error_msg]]
    body = {'values': values}
    execute_with_retry(sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id, range='Results Log!A:F',
        valueInputOption='USER_ENTERED', body=body))

def write_results_to_sheet(sheets_service, spreadsheet_id, rule_id, df, existing_tabs, max_rows=500):
    """Write a rule's result rows into a dedicated tab in the SAME spreadsheet
    (created once, overwritten each run). Avoids Google Drive entirely -- Sheets
    edits don't consume Drive storage quota, so there's no "service accounts have no
    storage quota" 403. All values are stringified so dates/Decimals serialise."""
    import re as _re, datetime as _dt
    tab = ("Res " + _re.sub(r"[^0-9A-Za-z_.\- ]", "_", str(rule_id)))[:95]
    if tab not in existing_tabs:
        execute_with_retry(sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]}))
        existing_tabs.add(tab)
    execute_with_retry(sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=f"'{tab}'!A:ZZ"))
    total = len(df)
    ts = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")
    header = f"Rule {rule_id} | updated {ts} | {total} violation row(s)"
    if total > max_rows:
        header += f" | showing first {max_rows}"
    values = [[header]]
    if total == 0:
        values.append(["No violations"])
    else:
        values.append([str(c) for c in df.columns])
        sub = df.head(max_rows).astype(object).where(pd.notnull(df.head(max_rows)), "")
        for rec in sub.itertuples(index=False, name=None):
            values.append([str(v) for v in rec])
    execute_with_retry(sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f"'{tab}'!A1",
        valueInputOption="RAW", body={"values": values}))
    return tab


def main():
    if not all([SPREADSHEET_ID, GCP_SERVICE_ACCOUNT_JSON]):
        print("Missing required environment variables (SPREADSHEET_ID, GCP_SERVICE_ACCOUNT_JSON).")
        return

    sheets_service = get_google_service('sheets', 'v4')
    # existing tab titles, so each rule's result tab is created only once
    try:
        _meta = sheets_service.spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID, fields='sheets.properties.title').execute()
        existing_tabs = {s['properties']['title'] for s in _meta.get('sheets', [])}
    except Exception:
        existing_tabs = set()

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range='Rules!A2:F'
        ).execute()
        rows = result.get('values', [])
    except Exception as e:
        print(f"Error reading rules: {e}")
        return

    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = True
    except Exception as e:
        print(f"DB connection failed: {e}")
        return

    for i, row in enumerate(rows):
        while len(row) < 6: row.append("")
        rule_id, rule_desc, sql_template, cron, last_run, is_active = row
        
        if is_active.lower() != 'true' or not is_due(cron, last_run):
            continue

        clean_sql = sql_template.strip()

        if not is_safe_sql(clean_sql):
            log_result(sheets_service, SPREADSHEET_ID, rule_id, rule_desc, "BLOCKED", 0, "Security: Only SELECT/WITH allowed.")
            time.sleep(RULE_DELAY_SECONDS)
            continue

        print(f"Running Rule {rule_id}...")
        try:
            # Read-only: fetch the rule's result rows. The DB is never modified.
            df = pd.read_sql_query(clean_sql, conn)

            # Write the result rows into a dedicated tab in this same spreadsheet
            # (no Drive files -> no storage-quota 403). Best-effort: a write hiccup
            # doesn't fail the rule -- the row_count is still logged.
            result_note = ""
            try:
                write_results_to_sheet(sheets_service, SPREADSHEET_ID, rule_id, df, existing_tabs)
            except Exception as we:
                result_note = f"result write skipped: {str(we)[:150]}"
                print(f"Rule {rule_id}: {result_note}")

            # Update last_run so cron scheduling advances
            execute_with_retry(sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f'Rules!E{i+2}',
                valueInputOption='RAW', body={'values': [[datetime.datetime.now(datetime.timezone.utc).isoformat()]]}))

            log_result(sheets_service, SPREADSHEET_ID, rule_id, rule_desc, "SUCCESS", len(df), result_note)
            print(f"Rule {rule_id} completed successfully ({len(df)} rows).")
        except Exception as e:
            error_str = str(e)
            print(f"Rule {rule_id} failed: {error_str}")
            log_result(sheets_service, SPREADSHEET_ID, rule_id, rule_desc, "FAILED", 0, error_str)

        # Pace rules apart to stay under the Sheets 60-writes/min/user limit.
        time.sleep(RULE_DELAY_SECONDS)

    if conn: conn.close()

if __name__ == "__main__":
    main()
