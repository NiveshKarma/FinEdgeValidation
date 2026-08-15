import os
import json
import datetime
import pandas as pd
import psycopg2
from google.cloud import secretmanager
from googleapiclient.discovery import build
from google.oauth2 import service_account
from croniter import croniter
import functions_framework

# --- Configuration ---
# These should be set as environment variables in the Cloud Function
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID')
DB_CONFIG_SECRET_ID = os.environ.get('DB_CONFIG_SECRET_ID') # Or direct env vars if preferred

# Scopes for Google APIs
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def get_google_service(service_name, version):
    """Authenticates using the default service account and returns the service."""
    # When deployed, it uses the Cloud Function's identity
    # For local testing, set GOOGLE_APPLICATION_CREDENTIALS
    return build(service_name, version, cache_discovery=False)

def get_db_connection():
    """Returns a connection to the PostgreSQL database."""
    # It's better to fetch these from Secret Manager in a real GCP environment
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST'),
        database=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        port=os.environ.get('DB_PORT', 5432)
    )
    return conn

def is_due(cron_string, last_run_str):
    """Checks if a rule is due to run based on cron and last run timestamp."""
    if not cron_string:
        return False
    
    now = datetime.datetime.now(datetime.timezone.utc)
    
    if not last_run_str or last_run_str == "":
        return True
    
    try:
        last_run = datetime.datetime.fromisoformat(last_run_str.replace('Z', '+00:00'))
        iter = croniter(cron_string, last_run)
        next_run = iter.get_next(datetime.datetime)
        return now >= next_run
    except Exception as e:
        print(f"Error parsing cron or last_run: {e}")
        return False

def upload_to_drive(service, filename, data, folder_id):
    """Uploads a JSON string as a file to a specific Google Drive folder."""
    from googleapiclient.http import MediaInMemoryUpload
    
    file_metadata = {
        'name': f"{filename}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        'parents': [folder_id]
    }
    # default=str so pandas/psycopg2 Timestamps, dates and Decimals serialize
    # (df.to_dict keeps them as native objects that plain json.dumps can't encode).
    media = MediaInMemoryUpload(json.dumps(data, indent=2, default=str).encode('utf-8'), mimetype='application/json')
    # supportsAllDrives so an upload to a Shared Drive folder works (a service account
    # has no My-Drive storage quota, so a normal My-Drive folder still 403s).
    file = service.files().create(body=file_metadata, media_body=media, fields='id',
                                  supportsAllDrives=True).execute()
    return file.get('id')

def log_result(sheets_service, spreadsheet_id, rule_id, rule_desc, status, row_count, error_msg=""):
    """Appends a log entry to the 'Results Log' sheet."""
    now = datetime.datetime.now().isoformat()
    values = [[now, rule_id, rule_desc, status, row_count, error_msg]]
    body = {'values': values}
    sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range='Results Log!A:F',
        valueInputOption='USER_ENTERED',
        body=body
    ).execute()

def is_safe_sql(sql_query):
    """Checks if the SQL query starts with SELECT and doesn't contain destructive commands."""
    query = sql_query.strip().upper()
    
    # Must start with SELECT
    if not query.startswith("SELECT") and not query.startswith("WITH"):
        return False
    
    # Block destructive keywords (simple check, though not foolproof against all SQL injection,
    # it serves as a guardrail for rule configuration)
    forbidden_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "GRANT", "REVOKE"]
    for keyword in forbidden_keywords:
        # Check for keyword surrounded by spaces or at the end to avoid partial matches (e.g., 'SELECT ... FROM updates')
        if f" {keyword} " in f" {query} ":
            return False
            
    return True

def write_results_to_sheet(sheets_service, spreadsheet_id, rule_id, df, existing_tabs, max_rows=500):
    """Write a rule's result rows into a dedicated tab in the SAME spreadsheet
    (created once, overwritten each run). Avoids Google Drive entirely -- Sheets
    edits don't consume Drive storage quota, so there's no "service accounts have no
    storage quota" 403. All values are stringified so dates/Decimals serialise."""
    import re as _re
    tab = ("Res " + _re.sub(r"[^0-9A-Za-z_.\- ]", "_", str(rule_id)))[:95]
    if tab not in existing_tabs:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]}).execute()
        existing_tabs.add(tab)
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=f"'{tab}'!A:ZZ").execute()
    total = len(df)
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
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
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f"'{tab}'!A1",
        valueInputOption="RAW", body={"values": values}).execute()
    return tab


@functions_framework.http
def run_validation(request):
    """HTTP Cloud Function entry point."""
    sheets_service = get_google_service('sheets', 'v4')
    # existing tab titles, so each rule's result tab is created only once
    try:
        _meta = sheets_service.spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID, fields='sheets.properties.title').execute()
        existing_tabs = {s['properties']['title'] for s in _meta.get('sheets', [])}
    except Exception:
        existing_tabs = set()

    # 1. Read Rules from 'Rules' sheet
    # Expecting columns: id, rule_desc, template, cron_expression, last_run, is_active
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range='Rules!A2:F'
        ).execute()
        rows = result.get('values', [])
    except Exception as e:
        return f"Error reading rules sheet: {str(e)}", 500

    conn = None
    try:
        conn = get_db_connection()
    except Exception as e:
        return f"Database connection failed: {str(e)}", 500

    execution_summary = []

    for i, row in enumerate(rows):
        # Ensure row has enough columns
        while len(row) < 6:
            row.append("")
            
        rule_id, rule_desc, sql_template, cron, last_run, is_active = row
        
        if is_active.lower() != 'true':
            continue
            
        if not is_due(cron, last_run):
            continue

        if not is_safe_sql(sql_template):
            print(f"Skipping Rule {rule_id}: Non-SELECT or unsafe statement detected.")
            log_result(sheets_service, SPREADSHEET_ID, rule_id, rule_desc, "BLOCKED", 0, "Security: Only SELECT/WITH statements allowed.")
            execution_summary.append(f"Rule {rule_id}: Blocked (Unsafe SQL)")
            continue

        print(f"Executing Rule {rule_id}: {rule_desc}")
        
        try:
            # Read-only: fetch the rule's result rows. The DB is never modified.
            df = pd.read_sql_query(sql_template, conn)
            row_count = len(df)

            # Write the result rows into a dedicated tab in this same spreadsheet
            # (no Drive files -> no storage-quota 403). Best-effort: a write hiccup
            # doesn't fail the rule -- the row_count is still logged.
            result_note = ""
            try:
                write_results_to_sheet(sheets_service, SPREADSHEET_ID, rule_id, df, existing_tabs)
            except Exception as we:
                result_note = f"result write skipped: {str(we)[:150]}"
                print(f"Rule {rule_id}: {result_note}")

            # Update last_run so cron scheduling advances (Column E is index 4)
            cell_range = f'Rules!E{i+2}'
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=cell_range,
                valueInputOption='RAW',
                body={'values': [[datetime.datetime.now(datetime.timezone.utc).isoformat()]]}
            ).execute()

            log_result(sheets_service, SPREADSHEET_ID, rule_id, rule_desc, "SUCCESS", row_count, result_note)
            execution_summary.append(f"Rule {rule_id}: Success ({row_count} rows)")

        except Exception as e:
            error_msg = str(e)
            print(f"Error executing rule {rule_id}: {error_msg}")
            log_result(sheets_service, SPREADSHEET_ID, rule_id, rule_desc, "FAILED", 0, error_msg)
            execution_summary.append(f"Rule {rule_id}: Failed")

    if conn:
        conn.close()

    return f"Execution finished. Summary: {', '.join(execution_summary)}", 200
