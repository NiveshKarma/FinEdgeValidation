import os
import json
import datetime
import pandas as pd
import psycopg2
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaInMemoryUpload
from croniter import croniter

# --- Configuration ---
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID')
GCP_SERVICE_ACCOUNT_JSON = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')

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

def upload_to_drive(service, filename, data, folder_id):
    """Uploads JSON to Drive."""
    file_metadata = {
        'name': f"{filename}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        'parents': [folder_id]
    }
    media = MediaInMemoryUpload(json.dumps(data, indent=2).encode('utf-8'), mimetype='application/json')
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')

def log_result(sheets_service, spreadsheet_id, rule_id, rule_desc, status, row_count, error_msg=""):
    """Logs to 'Results Log' sheet."""
    now = datetime.datetime.now().isoformat()
    values = [[now, rule_id, rule_desc, status, row_count, error_msg]]
    body = {'values': values}
    sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id, range='Results Log!A:F',
        valueInputOption='USER_ENTERED', body=body
    ).execute()

def main():
    if not all([SPREADSHEET_ID, DRIVE_FOLDER_ID, GCP_SERVICE_ACCOUNT_JSON]):
        print("Missing required environment variables.")
        return

    sheets_service = get_google_service('sheets', 'v4')
    drive_service = get_google_service('drive', 'v3')
    
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
        conn.autocommit = True # This prevents the "transaction aborted" error chain
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
            continue

        print(f"Running Rule {rule_id}...")
        try:
            df = pd.read_sql_query(clean_sql, conn)
            file_id = upload_to_drive(drive_service, f"Rule_{rule_id}", df.to_dict(orient='records'), DRIVE_FOLDER_ID)
            
            # Update last_run
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID, range=f'Rules!E{i+2}',
                valueInputOption='RAW', body={'values': [[datetime.datetime.now(datetime.timezone.utc).isoformat()]]}
            ).execute()
            
            log_result(sheets_service, SPREADSHEET_ID, rule_id, rule_desc, "SUCCESS", len(df))
        except Exception as e:
            print(f"Rule {rule_id} failed: {e}")
            log_result(sheets_service, SPREADSHEET_ID, rule_id, rule_desc, "FAILED", 0, str(e))

    if conn: conn.close()

if __name__ == "__main__":
    main()
