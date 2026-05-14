# Setup Guide: GCP Data Validation System

Follow these steps to deploy and configure the data validation system.

## 1. Google Sheets Setup
1.  Upload `Shareholding_sql_v2.xlsx` to your Google Drive.
2.  Open it with **Google Sheets**.
3.  Rename the main rule sheet to `Rules`.
4.  Ensure the `Rules` sheet has the following columns (A to F):
    *   **A: id** (Rule ID)
    *   **B: rule_desc** (Description)
    *   **C: template** (The SQL query)
    *   **D: cron_expression** (e.g., `0 * * * *` for hourly, `0 0 * * *` for daily)
    *   **E: last_run** (Leave empty initially)
    *   **F: is_active** (Set to `TRUE` for rules you want to run)
5.  Create a second sheet named `Results Log` with these headers:
    *   Timestamp, Rule ID, Rule Description, Status, Row Count, Error Message.
6.  **Copy the Spreadsheet ID** from the URL (the long string between `/d/` and `/edit`).

## 2. Google Drive Setup
1.  In your Google Drive, you have created: `3. FinEdge API` > `Data Validation Automated`.
2.  **Base Folder:** Open the `Data Validation Automated` folder.
3.  **JSON Results Folder:** Inside `Data Validation Automated`, create a sub-folder called `Raw Results`. 
4.  **Copy the Folder ID** of the `Raw Results` folder from the URL (the long string at the end of the URL). This will be your `DRIVE_FOLDER_ID`.
5.  **Move the Google Sheet:** Move the Google Sheet you created in Step 1 into the `Data Validation Automated` folder for better organization.

## 3. Google Cloud Platform (GCP) Setup
1.  **Enable APIs:** Enable "Google Sheets API", "Google Drive API", and "Cloud Functions API" in your GCP project.
2.  **Service Account:**
    *   Create a Service Account.
    *   Grant it the "Cloud Functions Invoker" role (if you want to restrict access).
    *   **Crucial:** Share your Google Sheet and the Google Drive folder with the Service Account's email address (with "Editor" access).
3.  **Cloud Function Deployment:**
    *   Create a new Cloud Function (2nd Gen).
    *   Trigger: **HTTPS**.
    *   Runtime: **Python 3.11** (or higher).
    *   Entry point: `run_validation`.
    *   Upload the contents of the `cloud_function` directory.
    *   **Environment Variables:** Add the following:
        *   `SPREADSHEET_ID`: (The ID from step 1.6)
        *   `DRIVE_FOLDER_ID`: (The ID from step 2.2)
        *   `DB_HOST`: Your PostgreSQL host.
        *   `DB_NAME`: Your database name.
        *   `DB_USER`: Your database user.
        *   `DB_PASSWORD`: Your database password.
        *   `DB_PORT`: `5432` (default).

## 4. Scheduling
1.  Go to **Cloud Scheduler**.
2.  Create a job (e.g., name: `validation-dispatcher`).
3.  Frequency: `0 * * * *` (runs every hour to check for due rules).
4.  Target: **HTTP**.
5.  URL: The URL of your deployed Cloud Function.
6.  Auth header: **Add OIDC token** (select your service account).

## 5. Dashboarding
1.  Open **Looker Studio**.
2.  Create a new report.
3.  Add a Data Source: **Google Sheets**.
4.  Select your spreadsheet and the `Results Log` sheet.
5.  Build your dashboard using the execution logs!
