# GitHub Actions Setup Guide: Data Validation

This project is now configured to run on **GitHub Actions**, which is 100% free and requires no credit card.

## 1. Prepare GCP Service Account Key
1.  Go to the [GCP Service Accounts Console](https://console.cloud.google.com/iam-admin/serviceaccounts).
2.  Click on your service account (e.g., `finedgevalidation@appspot.gserviceaccount.com`).
3.  Go to the **Keys** tab.
4.  Click **Add Key** > **Create new key** > **JSON**.
5.  Save the file to your computer.

## 2. Configure GitHub Secrets
1.  Go to your GitHub repository.
2.  Click **Settings** > **Secrets and variables** > **Actions**.
3.  Click **New repository secret** for each of the following:

| Secret Name | Description |
| :--- | :--- |
| `DB_HOST` | Your PostgreSQL Host IP/URL |
| `DB_NAME` | Database Name |
| `DB_USER` | Database Username |
| `DB_PASSWORD` | Database Password |
| `SPREADSHEET_ID` | The ID of your Google Sheet |
| `DRIVE_FOLDER_ID` | The ID of the folder where JSONs should be saved |
| `GCP_SERVICE_ACCOUNT_JSON` | **Copy-paste the entire content** of the JSON key file you downloaded in Step 1. |

## 3. Activate the Workflow
1.  Push your code to the GitHub repository.
2.  Go to the **Actions** tab in GitHub.
3.  On the left, click **Data Validation Pipeline**.
4.  Click **Run workflow** > **Run workflow** to test it immediately.

## 4. How the Schedule Works
*   The script is set to run **every hour** automatically (via `.github/workflows/data_validation.yml`).
*   It will check the `cron_expression` and `is_active` columns in your Google Sheet.
*   If a rule is due, it runs the SQL, saves to Drive, and logs the result back to the sheet.
