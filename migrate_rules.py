import pandas as pd
import os

def migrate():
    excel_file = 'Shareholding_sql_v2.xlsx'
    output_file = 'rules_for_gsheet.csv'
    
    if not os.path.exists(excel_file):
        print(f"Error: {excel_file} not found.")
        return

    # Sheets identified during research
    sheets_to_read = ['Analysis sql', 'Shareholding']
    
    all_rules = []
    
    # Process 'Analysis sql'
    try:
        df1 = pd.read_excel(excel_file, sheet_name='Analysis sql')
        # Columns: id, template, rule_desc
        df1 = df1[['id', 'rule_desc', 'template']].dropna(subset=['template'])
        all_rules.append(df1)
    except Exception as e:
        print(f"Skipping Analysis sql: {e}")

    # Process 'Shareholding'
    try:
        df2 = pd.read_excel(excel_file, sheet_name='Shareholding')
        # Columns: id, Sql template, rule_desc
        df2 = df2.rename(columns={'Sql template': 'template'})
        df2 = df2[['id', 'rule_desc', 'template']].dropna(subset=['template'])
        all_rules.append(df2)
    except Exception as e:
        print(f"Skipping Shareholding: {e}")

    if not all_rules:
        print("No rules found to migrate.")
        return

    final_df = pd.concat(all_rules, ignore_index=True)
    
    # Add new columns for the system
    final_df['cron_expression'] = '0 0 * * *'  # Default to daily
    final_df['last_run'] = ''
    final_df['is_active'] = 'TRUE'
    
    # Ensure correct column order for the GSheet guide
    # A: id, B: rule_desc, C: template, D: cron_expression, E: last_run, F: is_active
    final_df = final_df[['id', 'rule_desc', 'template', 'cron_expression', 'last_run', 'is_active']]
    
    final_df.to_csv(output_file, index=False)
    print(f"Migration complete! Upload '{output_file}' to Google Sheets.")

if __name__ == "__main__":
    migrate()
