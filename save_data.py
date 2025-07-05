import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def save_data():
    """
    Envoyer le fichier combined_data.csv dans google sheet en ligne
    """
    df = pd.read_csv("/home/mitsoa/airflow/weather_pipeline/dags/data/combined_data.csv")

    df.replace([float('inf'), float('-inf')], pd.NA, inplace=True)
    df.fillna('', inplace=True)

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("/home/mitsoa/airflow/weather_pipeline/config/credentials.json", scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open("Weather Pipeline")
    try:
        worksheet = spreadsheet.worksheet("Data")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title="Data", rows="1000", cols="20")

    worksheet.clear()

    worksheet.update([df.columns.values.tolist()] + df.values.tolist())