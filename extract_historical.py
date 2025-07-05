import pandas as pd

def extract_historical():
    """
    Charge les données historiques météo depuis un fichier CSV.
    """
    df = pd.read_csv("/home/mitsoa/airflow/weather_pipeline/dags/data/cleaned_historical_data/historical_weather.csv", parse_dates=["date"])
    df.to_csv("/home/mitsoa/airflow/weather_pipeline/dags/data/extracted_historical.csv", index=False)