import pandas as pd
import os

def extract_historical():
    """
    Charge les données historiques météo depuis un fichier CSV.
    """
    output_path = "/home/mitsoa/airflow/weather_pipeline/dags/data/extracted_historical.csv"

    if os.path.exists(output_path):
        print("Fichier déjà extrait. Lecture sans réécriture.")
        df = pd.read_csv(output_path, parse_dates=["date"])
    else:
        df = pd.read_csv("/home/mitsoa/airflow/weather_pipeline/dags/data/cleaned_historical_data/historical_weather.csv", parse_dates=["date"])
        df.to_csv(output_path, index=False)
        print("✅ Données extraites et enregistrées.")