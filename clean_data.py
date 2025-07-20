import pandas as pd
import os

def clean_data():
    """
    Transforme les données historiques + actuelles + journalières en un format unifié propre.
    """
    hist = pd.read_csv("/home/mitsoa/airflow/weather_pipeline/dags/data/extracted_historical.csv", parse_dates=["date"])
    current = pd.read_csv("/home/mitsoa/airflow/weather_pipeline/dags/data/current_data.csv", parse_dates=["date"])

    df_long = pd.DataFrame()

    for city in ["los_angeles", "san_diego", "las_vegas", "saint_louis", "miami"]:
        temp = hist[["date", f"{city}_temp"]].rename(columns={f"{city}_temp": "temp"})
        hum = hist[["date", f"{city}_humidity"]].rename(columns={f"{city}_humidity": "humidity"})
        pres = hist[["date", f"{city}_pressure"]].rename(columns={f"{city}_pressure": "pressure"})
        wind = hist[["date", f"{city}_wind"]].rename(columns={f"{city}_wind": "wind"})
        
        df = temp.merge(hum, on="date").merge(pres, on="date").merge(wind, on="date")
        df["city"] = city.replace("_", " ").title()
        df_long = pd.concat([df_long, df], ignore_index=True)

    # Fusion avec current_data.csv
    new_data = pd.concat([df_long, current], ignore_index=True)
    new_data.dropna(inplace=True)

    # 🔽 Charger tous les fichiers journaliers dans processed/daily/
    processed_dir = "/home/mitsoa/airflow/weather_pipeline/dags/data/processed/daily"
    daily_data = pd.DataFrame()

    for file in os.listdir(processed_dir):
        if file.endswith(".csv"):
            path = os.path.join(processed_dir, file)
            df = pd.read_csv(path, parse_dates=["date"])
            df.dropna(inplace=True)
            daily_data = pd.concat([daily_data, df], ignore_index=True)

    # 🔁 Fusionner avec les nouvelles données
    all_data = pd.concat([new_data, daily_data], ignore_index=True)

    # 🔁 Supprimer les doublons + données manquantes
    all_data.dropna(inplace=True)
    all_data.drop_duplicates(subset=["date", "city"], inplace=True)

    # 📤 Sauvegarder
    combined_path = "/home/mitsoa/airflow/weather_pipeline/dags/data/combined_data.csv"
    all_data.to_csv(combined_path, index=False)
    print(f" Données combinées sauvegardées dans : {combined_path}")
