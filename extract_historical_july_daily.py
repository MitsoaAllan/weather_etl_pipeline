import pandas as pd
import os

input_dir = "/home/mitsoa/airflow/weather_pipeline/dags/data/processed/hourly"
output_dir = "/home/mitsoa/airflow/weather_pipeline/dags/data/processed/daily"
os.makedirs(output_dir, exist_ok=True)

for file in os.listdir(input_dir):
    if not file.endswith(".csv"):
        continue

    filepath = os.path.join(input_dir, file)
    df = pd.read_csv(filepath)

    # Agréger par date et ville
    df_daily = df.groupby(['date', 'city']).agg({
        'temp': 'mean',
        'humidity': 'mean',
        'pressure': 'mean',
        'wind': 'mean'
    }).reset_index()

    # Arrondir pour plus de lisibilité
    df_daily[['temp', 'humidity', 'pressure', 'wind']] = df_daily[[
        'temp', 'humidity', 'pressure', 'wind'
    ]].round(2)

    df_daily = df_daily[['date', 'temp', 'humidity', 'pressure', 'wind', 'city']]

    output_path = os.path.join(output_dir, file)
    df_daily.to_csv(output_path, index=False)
    print(f" Fichier journalier généré dans : {output_path}")
