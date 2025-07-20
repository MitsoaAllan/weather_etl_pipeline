import pandas as pd
from functools import reduce

"""
Récupération et fusion des fichiers qui contiennent l'historique de données des météos des villes
"""
files = {
    "/home/mitsoa/airflow/weather_pipeline/dags/data/historical_data/temperature.csv": "temp",
    "/home/mitsoa/airflow/weather_pipeline/dags/data/historical_data/humidity.csv": "humidity",
    "/home/mitsoa/airflow/weather_pipeline/dags/data/historical_data/pressure.csv": "pressure",
    "/home/mitsoa/airflow/weather_pipeline/dags/data/historical_data/wind_speed.csv": "wind",
}

dataframes = []

for file, name in files.items():
    df = pd.read_csv(file)
    df.columns=df.columns.str.strip().str.lower().str.replace(" ","_")
    
    df['datetime'] = pd.to_datetime(df['datetime'])

    if name == "temp":
        cities = df.columns.drop("datetime")
        df[cities] = df[cities] - 273.15
        
    df['date'] = df['datetime'].dt.date

    df_daily = df.drop(columns=['datetime']).groupby('date').mean()

    df_daily = df_daily.add_suffix(f'_{name}')

    df_daily.reset_index(inplace=True)

    dataframes.append(df_daily)

df_merged = reduce(lambda left, right: pd.merge(left, right, on='date'), dataframes)

df_merged = df_merged.round(2)

df_merged.to_csv('/home/mitsoa/airflow/weather_pipeline/dags/data/cleaned_historical_data/historical_weather.csv', index=False)