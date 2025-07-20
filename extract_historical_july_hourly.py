import requests
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta

def kelvin_to_celsius(k):
    return round(k - 273.15, 2)

def unix_timestamp(date_str):
    return int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())

def fetch_data_for_city(api_key, city, start_date, end_date):
    lat = city["lat"]
    lon = city["lon"]
    name = city["name"]
    output_path = f"/home/mitsoa/airflow/weather_pipeline/dags/data/processed/hourly/{name.lower().replace(' ', '_')}.csv"

    # Créer le dossier si besoin
    os.makedirs("/home/mitsoa/airflow/weather_pipeline/dags/data/processed/hourly", exist_ok=True)

    start_ts = unix_timestamp(start_date)
    end_ts = unix_timestamp(end_date)

    # Ajouter 5 jours
    step = 432000
    all_rows = []

    print(f"\n Traitement de la ville : {name}")

    while start_ts < end_ts:
        current_end = min(start_ts + step - 1, end_ts)

        url = (
            f"https://history.openweathermap.org/data/2.5/history/city?"
            f"lat={lat}&lon={lon}&type=hour&start={start_ts}&end={current_end}&appid={api_key}"
        )

        print(f"📡 API call: {datetime.utcfromtimestamp(start_ts)} ==> {datetime.utcfromtimestamp(current_end)}")
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Erreur ({response.status_code}) : {response.text}")
            start_ts += step
            continue

        data = response.json()
        for entry in data.get("list", []):
            dt = datetime.utcfromtimestamp(entry["dt"]).strftime("%Y-%m-%d")
            temp = kelvin_to_celsius(entry["main"]["temp"])
            humidity = entry["main"]["humidity"]
            pressure = entry["main"]["pressure"]
            wind = entry.get("wind", {}).get("speed", 0)

            all_rows.append({
                "date": dt,
                "temp": temp,
                "humidity": humidity,
                "pressure": pressure,
                "wind": wind,
                "city": name
            })

        time.sleep(1)
        start_ts += step

    if all_rows:
        df = pd.DataFrame(all_rows)
        df.to_csv(output_path, index=False)
        print(f"Données sauvegardées pour : {name}")
    else:
        print(f"Aucune donnée récupérée pour {name}")

def main():
    with open("/home/mitsoa/airflow/weather_pipeline/dags/scripts/openweather_config.json", "r") as f:
        config = json.load(f)

    api_key = config["api_key"]
    cities = config["cities"]

    start_date = "2025-07-04"
    end_date = "2025-07-17"

    for city in cities:
        fetch_data_for_city(api_key, city, start_date, end_date)

if __name__ == "__main__":
    main()
