import requests
import pandas as pd
from datetime import datetime
import json

def extract_current():
    """
    Récupère les données météo actuelles depuis l'API OpenWeather pour 5 villes.
    """

    with open("openweather_config.json", "r") as f:
        config = json.load(f)

    api_key = config["api_key"]
    cities = config["cities"]
    

    data = []

    for city in cities:
        name = city["name"]
        url = f"https://api.openweathermap.org/data/2.5/weather?appid={api_key}&q={name}"
        
        response = requests.get(url)
        if response.status_code == 200:
            weather = response.json()
            data.append({
                "date": datetime.utcnow().date(),
                "city": name,
                "temp": weather["main"]["temp"] - 273.15,
                "humidity": weather["main"]["humidity"],
                "pressure": weather["main"]["pressure"],
                "wind": weather["wind"]["speed"]
            })
        else:
            print(f"Échec pour {name} : {response.text}")

    df = pd.DataFrame(data)
    df.to_csv("/home/mitsoa/airflow/weather_pipeline/dags/data/current_data.csv", index=False)