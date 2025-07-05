import requests
import pandas as pd
from datetime import datetime
import json

def extract_current():
    """
    Récupère les données météo actuelles depuis l'API OpenWeather pour 5 villes.
    """

    api_key = "51bb7fc76317db88ffc9c704ee5b45ec"
    cities = [
    {"name": "Los Angeles", "lat": 34.05, "lon": -118.25},
    {"name": "San Diego", "lat": 32.72, "lon": -117.16},
    {"name": "Las Vegas", "lat": 36.17, "lon": -115.14},
    {"name": "Saint Louis", "lat": 38.63, "lon": -90.20},
    {"name": "Miami", "lat": 25.76, "lon": -80.19}
  ]

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