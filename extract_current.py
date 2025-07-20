import requests
import pandas as pd
from datetime import datetime
import json

def extract_current():
    """
    Récupère les données météo actuelles depuis l'API OpenWeather pour 5 villes.
    """

    with open("/home/mitsoa/airflow/weather_pipeline/dags/scripts/openweather_config.json", "r") as f:
        config = json.load(f)

    api_key = config["api_key"]
    cities = config["cities"]
    
    today = datetime.utcnow().date().isoformat()

    data = []

    for city in cities:
        name = city["name"]
        url = f"https://api.openweathermap.org/data/2.5/weather?appid={api_key}&q={name}"
        
        response = requests.get(url)
        if response.status_code == 200:
            weather = response.json()
            temp=weather["main"]["temp"] - 273.15;
            data.append({
                "date": datetime.utcnow().date(),
                "temp":round(temp,2) ,
                "humidity": weather["main"]["humidity"],
                "pressure": weather["main"]["pressure"],
                "wind": weather["wind"]["speed"],
                "city": name
            })
        else:
            print(f"Échec pour {name} : {response.text}")

    df = pd.DataFrame(data)
    df.to_csv("/home/mitsoa/airflow/weather_pipeline/dags/data/current_data.csv", index=False)
    df.to_csv(f"/home/mitsoa/airflow/weather_pipeline/dags/data/processed/weather_{today}.csv", index=False)