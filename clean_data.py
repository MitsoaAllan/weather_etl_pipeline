import pandas as pd

def clean_data():
    """
    Transforme les données historiques + actuelles dans un format unifié long.
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

    combined = pd.concat([df_long, current], ignore_index=True)
    combined.to_csv("/home/mitsoa/airflow/weather_pipeline/dags/data/combined_data.csv", index=False)