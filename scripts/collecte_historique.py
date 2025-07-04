from meteostat import Stations, Daily
from datetime import datetime
import pandas as pd

def collect_historical_data(start_year=2011, end_year=2015):
    cities = [
        {"name": "Antananarivo", "lat": -18.8792, "lon": 47.5079},
        {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
        {"name": "Tokyo", "lat": 35.6895, "lon": 139.6917},
        {"name": "New York", "lat": 40.7128, "lon": -74.0060},
        {"name": "Le Caire", "lat": 30.0444, "lon": 31.2357}
    ]

    all_data = []

    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)

    for city in cities:
        stations = Stations()
        stations = stations.nearby(city["lat"], city["lon"])
        station = stations.fetch(1)
        station_id = station.index[0]
        print(f"Récupération données pour {city['name']} (station {station_id}) de {start_year} à {end_year}")

        try:
            data = Daily(station_id, start, end)
            df = data.fetch()
            df['city'] = city['name']
            all_data.append(df)
        except Exception as e:
            print(f" Erreur lors de la récupération pour {city['name']} : {e}")
            continue

    if all_data:
        historical_df = pd.concat(all_data)
        historical_df.reset_index(inplace=True)
        return historical_df
    else:
        print("Aucune donnée historique récupérée.")
        return pd.DataFrame()  # DataFrame vide

if __name__ == "__main__":
    df = collect_historical_data(2011, 2015)
    print(df.head())
    df.to_csv("historique_complet.csv", index=False)
