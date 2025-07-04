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
            print(f"Erreur lors de la récupération pour {city['name']} : {e}")
            continue

    if all_data:
        historical_df = pd.concat(all_data)
        historical_df.reset_index(inplace=True)
        return historical_df
    else:
        print("Aucune donnée historique récupérée.")
        return pd.DataFrame()  # DataFrame vide

def transformer_meteostat(df_meteo, nom_ville=None):
    df = df_meteo.copy()

    if 'city' not in df.columns and nom_ville:
        df['city'] = nom_ville

    df = df.rename(columns={
        'time': 'date',
        'tavg': 'temp_moyenne',
        'tmin': 'temp_min',
        'tmax': 'temp_max',
        'prcp': 'pluie',
        'wspd': 'vent',
    })

    df['ville'] = df['city'] if 'city' in df.columns else nom_ville
    df['humidite'] = None
    df['pluvieux'] = df['pluie'].apply(lambda x: x > 0.1 if pd.notnull(x) else False)
    df['temp_diff'] = df['temp_max'] - df['temp_min']
    df['description_meteo'] = None

    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')

    colonnes = ['ville', 'date', 'temp_moyenne', 'temp_min', 'temp_max', 'temp_diff',
                'humidite', 'vent', 'pluie', 'pluvieux', 'description_meteo']

    df_final = df[colonnes]

    return df_final

if __name__ == "__main__":
    df_histo_brut = collect_historical_data(2011, 2015)
    if not df_histo_brut.empty:
        df_histo = transformer_meteostat(df_histo_brut)
        print(df_histo.head())
        df_histo.to_csv("historique_transforme.csv", index=False)
