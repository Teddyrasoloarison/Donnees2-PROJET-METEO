import requests
import pandas as pd
from datetime import datetime, timezone
import os

# === CONFIGURATION GÉNÉRALE ===
API_KEY = "fc1f5102f96e2c4b8bbc233e3b808aa5"  
CITIES = [  # Liste des villes à analyser
    {"name": "Antananarivo", "lat": -18.8792, "lon": 47.5079},
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"name": "Tokyo", "lat": 35.6895, "lon": 139.6917},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Le Caire", "lat": 30.0444, "lon": 31.2357}
]

# === ETAPE 1 : EXTRACT ===
def collect_weather_data():
    """
    Récupère les données météo actuelles pour chaque ville via l'API OpenWeather.
    Retourne une liste de dictionnaires contenant les données brutes extraites.
    """
    data = []

    for city in CITIES:
        params = {
            "lat": city["lat"],
            "lon": city["lon"],
            "appid": API_KEY,
            "units": "metric"
        }

        try:
            response = requests.get("https://api.openweathermap.org/data/2.5/weather", params=params, timeout=10)
            response.raise_for_status()
            result = response.json()

            # === ETAPE 2 : TRANSFORM ===
            # Extraction des champs utiles
            main = result.get("main", {})
            weather_list = result.get("weather", [{}])
            wind = result.get("wind", {})

            # Pluie : 0 si champ absent
            rain = result.get("rain", {}).get("1h", 0.0)
            temp_min = main.get("temp_min")
            temp_max = main.get("temp_max")
            temp_avg = main.get("temp")
            humidity = main.get("humidity")
            wind_speed = wind.get("speed")
            weather_desc = weather_list[0].get("description") if weather_list else None

            # Transformation en structure propre
            data.append({
                "ville": city["name"],
                "date": datetime.fromtimestamp(result.get("dt", 0), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                "temp_moyenne": temp_avg,
                "temp_min": temp_min,
                "temp_max": temp_max,
                "temp_diff": temp_max - temp_min if temp_max is not None and temp_min is not None else None,
                "humidite": humidity,
                "vent": wind_speed,
                "pluie": rain,
                "pluvieux": rain > 0.1,  # booléen utile pour analyser les jours de pluie
                "description_meteo": weather_desc
            })

        except requests.RequestException as e:
            print(f" Erreur API pour {city['name']}: {e}")
        except Exception as e:
            print(f" Erreur inattendue pour {city['name']}: {e}")

    # Retour sous forme de DataFrame pandas
    return pd.DataFrame(data)


# === ETAPE 3 : LOAD ===
def save_data(df, filename="historique_meteo.csv"):
    """
    Enregistre les données météo dans un fichier CSV historique.
    - Si le fichier existe : ajoute à la suite (append)
    - Sinon : crée un nouveau fichier avec l'en-tête
    """
    if os.path.exists(filename):
        df.to_csv(filename, mode='a', index=False, header=False)
    else:
        df.to_csv(filename, index=False)


# === PIPELINE COMPLET ===
def run_etl():
    """
    Exécute l'ensemble du pipeline ETL :
    - Extraction des données météo
    - Transformation en format structuré
    - Sauvegarde en fichiers CSV
    """
    df = collect_weather_data()

    if df.empty:
        print(" Aucune donnée collectée, arrêt de l'ETL.")
        return

    save_data(df)  # Fichier cumulatif (historique)
    df.to_csv("data_realtime.csv", index=False) 

    print(" Données météo collectées et enregistrées avec succès.")
    print(df.head())


# === POINT D’ENTRÉE ===
if __name__ == "__main__":
    run_etl()
