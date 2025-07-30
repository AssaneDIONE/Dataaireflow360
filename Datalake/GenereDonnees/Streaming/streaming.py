import requests
import pandas as pd
import csv
    
# "https://transit.land/api/v2/feeds?api_key=xxx"

API_KEY = "HSCv5uMQXF4CkMcqaQTJXHcf797iSvHw"
BASE_URL = "https://app.interline.io/transitland_datasets"

headers = {
    "Authorization": f"Bearer {API_KEY}"
}

def get_stops(country_code='SN', max_records=200):
    url = f"{BASE_URL}/stops?country={country_code}&per_page={max_records}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()['stops']
        df = pd.DataFrame(data)
        df.to_csv("donnees_arrets.csv", index=False)
        print("Données sur les arrêts enregistrées dans donnees_arrets.csv")
    else:
        print("Erreur API:", response.status_code, response.text)

def get_routes(country_code='SN', max_records=200):
    url = f"{BASE_URL}/routes?country={country_code}&per_page={max_records}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()['routes']
        df = pd.DataFrame(data)
        df.to_csv("donnees_itineraires.csv", index=False)
        print("Données sur les itinéraires enregistrées dans donnees_itineraires.csv")
    else:
        print("Erreur API:", response.status_code, response.text)

# Exemple pour le Sénégal
get_stops('SN', 200)
get_routes('SN', 200)
