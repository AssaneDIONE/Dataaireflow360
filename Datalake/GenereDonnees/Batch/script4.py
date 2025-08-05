import csv
import random
from faker import Faker

fake = Faker('fr_FR')

# Noms des gares
senegalese_stop_names = [
    "Terminus Liberté 5", "Marché Sandaga", "Place de l'Indépendance", "Gare de Rufisque", 
    "Gare de Pikine", "Terminus Parcelles", "Gare de Yoff", "Gare de Guédiawaye", 
    "Terminus Keur Massar", "Gare de Thiaroye", "Avenue Cheikh Anta Diop", "Université UCAD", 
    "Gare de Grand Yoff", "Hôpital Fann", "Gare de Diamniadio", "AIBD", "Centre-ville", 
    "Gare de Cambérène", "Pont de Hann", "Gare des Mamelles", "Gare de Ouakam", "Port Autonome", 
    "Cité Keur Gorgui", "Cité Mixta", "HLM Grand Médine", "Cité Djily Mbaye", 
    "Gare des Parcelles Assainies", "Gare de Colobane", "Station Sicap Mbao", 
    "Terminus Malika", "Marché Tilène", "Hôpital Abass Ndao", "Gare de Médina", 
    "Cité Gadaye", "Zone de Captage", "Station Patte d'Oie", "Station Grand Dakar", 
    "Station Dalifort", "Station Liberté 6", "Cité Fadia"
]

zones = ["Zone Nord", "Centre-ville", "Banlieue", "Zone Sud", "Zone Est"]

# 1. Générer et stocker des coordonnées fixes pour chaque stop name
random.seed(42)  # pour que ce soit reproductible
coords_par_stop = {
    name: (
        round(random.uniform(14.65, 14.85), 6),
        round(random.uniform(-17.55, -17.25), 6)
    )
    for name in senegalese_stop_names
}

# 2. Écriture dans le fichier CSV
with open("stops.csv", "w", newline='', encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["stop_id", "name", "latitude", "longitude", "zone", "shelter"])

    for i, name in enumerate(senegalese_stop_names):
        stop_id = f"SENGAR_{i+1}"
        latitude, longitude = coords_par_stop[name]
        zone = random.choice(zones)
        shelter = random.choice([True, False])

        writer.writerow([stop_id, name, latitude, longitude, zone, shelter])
