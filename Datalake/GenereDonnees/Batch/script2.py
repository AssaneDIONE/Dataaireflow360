import csv
import random
from faker import Faker

# Initialiser Faker
fake = Faker()

# Créer une liste de véhicules disponibles (VEH_1000 à VEH_1099)
vehicle_ids = [f"VEH_{i}" for i in range(1000, 1100)]

# Créer le fichier CSV
with open('position_GPS.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # En-têtes des colonnes
    writer.writerow(['route_id','vehicle_id', 'timeshamp', 'latitude', 'longitude', 'speek_kmh','traffic_level'])

    # Générer les données
    for i in range(100):
        route_id = f"RO_{10 + i}"
        vehicle_id = random.choice(vehicle_ids)  # Choix aléatoire dans l'intervalle demandé
        timestamp = fake.date_time()
        latitude = fake.latitude()
        longitude = fake.longitude()
        speed_kmh = random.randint(20, 120)  # Vitesse simulée entre 20 et 120 km/h
        traffic_level = random.choice(['Faible', 'Modéré', 'Élevé'])      

        # Écrire la ligne
        writer.writerow([route_id, vehicle_id, timestamp, latitude, longitude, speed_kmh, traffic_level])
