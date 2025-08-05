import csv
import random
from faker import Faker
from collections import defaultdict

# Initialisation de la bibliothèque Faker pour générer des données réalistes
fake = Faker()

# Liste des routes disponibles (20 au total, de RUE_1 à RUE_20)
route_ids = [f"RUE_{i}" for i in range(1, 21)]

# Liste des véhicules disponibles (100 véhicules, de NUM_1000 à NUM_1099)
vehicle_ids = [f"NUM_{i}" for i in range(1000, 1100)]

# Nombre total de lignes qu'on souhaite générer
total_rows = 300

# Dictionnaire pour enregistrer les timestamps déjà utilisés pour chaque véhicule
# Objectif : éviter qu’un même véhicule soit sur deux routes différentes au même moment
vehicle_time_map = defaultdict(set) 

# Ouverture (création) du fichier CSV en mode écriture
with open('SEposition_GPS.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # Écriture de la ligne d’en-tête (colonnes du fichier)
    writer.writerow(['route_id', 'vehicle_id', 'timestamp', 'latitude', 'longitude', 'speed_kmh', 'traffic_level'])

    rows_written = 0   # Compteur de lignes effectivement écrites
    attempts = 0       # Compteur de tentatives (évite les boucles infinies)

    # Boucle jusqu'à ce que toutes les lignes soient générées ou que le nombre de tentatives dépasse une limite
    while rows_written < total_rows and attempts < total_rows * 10:
        attempts += 1

        # Choix aléatoire d'une route et d'un véhicule
        route_id = random.choice(route_ids)
        vehicle_id = random.choice(vehicle_ids)

        # Génération d'un timestamp réaliste (dans le mois courant)
        timestamp = fake.date_time_this_month()
        timestamp_str = str(timestamp)

        # Vérification : le véhicule n’est pas déjà affecté à une autre route au même moment
        if timestamp_str in vehicle_time_map[vehicle_id]:
            continue  # On ignore cette tentative 

        # Ajout du timestamp dans la mémoire du véhicule pour bloquer ce créneau
        vehicle_time_map[vehicle_id].add(timestamp_str)

        # Génération de la position GPS, vitesse et niveau de trafic
        latitude = fake.latitude()
        longitude = fake.longitude()
        speed_kmh = random.randint(20, 120)
        traffic_level = random.choice(['Faible', 'Modéré', 'Élevé'])

        # Écriture de la ligne dans le fichier CSV
        writer.writerow([route_id, vehicle_id, timestamp, latitude, longitude, speed_kmh, traffic_level])
        rows_written += 1  # Incrément du nombre de lignes écrites
