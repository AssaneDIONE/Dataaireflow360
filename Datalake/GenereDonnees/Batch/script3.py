import csv
import random
from faker import Faker
from collections import defaultdict

# Initialisation de Faker
fake = Faker()

# Liste des véhicules (VEH_1000 à VEH_1099)
vehicle_ids = [f"NUM_{i}" for i in range(1000, 1100)]

# Liste des arrêts (SENGAR_1 à SENGAR_40)
stop_ids = [f"SENGAR_{i}" for i in range(1, 41)]

# Dictionnaire pour empêcher qu’un même véhicule soit à deux arrêts à la même heure
# { vehicle_id : set(timestamps déjà utilisés) }
vehicle_time_map = defaultdict(set)

# Création du fichier CSV
with open('Passagers.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # Écriture de l'en-tête
    writer.writerow(['passenger_count', 'timestamp', 'vehicle_id', 'stop_id', 'boarding', 'alighting'])

    rows_written = 0
    attempts = 0
    total_rows = 100  # Nombre total de lignes à générer

    # Génération contrôlée des données
    while rows_written < total_rows and attempts < total_rows * 10:
        attempts += 1

        # Sélection aléatoire des valeurs
        vehicle_id = random.choice(vehicle_ids)
        timestamp = fake.date_time_this_month()
        timestamp_str = str(timestamp)

        # Vérification : le véhicule ne doit pas déjà être utilisé à ce moment
        if timestamp_str in vehicle_time_map[vehicle_id]:
            continue  # On saute si déjà utilisé

        # Enregistrement du timestamp pour ce véhicule
        vehicle_time_map[vehicle_id].add(timestamp_str)

        stop_id = random.choice(stop_ids)
        passenger_count = random.randint(0, 60)
        boarding = random.choice([True, False])
        alighting = random.choice([True, False])

        # Écriture de la ligne dans le fichier
        writer.writerow([passenger_count, timestamp, vehicle_id, stop_id, boarding, alighting])
        rows_written += 1
