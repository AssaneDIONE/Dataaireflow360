import csv
import random
from faker import Faker

# Initialiser Faker
fake = Faker()

# Créer une liste de véhicules disponibles (VEH_1000 à VEH_1099)
vehicle_ids = [f"VEH_{i}" for i in range(1000, 1100)]

# Créer une liste de stops fictifs
stop_ids = [fake.city() for i in range(20)]

# Créer le fichier CSV
with open('Passagers.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # En-têtes des colonnes
    writer.writerow(['passenger_count', 'timestamp', 'vehicle_id', 'stop_id', 'boarding', 'alighting'])

    # Générer les données
    for i in range(100):
        passenger_count = random.randint(0, 60)
        timestamp = fake.date_time()
        vehicle_id = random.choice(vehicle_ids)
        stop_id = random.choice(stop_ids)
        boarding = random.choice([True, False])
        alighting = random.choice([True, False])

        # Écrire la ligne
        writer.writerow([passenger_count, timestamp, vehicle_id, stop_id, boarding, alighting])
