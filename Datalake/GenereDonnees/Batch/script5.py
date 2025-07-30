import csv
import random
from faker import Faker

fake = Faker('fr_FR')

descriptions = [
    "Panne moteur", "Crevaison", "Problème de freins", 
    "Problème électrique", "Retard dû au trafic", "Accident mineur"
]

# Liste fixe de véhicules
vehicle_ids = [f"VEH_{i}" for i in range(1000, 1100)]

with open("incidents.csv", "w", newline='', encoding="utf-8") as file:
    writer = csv.writer(file)

    # En-têtes
    writer.writerow(['incident_id', 'vehicle_id', 'timestamp', 'description', 'delay_minutes', 'severity'])

    # Générer 100 incidents
    for i in range(100):
        incident_id = f"INC_{i+300}"
        vehicle_id = random.choice(vehicle_ids)
        timestamp = fake.date_time()
        description = random.choice(descriptions)
        delay_minutes = random.randint(0, 60)
        severity = random.randint(1, 5)

        writer.writerow([incident_id, vehicle_id, timestamp, description, delay_minutes, severity])
