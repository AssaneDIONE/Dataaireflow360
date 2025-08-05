import csv
import random
from faker import Faker
from datetime import datetime

fake = Faker('fr_FR')

descriptions = [
    "Panne moteur", "Crevaison", "Problème de freins", 
    "Problème électrique", "Retard dû au trafic", "Accident mineur"
]

# Liste fixe de véhicules
vehicle_ids = [f"NUM_{i}" for i in range(1000, 1100)]

with open("incidents.csv", "w", newline='', encoding="utf-8") as file:
    writer = csv.writer(file)

    # En-têtes
    writer.writerow(['Incidents_id', 'vehicle_id', 'timestamp', 'description', 'delay_minutes', 'severity'])

    # Générer 100 incidents
    for i in range(100):
        incident_id = f"INC_{i + 300}"
        vehicle_id = random.choice(vehicle_ids)

        # Générer une date entre janvier et juin 2025
        timestamp = fake.date_time_between(
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 6, 30)
        )

        description = random.choice(descriptions)
        delay_minutes = random.randint(0, 60)
        severity = random.randint(1, 5)

        writer.writerow([incident_id, vehicle_id, timestamp, description, delay_minutes, severity])
