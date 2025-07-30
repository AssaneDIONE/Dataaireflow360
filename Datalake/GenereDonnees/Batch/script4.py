import csv
import random
from faker import Faker

fake = Faker('fr_FR')  # Pour générer des noms réalistes

# Zones possibles
zones = ['Zone Nord', 'Centre-ville', 'Banlieue Ouest', 'Zone Sud']

# Création du fichier CSV
with open('stations.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # Écrire les en-têtes
    writer.writerow(['stop_id', 'name', 'latitude', 'longitude', 'zone', 'shelter'])

    # Générer 50 stations
    for i in range(100):
        stop_id = f"STOP_{str(i+1).zfill(3)}"
        name = f"Terminus {fake.street_name()}"
        latitude = round(fake.latitude(), 6)
        longitude = round(fake.longitude(), 6)
        zone = random.choice(zones)
        shelter = random.choice([True, False])

        # Écrire la ligne
        writer.writerow([stop_id, name, latitude, longitude, zone, shelter])
