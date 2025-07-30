import csv
import random
from faker import Faker

# Initialiser Faker
fake = Faker()

# Créer le fichier CSV
with open('vehicules_transport.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # En-têtes des colonnes
    writer.writerow(['vehicle_id', 'type_transport', 'company_name','capacity', 'fuel_type', 'status'])

    # Générer les données
    for i in range(100):
        vehicle_id = f"VEH_{1000 + i}"
        type_transport = fake.job()  # Utilisé ici à défaut (remplaçable si besoin)
        company_name = fake.company()
        capacity = fake.random_int(min=10, max=100)
        fuel_type = fake.word()      # Génère un mot aléatoire
        status = fake.word()         # Génère un mot aléatoire

        # Écrire la ligne
        writer.writerow([vehicle_id, type_transport, company_name, capacity, fuel_type, status])
