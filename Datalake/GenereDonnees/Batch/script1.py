import csv
import random
from faker import Faker

# Initialiser Faker
fake = Faker('fr_FR')  # français pour des noms plus locaux

# Liste de types de transport
types_transport = [
    "Bus interurbain", "Taxi-Clin", "Minicar", "Car rapide",
]

# Capacités possibles par type
capacites_par_type = {
    "Bus interurbain": [60, 70, 80],
    "Taxi-Clin": [4, 7, 8],
    "Minicar": [20, 25, 30],
    "Car rapide": [20, 25, 30]
}

# Liste d'entreprises de transport au Sénégal
companies_senegal = [
    "SEN_transport", "Touba Transport", "Sen Transit", "Transcap",
    "Tivaouane Express", "Keur Transport", "SenMobil"
]

# Types de carburant
fuel_types = ["Diesel", "Essence", "Hybride"]

# Statuts des véhicules
statuses = ["actif", "en maintenance", "hors service"]

# Créer le fichier CSV
with open('OTO_transport.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # En-têtes des colonnes
    writer.writerow(['vehicle_id', 'type_transport', 'company_name', 'capacity', 'fuel_type', 'status'])

    # Générer 100 lignes de données
    for i in range(100):
        vehicle_id = f"NUM_{1000 + i}"
        type_transport = random.choice(types_transport)
        company_name = random.choice(companies_senegal)
        capacity = random.choice(capacites_par_type[type_transport])
        fuel_type = random.choice(fuel_types)
        status = random.choice(statuses)

        # Écrire la ligne
        writer.writerow([vehicle_id, type_transport, company_name, capacity, fuel_type, status])
