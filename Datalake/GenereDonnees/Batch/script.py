import random
import csv

# Ouvre (ou crée) un fichier CSV en mode écriture
with open('vehiculestranspot.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # Écrit l'en-tête du fichier CSV
    writer.writerow(['vehicle_id', 'types_transport', 'name_company', 'types_fuel', 'status'])

    # Listes possibles de valeurs pour chaque attribut
    types_transport = ['Bus', 'Taxi', 'Car rapide', 'Minibus']
    companies = ['Dakar Dem Dikk', 'SENBUS', 'ALLO Taxi', 'TATA', 'Clando Express']
    fuels = ['Diesel', 'Essence', 'Électrique']
    statuses = ['En service', 'En panne', 'Garé']

    # Génération de 50 lignes de données simulées
    for i in range(100):
        # Choisir un type de transport au hasard
        t_type = random.choice(types_transport)

        # Générer un ID unique basé sur le type et l'indice
        vehicle_id = f"{"VH"}_{100 + i}"

        # Choisir une entreprise, un type de carburant et un statut aléatoire
        company = random.choice(companies)
        fuel = random.choice(fuels)
        status = random.choice(statuses)

        # Écrire la ligne dans le fichier CSV
        writer.writerow([vehicle_id, t_type, company, fuel, status])
