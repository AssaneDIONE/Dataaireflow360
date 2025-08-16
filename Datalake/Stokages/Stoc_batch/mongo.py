
# import pymongo
# import csv
# from pathlib import Path

# # Configuration
# MONGO_URI = "mongodb://localhost:27017/"  # MongoDB exposé via Docker
# DB_NAME = "testdb"
# CSV_DIR = "/home/assane-dione/Bureau/Dataaireflow360/Datalake/Stokages/Stoc_batch/"

# print(f"Tentative de connexion à: {MONGO_URI}")
# try:
#     client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
#     # Test de connexion
#     server_info = client.server_info()
#     db = client[DB_NAME]
#     print(f" Connexion MongoDB OK - Version: {server_info['version']}")
# except pymongo.errors.ServerSelectionTimeoutError as e:
#     print(f" MongoDB inaccessible: {e}")
#     print(" Vérifiez: docker logs mongodb")
#     exit(1)
# except Exception as e:
#     print(f" Erreur MongoDB: {e}")
#     exit(1)

# # Import de tous les CSV
# for csv_file in Path(CSV_DIR).glob("*.csv"):
#     collection = db[csv_file.stem]  # nom du fichier = nom collection
    
#     with open(csv_file, 'r') as f:
#         data = list(csv.DictReader(f))
#         if data:
#             collection.insert_many(data)
#             print(f" {len(data)} docs → {csv_file.stem}")

# client.close()
# print("Terminé!")

import pymongo
import csv
import os
from pathlib import Path
import time

# Détection de l'environnement
DOCKER_ENV = os.getenv('DOCKER_ENV', 'false').lower() == 'true'

if DOCKER_ENV:
    # Configuration Docker (container)
    MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
    CSV_DIR = "/app"
else:
    # Configuration locale (machine hôte)
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    CSV_DIR = "/home/assane-dione/Bureau/Dataaireflow360/Datalake/Stokages/Stoc_batch/"

MONGO_USER = os.getenv('MONGO_USER', 'root')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'monmongo')
MONGO_PORT = os.getenv('MONGO_PORT', '27017')
MONGO_DB = os.getenv('MONGO_DB', 'testdb')

# Construction de l'URI MongoDB avec authentification
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"

print(f"Environnement: {'Docker' if DOCKER_ENV else 'Local'}")
print(f"Répertoire CSV: {CSV_DIR}")

print(f"Tentative de connexion à: mongodb://{MONGO_USER}:***@{MONGO_HOST}:{MONGO_PORT}/")

# Attendre que MongoDB soit prêt (retry logic)
max_retries = 10
retry_count = 0

while retry_count < max_retries:
    try:
        print(f"Tentative de connexion {retry_count + 1}/{max_retries}...")
        client = pymongo.MongoClient(
            MONGO_URI, 
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000
        )
        
        # Test de connexion
        server_info = client.server_info()
        db = client[MONGO_DB]
        print(f"Connexion MongoDB OK - Version: {server_info['version']}")
        break
        
    except pymongo.errors.ServerSelectionTimeoutError as e:
        retry_count += 1
        print(f"MongoDB inaccessible (tentative {retry_count}): {e}")
        if retry_count < max_retries:
            print(f"Nouvelle tentative dans 5 secondes...")
            time.sleep(5)
        else:
            print("Échec de connexion après toutes les tentatives")
            print("Vérifiez: docker logs mongodb")
            exit(1)
            
    except pymongo.errors.OperationFailure as e:
        print(f"Erreur d'authentification: {e}")
        print("Vérifiez les credentials MongoDB dans votre .env")
        exit(1)
        
    except Exception as e:
        print(f"Erreur MongoDB inattendue: {e}")
        exit(1)

# Vérifier si le répertoire CSV existe
csv_path = Path(CSV_DIR)
if not csv_path.exists():
    print(f"Répertoire CSV introuvable: {CSV_DIR}")
    print(f"Contenu du répertoire parent:")
    parent_dir = csv_path.parent
    if parent_dir.exists():
        for item in parent_dir.iterdir():
            print(f"   - {item}")
    exit(1)

# Rechercher les fichiers CSV
csv_files = list(csv_path.glob("*.csv"))
if not csv_files:
    print(f"Aucun fichier CSV trouvé dans: {CSV_DIR}")
    print(f"Contenu du répertoire:")
    for item in csv_path.iterdir():
        print(f"   - {item}")
else:
    print(f"{len(csv_files)} fichier(s) CSV trouvé(s)")

# Import de tous les CSV
total_docs = 0
for csv_file in csv_files:
    try:
        collection_name = csv_file.stem
        collection = db[collection_name]
        
        print(f"Traitement: {csv_file.name}")
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            data = list(csv.DictReader(f))
            
            if data:
                # Supprimer les documents existants dans la collection (optionnel)
                # collection.delete_many({})
                
                result = collection.insert_many(data)
                inserted_count = len(result.inserted_ids)
                total_docs += inserted_count
                print(f"{inserted_count} docs → collection '{collection_name}'")
            else:
                print(f"Fichier vide: {csv_file.name}")
                
    except Exception as e:
        print(f"Erreur lors du traitement de {csv_file.name}: {e}")
        continue

print(f"\nImport terminé! Total: {total_docs} documents importés")

# Afficher un résumé des collections
try:
    collections = db.list_collection_names()
    print(f"\nCollections dans la base '{MONGO_DB}':")
    for coll_name in collections:
        count = db[coll_name].count_documents({})
        print(f"   - {coll_name}: {count} documents")
except Exception as e:
    print(f"Erreur lors de l'affichage du résumé: {e}")

client.close()
print("Connexion fermée")