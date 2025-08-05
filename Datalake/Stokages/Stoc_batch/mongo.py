
import pymongo
import csv
from pathlib import Path

# Configuration
MONGO_URI = "mongodb://localhost:27017/"  # MongoDB exposé via Docker
DB_NAME = "testdb"
CSV_DIR = "/home/assane-dione/Bureau/Dataaireflow360/Datalake/Stokages/Stoc_batch/"

print(f"🔍 Tentative de connexion à: {MONGO_URI}")
try:
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    # Test de connexion
    server_info = client.server_info()
    db = client[DB_NAME]
    print(f" Connexion MongoDB OK - Version: {server_info['version']}")
except pymongo.errors.ServerSelectionTimeoutError as e:
    print(f" MongoDB inaccessible: {e}")
    print(" Vérifiez: docker logs mongodb")
    exit(1)
except Exception as e:
    print(f" Erreur MongoDB: {e}")
    exit(1)

# Import de tous les CSV
for csv_file in Path(CSV_DIR).glob("*.csv"):
    collection = db[csv_file.stem]  # nom du fichier = nom collection
    
    with open(csv_file, 'r') as f:
        data = list(csv.DictReader(f))
        if data:
            collection.insert_many(data)
            print(f" {len(data)} docs → {csv_file.stem}")

client.close()
print("Terminé!")