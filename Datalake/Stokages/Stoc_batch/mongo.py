import pymongo
import csv

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["testdb"]
collection = db["users"]

# Lire le CSV et insérer les données
with open('/Dataaireflow360/Datalake/Stokages/Stoc_batch/users.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    users = [row for row in reader]
    collection.insert_many(users)

print("Données insérées dans MongoDB avec succès.")
