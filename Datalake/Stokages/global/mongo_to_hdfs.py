import pymongo
import json
import subprocess

# Connexion MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["testdb"]  # Changez le nom de la DB ici

print("Récupération des données MongoDB...")

# Récupérer toutes les collections
collections = db.list_collection_names()
print(f"Collections trouvées: {collections}")

for collection_name in collections:
    print(f"Export de {collection_name} vers HDFS...")
    
    collection = db[collection_name]
    documents = list(collection.find())
    
    if documents:
        # Convertir en JSON Lines
        json_data = ""
        for doc in documents:
            doc['_id'] = str(doc['_id'])
            json_data += json.dumps(doc) + '\n'
        
        # Écrire directement vers HDFS avec hdfs dfs -put
        hdfs_path = f"/user/data/mongodb/{collection_name}.json"
        
        # Utiliser echo + pipe vers HDFS
        cmd = f'echo "{json_data}" | docker exec -i namenode hdfs dfs -put - {hdfs_path}'
        
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"{len(documents)} documents transférés vers HDFS: {hdfs_path}")
            else:
                print(f"Erreur HDFS: {result.stderr}")
        except Exception as e:
            print(f"Erreur lors du transfert: {e}")
    else:
        print(f" Collection {collection_name} vide")

print("Export direct vers HDFS terminé!")
client.close()