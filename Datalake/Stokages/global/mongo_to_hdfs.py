#!/usr/bin/env python3

import pymongo
import json
import subprocess
import os

# Connexion MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["testdb"]

print("Export MongoDB vers HDFS...")

# Créer le répertoire HDFS
subprocess.run('docker exec namenode hdfs dfs -mkdir -p /data/mongodb', shell=True)

# Récupérer toutes les collections
collections = db.list_collection_names()
print(f"Collections: {collections}")

for collection_name in collections:
    print(f"Export {collection_name}...")
    
    collection = db[collection_name]
    documents = list(collection.find())
    
    if documents:
        # Convertir en JSON
        json_data = ""
        for doc in documents:
            doc['_id'] = str(doc['_id'])
            json_data += json.dumps(doc) + '\n'
        
        # Sauvegarder localement
        temp_file = f"./{collection_name}.json"
        with open(temp_file, 'w') as f:
            f.write(json_data)
        
        # Supprimer l'ancien fichier HDFS (éviter doublons)
        hdfs_path = f"/data/mongodb/{collection_name}.json"
        subprocess.run(f'docker exec namenode hdfs dfs -rm -f {hdfs_path}', shell=True)
        
        # Copier vers conteneur puis HDFS
        subprocess.run(f'docker cp {temp_file} namenode:/tmp/', shell=True)
        subprocess.run(f'docker exec namenode hdfs dfs -put /tmp/{collection_name}.json {hdfs_path}', shell=True)
        
        # Nettoyer
        os.remove(temp_file)
        subprocess.run(f'docker exec namenode rm -f /tmp/{collection_name}.json', shell=True)
        
        print(f"{len(documents)} documents -> {hdfs_path}")
    else:
        print(f"Collection {collection_name} vide")

print("Export terminé!")
client.close()