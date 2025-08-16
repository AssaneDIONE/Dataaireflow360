# # #!/usr/bin/env python3

# # import pymongo
# # import json
# # import subprocess
# # import os

# # # Connexion MongoDB
# # client = pymongo.MongoClient("mongodb://localhost:27017/")
# # db = client["testdb"]

# # print("Export MongoDB vers HDFS...")

# # # Créer le répertoire HDFS
# # subprocess.run('docker exec namenode hdfs dfs -mkdir -p /data/mongodb', shell=True)

# # # Récupérer toutes les collections
# # collections = db.list_collection_names()
# # print(f"Collections: {collections}")

# # for collection_name in collections:
# #     print(f"Export {collection_name}...")
    
# #     collection = db[collection_name]
# #     documents = list(collection.find())
    
# #     if documents:
# #         # Convertir en JSON
# #         json_data = ""
# #         for doc in documents:
# #             doc['_id'] = str(doc['_id'])
# #             json_data += json.dumps(doc) + '\n'
        
# #         # Sauvegarder localement
# #         temp_file = f"./{collection_name}.json"
# #         with open(temp_file, 'w') as f:
# #             f.write(json_data)
        
# #         # Supprimer l'ancien fichier HDFS (éviter doublons)
# #         hdfs_path = f"/data/mongodb/{collection_name}.json"
# #         subprocess.run(f'docker exec namenode hdfs dfs -rm -f {hdfs_path}', shell=True)
        
# #         # Copier vers conteneur puis HDFS
# #         subprocess.run(f'docker cp {temp_file} namenode:/tmp/', shell=True)
# #         subprocess.run(f'docker exec namenode hdfs dfs -put /tmp/{collection_name}.json {hdfs_path}', shell=True)
        
# #         # Nettoyer
# #         os.remove(temp_file)
# #         subprocess.run(f'docker exec namenode rm -f /tmp/{collection_name}.json', shell=True)
        
# #         print(f"{len(documents)} documents -> {hdfs_path}")
# #     else:
# #         print(f"Collection {collection_name} vide")

# # print("Export terminé!")
# # client.close()

# #!/usr/bin/env python3

# import pymongo
# import json
# import subprocess
# import os
# import time
# import sys

# def wait_for_service(host, port, service_name, max_retries=30):
#     """Attend qu'un service soit disponible"""
#     import socket
#     for i in range(max_retries):
#         try:
#             sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             sock.settimeout(2)
#             result = sock.connect_ex((host, port))
#             sock.close()
#             if result == 0:
#                 print(f"✓ {service_name} est disponible")
#                 return True
#         except:
#             pass
#         print(f"Attente de {service_name}... ({i+1}/{max_retries})")
#         time.sleep(5)
#     return False

# def main():
#     # Configuration depuis les variables d'environnement
#     MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
#     MONGO_USER = os.getenv('MONGO_USER', 'root')
#     MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'monmongo')
#     MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))
    
#     # Base de données MongoDB (changez selon vos besoins)
#     MONGO_DB = os.getenv('MONGO_DB', 'testdb')
    
#     print("=" * 50)
#     print("Export MongoDB vers HDFS")
#     print("=" * 50)
    
#     # Attendre que MongoDB soit disponible
#     if not wait_for_service(MONGO_HOST, MONGO_PORT, 'MongoDB'):
#         print("Impossible de se connecter à MongoDB")
#         sys.exit(1)
    
#     # Attendre que Namenode soit disponible
#     if not wait_for_service('namenode', 9870, 'Hadoop Namenode'):
#         print("Impossible de se connecter à Hadoop Namenode")
#         sys.exit(1)
    
#     # Connexion MongoDB avec authentification
#     try:
#         mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
#         client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        
#         # Test de connexion
#         client.admin.command('ping')
#         print(f"Connecté à MongoDB: {mongo_uri}")
        
#         db = client[MONGO_DB]
#         print(f"Base de données: {MONGO_DB}")
        
#     except Exception as e:
#         print(f"Erreur de connexion MongoDB: {e}")
#         sys.exit(1)
    
#     # Créer le répertoire HDFS
#     try:
#         print("Création du répertoire HDFS...")
#         result = subprocess.run(
#             'docker exec namenode hdfs dfs -mkdir -p /data/mongodb', 
#             shell=True, 
#             capture_output=True, 
#             text=True
#         )
#         if result.returncode != 0 and "File exists" not in result.stderr:
#             print(f"Avertissement HDFS mkdir: {result.stderr}")
#     except Exception as e:
#         print(f"Erreur création répertoire HDFS: {e}")
#         sys.exit(1)
    
#     # Récupérer toutes les collections
#     try:
#         collections = db.list_collection_names()
#         print(f"Collections trouvées: {collections}")
        
#         if not collections:
#             print("Aucune collection trouvée dans la base de données")
#             return
            
#     except Exception as e:
#         print(f"Erreur lors de la récupération des collections: {e}")
#         sys.exit(1)
    
#     # Export de chaque collection
#     total_documents = 0
    
#     for collection_name in collections:
#         print(f"\nExport de la collection: {collection_name}")
        
#         try:
#             collection = db[collection_name]
#             documents = list(collection.find())
            
#             if not documents:
#                 print(f"  Collection {collection_name} est vide")
#                 continue
            
#             # Convertir en JSON Lines (une ligne par document)
#             json_lines = []
#             for doc in documents:
#                 # Convertir ObjectId en string
#                 if '_id' in doc:
#                     doc['_id'] = str(doc['_id'])
#                 json_lines.append(json.dumps(doc, ensure_ascii=False))
            
#             json_data = '\n'.join(json_lines)
            
#             # Sauvegarder localement
#             temp_file = f"/tmp/{collection_name}.json"
#             with open(temp_file, 'w', encoding='utf-8') as f:
#                 f.write(json_data)
            
#             print(f"  Fichier temporaire créé: {temp_file}")
            
#             # Chemin HDFS
#             hdfs_path = f"/data/mongodb/{collection_name}.json"
            
#             # Supprimer l'ancien fichier HDFS si existe
#             subprocess.run(
#                 f'docker exec namenode hdfs dfs -rm -f {hdfs_path}', 
#                 shell=True, 
#                 capture_output=True
#             )
            
#             # Copier vers le conteneur namenode
#             copy_result = subprocess.run(
#                 f'docker cp {temp_file} namenode:/tmp/{collection_name}.json', 
#                 shell=True, 
#                 capture_output=True, 
#                 text=True
#             )
            
#             if copy_result.returncode != 0:
#                 print(f"  Erreur copie vers namenode: {copy_result.stderr}")
#                 continue
            
#             # Copier vers HDFS
#             hdfs_result = subprocess.run(
#                 f'docker exec namenode hdfs dfs -put /tmp/{collection_name}.json {hdfs_path}', 
#                 shell=True, 
#                 capture_output=True, 
#                 text=True
#             )
            
#             if hdfs_result.returncode != 0:
#                 print(f"  Erreur copie vers HDFS: {hdfs_result.stderr}")
#                 continue
            
#             # Nettoyer les fichiers temporaires
#             os.remove(temp_file)
#             subprocess.run(
#                 f'docker exec namenode rm -f /tmp/{collection_name}.json', 
#                 shell=True, 
#                 capture_output=True
#             )
            
#             print(f" {len(documents)} documents exportés vers {hdfs_path}")
#             total_documents += len(documents)
            
#         except Exception as e:
#             print(f"Erreur lors de l'export de {collection_name}: {e}")
#             continue
    
#     # Vérification finale
#     print(f"\nExport terminé!")
#     print(f"Total: {total_documents} documents exportés")
    
#     # Lister les fichiers dans HDFS pour vérification
#     try:
#         print("\nContenu du répertoire HDFS /data/mongodb:")
#         result = subprocess.run(
#             'docker exec namenode hdfs dfs -ls /data/mongodb', 
#             shell=True, 
#             capture_output=True, 
#             text=True
#         )
#         if result.returncode == 0:
#             print(result.stdout)
#         else:
#             print(f"Impossible de lister le répertoire: {result.stderr}")
#     except Exception as e:
#         print(f"Erreur lors de la vérification: {e}")
    
#     client.close()
#     print("\nConnexion MongoDB fermée")

# if __name__ == "__main__":
#     main()

#!/usr/bin/env python3
"""
Script simple pour exporter les collections MongoDB vers HDFS en format CSV
"""

import pymongo
import csv
import subprocess
import os
import time
import sys
from io import StringIO

def main():
    print("=" * 50)
    print(" EXPORT MONGODB → HDFS (CSV)")
    print("=" * 50)
    
    # Configuration
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    MONGO_USER = os.getenv('MONGO_USER', 'root')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'monmongo')
    MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))
    MONGO_DB = os.getenv('MONGO_DB', 'testdb')
    
    print(f" MongoDB: {MONGO_HOST}:{MONGO_PORT}")
    print(f"  Database: {MONGO_DB}")
    print(f" User: {MONGO_USER}")
    
    # 1. Connexion MongoDB
    print(f"\n Connexion à MongoDB...")
    try:
        mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        
        # Test de connexion
        client.admin.command('ping')
        db = client[MONGO_DB]
        
        print(f" Connexion MongoDB réussie")
        
    except Exception as e:
        print(f" Erreur MongoDB: {e}")
        sys.exit(1)
    
    # 2. Récupérer les collections
    print(f"\n Récupération des collections...")
    try:
        collections = db.list_collection_names()
        if not collections:
            print("  Aucune collection trouvée")
            client.close()
            return
        
        print(f" Collections trouvées: {collections}")
        
    except Exception as e:
        print(f" Erreur: {e}")
        client.close()
        sys.exit(1)
    
    # 3. Test HDFS
    print(f"\n Test HDFS...")
    try:
        result = subprocess.run(
            'docker exec namenode hdfs dfs -ls /', 
            shell=True, 
            capture_output=True, 
            text=True,
            timeout=5
        )
        if result.returncode != 0:
            print(f" HDFS inaccessible: {result.stderr}")
            client.close()
            sys.exit(1)
        print(" HDFS accessible")
    except Exception as e:
        print(f" Erreur HDFS: {e}")
        client.close()
        sys.exit(1)
    
    # 4. Créer répertoire HDFS
    hdfs_dir = "/data/mongodb_csv"
    print(f"\n Création répertoire HDFS: {hdfs_dir}")
    try:
        subprocess.run(
            f'docker exec namenode hdfs dfs -mkdir -p {hdfs_dir}', 
            shell=True, 
            timeout=10
        )
        print(" Répertoire HDFS prêt")
    except Exception as e:
        print(f" Erreur création répertoire: {e}")
    
    # 5. Export des collections
    total_docs = 0
    success_count = 0
    
    for collection_name in collections:
        print(f"\n Export: {collection_name}")
        
        try:
            # Récupérer les documents
            collection = db[collection_name]
            documents = list(collection.find())
            
            if not documents:
                print(f" Collection vide")
                continue
            
            print(f" {len(documents)} documents trouvés")
            
            # Préparer les données pour CSV
            processed_docs = []
            all_keys = set()
            
            for doc in documents:
                processed_doc = {}
                for key, value in doc.items():
                    all_keys.add(key)
                    if key == '_id':
                        processed_doc[key] = str(value)
                    elif hasattr(value, 'isoformat'):  # datetime
                        processed_doc[key] = value.isoformat()
                    elif isinstance(value, (dict, list)):
                        processed_doc[key] = str(value)
                    else:
                        processed_doc[key] = str(value) if value is not None else ''
                processed_docs.append(processed_doc)
            
            # Créer CSV
            fieldnames = sorted(list(all_keys))
            csv_buffer = StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            
            for doc in processed_docs:
                # S'assurer que tous les champs sont présents
                row = {field: doc.get(field, '') for field in fieldnames}
                writer.writerow(row)
            
            csv_data = csv_buffer.getvalue()
            csv_buffer.close()
            
            print(f" CSV généré ({len(csv_data)} caractères)")
            print(f" Colonnes: {', '.join(fieldnames[:5])}{'...' if len(fieldnames) > 5 else ''} ({len(fieldnames)} total)")
            
            # Sauvegarder temporairement
            temp_file = f"/tmp/{collection_name}_{int(time.time())}.csv"
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(csv_data)
            
            print(f" Fichier temporaire: {temp_file}")
            
            # Paths HDFS
            container_file = f"/tmp/{collection_name}.csv"
            hdfs_file = f"{hdfs_dir}/{collection_name}.csv"
            
            # Supprimer ancien fichier HDFS
            subprocess.run(
                f'docker exec namenode hdfs dfs -rm -f {hdfs_file}', 
                shell=True, 
                capture_output=True
            )
            
            # Copier vers namenode
            copy_cmd = f'docker cp {temp_file} namenode:{container_file}'
            copy_result = subprocess.run(copy_cmd, shell=True, capture_output=True, text=True)
            
            if copy_result.returncode != 0:
                print(f" Erreur copie vers namenode: {copy_result.stderr}")
                os.remove(temp_file)
                continue
            
            print(f" Copié vers namenode")
            
            # Copier vers HDFS
            hdfs_cmd = f'docker exec namenode hdfs dfs -put {container_file} {hdfs_file}'
            hdfs_result = subprocess.run(hdfs_cmd, shell=True, capture_output=True, text=True)
            
            if hdfs_result.returncode != 0:
                print(f" Erreur HDFS: {hdfs_result.stderr}")
                os.remove(temp_file)
                continue
            
            # Nettoyage
            os.remove(temp_file)
            subprocess.run(
                f'docker exec namenode rm -f {container_file}', 
                shell=True, 
                capture_output=True
            )
            
            print(f" Export réussi → {hdfs_file}")
            total_docs += len(documents)
            success_count += 1
            
        except Exception as e:
            print(f" Erreur export {collection_name}: {e}")
            continue
    
    # 6. Rapport final
    print(f"\n" + "=" * 50)
    print(f"RAPPORT FINAL")
    print(f"=" * 50)
    print(f"Collections exportées: {success_count}/{len(collections)}")
    print(f" Total documents: {total_docs}")
    
    # Lister les fichiers HDFS
    try:
        print(f"\n Fichiers CSV dans HDFS:")
        result = subprocess.run(
            f'docker exec namenode hdfs dfs -ls {hdfs_dir}', 
            shell=True, 
            capture_output=True, 
            text=True
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if '.csv' in line:
                    parts = line.split()
                    if len(parts) >= 8:
                        size = parts[4]
                        filename = parts[7]
                        print(f"    {filename.split('/')[-1]} ({size} bytes)")
        else:
            print(f" Erreur listage: {result.stderr}")
    except Exception as e:
        print(f" Erreur: {e}")
    
    # Fermer connexion
    client.close()
    print(f"\n Export terminé!")

if __name__ == "__main__":
    main()