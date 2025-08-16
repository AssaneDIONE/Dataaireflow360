#!/usr/bin/env python3
"""
Script pour récupérer les fichiers CSV MongoDB stockés dans Hadoop HDFS
Adapté pour votre configuration docker-compose
"""

import requests
import json
import os
import pandas as pd
from io import StringIO
import subprocess

class HadoopMongoCSVRetriever:
    def __init__(self, namenode_host="localhost", namenode_port="9870"):
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.webhdfs_url = f"http://{namenode_host}:{namenode_port}/webhdfs/v1"
        
        # Répertoire où vos CSV sont stockés (selon votre script)
        self.mongodb_csv_dir = "/data/mongodb_csv"
        
    def test_connection(self):
        """Test la connexion au cluster Hadoop"""
        try:
            url = f"http://{self.namenode_host}:{self.namenode_port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print("✓ Connexion au NameNode réussie")
                return True
            else:
                print(f"✗ Erreur de connexion au NameNode: {response.status_code}")
                return False
        except Exception as e:
            print(f"✗ Impossible de se connecter au NameNode: {e}")
            return False
    
    def list_mongodb_csv_files(self):
        """Liste tous les fichiers CSV MongoDB dans HDFS"""
        try:
            url = f"{self.webhdfs_url}{self.mongodb_csv_dir}?op=LISTSTATUS"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            files = data['FileStatuses']['FileStatus']
            
            csv_files = []
            for file_info in files:
                if file_info['type'] == 'FILE' and file_info['pathSuffix'].endswith('.csv'):
                    csv_files.append({
                        'name': file_info['pathSuffix'],
                        'path': f"{self.mongodb_csv_dir}/{file_info['pathSuffix']}",
                        'size': file_info['length'],
                        'size_mb': round(file_info['length'] / (1024 * 1024), 2),
                        'modified': file_info['modificationTime'],
                        'collection': file_info['pathSuffix'].replace('.csv', '')
                    })
            
            return sorted(csv_files, key=lambda x: x['name'])
            
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la liste des fichiers: {e}")
            return []
    
    def download_csv_file(self, hdfs_path, local_path=None, as_dataframe=False):
        """Télécharge un fichier CSV depuis HDFS"""
        try:
            url = f"{self.webhdfs_url}{hdfs_path}?op=OPEN"
            response = requests.get(url, allow_redirects=True, timeout=30)
            response.raise_for_status()
            
            if as_dataframe:
                # Retourne directement un DataFrame pandas
                content = response.content.decode('utf-8')
                df = pd.read_csv(StringIO(content))
                return df
            
            elif local_path:
                # Sauvegarde dans un fichier local
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                print(f"✓ Fichier téléchargé: {local_path}")
                return local_path
            
            else:
                # Retourne le contenu brut
                return response.content.decode('utf-8')
                
        except Exception as e:
            print(f"✗ Erreur lors du téléchargement de {hdfs_path}: {e}")
            return None
    
    def preview_csv(self, hdfs_path, num_rows=10):
        """Prévisualise un fichier CSV"""
        df = self.download_csv_file(hdfs_path, as_dataframe=True)
        if df is not None:
            print(f"\n📊 Aperçu du fichier: {hdfs_path}")
            print(f"📈 Dimensions: {df.shape[0]} lignes × {df.shape[1]} colonnes")
            print(f"📋 Colonnes: {', '.join(df.columns.tolist())}")
            print("\n🔍 Premiers enregistrements:")
            print(df.head(num_rows).to_string(index=False))
            
            # Informations sur les types de données
            print(f"\n📊 Types de données:")
            for col, dtype in df.dtypes.items():
                non_null = df[col].notna().sum()
                print(f"  {col}: {dtype} ({non_null}/{len(df)} valeurs non-nulles)")
            
            return df
        return None
    
    def get_file_info_via_docker(self):
        """Alternative: utilise docker exec pour obtenir les infos"""
        try:
            result = subprocess.run(
                f'docker exec namenode hdfs dfs -ls {self.mongodb_csv_dir}',
                shell=True,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                print("📁 Fichiers trouvés via docker exec:")
                print(result.stdout)
                return True
            else:
                print(f"Erreur docker exec: {result.stderr}")
                return False
        except Exception as e:
            print(f"Erreur docker exec: {e}")
            return False

def main():
    """Fonction principale pour récupérer les fichiers CSV MongoDB"""
    
    print("=" * 60)
    print("   RÉCUPÉRATION DES FICHIERS CSV MONGODB DEPUIS HADOOP")
    print("=" * 60)
    
    # Initialisation
    retriever = HadoopMongoCSVRetriever()
    
    # Test de connexion
    if not retriever.test_connection():
        print("\n⚠️  Vérifications à faire:")
        print("   1. Les conteneurs Hadoop sont-ils démarrés?")
        print("      → docker-compose ps")
        print("   2. Le NameNode est-il accessible?")
        print("      → http://localhost:9870")
        
        # Tentative avec docker exec
        print("\n🔄 Tentative alternative via docker exec...")
        if not retriever.get_file_info_via_docker():
            return
    
    # Recherche des fichiers CSV
    print(f"\n🔍 Recherche des fichiers CSV MongoDB dans {retriever.mongodb_csv_dir}...")
    csv_files = retriever.list_mongodb_csv_files()
    
    if not csv_files:
        print("❌ Aucun fichier CSV trouvé.")
        
        # Suggestions de dépannage
        print("\n🛠️  Suggestions:")
        print("   1. Vérifiez que mongo_to_hdfs.py s'est bien exécuté")
        print("   2. Vérifiez le répertoire HDFS:")
        print(f"      → docker exec namenode hdfs dfs -ls {retriever.mongodb_csv_dir}")
        print("   3. Listez tous les répertoires:")
        print("      → docker exec namenode hdfs dfs -ls /")
        
        # Essayer de lister le répertoire racine
        try:
            root_files_url = f"{retriever.webhdfs_url}/?op=LISTSTATUS"
            response = requests.get(root_files_url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                dirs = [f['pathSuffix'] for f in data['FileStatuses']['FileStatus'] if f['type'] == 'DIRECTORY']
                print(f"\n📁 Répertoires disponibles dans HDFS: {dirs}")
        except:
            pass
        
        return
    
    # Affichage des fichiers trouvés
    print(f"\n🎉 {len(csv_files)} fichier(s) CSV trouvé(s):")
    print("-" * 80)
    
    for i, file_info in enumerate(csv_files, 1):
        print(f"{i:2}. 📄 {file_info['name']}")
        print(f"     Collection MongoDB: {file_info['collection']}")
        print(f"     Taille: {file_info['size_mb']} MB ({file_info['size']} bytes)")
        print(f"     Chemin HDFS: {file_info['path']}")
        print()
    
    # Menu interactif
    while True:
        print("\n" + "=" * 60)
        print("OPTIONS DISPONIBLES:")
        print("=" * 60)
        print("1. 👁️  Prévisualiser un fichier")
        print("2. ⬇️  Télécharger un fichier spécifique")
        print("3. 📦 Télécharger tous les fichiers")
        print("4. 📊 Analyser un fichier (statistiques)")
        print("5. 🔍 Rechercher dans un fichier")
        print("6. ❌ Quitter")
        
        choice = input(f"\n➤ Choisissez une option (1-6): ").strip()
        
        if choice == "1":
            # Prévisualisation
            try:
                file_num = int(input(f"Numéro du fichier (1-{len(csv_files)}): ")) - 1
                if 0 <= file_num < len(csv_files):
                    file_info = csv_files[file_num]
                    retriever.preview_csv(file_info['path'])
                else:
                    print("❌ Numéro de fichier invalide")
            except ValueError:
                print("❌ Veuillez entrer un nombre valide")
        
        elif choice == "2":
            # Téléchargement d'un fichier
            try:
                file_num = int(input(f"Numéro du fichier (1-{len(csv_files)}): ")) - 1
                if 0 <= file_num < len(csv_files):
                    file_info = csv_files[file_num]
                    local_filename = f"./downloaded_{file_info['name']}"
                    
                    result = retriever.download_csv_file(file_info['path'], local_filename)
                    if result:
                        print(f"✅ Téléchargement réussi: {local_filename}")
                else:
                    print("❌ Numéro de fichier invalide")
            except ValueError:
                print("❌ Veuillez entrer un nombre valide")
        
        elif choice == "3":
            # Téléchargement de tous les fichiers
            download_dir = "./mongodb_csv_downloads"
            os.makedirs(download_dir, exist_ok=True)
            
            print(f"\n📥 Téléchargement de tous les fichiers dans: {download_dir}")
            success_count = 0
            
            for file_info in csv_files:
                local_path = os.path.join(download_dir, file_info['name'])
                result = retriever.download_csv_file(file_info['path'], local_path)
                if result:
                    success_count += 1
            
            print(f"\n✅ {success_count}/{len(csv_files)} fichiers téléchargés avec succès")
        
        elif choice == "4":
            # Analyse statistique
            try:
                file_num = int(input(f"Numéro du fichier (1-{len(csv_files)}): ")) - 1
                if 0 <= file_num < len(csv_files):
                    file_info = csv_files[file_num]
                    df = retriever.download_csv_file(file_info['path'], as_dataframe=True)
                    
                    if df is not None:
                        print(f"\n📊 ANALYSE STATISTIQUE: {file_info['name']}")
                        print("=" * 50)
                        print(f"📈 Forme: {df.shape}")
                        print(f"📋 Colonnes: {list(df.columns)}")
                        print(f"📊 Informations générales:")
                        print(df.info())
                        print(f"\n📊 Statistiques descriptives:")
                        print(df.describe())
                else:
                    print("❌ Numéro de fichier invalide")
            except ValueError:
                print("❌ Veuillez entrer un nombre valide")
        
        elif choice == "5":
            # Recherche dans un fichier
            try:
                file_num = int(input(f"Numéro du fichier (1-{len(csv_files)}): ")) - 1
                if 0 <= file_num < len(csv_files):
                    file_info = csv_files[file_num]
                    search_term = input("Terme à rechercher: ").strip()
                    
                    if search_term:
                        df = retriever.download_csv_file(file_info['path'], as_dataframe=True)
                        if df is not None:
                            # Recherche dans toutes les colonnes de type string
                            mask = df.astype(str).apply(
                                lambda x: x.str.contains(search_term, case=False, na=False)
                            ).any(axis=1)
                            
                            results = df[mask]
                            print(f"\n🔍 Résultats pour '{search_term}': {len(results)} ligne(s)")
                            if len(results) > 0:
                                print(results.head(20).to_string(index=False))
                            else:
                                print("Aucun résultat trouvé")
                else:
                    print("❌ Numéro de fichier invalide")
            except ValueError:
                print("❌ Veuillez entrer un nombre valide")
        
        elif choice == "6":
            print("\n👋 Au revoir!")
            break
        
        else:
            print("❌ Option invalide")

if __name__ == "__main__":
    # Vérification des dépendances
    try:
        import pandas as pd
        import requests
    except ImportError:
        print("📦 Installation des dépendances requises...")
        os.system("pip install pandas requests")
    
    main()