#!/usr/bin/env python3
"""
Script pour récupérer les fichiers CSV MongoDB stockés dans Hadoop HDFS
Converti pour utiliser PySpark au lieu de pandas
"""

import requests
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import subprocess

class HadoopMongoSparkCSVRetriever:
    def __init__(self, namenode_host="localhost", namenode_port="9870"):
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.webhdfs_url = f"http://{namenode_host}:{namenode_port}/webhdfs/v1"
        
        # Répertoire où vos CSV sont stockés (selon votre script)
        self.mongodb_csv_dir = "/data/mongodb_csv"
        
        # Initialisation de SparkSession
        self.spark = SparkSession.builder \
            .appName("HadoopMongoCSVRetriever") \
            .config("spark.hadoop.fs.defaultFS", f"hdfs://{namenode_host}:9000") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Réduire le niveau de log pour éviter trop de verbosité
        self.spark.sparkContext.setLogLevel("WARN")
        
    def __del__(self):
        """Ferme la session Spark à la fin"""
        if hasattr(self, 'spark'):
            self.spark.stop()
        
    def test_connection(self):
        """Test la connexion au cluster Hadoop"""
        try:
            url = f"http://{self.namenode_host}:{self.namenode_port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print("✓ Connexion au NameNode réussie")
                
                # Test aussi la connexion Spark
                try:
                    # Test simple avec Spark
                    test_df = self.spark.range(1)
                    test_df.count()
                    print("✓ SparkSession initialisée avec succès")
                    return True
                except Exception as e:
                    print(f"✗ Erreur SparkSession: {e}")
                    return False
                    
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
                    # Calcul de la taille en MB sans utiliser round() pour éviter le conflit avec Spark
                    size_mb = int((file_info['length'] / (1024 * 1024)) * 100) / 100  # Équivalent de round(x, 2)
                    
                    csv_files.append({
                        'name': file_info['pathSuffix'],
                        'path': f"{self.mongodb_csv_dir}/{file_info['pathSuffix']}",
                        'hdfs_path': f"hdfs://{self.namenode_host}:9000{self.mongodb_csv_dir}/{file_info['pathSuffix']}",
                        'size': file_info['length'],
                        'size_mb': size_mb,
                        'modified': file_info['modificationTime'],
                        'collection': file_info['pathSuffix'].replace('.csv', '')
                    })
            
            return sorted(csv_files, key=lambda x: x['name'])
            
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la liste des fichiers: {e}")
            return []
    
    def read_csv_with_spark(self, hdfs_path):
        """Lit un fichier CSV avec Spark et retourne un DataFrame Spark"""
        try:
            # Construction du chemin HDFS complet
            full_hdfs_path = f"hdfs://{self.namenode_host}:9000{hdfs_path}"
            
            # Lecture avec Spark
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiline", "true") \
                .option("escape", '"') \
                .csv(full_hdfs_path)
            
            return df
            
        except Exception as e:
            print(f"✗ Erreur lors de la lecture avec Spark de {hdfs_path}: {e}")
            return None
    
    def preview_csv(self, hdfs_path, num_rows=10):
        """Prévisualise un fichier CSV avec Spark"""
        df = self.read_csv_with_spark(hdfs_path)
        if df is not None:
            print(f"\nAperçu du fichier: {hdfs_path}")
            
            # Comptage des lignes (peut être lent sur de gros fichiers)
            try:
                row_count = df.count()
                col_count = len(df.columns)
                print(f"Dimensions: {row_count} lignes × {col_count} colonnes")
            except:
                print(f"Colonnes: {len(df.columns)} colonnes (comptage des lignes omis pour la performance)")
                
            print(f"Colonnes: {', '.join(df.columns)}")
            
            print(f"\nPremiers enregistrements:")
            df.show(num_rows, truncate=False)
            
            # Informations sur les types de données
            print(f"\nTypes de données:")
            for field in df.schema.fields:
                print(f"  {field.name}: {field.dataType}")
            
            # Schema complet
            print(f"\nSchéma complet:")
            df.printSchema()
            
            return df
        return None
    
    def analyze_csv_statistics(self, hdfs_path):
        """Analyse statistique d'un fichier CSV avec Spark"""
        df = self.read_csv_with_spark(hdfs_path)
        if df is not None:
            print(f"\nANALYSE STATISTIQUE")
            print("=" * 50)
            
            # Informations générales
            print(f"Nombre de colonnes: {len(df.columns)}")
            print(f"Colonnes: {df.columns}")
            
            try:
                row_count = df.count()
                print(f"Nombre de lignes: {row_count}")
            except:
                print("Nombre de lignes: (non calculé pour des raisons de performance)")
            
            # Statistiques descriptives pour les colonnes numériques
            numeric_columns = [field.name for field in df.schema.fields 
                             if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType))]
            
            if numeric_columns:
                print(f"\nStatistiques descriptives (colonnes numériques):")
                df.select(numeric_columns).describe().show()
            
            # Compter les valeurs nulles
            print(f"\nValeurs nulles par colonne:")
            null_counts = []
            for col in df.columns:
                null_count = df.filter(df[col].isNull()).count()
                null_counts.append((col, null_count))
                print(f"  {col}: {null_count}")
            
            return df
        return None
    
    def search_in_csv(self, hdfs_path, search_term):
        """Recherche dans un fichier CSV avec Spark"""
        df = self.read_csv_with_spark(hdfs_path)
        if df is not None and search_term:
            print(f"\nRecherche de '{search_term}' dans le fichier")
            
            # Créer une condition de recherche pour toutes les colonnes string
            search_condition = None
            string_columns = [field.name for field in df.schema.fields 
                            if isinstance(field.dataType, StringType)]
            
            for col in string_columns:
                condition = df[col].contains(search_term)
                if search_condition is None:
                    search_condition = condition
                else:
                    search_condition = search_condition | condition
            
            if search_condition is not None:
                results = df.filter(search_condition)
                result_count = results.count()
                
                print(f"Résultats trouvés: {result_count} ligne(s)")
                if result_count > 0:
                    print("Premiers résultats:")
                    results.show(20, truncate=False)
                else:
                    print("Aucun résultat trouvé")
                
                return results
            else:
                print("Aucune colonne string trouvée pour la recherche")
                return None
        return None
    
    def save_spark_df_to_local(self, spark_df, local_path, format="csv"):
        """Sauvegarde un DataFrame Spark vers un fichier local"""
        try:
            if format.lower() == "csv":
                # Collecter toutes les données vers le driver (attention à la mémoire)
                pandas_df = spark_df.toPandas()
                pandas_df.to_csv(local_path, index=False)
                print(f"DataFrame sauvegardé en CSV: {local_path}")
            elif format.lower() == "parquet":
                # Sauvegarde en parquet (plus efficace)
                spark_df.write.mode("overwrite").parquet(local_path)
                print(f"DataFrame sauvegardé en Parquet: {local_path}")
            else:
                print(f"Format non supporté: {format}")
                return False
            
            return True
            
        except Exception as e:
            print(f"Erreur lors de la sauvegarde: {e}")
            return False
    
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
                print("Fichiers trouvés via docker exec:")
                print(result.stdout)
                return True
            else:
                print(f"Erreur docker exec: {result.stderr}")
                return False
        except Exception as e:
            print(f"Erreur docker exec: {e}")
            return False

def main():
    """Fonction principale pour récupérer les fichiers CSV MongoDB avec Spark"""
    
    print("=" * 70)
    print("   RÉCUPÉRATION DES FICHIERS CSV MONGODB AVEC PYSPARK")
    print("=" * 70)
    
    # Initialisation
    retriever = HadoopMongoSparkCSVRetriever()
    
    # Test de connexion
    if not retriever.test_connection():
        print("\nVérifications à faire:")
        print("   1. Les conteneurs Hadoop sont-ils démarrés?")
        print("      → docker-compose ps")
        print("   2. Le NameNode est-il accessible?")
        print("      → http://localhost:9870")
        print("   3. Spark peut-il accéder à HDFS?")
        
        # Tentative avec docker exec
        print("\nTentative alternative via docker exec...")
        if not retriever.get_file_info_via_docker():
            return
    
    # Recherche des fichiers CSV
    print(f"\nRecherche des fichiers CSV MongoDB dans {retriever.mongodb_csv_dir}...")
    csv_files = retriever.list_mongodb_csv_files()
    
    if not csv_files:
        print("Aucun fichier CSV trouvé.")
        
        # Suggestions de dépannage
        print("\n  Suggestions:")
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
                print(f"\n Répertoires disponibles dans HDFS: {dirs}")
        except:
            pass
        
        return
    
    # Affichage des fichiers trouvés
    print(f"\n {len(csv_files)} fichier(s) CSV trouvé(s):")
    print("-" * 80)
    
    for i, file_info in enumerate(csv_files, 1):
        print(f"{i:2}.  {file_info['name']}")
        print(f"     Collection MongoDB: {file_info['collection']}")
        print(f"     Taille: {file_info['size_mb']} MB ({file_info['size']} bytes)")
        print(f"     Chemin HDFS: {file_info['hdfs_path']}")
        print()
    
    # Menu interactif
    while True:
        print("\n" + "=" * 70)
        print("OPTIONS DISPONIBLES (avec PySpark):")
        print("=" * 70)
        print("1.   Prévisualiser un fichier")
        print("2.  Analyser un fichier (statistiques Spark)")
        print("3.  Rechercher dans un fichier")
        print("4.  Sauvegarder DataFrame vers fichier local")
        print("5.  Exécuter requête SQL personnalisée")
        print("6.  Quitter")
        
        choice = input(f"\n➤ Choisissez une option (1-6): ").strip()
        
        if choice == "1":
            # Prévisualisation
            try:
                file_num = int(input(f"Numéro du fichier (1-{len(csv_files)}): ")) - 1
                if 0 <= file_num < len(csv_files):
                    file_info = csv_files[file_num]
                    retriever.preview_csv(file_info['path'])
                else:
                    print(" Numéro de fichier invalide")
            except ValueError:
                print(" Veuillez entrer un nombre valide")
        
        elif choice == "2":
            # Analyse statistique
            try:
                file_num = int(input(f"Numéro du fichier (1-{len(csv_files)}): ")) - 1
                if 0 <= file_num < len(csv_files):
                    file_info = csv_files[file_num]
                    retriever.analyze_csv_statistics(file_info['path'])
                else:
                    print(" Numéro de fichier invalide")
            except ValueError:
                print(" Veuillez entrer un nombre valide")
        
        elif choice == "3":
            # Recherche dans un fichier
            try:
                file_num = int(input(f"Numéro du fichier (1-{len(csv_files)}): ")) - 1
                if 0 <= file_num < len(csv_files):
                    file_info = csv_files[file_num]
                    search_term = input("Terme à rechercher: ").strip()
                    
                    if search_term:
                        retriever.search_in_csv(file_info['path'], search_term)
                else:
                    print(" Numéro de fichier invalide")
            except ValueError:
                print(" Veuillez entrer un nombre valide")
        
        elif choice == "4":
            # Sauvegarde DataFrame
            try:
                file_num = int(input(f"Numéro du fichier (1-{len(csv_files)}): ")) - 1
                if 0 <= file_num < len(csv_files):
                    file_info = csv_files[file_num]
                    df = retriever.read_csv_with_spark(file_info['path'])
                    
                    if df is not None:
                        local_filename = f"./spark_output_{file_info['name']}"
                        format_choice = input("Format (csv/parquet): ").strip().lower()
                        if format_choice not in ['csv', 'parquet']:
                            format_choice = 'csv'
                        
                        retriever.save_spark_df_to_local(df, local_filename, format_choice)
                else:
                    print(" Numéro de fichier invalide")
            except ValueError:
                print(" Veuillez entrer un nombre valide")
        
        elif choice == "5":
            # Requête SQL personnalisée
            try:
                file_num = int(input(f"Numéro du fichier (1-{len(csv_files)}): ")) - 1
                if 0 <= file_num < len(csv_files):
                    file_info = csv_files[file_num]
                    df = retriever.read_csv_with_spark(file_info['path'])
                    
                    if df is not None:
                        # Créer une vue temporaire
                        table_name = f"table_{file_info['collection']}"
                        df.createOrReplaceTempView(table_name)
                        
                        print(f" Table temporaire créée: {table_name}")
                        print(f" Colonnes disponibles: {df.columns}")
                        print(f" Exemple de requête: SELECT * FROM {table_name} LIMIT 10")
                        
                        sql_query = input("Entrez votre requête SQL: ").strip()
                        if sql_query:
                            try:
                                result = retriever.spark.sql(sql_query)
                                result.show(20, truncate=False)
                                print(f" Requête exécutée avec succès")
                            except Exception as e:
                                print(f" Erreur SQL: {e}")
                else:
                    print(" Numéro de fichier invalide")
            except ValueError:
                print(" Veuillez entrer un nombre valide")
        
        elif choice == "6":
            print("\n Fermeture de la session Spark...")
            retriever.spark.stop()
            print("Au revoir!")
            break
        
        else:
            print(" Option invalide")

if __name__ == "__main__":
    # Vérification des dépendances
    try:
        from pyspark.sql import SparkSession
        import requests
    except ImportError as e:
        print(f" Dépendance manquante: {e}")
        print("Installation requise:")
        print("pip install pyspark requests")
        exit(1)
    
    main()