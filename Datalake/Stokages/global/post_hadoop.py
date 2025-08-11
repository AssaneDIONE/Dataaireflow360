#!/usr/bin/env python3

import psycopg2
import pandas as pd
import os

conn = None

try:
    print("Connexion PostgreSQL...")
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="db_postgres",
        user="dione", 
        password="Pass123"
    )
    
    print("Lecture des données...")
    df = pd.read_sql("SELECT * FROM operators", conn)
    print(f"Données lues: {len(df)} lignes")
    
    print("Export en cours...")
    # Sauvegarder dans le dossier actuel
    df.to_csv('./operators.csv', index=False)
    print("Fichier CSV créé localement")
    
    # Copier le fichier dans le conteneur namenode d'abord
    os.system('docker cp ./operators.csv namenode:/tmp/operators.csv')
    
    # Créer le répertoire HDFS et copier le fichier
    os.system('docker exec namenode hdfs dfs -mkdir -p /data')
    os.system('docker exec namenode hdfs dfs -put -f /tmp/operators.csv /data/')
    
    print("Export terminé!")

except Exception as e:
    print(f"Erreur: {e}")

finally:
    if conn:
        conn.close()